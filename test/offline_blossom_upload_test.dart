import 'dart:async';
import 'dart:typed_data';

import 'package:blossom_cache/blossom_cache.dart';
import 'package:blossom_upload_queue_shim_for_ndk/blossom_upload_queue_shim_for_ndk.dart';
import 'package:idb_shim/idb_client_memory.dart' hide Database;
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

/// Hex sha256 placeholder. The cache treats it as an opaque key.
final _sha = 'a' * 64;

Uint8List _bytes() => Uint8List.fromList(List<int>.generate(8, (i) => i));

/// Records every call made to the fake upload function and lets the test
/// dictate what each server should answer.
class FakeUploader {
  final Map<String, BlobUploadResult Function()> responders = {};

  /// Throws on every call until cleared.
  Object? syncError;

  final List<
    ({
      Uint8List data,
      List<String> servers,
      String precomputedSha256,
      String? contentType,
    })
  >
  calls = [];

  BlobUploadFn get fn =>
      ({
        required Uint8List data,
        required List<String> serverUrls,
        required String precomputedSha256,
        String? contentType,
      }) async {
        calls.add((
          data: data,
          servers: List.of(serverUrls),
          precomputedSha256: precomputedSha256,
          contentType: contentType,
        ));
        if (syncError != null) throw syncError!;
        final results = <BlobUploadResult>[];
        for (final s in serverUrls) {
          final responder = responders[s];
          if (responder != null) results.add(responder());
        }
        return results;
      };

  void ackAll(List<String> servers) {
    for (final s in servers) {
      responders[s] = () => BlobUploadResult(serverUrl: s, success: true);
    }
  }

  void fail(String server, {String error = 'connection refused'}) {
    responders[server] = () =>
        BlobUploadResult(serverUrl: server, success: false, error: error);
  }
}

Future<BlossomCache> _openCache() =>
    IdbBlossomCache.open(factory: newIdbFactoryMemory());

Future<QueuedBlobUpload> _waitFor(
  OfflineBlossomUpload outbox,
  String sha,
  bool Function(QueuedBlobUpload r) predicate,
) async {
  for (var i = 0; i < 200; i++) {
    final r = await outbox.get(sha);
    if (r != null && predicate(r)) return r;
    await Future.delayed(const Duration(milliseconds: 5));
  }
  throw TimeoutException('condition not met for $sha');
}

void main() {
  late Database db;
  late BlossomCache cache;

  setUp(() async {
    db = await newDatabaseFactoryMemory().openDatabase('test.db');
    cache = await _openCache();
    await cache.put(_sha, _bytes(), type: 'image/png');
  });

  tearDown(() async {
    await db.close();
  });

  test(
    'upload persists immediately and marks delivered once every server acks',
    () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a', 'https://b']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        db: db,
        initialBackoff: const Duration(milliseconds: 10),
      );

      final record = await outbox.upload(
        sha256: _sha,
        servers: const ['https://a', 'https://b'],
      );
      expect(record.status, BlobUploadStatus.pending);

      final delivered = await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
      );
      expect(
        delivered.ackedServers,
        unorderedEquals(['https://a', 'https://b']),
      );
      expect(delivered.lastErrors, isEmpty);
      expect(fake.calls.length, 1);
      expect(fake.calls.single.contentType, 'image/png');

      await outbox.dispose();
    },
  );

  test('partial success leaves remaining servers pending', () async {
    final fake = FakeUploader();
    fake.ackAll(['https://a']);
    fake.fail('https://b', error: 'quota exceeded');
    final outbox = OfflineBlossomUpload(
      uploadFn: fake.fn,
      cache: cache,
      db: db,
      initialBackoff: const Duration(milliseconds: 5),
    );

    await outbox.upload(
      sha256: _sha,
      servers: const ['https://a', 'https://b'],
    );

    final pending = await _waitFor(outbox, _sha, (r) => r.attempts >= 1);
    expect(pending.status, BlobUploadStatus.pending);
    expect(pending.ackedServers, ['https://a']);
    expect(pending.remainingServers, ['https://b']);
    expect(pending.lastErrors['https://b'], 'quota exceeded');

    await outbox.dispose();
  });

  test('retryNow only targets remaining servers', () async {
    final fake = FakeUploader();
    fake.ackAll(['https://a']);
    fake.fail('https://b');
    final outbox = OfflineBlossomUpload(
      uploadFn: fake.fn,
      cache: cache,
      db: db,
      initialBackoff: const Duration(milliseconds: 1),
    );

    await outbox.upload(
      sha256: _sha,
      servers: const ['https://a', 'https://b'],
    );
    await _waitFor(outbox, _sha, (r) => r.attempts >= 1);

    fake.ackAll(['https://b']);
    await outbox.retryNow();
    final delivered = await _waitFor(
      outbox,
      _sha,
      (r) => r.status == BlobUploadStatus.delivered,
    );

    expect(delivered.ackedServers, unorderedEquals(['https://a', 'https://b']));
    expect(fake.calls.last.servers, ['https://b']);

    await outbox.dispose();
  });

  test(
    'reupload() forces a push to every server without losing acks or delivered status',
    () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a', 'https://b']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        servers: const ['https://a', 'https://b'],
      );
      final delivered = await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
      );
      final originalDeliveredAt = delivered.deliveredAt!;
      final attemptsBefore = delivered.attempts;

      fake.responders.clear();
      fake.ackAll(['https://a']);
      fake.fail('https://b', error: 'temporary glitch');

      await outbox.reupload(_sha);

      final after = await _waitFor(
        outbox,
        _sha,
        (r) => r.attempts > attemptsBefore,
      );
      expect(
        after.ackedServers,
        unorderedEquals(['https://a', 'https://b']),
        reason: 'past acks must be monotonic',
      );
      expect(after.status, BlobUploadStatus.delivered);
      expect(
        after.deliveredAt,
        originalDeliveredAt,
        reason: 'deliveredAt is a historical fact and must not move',
      );
      expect(after.lastErrors, isEmpty);
      expect(
        fake.calls.last.servers,
        unorderedEquals(['https://a', 'https://b']),
      );

      await outbox.dispose();
    },
  );

  test(
    'reupload(sha, server:) adds a new server and only pushes to it',
    () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(sha256: _sha, servers: const ['https://a']);
      await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
      );

      fake.fail('https://c');
      await outbox.reupload(_sha, server: 'https://c');

      final withC = await _waitFor(
        outbox,
        _sha,
        (r) => r.servers.contains('https://c') && r.attempts >= 2,
      );
      expect(withC.servers, containsAll(['https://a', 'https://c']));
      expect(
        withC.status,
        BlobUploadStatus.pending,
        reason: 'a new unacked server demotes the entry from delivered',
      );
      expect(withC.ackedServers, [
        'https://a',
      ], reason: 'the original ack stays monotonic');
      expect(withC.remainingServers, ['https://c']);
      expect(fake.calls.last.servers, ['https://c']);

      await outbox.dispose();
    },
  );

  test(
    'reupload(sha, server:) on an already-acked server re-pushes without losing the ack',
    () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(sha256: _sha, servers: const ['https://a']);
      final delivered = await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
      );
      final attemptsBefore = delivered.attempts;
      final originalDeliveredAt = delivered.deliveredAt!;

      fake.responders.clear();
      fake.fail('https://a');
      await outbox.reupload(_sha, server: 'https://a');

      final after = await _waitFor(
        outbox,
        _sha,
        (r) => r.attempts > attemptsBefore,
      );
      expect(after.ackedServers, ['https://a']);
      expect(after.status, BlobUploadStatus.delivered);
      expect(after.deliveredAt, originalDeliveredAt);
      expect(after.lastErrors, isEmpty);
      expect(fake.calls.last.servers, ['https://a']);

      await outbox.dispose();
    },
  );

  test('upload() throws on empty servers', () async {
    final outbox = OfflineBlossomUpload(
      uploadFn: FakeUploader().fn,
      cache: cache,
      db: db,
    );
    expect(
      () => outbox.upload(sha256: _sha, servers: const []),
      throwsArgumentError,
    );
    await outbox.dispose();
  });

  test('upload() throws when the blob is not in the cache', () async {
    final outbox = OfflineBlossomUpload(
      uploadFn: FakeUploader().fn,
      cache: cache,
      db: db,
    );
    final missing = 'b' * 64;
    expect(
      () => outbox.upload(sha256: missing, servers: const ['https://a']),
      throwsStateError,
    );
    await outbox.dispose();
  });

  test('duplicate upload() merges servers and rearms', () async {
    final fake = FakeUploader();
    fake.ackAll(['https://a']);
    fake.fail('https://b');
    final outbox = OfflineBlossomUpload(
      uploadFn: fake.fn,
      cache: cache,
      db: db,
      initialBackoff: const Duration(milliseconds: 1),
    );

    await outbox.upload(sha256: _sha, servers: const ['https://a']);
    await _waitFor(outbox, _sha, (r) => r.attempts >= 1);

    await outbox.upload(sha256: _sha, servers: const ['https://b']);
    final merged = await _waitFor(outbox, _sha, (r) => r.servers.length == 2);
    expect(merged.servers, containsAll(['https://a', 'https://b']));

    await outbox.dispose();
  });

  test(
    'server URLs are normalized (case, trailing slash) on storage',
    () async {
      final fake = FakeUploader();
      fake.ackAll(['https://blob.example']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        servers: const ['HTTPS://Blob.Example/', 'https://blob.example'],
      );
      final r = await _waitFor(outbox, _sha, (r) => r.attempts >= 1);
      expect(r.servers, ['https://blob.example']);

      await outbox.dispose();
    },
  );

  test(
    'sync exception from uploader is recorded on remaining servers',
    () async {
      final fake = FakeUploader()..syncError = StateError('no signer');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        db: db,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(sha256: _sha, servers: const ['https://a']);
      final r = await _waitFor(outbox, _sha, (r) => r.attempts >= 1);
      expect(r.status, BlobUploadStatus.pending);
      expect(r.lastErrors['https://a'], contains('no signer'));

      await outbox.dispose();
    },
  );

  test('dispose() blocks further public calls', () async {
    final outbox = OfflineBlossomUpload(
      uploadFn: FakeUploader().fn,
      cache: cache,
      db: db,
    );
    await outbox.dispose();
    expect(
      () => outbox.upload(sha256: _sha, servers: const ['https://a']),
      throwsStateError,
    );
  });

  test(
    'bytes evicted from cache mid-flight surface a "missing" error and keep pending',
    () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        db: db,
        initialBackoff: const Duration(milliseconds: 5),
      );

      await outbox.upload(sha256: _sha, servers: const ['https://a']);
      await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
      );

      // Force a re-attempt against a server we haven't covered, after wiping
      // the bytes from the cache.
      fake.responders.clear();
      fake.ackAll(['https://b']);
      // Manually unpin first since cache.delete bypasses the pin flag check
      // anyway, but be explicit.
      await cache.delete(_sha);

      await outbox.reupload(_sha, server: 'https://b');
      final after = await _waitFor(
        outbox,
        _sha,
        (r) => r.servers.contains('https://b') && r.attempts >= 2,
      );
      expect(after.status, BlobUploadStatus.pending);
      expect(after.lastErrors['https://b'], contains('blob bytes missing'));
      expect(
        fake.calls.where((c) => c.servers.contains('https://b')),
        isEmpty,
        reason: 'no network call should fire when bytes are gone',
      );

      await outbox.dispose();
    },
  );

  test('shim pins on upload and unpins on delivered', () async {
    final fake = FakeUploader();
    fake.ackAll(['https://a']);
    final outbox = OfflineBlossomUpload(
      uploadFn: fake.fn,
      cache: cache,
      db: db,
      initialBackoff: const Duration(milliseconds: 1),
    );

    final initial = await cache.head(_sha);
    expect(initial!.pinned, isFalse);

    await outbox.upload(sha256: _sha, servers: const ['https://a']);
    // Right after enqueue, the shim should own the pin.
    final pending = await outbox.get(_sha);
    expect(pending!.pinnedByShim, isTrue);
    expect((await cache.head(_sha))!.pinned, isTrue);

    // Wait for full reconciliation: delivered AND the shim has released the
    // pin. The shim updates sembast in two steps (deliveredAt, then
    // pinnedByShim=false after cache.unpin), so waiting on `delivered` alone
    // would race against the cache release.
    await _waitFor(
      outbox,
      _sha,
      (r) => r.status == BlobUploadStatus.delivered && !r.pinnedByShim,
    );
    expect((await cache.head(_sha))!.pinned, isFalse);

    await outbox.dispose();
  });

  test('shim does not unpin a caller-owned pin', () async {
    // Caller pins before the shim ever sees the blob.
    await cache.pin(_sha);

    final fake = FakeUploader();
    fake.ackAll(['https://a']);
    final outbox = OfflineBlossomUpload(
      uploadFn: fake.fn,
      cache: cache,
      db: db,
      initialBackoff: const Duration(milliseconds: 1),
    );

    await outbox.upload(sha256: _sha, servers: const ['https://a']);
    final pending = await outbox.get(_sha);
    expect(
      pending!.pinnedByShim,
      isFalse,
      reason: 'pin was already there, so the shim does not claim it',
    );

    await _waitFor(outbox, _sha, (r) => r.status == BlobUploadStatus.delivered);

    // The caller-owned pin must survive delivery.
    expect((await cache.head(_sha))!.pinned, isTrue);

    await outbox.dispose();
  });
}
