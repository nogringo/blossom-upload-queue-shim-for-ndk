import 'dart:async';
import 'dart:typed_data';

import 'package:blossom_cache/blossom_cache.dart';
import 'package:blossom_upload_queue_shim_for_ndk/blossom_upload_queue_shim_for_ndk.dart';
import 'package:blossom_upload_queue_shim_for_ndk/sembast_store.dart';
import 'package:idb_shim/idb_client_memory.dart' hide Database;
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

/// Hex sha256 placeholder. The cache treats it as an opaque key.
final _sha = 'a' * 64;

const _shimHolder = 'blossom_upload_queue_shim:blob_uploads';

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
      String? pubkey,
    })
  >
  calls = [];

  BlobUploadFn get fn =>
      ({
        required Uint8List data,
        required List<String> serverUrls,
        required String precomputedSha256,
        String? contentType,
        String? pubkey,
      }) async {
        calls.add((
          data: data,
          servers: List.of(serverUrls),
          precomputedSha256: precomputedSha256,
          contentType: contentType,
          pubkey: pubkey,
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

/// Answers kind 10063 lookups from a table, `unavailable` when absent.
class FakeServerLists {
  final Map<String, UrlListLookup> answers = {};
  final List<String> calls = [];
  Completer<void>? gate;

  void set(String pubkey, UrlListKind kind, UrlListLookup answer) =>
      answers['${kind.name}|$pubkey'] = answer;

  void servers(String pubkey, List<String> urls) => this
    ..set(pubkey, UrlListKind.outbox, const UrlListLookup.notFound())
    ..set(pubkey, UrlListKind.blossom, UrlListLookup.found(urls));

  UrlListFn get fn => (pubkey, kind, outboxRelays) async {
    final id = '${kind.name}|$pubkey';
    calls.add(id);
    await gate?.future;
    return answers[id] ?? const UrlListLookup.unavailable('offline');
  };
}

Future<BlossomCache> _openCache() =>
    IdbBlossomCache.open(factory: newIdbFactoryMemory());

Future<QueuedBlobUpload> _waitFor(
  OfflineBlossomUpload outbox,
  String sha,
  bool Function(QueuedBlobUpload r) predicate, {
  String? pubkey,
}) async {
  for (var i = 0; i < 200; i++) {
    final r = await outbox.get(sha, pubkey: pubkey);
    if (r != null && predicate(r)) return r;
    await Future.delayed(const Duration(milliseconds: 5));
  }
  throw TimeoutException('condition not met for $sha');
}

void main() {
  late Database db;
  late UploadQueueStore store;
  late BlossomCache cache;

  setUp(() async {
    db = await newDatabaseFactoryMemory().openDatabase('test.db');
    store = SembastUploadQueueStore(db);
    cache = await _openCache();
    await cache.put(_bytes(), sha256: _sha, type: 'image/png');
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
        store: store,
        initialBackoff: const Duration(milliseconds: 10),
      );

      final record = await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a', 'https://b']),
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
      store: store,
      initialBackoff: const Duration(milliseconds: 5),
    );

    await outbox.upload(
      sha256: _sha,
      serverSet: const UrlSet.explicit(['https://a', 'https://b']),
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
      store: store,
      initialBackoff: const Duration(milliseconds: 1),
    );

    await outbox.upload(
      sha256: _sha,
      serverSet: const UrlSet.explicit(['https://a', 'https://b']),
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
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a', 'https://b']),
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
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
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
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
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
      store: store,
    );
    expect(
      () => outbox.upload(sha256: _sha, serverSet: const UrlSet.explicit([])),
      throwsArgumentError,
    );
    await outbox.dispose();
  });

  test('upload() throws when the blob is not in the cache', () async {
    final outbox = OfflineBlossomUpload(
      uploadFn: FakeUploader().fn,
      cache: cache,
      store: store,
    );
    final missing = 'b' * 64;
    expect(
      () => outbox.upload(
        sha256: missing,
        serverSet: const UrlSet.explicit(['https://a']),
      ),
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
      store: store,
      initialBackoff: const Duration(milliseconds: 1),
    );

    await outbox.upload(
      sha256: _sha,
      serverSet: const UrlSet.explicit(['https://a']),
    );
    await _waitFor(outbox, _sha, (r) => r.attempts >= 1);

    await outbox.upload(
      sha256: _sha,
      serverSet: const UrlSet.explicit(['https://b']),
    );
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
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit([
          'HTTPS://Blob.Example/',
          'https://blob.example',
        ]),
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
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
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
      store: store,
    );
    await outbox.dispose();
    expect(
      () => outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      ),
      throwsStateError,
    );
    expect(() => outbox.remove(_sha), throwsStateError);
  });

  test(
    'bytes evicted from cache mid-flight surface a "missing" error and keep pending',
    () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(milliseconds: 5),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
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
      store: store,
      initialBackoff: const Duration(milliseconds: 1),
    );

    final initial = await cache.head(_sha);
    expect(initial!.pinned, isFalse);

    await outbox.upload(
      sha256: _sha,
      serverSet: const UrlSet.explicit(['https://a']),
    );
    expect((await cache.head(_sha))!.pinnedBy, [_shimHolder]);

    await _waitFor(outbox, _sha, (r) => r.status == BlobUploadStatus.delivered);
    // The unpin follows the delivered write; dispose waits for it.
    await outbox.dispose();
    expect((await cache.head(_sha))!.pinned, isFalse);
  });

  test('shim does not unpin a caller-owned pin', () async {
    // Caller pins before the shim ever sees the blob.
    await cache.pin(_sha);

    final fake = FakeUploader();
    fake.ackAll(['https://a']);
    final outbox = OfflineBlossomUpload(
      uploadFn: fake.fn,
      cache: cache,
      store: store,
      initialBackoff: const Duration(milliseconds: 1),
    );

    await outbox.upload(
      sha256: _sha,
      serverSet: const UrlSet.explicit(['https://a']),
    );
    await _waitFor(outbox, _sha, (r) => r.status == BlobUploadStatus.delivered);
    await outbox.dispose();

    expect((await cache.head(_sha))!.pinnedBy, [BlossomCache.defaultHolder]);
  });

  test('a caller pin applied after upload survives delivery', () async {
    final fake = FakeUploader();
    fake.fail('https://a');
    final outbox = OfflineBlossomUpload(
      uploadFn: fake.fn,
      cache: cache,
      store: store,
      initialBackoff: const Duration(milliseconds: 1),
    );

    await outbox.upload(
      sha256: _sha,
      serverSet: const UrlSet.explicit(['https://a']),
    );
    await cache.pin(_sha);
    fake.ackAll(['https://a']);
    await outbox.retryNow();
    await _waitFor(outbox, _sha, (r) => r.status == BlobUploadStatus.delivered);
    await outbox.dispose();

    expect((await cache.head(_sha))!.pinnedBy, [BlossomCache.defaultHolder]);
  });

  test('moves pins left by 0.8.0 to the shim holder', () async {
    final raw = stringMapStoreFactory.store('blob_uploads');
    final legacy = QueuedBlobUpload(
      sha256: _sha,
      contentType: 'image/png',
      servers: const ['https://a'],
      ackedServers: const [],
      lastErrors: const {},
      attempts: 0,
      firstAttemptAt: null,
      lastAttemptAt: null,
      nextAttemptAt: 0,
      deliveredAt: null,
      createdAt: 0,
    );
    await raw.record(legacy.key).put(db, {
      ...legacy.toMap(),
      'pinnedByShim': true,
    });
    await cache.pin(_sha);

    final fake = FakeUploader();
    fake.fail('https://a');
    final outbox = OfflineBlossomUpload(
      uploadFn: fake.fn,
      cache: cache,
      store: store,
      initialBackoff: const Duration(seconds: 30),
    );
    await outbox.retryNow();
    await outbox.dispose();

    expect((await cache.head(_sha))!.pinnedBy, [_shimHolder]);
    expect(
      (await raw.record(legacy.key).get(db))!.containsKey('pinnedByShim'),
      isFalse,
    );
  });

  group('account binding', () {
    const alice = 'alice-pubkey';
    const bob = 'bob-pubkey';

    test('pubkey is persisted and forwarded to the uploader', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      final delivered = await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
        pubkey: alice,
      );

      expect(delivered.pubkey, alice);
      expect(fake.calls.single.pubkey, alice);
      expect(
        await outbox.get(_sha),
        isNull,
        reason: 'the account-less key must stay free',
      );

      await outbox.dispose();
    });

    test('the same blob queued by two accounts yields two records', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a', 'https://b']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://b']),
        pubkey: bob,
      );

      final all = await outbox.listAll();
      expect(all.length, 2);
      expect(all.singleWhere((r) => r.pubkey == alice).servers, const [
        'https://a',
      ], reason: 'bob must not widen alice\'s target list');
      expect(all.singleWhere((r) => r.pubkey == bob).servers, const [
        'https://b',
      ]);

      await outbox.dispose();
    });

    test('an entry whose account cannot sign is deferred untouched', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
        canSignFor: (pubkey) => pubkey == bob,
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.retryNow();
      await Future.delayed(const Duration(milliseconds: 50));

      expect(fake.calls, isEmpty, reason: 'alice cannot sign right now');
      final record = await outbox.get(_sha, pubkey: alice);
      expect(record!.status, BlobUploadStatus.pending);
      expect(
        record.attempts,
        0,
        reason: 'deferring must not burn an attempt or arm the backoff',
      );
      expect(record.lastErrors, isEmpty);

      await outbox.dispose();
    });

    test('a deferred entry resumes once its account can sign again', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      var loggedIn = false;
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
        canSignFor: (_) => loggedIn,
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.retryNow();
      await Future.delayed(const Duration(milliseconds: 20));
      expect(fake.calls, isEmpty);

      loggedIn = true;
      await outbox.retryNow();

      final delivered = await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
        pubkey: alice,
      );
      expect(delivered.attempts, 1, reason: 'the deferrals cost nothing');
      expect(fake.calls.single.pubkey, alice);

      await outbox.dispose();
    });

    test('an account-less entry is attempted without a signer check', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(milliseconds: 1),
        canSignFor: (_) => false,
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
      );
      expect(fake.calls.single.pubkey, isNull);

      await outbox.dispose();
    });
  });

  group('local data clearing', () {
    const alice = 'alice-pubkey';
    const bob = 'bob-pubkey';

    test('clearLocalAccountData only removes that account', () async {
      final fake = FakeUploader();
      fake.fail('https://a');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(seconds: 30),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: bob,
      );
      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );

      await outbox.clearLocalAccountData(pubkey: alice);

      expect(await outbox.get(_sha, pubkey: alice), isNull);
      expect(await outbox.get(_sha, pubkey: bob), isNotNull);
      expect(
        await outbox.get(_sha),
        isNotNull,
        reason: 'account-less entries are not owned by any account',
      );

      await outbox.dispose();
    });

    test('clearing one account keeps the pin a sibling still needs', () async {
      final fake = FakeUploader();
      fake.fail('https://a');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(seconds: 30),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: bob,
      );
      expect((await cache.head(_sha))!.pinned, isTrue);

      await outbox.clearLocalAccountData(pubkey: alice);
      expect(
        (await cache.head(_sha))!.pinned,
        isTrue,
        reason: 'bob still owes an upload',
      );

      await outbox.clearLocalAccountData(pubkey: bob);
      expect((await cache.head(_sha))!.pinned, isFalse);

      await outbox.dispose();
    });

    test(
      'a delivered account does not unpin while another is pending',
      () async {
        final fake = FakeUploader();
        fake.ackAll(['https://a']);
        fake.fail('https://b');
        final outbox = OfflineBlossomUpload(
          uploadFn: fake.fn,
          cache: cache,
          store: store,
          initialBackoff: const Duration(seconds: 30),
        );

        await outbox.upload(
          sha256: _sha,
          serverSet: const UrlSet.explicit(['https://b']),
          pubkey: bob,
        );
        await outbox.upload(
          sha256: _sha,
          serverSet: const UrlSet.explicit(['https://a']),
          pubkey: alice,
        );

        await _waitFor(
          outbox,
          _sha,
          (r) => r.status == BlobUploadStatus.delivered,
          pubkey: alice,
        );
        expect(
          (await cache.head(_sha))!.pinned,
          isTrue,
          reason: 'bob would lose the bytes to eviction',
        );

        await outbox.dispose();
      },
    );

    test('both clear methods are idempotent', () async {
      final fake = FakeUploader();
      fake.fail('https://a');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(seconds: 30),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );

      await outbox.clearLocalAccountData(pubkey: alice);
      await outbox.clearLocalAccountData(pubkey: alice);
      await outbox.clearLocalAccountData(pubkey: 'never-seen-pubkey');
      await outbox.clearAllLocalData();
      await outbox.clearAllLocalData();

      expect(await outbox.listAll(), isEmpty);
      expect((await cache.head(_sha))!.pinned, isFalse);

      await outbox.dispose();
    });

    test('clearAllLocalData empties the store and releases our pin', () async {
      final fake = FakeUploader();
      fake.fail('https://a');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(seconds: 30),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );

      await outbox.clearAllLocalData();

      expect(await outbox.listAll(), isEmpty);
      expect((await cache.head(_sha))!.pinned, isFalse);
      expect(
        await cache.get(_sha),
        isNotNull,
        reason: 'blob bytes belong to the caller, not to the shim',
      );

      await outbox.dispose();
    });

    test('clearAllLocalData leaves a caller-owned pin alone', () async {
      await cache.pin(_sha);

      final fake = FakeUploader();
      fake.fail('https://a');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(seconds: 30),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      await outbox.clearAllLocalData();

      expect((await cache.head(_sha))!.pinned, isTrue);

      await outbox.dispose();
    });

    test('remove deletes a pending entry and releases our pin', () async {
      final fake = FakeUploader();
      fake.fail('https://a');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(seconds: 30),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      await outbox.remove(_sha);

      expect(await outbox.get(_sha), isNull);
      expect((await cache.head(_sha))!.pinned, isFalse);
      expect(await cache.get(_sha), isNotNull);

      await outbox.dispose();
    });

    test('remove only touches the record it names', () async {
      final fake = FakeUploader();
      fake.fail('https://a');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(seconds: 30),
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: bob,
      );
      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );

      await outbox.remove(_sha, pubkey: alice);

      expect(await outbox.get(_sha, pubkey: alice), isNull);
      expect(await outbox.get(_sha, pubkey: bob), isNotNull);
      expect(await outbox.get(_sha), isNotNull);
      expect(
        (await cache.head(_sha))!.pinnedBy,
        unorderedEquals([_shimHolder, '$_shimHolder:$bob']),
      );

      await outbox.dispose();
    });

    test('remove deletes a delivered entry', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      await _waitFor(
        outbox,
        _sha,
        (r) => r.status == BlobUploadStatus.delivered,
      );
      await outbox.remove(_sha);

      expect(await outbox.listAll(), isEmpty);

      await outbox.dispose();
    });

    test('remove of a missing record leaves a caller pin alone', () async {
      await cache.pin(_sha);
      final outbox = OfflineBlossomUpload(
        uploadFn: FakeUploader().fn,
        cache: cache,
        store: store,
      );

      await outbox.remove(_sha);
      await outbox.remove(_sha, pubkey: alice);
      await outbox.remove('b' * 64);

      expect((await cache.head(_sha))!.pinnedBy, [BlossomCache.defaultHolder]);

      await outbox.dispose();
    });

    test('an attempt in flight does not bring a removed record back', () async {
      final gate = Completer<void>();
      final outbox = OfflineBlossomUpload(
        uploadFn:
            ({
              required data,
              required serverUrls,
              required precomputedSha256,
              contentType,
              pubkey,
            }) async {
              await gate.future;
              return [
                for (final s in serverUrls)
                  BlobUploadResult(serverUrl: s, success: true),
              ];
            },
        cache: cache,
        store: store,
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      await Future.delayed(const Duration(milliseconds: 20));
      await outbox.remove(_sha);
      gate.complete();
      await outbox.dispose();

      expect(await outbox.get(_sha), isNull);
      expect((await cache.head(_sha))!.pinned, isFalse);
    });
  });

  group('enqueue and sync', () {
    const alice = 'alice-pubkey';
    const bob = 'bob-pubkey';

    test('enqueue persists and pins without attempting', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
      );

      final record = await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      await Future.delayed(const Duration(milliseconds: 20));

      expect(record.status, BlobUploadStatus.pending);
      expect(fake.calls, isEmpty);
      expect((await outbox.get(_sha))!.attempts, 0);
      expect((await cache.head(_sha))!.pinnedBy, [_shimHolder]);

      await outbox.dispose();
    });

    test('sync delivers enqueued entries and reports them', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
      );

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      final report = await outbox.sync();

      expect(report.delivered.single.sha256, _sha);
      expect(report.failed, isEmpty);
      expect(report.skipped, isEmpty);
      expect((await cache.head(_sha))!.pinned, isFalse);

      final again = await outbox.sync();
      expect(again.delivered, isEmpty, reason: 'delivered entries are done');
      expect(fake.calls.length, 1);

      await outbox.dispose();
    });

    test('sync ignores the backoff', () async {
      final fake = FakeUploader();
      fake.fail('https://a');
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        initialBackoff: const Duration(minutes: 5),
      );

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      final first = await outbox.sync();
      expect(first.failed.single.lastErrors['https://a'], 'connection refused');

      fake.ackAll(['https://a']);
      final second = await outbox.sync();
      expect(second.delivered.single.sha256, _sha);
      expect(fake.calls.length, 2);

      await outbox.dispose();
    });

    test('sync skips entries whose account cannot sign', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
        canSignFor: (pubkey) => pubkey == bob,
      );

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: bob,
      );
      final report = await outbox.sync();

      expect(report.delivered.single.pubkey, bob);
      expect(report.skipped.single.pubkey, alice);
      expect(report.skipped.single.attempts, 0);
      expect(fake.calls.single.pubkey, bob);

      await outbox.dispose();
    });

    test('sync(pubkey:) only attempts that account', () async {
      final fake = FakeUploader();
      fake.ackAll(['https://a']);
      final outbox = OfflineBlossomUpload(
        uploadFn: fake.fn,
        cache: cache,
        store: store,
      );

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
        pubkey: alice,
      );
      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      final report = await outbox.sync(pubkey: alice);

      expect(report.delivered.single.pubkey, alice);
      expect((await outbox.get(_sha))!.status, BlobUploadStatus.pending);

      await outbox.dispose();
    });

    test('sync keeps at most `concurrency` attempts in flight', () async {
      var active = 0;
      var peak = 0;
      final outbox = OfflineBlossomUpload(
        uploadFn:
            ({
              required data,
              required serverUrls,
              required precomputedSha256,
              contentType,
              pubkey,
            }) async {
              if (++active > peak) peak = active;
              await Future.delayed(const Duration(milliseconds: 5));
              active--;
              return [
                for (final s in serverUrls)
                  BlobUploadResult(serverUrl: s, success: true),
              ];
            },
        cache: cache,
        store: store,
      );

      for (var i = 0; i < 6; i++) {
        final sha = '$i' * 64;
        await cache.put(_bytes(), sha256: sha);
        await outbox.enqueue(
          sha256: sha,
          serverSet: const UrlSet.explicit(['https://a']),
        );
      }
      final report = await outbox.sync(concurrency: 2);

      expect(report.delivered.length, 6);
      expect(peak, 2);

      await outbox.dispose();
    });

    test('sync waits for an attempt already in flight', () async {
      final gate = Completer<void>();
      var calls = 0;
      final outbox = OfflineBlossomUpload(
        uploadFn:
            ({
              required data,
              required serverUrls,
              required precomputedSha256,
              contentType,
              pubkey,
            }) async {
              calls++;
              await gate.future;
              return [
                for (final s in serverUrls)
                  BlobUploadResult(serverUrl: s, success: true),
              ];
            },
        cache: cache,
        store: store,
      );

      await outbox.upload(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      final syncing = outbox.sync();
      await Future.delayed(const Duration(milliseconds: 20));
      gate.complete();
      final report = await syncing;

      expect(report.delivered.single.sha256, _sha);
      expect(calls, 1, reason: 'joining must not start a duplicate push');

      await outbox.dispose();
    });

    test('sync rejects a concurrency below 1', () async {
      final outbox = OfflineBlossomUpload(
        uploadFn: FakeUploader().fn,
        cache: cache,
        store: store,
      );
      expect(() => outbox.sync(concurrency: 0), throwsArgumentError);
      await outbox.dispose();
    });
  });

  group('server sets', () {
    const alice = 'alice-pubkey';

    OfflineBlossomUpload outboxFor(FakeUploader fake, FakeServerLists? lists) =>
        OfflineBlossomUpload(
          uploadFn: fake.fn,
          serverListFn: lists?.fn,
          cache: cache,
          store: store,
          initialBackoff: const Duration(minutes: 5),
        );

    test('persists unresolved and pinned without a lookup', () async {
      final lists = FakeServerLists()..gate = Completer<void>();
      final fake = FakeUploader()..ackAll(['https://a']);
      final outbox = outboxFor(fake, lists);

      final record = await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.blossom(alice),
      );
      expect(record.status, BlobUploadStatus.pending);
      expect(record.servers, isEmpty);
      expect(record.pendingServerSet, isA<BlossomUrls>());
      expect(lists.calls, isEmpty);
      expect((await cache.head(_sha))!.pinnedBy, [_shimHolder]);

      lists
        ..servers(alice, ['https://A/'])
        ..gate!.complete();
      final report = await outbox.sync();
      final delivered = report.delivered.single;
      expect(delivered.servers, ['https://a']);
      expect(delivered.pendingServerSet, isNull);
      expect(fake.calls.single.servers, ['https://a']);
      expect((await cache.head(_sha))!.pinned, isFalse);

      await outbox.dispose();
    });

    test('an unavailable lookup backs off and is retried', () async {
      final lists = FakeServerLists();
      final fake = FakeUploader()..ackAll(['https://a']);
      final outbox = outboxFor(fake, lists);

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.blossom(alice),
      );
      final first = await outbox.sync();
      final waiting = first.failed.single;
      expect(waiting.resolutionAttempts, 1);
      expect(waiting.resolutionError, contains('offline'));
      expect(waiting.attempts, 0);
      expect(fake.calls, isEmpty);

      lists.servers(alice, ['https://a']);
      final second = await outbox.sync();
      final delivered = second.delivered.single;
      expect(delivered.resolutionAttempts, 0);
      expect(delivered.resolutionError, isNull);

      await outbox.dispose();
    });

    test('falls back to default servers without a server list', () async {
      final lists = FakeServerLists()
        ..set(alice, UrlListKind.outbox, const UrlListLookup.notFound())
        ..set(alice, UrlListKind.blossom, const UrlListLookup.notFound());
      final fake = FakeUploader()..ackAll(['https://default']);
      final outbox = outboxFor(fake, lists);

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.fallback([
          UrlSet.blossom(alice),
          UrlSet.explicit(['https://default']),
        ]),
      );
      final report = await outbox.sync();
      expect(report.delivered.single.servers, ['https://default']);

      await outbox.dispose();
    });

    test('a set yielding no server stays pending and is retried', () async {
      final lists = FakeServerLists()
        ..set(alice, UrlListKind.outbox, const UrlListLookup.notFound())
        ..set(alice, UrlListKind.blossom, const UrlListLookup.notFound());
      final fake = FakeUploader()..ackAll(['https://a']);
      final outbox = outboxFor(fake, lists);

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.blossom(alice),
      );
      final first = await outbox.sync();
      expect(
        first.failed.single.resolutionError,
        'server set resolved to no server',
      );
      expect(first.failed.single.pendingServerSet, isNotNull);
      expect(fake.calls, isEmpty);

      lists.servers(alice, ['https://a']);
      final second = await outbox.sync();
      expect(second.delivered.single.servers, ['https://a']);

      await outbox.dispose();
    });

    test('freezes servers at the first resolution', () async {
      final lists = FakeServerLists()..servers(alice, ['https://a']);
      final fake = FakeUploader()..fail('https://a');
      final outbox = outboxFor(fake, lists);

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.blossom(alice),
      );
      await outbox.sync();
      lists.servers(alice, ['https://b']);
      final report = await outbox.sync();

      expect(report.failed.single.servers, ['https://a']);
      expect(fake.calls.map((c) => c.servers), [
        ['https://a'],
        ['https://a'],
      ]);
      expect(lists.calls, ['outbox|$alice', 'blossom|$alice']);

      await outbox.dispose();
    });

    test('a union with explicit servers resolves as a whole', () async {
      final lists = FakeServerLists()
        ..servers(alice, ['https://b', 'https://a']);
      final fake = FakeUploader()..ackAll(['https://a', 'https://b']);
      final outbox = outboxFor(fake, lists);

      final record = await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.union([
          UrlSet.explicit(['https://a']),
          UrlSet.blossom(alice),
        ]),
      );
      expect(record.servers, isEmpty);

      final report = await outbox.sync();
      expect(report.delivered.single.servers, ['https://a', 'https://b']);

      await outbox.dispose();
    });

    test('a set without lookup resolves at enqueue time', () async {
      final fake = FakeUploader();
      final outbox = outboxFor(fake, null);

      final record = await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.fallback([
          UrlSet.explicit([]),
          UrlSet.explicit(['https://b']),
        ]),
      );
      expect(record.servers, ['https://b']);
      expect(record.pendingServerSet, isNull);

      await outbox.dispose();
    });

    test('a new set on a delivered entry reopens it', () async {
      final lists = FakeServerLists()..servers(alice, ['https://b']);
      final fake = FakeUploader()..ackAll(['https://a', 'https://b']);
      final outbox = outboxFor(fake, lists);

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      await outbox.sync();

      final reopened = await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.blossom(alice),
      );
      expect(reopened.status, BlobUploadStatus.pending);
      expect((await cache.head(_sha))!.pinnedBy, [_shimHolder]);

      final report = await outbox.sync();
      final delivered = report.delivered.single;
      expect(delivered.servers, ['https://a', 'https://b']);
      expect(fake.calls.last.servers, ['https://b']);
      expect((await cache.head(_sha))!.pinned, isFalse);

      await outbox.dispose();
    });

    test('a set already covered by acks delivers without uploading', () async {
      final lists = FakeServerLists()..servers(alice, ['https://a']);
      final fake = FakeUploader()..ackAll(['https://a']);
      final outbox = outboxFor(fake, lists);

      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.explicit(['https://a']),
      );
      await outbox.sync();
      await outbox.enqueue(
        sha256: _sha,
        serverSet: const UrlSet.blossom(alice),
      );
      final report = await outbox.sync();

      expect(report.delivered.single.status, BlobUploadStatus.delivered);
      expect(fake.calls.length, 1);

      await outbox.dispose();
    });

    test('an unresolved set survives a restart', () async {
      final fake = FakeUploader()..ackAll(['https://a']);
      final first = outboxFor(fake, FakeServerLists());
      await first.enqueue(sha256: _sha, serverSet: const UrlSet.blossom(alice));
      await first.sync();
      await first.dispose();

      final second = outboxFor(
        fake,
        FakeServerLists()..servers(alice, ['https://a']),
      );
      final report = await second.sync();
      expect(report.delivered.single.servers, ['https://a']);

      await second.dispose();
    });

    test('dispose abandons a resolution waiting on a lookup', () async {
      final lists = FakeServerLists()..gate = Completer<void>();
      final outbox = outboxFor(FakeUploader(), lists);

      await outbox.upload(sha256: _sha, serverSet: const UrlSet.blossom(alice));
      while (lists.calls.isEmpty) {
        await Future.delayed(const Duration(milliseconds: 5));
      }
      await outbox.dispose().timeout(const Duration(seconds: 1));

      final record = await store.get(_sha);
      expect(record!.pendingServerSet, isA<BlossomUrls>());
      expect(record.resolutionAttempts, 0);
      lists.gate!.complete();
    });

    test('rejects invalid arguments', () async {
      final outbox = outboxFor(FakeUploader(), null);
      await expectLater(
        outbox.enqueue(sha256: _sha, serverSet: const UrlSet.blossom(alice)),
        throwsStateError,
      );

      final withLookup = outboxFor(FakeUploader(), FakeServerLists());
      for (final serverSet in const [
        UrlSet.explicit([]),
        UrlSet.explicit(['', ' /']),
        UrlSet.fallback([UrlSet.explicit([]), UrlSet.union([])]),
        UrlSet.dm(alice),
        UrlSet.fallback([UrlSet.blossom(alice), UrlSet.outbox(alice)]),
      ]) {
        await expectLater(
          withLookup.enqueue(sha256: _sha, serverSet: serverSet),
          throwsArgumentError,
        );
      }
      expect(await withLookup.listAll(), isEmpty);

      await outbox.dispose();
      await withLookup.dispose();
    });
  });
}
