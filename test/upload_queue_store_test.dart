import 'dart:async';
import 'dart:typed_data';

import 'package:blossom_cache/blossom_cache.dart';
import 'package:blossom_upload_queue_shim_for_ndk/blossom_upload_queue_shim_for_ndk.dart';
import 'package:blossom_upload_queue_shim_for_ndk/sembast_store.dart';
import 'package:idb_shim/idb_client_memory.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

QueuedBlobUpload _record(
  String sha256, {
  String? pubkey,
  int nextAttemptAt = 0,
  int? deliveredAt,
  List<String>? forcedServers,
  int attempts = 0,
}) {
  return QueuedBlobUpload(
    sha256: sha256,
    pubkey: pubkey,
    contentType: null,
    servers: const ['https://a'],
    ackedServers: const [],
    lastErrors: const {},
    attempts: attempts,
    firstAttemptAt: null,
    lastAttemptAt: null,
    nextAttemptAt: nextAttemptAt,
    deliveredAt: deliveredAt,
    createdAt: 0,
    forcedServers: forcedServers,
  );
}

Future<void> _until(FutureOr<bool> Function() condition) async {
  for (var i = 0; i < 200; i++) {
    if (await condition()) return;
    await Future.delayed(const Duration(milliseconds: 5));
  }
  throw TimeoutException('condition not met');
}

void main() {
  final backends = <String, Future<UploadQueueStore> Function()>{
    'SembastUploadQueueStore': () async => SembastUploadQueueStore(
      await newDatabaseFactoryMemory().openDatabase('test.db'),
    ),
    'InMemoryUploadQueueStore': () async => InMemoryUploadQueueStore(),
  };

  for (final MapEntry(key: label, value: open) in backends.entries) {
    group(label, () {
      late UploadQueueStore store;

      setUp(() async {
        store = await open();
      });

      test('defaults its name to blob_uploads', () {
        expect(store.name, 'blob_uploads');
      });

      test('put then get round-trips, keyed by pubkey and sha256', () async {
        await store.put(_record('s1', pubkey: 'alice', attempts: 3));

        expect(await store.get('s1'), isNull);
        final r = await store.get(
          QueuedBlobUpload.keyFor(sha256: 's1', pubkey: 'alice'),
        );
        expect(r!.pubkey, 'alice');
        expect(r.attempts, 3);
      });

      test('update skips missing keys and keeps null mutations', () async {
        var called = false;
        expect(
          await store.update('missing', (r) {
            called = true;
            return r;
          }),
          isNull,
        );
        expect(called, isFalse);

        await store.put(_record('s1', attempts: 1));
        expect((await store.update('s1', (_) => null))!.attempts, 1);

        final next = await store.update(
          's1',
          (r) => r.copyWith(attempts: r.attempts + 1),
        );
        expect(next!.attempts, 2);
        expect((await store.get('s1'))!.attempts, 2);
      });

      test(
        'findDue keeps due, undelivered or forced records in order',
        () async {
          await store.put(_record('late', nextAttemptAt: 20));
          await store.put(_record('early', nextAttemptAt: 10));
          await store.put(_record('future', nextAttemptAt: 99));
          await store.put(_record('done', deliveredAt: 1));
          await store.put(
            _record(
              'forced',
              nextAttemptAt: 15,
              deliveredAt: 1,
              forcedServers: const ['https://a'],
            ),
          );

          final due = await store.findDue(now: 50);
          expect(due.map((r) => r.sha256), ['early', 'forced', 'late']);
        },
      );

      test('findByPubkey and findAll', () async {
        await store.put(_record('s1', pubkey: 'alice'));
        await store.put(_record('s2', pubkey: 'bob'));
        await store.put(_record('s3'));

        expect((await store.findByPubkey('alice')).map((r) => r.sha256), [
          's1',
        ]);
        expect(
          (await store.findAll()).map((r) => r.sha256),
          unorderedEquals(['s1', 's2', 's3']),
        );
      });

      test('deleteKeys ignores missing keys, deleteAll empties', () async {
        await store.put(_record('s1'));
        await store.put(_record('s2'));
        await store.put(_record('s3'));

        await store.deleteKeys(['s1', 'missing']);
        expect(
          (await store.findAll()).map((r) => r.sha256),
          unorderedEquals(['s2', 's3']),
        );

        await store.deleteAll();
        expect(await store.findAll(), isEmpty);
      });

      test('watch emits on listen, then on every change to the key', () async {
        final events = <QueuedBlobUpload?>[];
        final sub = store.watch('s1').listen(events.add);

        await _until(() => events.length == 1);
        expect(events.single, isNull);

        await store.put(_record('s1'));
        await _until(() => events.length == 2);
        expect(events.last!.attempts, 0);

        await store.update('s1', (r) => r.copyWith(attempts: 1));
        await _until(() => events.length == 3);
        expect(events.last!.attempts, 1);

        await store.deleteKeys(['s1']);
        await _until(() => events.length == 4);
        expect(events.last, isNull);

        await sub.cancel();
      });

      test('watchPending tracks undelivered records, per account', () async {
        await store.put(_record('s1', pubkey: 'alice'));
        await store.put(_record('s2', pubkey: 'bob'));

        List<String>? all;
        List<String>? alice;
        final subs = [
          store.watchPending().listen(
            (rs) => all = rs.map((r) => r.sha256).toList(),
          ),
          store
              .watchPending(pubkey: 'alice')
              .listen((rs) => alice = rs.map((r) => r.sha256).toList()),
        ];

        await _until(() => all != null && alice != null);
        expect(all, unorderedEquals(['s1', 's2']));
        expect(alice, ['s1']);

        await store.update(
          QueuedBlobUpload.keyFor(sha256: 's1', pubkey: 'alice'),
          (r) => r.copyWith(deliveredAt: 1),
        );
        await _until(() => alice!.isEmpty && all!.length == 1);
        expect(all, ['s2']);

        for (final sub in subs) {
          await sub.cancel();
        }
      });
    });
  }

  test('the shim runs end to end on InMemoryUploadQueueStore', () async {
    const sha = 'c';
    final cache = await IdbBlossomCache.open(factory: newIdbFactoryMemory());
    await cache.put(Uint8List.fromList([1, 2, 3]), sha256: sha);

    final outbox = OfflineBlossomUpload(
      uploadFn:
          ({
            required data,
            required serverUrls,
            required precomputedSha256,
            contentType,
            pubkey,
          }) async => [
            for (final s in serverUrls)
              BlobUploadResult(serverUrl: s, success: true),
          ],
      cache: cache,
      store: InMemoryUploadQueueStore(),
    );

    await outbox.upload(sha256: sha, servers: const ['https://a']);
    expect((await cache.head(sha))!.pinnedBy, [
      'blossom_upload_queue_shim:blob_uploads',
    ]);

    await _until(() async => (await outbox.get(sha))?.deliveredAt != null);
    await outbox.dispose();
    expect((await cache.head(sha))!.pinned, isFalse);
  });
}
