import 'package:sembast/sembast.dart';

import 'legacy_pinned_records.dart';
import 'queued_blob_upload.dart';
import 'upload_queue_store.dart';

/// [UploadQueueStore] backed by a sembast [Database].
///
/// Records live in the sembast store named [name], so several queues can share
/// one database. All writes are serialized through sembast transactions.
class SembastUploadQueueStore implements UploadQueueStore, LegacyPinnedRecords {
  final Database _db;
  final StoreRef<String, Map<String, Object?>> _store;

  @override
  final String name;

  SembastUploadQueueStore(Database db, {String storeName = 'blob_uploads'})
    : _db = db,
      name = storeName,
      _store = stringMapStoreFactory.store(storeName);

  @override
  Future<QueuedBlobUpload?> get(String key) async {
    final map = await _store.record(key).get(_db);
    if (map == null) return null;
    return QueuedBlobUpload.fromMap(_normalize(map));
  }

  @override
  Future<void> put(QueuedBlobUpload record) async {
    await _store.record(record.key).put(_db, record.toMap());
  }

  @override
  Future<QueuedBlobUpload?> update(
    String key,
    QueuedBlobUpload? Function(QueuedBlobUpload current) mutate,
  ) async {
    return _db.transaction((txn) async {
      final raw = await _store.record(key).get(txn);
      if (raw == null) return null;
      final current = QueuedBlobUpload.fromMap(_normalize(raw));
      final next = mutate(current);
      if (next == null) return current;
      await _store.record(key).put(txn, next.toMap());
      return next;
    });
  }

  @override
  Future<List<QueuedBlobUpload>> findDue({required int now}) async {
    final finder = Finder(
      filter: Filter.custom((record) {
        final m = record.value as Map;
        final nextAttemptAt = m['nextAttemptAt'] as int;
        if (nextAttemptAt > now) return false;
        if (m['deliveredAt'] == null) return true;
        return m['forcedServers'] != null;
      }),
      sortOrders: [SortOrder('nextAttemptAt')],
    );
    return _find(finder);
  }

  @override
  Future<List<QueuedBlobUpload>> findAll() => _find(null);

  @override
  Future<List<QueuedBlobUpload>> findLegacyPinned() =>
      _find(Finder(filter: Filter.equals('pinnedByShim', true)));

  @override
  Future<List<QueuedBlobUpload>> findByPubkey(String pubkey) =>
      _find(Finder(filter: Filter.equals('pubkey', pubkey)));

  @override
  Future<void> deleteKeys(List<String> keys) async {
    if (keys.isEmpty) return;
    await _db.transaction((txn) => _store.records(keys).delete(txn));
  }

  @override
  Future<void> deleteAll() async {
    await _store.delete(_db);
  }

  @override
  Stream<QueuedBlobUpload?> watch(String key) {
    return _store
        .record(key)
        .onSnapshot(_db)
        .map(
          (snap) => snap == null
              ? null
              : QueuedBlobUpload.fromMap(_normalize(snap.value)),
        );
  }

  @override
  Stream<List<QueuedBlobUpload>> watchPending({String? pubkey}) {
    final filters = [
      Filter.equals('deliveredAt', null),
      if (pubkey != null) Filter.equals('pubkey', pubkey),
    ];
    final finder = Finder(
      filter: filters.length == 1 ? filters.first : Filter.and(filters),
    );
    return _store
        .query(finder: finder)
        .onSnapshots(_db)
        .map(
          (snaps) => snaps
              .map((s) => QueuedBlobUpload.fromMap(_normalize(s.value)))
              .toList(growable: false),
        );
  }

  Future<List<QueuedBlobUpload>> _find(Finder? finder) async {
    final records = await _store.find(_db, finder: finder);
    return records
        .map((r) => QueuedBlobUpload.fromMap(_normalize(r.value)))
        .toList(growable: false);
  }

  Map<String, dynamic> _normalize(Map<String, Object?> raw) =>
      Map<String, dynamic>.from(raw);
}
