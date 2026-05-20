import 'package:sembast/sembast.dart';

import 'queued_blob_upload.dart';

/// Sembast-backed persistent store for [QueuedBlobUpload] records.
///
/// All writes are serialized through sembast transactions. The store name is
/// caller-provided so multiple shims can coexist in the same database.
class QueueStore {
  final Database _db;
  final StoreRef<String, Map<String, Object?>> _store;

  QueueStore({required Database db, required String storeName})
    : _db = db,
      _store = stringMapStoreFactory.store(storeName);

  Future<QueuedBlobUpload?> get(String sha256) async {
    final map = await _store.record(sha256).get(_db);
    if (map == null) return null;
    return QueuedBlobUpload.fromMap(_normalize(map));
  }

  Future<void> put(QueuedBlobUpload record) async {
    await _store.record(record.sha256).put(_db, record.toMap());
  }

  /// Atomically read-modify-write a record. The mutator runs inside a sembast
  /// transaction; returning null leaves the record unchanged.
  Future<QueuedBlobUpload?> update(
    String sha256,
    QueuedBlobUpload? Function(QueuedBlobUpload current) mutate,
  ) async {
    return _db.transaction((txn) async {
      final raw = await _store.record(sha256).get(txn);
      if (raw == null) return null;
      final current = QueuedBlobUpload.fromMap(_normalize(raw));
      final next = mutate(current);
      if (next == null) return current;
      await _store.record(sha256).put(txn, next.toMap());
      return next;
    });
  }

  /// Records eligible for an attempt right now: either still pending, or
  /// delivered but carrying a [QueuedBlobUpload.forcedServers] override that
  /// hasn't been consumed yet.
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
    final records = await _store.find(_db, finder: finder);
    return records
        .map((r) => QueuedBlobUpload.fromMap(_normalize(r.value)))
        .toList(growable: false);
  }

  Future<List<QueuedBlobUpload>> findAll() async {
    final records = await _store.find(_db);
    return records
        .map((r) => QueuedBlobUpload.fromMap(_normalize(r.value)))
        .toList(growable: false);
  }

  Stream<QueuedBlobUpload?> watch(String sha256) {
    return _store
        .record(sha256)
        .onSnapshot(_db)
        .map(
          (snap) => snap == null
              ? null
              : QueuedBlobUpload.fromMap(_normalize(snap.value)),
        );
  }

  Stream<List<QueuedBlobUpload>> watchPending() {
    final finder = Finder(filter: Filter.equals('deliveredAt', null));
    return _store
        .query(finder: finder)
        .onSnapshots(_db)
        .map(
          (snaps) => snaps
              .map((s) => QueuedBlobUpload.fromMap(_normalize(s.value)))
              .toList(growable: false),
        );
  }

  Map<String, dynamic> _normalize(Map<String, Object?> raw) =>
      Map<String, dynamic>.from(raw);
}
