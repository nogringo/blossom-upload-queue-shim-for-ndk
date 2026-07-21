import 'package:sembast/sembast.dart';

import 'queued_blob_upload.dart';

/// Sembast-backed persistent store for [QueuedBlobUpload] records.
///
/// Records are keyed by [QueuedBlobUpload.key], i.e. `pubkey|sha256` for
/// account-bound entries and the bare sha256 otherwise. All writes are
/// serialized through sembast transactions. The store name is caller-provided
/// so multiple shims can coexist in the same database.
class QueueStore {
  final Database _db;
  final StoreRef<String, Map<String, Object?>> _store;

  QueueStore({required Database db, required String storeName})
    : _db = db,
      _store = stringMapStoreFactory.store(storeName);

  Future<QueuedBlobUpload?> get(String key) async {
    final map = await _store.record(key).get(_db);
    if (map == null) return null;
    return QueuedBlobUpload.fromMap(_normalize(map));
  }

  Future<void> put(QueuedBlobUpload record) async {
    await _store.record(record.key).put(_db, record.toMap());
  }

  /// Atomically read-modify-write a record. The mutator runs inside a sembast
  /// transaction; returning null leaves the record unchanged.
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
    return _find(finder);
  }

  Future<List<QueuedBlobUpload>> findAll() => _find(null);

  /// Every record for [sha256], across all accounts. Used to reason about the
  /// shared cache pin, which is per-blob while records are per (account, blob).
  Future<List<QueuedBlobUpload>> findBySha256(String sha256) =>
      _find(Finder(filter: Filter.equals('sha256', sha256)));

  Future<List<QueuedBlobUpload>> findByPubkey(String pubkey) =>
      _find(Finder(filter: Filter.equals('pubkey', pubkey)));

  Future<void> deleteKeys(List<String> keys) async {
    if (keys.isEmpty) return;
    await _db.transaction((txn) => _store.records(keys).delete(txn));
  }

  Future<void> deleteAll() async {
    await _store.delete(_db);
  }

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
