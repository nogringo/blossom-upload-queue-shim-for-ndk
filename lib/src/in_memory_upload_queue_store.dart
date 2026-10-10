import 'dart:async';

import 'queued_blob_upload.dart';
import 'upload_queue_store.dart';

/// [UploadQueueStore] that keeps records in memory.
///
/// Nothing survives a restart, so pending uploads are lost with the process.
/// Meant for tests, and as a reference when implementing a store on top of
/// your own database.
class InMemoryUploadQueueStore implements UploadQueueStore {
  @override
  final String name;

  final Map<String, QueuedBlobUpload> _records = {};
  final StreamController<Set<String>> _changes =
      StreamController<Set<String>>.broadcast();

  InMemoryUploadQueueStore({this.name = 'blob_uploads'});

  @override
  Future<QueuedBlobUpload?> get(String key) async => _records[key];

  @override
  Future<void> put(QueuedBlobUpload record) async {
    _records[record.key] = record;
    _changes.add({record.key});
  }

  @override
  Future<QueuedBlobUpload?> update(
    String key,
    QueuedBlobUpload? Function(QueuedBlobUpload current) mutate,
  ) async {
    final current = _records[key];
    if (current == null) return null;
    final next = mutate(current);
    if (next == null) return current;
    _records[key] = next;
    _changes.add({key});
    return next;
  }

  @override
  Future<List<QueuedBlobUpload>> findDue({required int now}) async {
    return _records.values
        .where(
          (r) =>
              r.nextAttemptAt <= now &&
              (r.deliveredAt == null || r.forcedServers != null),
        )
        .toList()
      ..sort((a, b) => a.nextAttemptAt.compareTo(b.nextAttemptAt));
  }

  @override
  Future<List<QueuedBlobUpload>> findAll() async =>
      _records.values.toList(growable: false);

  @override
  Future<List<QueuedBlobUpload>> findByPubkey(String pubkey) async =>
      _records.values.where((r) => r.pubkey == pubkey).toList(growable: false);

  @override
  Future<void> deleteKeys(List<String> keys) async {
    final removed = keys.where((k) => _records.remove(k) != null).toSet();
    if (removed.isNotEmpty) _changes.add(removed);
  }

  @override
  Future<void> deleteAll() async {
    if (_records.isEmpty) return;
    final removed = _records.keys.toSet();
    _records.clear();
    _changes.add(removed);
  }

  @override
  Stream<QueuedBlobUpload?> watch(String key) =>
      _watch(() => _records[key], (changed) => changed.contains(key));

  @override
  Stream<List<QueuedBlobUpload>> watchPending({String? pubkey}) => _watch(
    () => _records.values
        .where(
          (r) =>
              r.deliveredAt == null && (pubkey == null || r.pubkey == pubkey),
        )
        .toList(growable: false),
    (_) => true,
  );

  Stream<T> _watch<T>(T Function() snapshot, bool Function(Set<String>) when) {
    return Stream.multi((controller) {
      controller.add(snapshot());
      final sub = _changes.stream
          .where(when)
          .listen((_) => controller.add(snapshot()));
      controller.onCancel = sub.cancel;
    });
  }
}
