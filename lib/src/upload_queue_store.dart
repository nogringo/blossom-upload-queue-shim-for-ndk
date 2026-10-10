import 'queued_blob_upload.dart';

/// Persistent storage for the shim's [QueuedBlobUpload] records.
///
/// Records are keyed by [QueuedBlobUpload.key]. The shim only stores metadata
/// here; blob bytes live in the `BlossomCache`. Implement this to back the
/// queue with your app's own database. `SembastUploadQueueStore` (from
/// `package:blossom_upload_queue_shim_for_ndk/sembast_store.dart`) and
/// [InMemoryUploadQueueStore] are provided.
///
/// [QueuedBlobUpload.toMap] and [QueuedBlobUpload.fromMap] give a
/// JSON-compatible representation for stores that persist maps.
abstract class UploadQueueStore {
  /// Identifies this queue. The shim pins pending blobs in the `BlossomCache`
  /// under `blossom_upload_queue_shim:<name>`, so queues sharing one cache
  /// need distinct names. Keep it stable across restarts: renaming a queue
  /// orphans the pins of its pending entries.
  String get name;

  /// The record stored under [key], or null.
  Future<QueuedBlobUpload?> get(String key);

  /// Inserts or fully replaces the record stored under [QueuedBlobUpload.key].
  Future<void> put(QueuedBlobUpload record);

  /// Atomically reads the record under [key], passes it to [mutate] and stores
  /// the result. No other write to [key] may interleave.
  ///
  /// Returns null when no record exists (and [mutate] is not called), the
  /// unchanged record when [mutate] returns null, and the stored result
  /// otherwise.
  Future<QueuedBlobUpload?> update(
    String key,
    QueuedBlobUpload? Function(QueuedBlobUpload current) mutate,
  );

  /// Records eligible for an attempt at [now] (millis since epoch), ordered by
  /// [QueuedBlobUpload.nextAttemptAt]: those whose `nextAttemptAt <= now` and
  /// that are either undelivered or carry [QueuedBlobUpload.forcedServers].
  Future<List<QueuedBlobUpload>> findDue({required int now});

  /// Every record, delivered or not.
  Future<List<QueuedBlobUpload>> findAll();

  /// Every record bound to [pubkey].
  Future<List<QueuedBlobUpload>> findByPubkey(String pubkey);

  /// Deletes the records under [keys]. Missing keys are ignored.
  Future<void> deleteKeys(List<String> keys);

  /// Deletes every record.
  Future<void> deleteAll();

  /// Emits the record under [key] on listen, then after every change to it.
  /// Emits null while the record is absent.
  Stream<QueuedBlobUpload?> watch(String key);

  /// Emits the undelivered records on listen, then after every change to
  /// them. Narrowed to [pubkey]'s records when given.
  Stream<List<QueuedBlobUpload>> watchPending({String? pubkey});
}
