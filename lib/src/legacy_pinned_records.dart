import 'queued_blob_upload.dart';

/// Implemented by stores that may hold records written by 0.8.0 or earlier,
/// which still carry `pinnedByShim: true`.
abstract interface class LegacyPinnedRecords {
  /// The flagged records. The store's `put` must drop the flag.
  Future<List<QueuedBlobUpload>> findLegacyPinned();
}
