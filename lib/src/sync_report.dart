import 'queued_blob_upload.dart';

/// Outcome of `OfflineBlossomUpload.sync`, sorting every entry it considered
/// into one bucket.
class SyncReport {
  /// Entries that every targeted server has acknowledged.
  final List<QueuedBlobUpload> delivered;

  /// Entries attempted that still owe an ack to at least one server. The
  /// reason per server is in [QueuedBlobUpload.lastErrors].
  final List<QueuedBlobUpload> failed;

  /// Entries left untouched because their account cannot sign right now.
  final List<QueuedBlobUpload> skipped;

  const SyncReport({
    required this.delivered,
    required this.failed,
    required this.skipped,
  });
}
