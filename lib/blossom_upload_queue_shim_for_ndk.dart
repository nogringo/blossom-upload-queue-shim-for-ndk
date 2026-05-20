/// Offline-first shim around the [ndk](https://pub.dev/packages/ndk) package's
/// Blossom upload use case.
///
/// Use [OfflineBlossomUpload.withNdk] to wrap an existing `Ndk` instance,
/// persist per-upload metadata in a sembast database, read blob bytes from a
/// `BlossomCache`, and retry until every targeted server has acknowledged
/// each blob.
library;

export 'src/offline_blossom_upload.dart'
    show BlobUploadFn, OfflineBlossomUpload;
export 'src/queued_blob_upload.dart' show BlobUploadStatus, QueuedBlobUpload;
