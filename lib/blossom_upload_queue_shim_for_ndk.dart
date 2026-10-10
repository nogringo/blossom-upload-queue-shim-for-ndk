/// Offline-first shim around the [ndk](https://pub.dev/packages/ndk) package's
/// Blossom upload use case.
///
/// Use [OfflineBlossomUpload.withNdk] to wrap an existing `Ndk` instance,
/// persist per-upload metadata in an [UploadQueueStore], read blob bytes from
/// a `BlossomCache`, and retry until every targeted server has acknowledged
/// each blob. A sembast-backed store lives in
/// `package:blossom_upload_queue_shim_for_ndk/sembast_store.dart`.
library;

export 'src/offline_blossom_upload.dart'
    show BlobUploadFn, CanSignForFn, OfflineBlossomUpload;
export 'src/in_memory_upload_queue_store.dart' show InMemoryUploadQueueStore;
export 'src/queued_blob_upload.dart' show BlobUploadStatus, QueuedBlobUpload;
export 'src/upload_queue_store.dart' show UploadQueueStore;
