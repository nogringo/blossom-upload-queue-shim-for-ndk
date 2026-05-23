## 0.3.0

- **Breaking:** Bump `blossom_cache` to `^0.2.0`. The cache's `put()` now
  takes `bytes` positionally with `sha256` as a named argument
  (`cache.put(bytes, sha256: sha, type: 'image/png')` instead of
  `cache.put(sha, bytes, type: 'image/png')`). Callers that populate the
  cache themselves before scheduling an upload must update those call
  sites.

## 0.2.0

- Bump `ndk` to `^0.8.4-dev.1` and forward the queue's sha256 to
  `ndk.blossom.uploadBlob` via the new `precomputedSha256` parameter, so
  each retry skips re-hashing the blob bytes.
- **Breaking:** `BlobUploadFn` gains a `required String precomputedSha256`
  named parameter. Custom implementations passed via the default
  constructor must accept it.

## 0.1.0

- Initial release.
- `OfflineBlossomUpload` shim around `ndk.blossom.uploadBlob` with
  sembast-backed queue metadata and `blossom_cache`-backed blob bytes.
- 100% delivery guarantee: every targeted server must ack before a record is
  marked delivered. Monotonic `ackedServers` and `deliveredAt`.
- `upload()` and `reupload()` with `forcedServers` semantics matching the
  broadcast shim.
- Cache pin ownership: the shim only releases pins it applied itself.
- Connectivity-aware retry loop via NDK relay connectivity as a proxy for
  internet reachability, plus a generic `onlineSignal` injection point.
