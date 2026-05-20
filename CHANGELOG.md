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
