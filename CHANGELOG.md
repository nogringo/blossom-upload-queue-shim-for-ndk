## 0.6.0

- Require `blossom_cache: ^0.4.0`. Custom `BlossomCache` implementations must
  now provide `clearAllLocalData`; callers of the built-in caches are
  unaffected. The shim's own API is unchanged.

## 0.5.0

- **Breaking:** `BlobUploadFn` gains a `String? pubkey` named parameter.
  Custom `uploadFn` implementations must accept it and sign with that account.
  `OfflineBlossomUpload.withNdk` is unaffected.
- Add an optional `pubkey` to `upload()`, binding the entry to a nostr account.
  Records are then keyed by `(pubkey, sha256)` instead of `sha256` alone, so
  the same blob queued by two accounts yields two independent entries.
  Account-less entries keep the bare `sha256` key, so existing databases keep
  working without migration. `get()`, `watch()`, `watchPending()` and
  `reupload()` gain a matching optional `pubkey`.
- Account-bound entries are signed by their own account on every attempt, via
  NDK's `customSigner`, rather than by whichever account is logged in when the
  retry fires.
- Add `canSignFor`, and wire it in `withNdk`. An entry whose account can no
  longer sign (logged out, or read-only) is deferred untouched instead of being
  uploaded under the throwaway keypair NDK silently falls back to.
- Add `clearLocalAccountData(pubkey:)` and `clearAllLocalData()`. Blob bytes
  are left in the cache; only shim-owned pins are released.
- Fix pin ownership across accounts. The cache pin is a per-blob boolean while
  records are now per `(pubkey, sha256)`, so ownership is shared: the pin is
  released only once every record for that blob is delivered or deleted. One
  account finishing no longer exposes another's pending bytes to eviction.

## 0.4.1

- Fix `int64` overflow in `computeBackoff` on native platforms (Android, iOS,
  Linux, macOS, Windows). After ~52 failed attempts (with the default 5s
  initial delay), `pow(2, attempts)` wrapped to a negative integer, causing
  `double.clamp` to throw `Invalid argument(s)`. The calculation now uses
  double arithmetic (`pow(2.0, ...)`) to avoid the overflow.

## 0.4.0

- **Breaking (transitive on-disk):** Bump `blossom_cache` to `^0.3.0`. The
  cache database schema is bumped to version 2, so any cache written by
  `blossom_cache` 0.2.0 or earlier is dropped on first open after the
  upgrade. Queue records still in flight whose blob bytes lived only in the
  dropped cache will fail to retry. The `BlossomCache` API itself is
  unchanged, so no source changes are required in callers of this shim.
- `IdbBlossomCache.open()` now accepts a `chunkSize` parameter (default
  1 MB) that fixes the Android `CursorWindow` ~2 MB row limit for large
  blobs when backed by sqflite.

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
