# blossom_upload_queue_shim_for_ndk

Offline-first wrapper around the [`ndk`](https://pub.dev/packages/ndk) package's
Blossom upload use case.

NDK's `blossom.uploadBlob` ships a blob to a set of Blossom servers and reports
per-server results. If every server is unreachable (flaky network, app
backgrounded, process killed mid-upload), the blob is gone from the caller's
perspective. This shim sits in front of `ndk.blossom.uploadBlob` and adds:

- **Local persistence first.** The queue entry is committed to a pluggable
  [queue store](#queue-storage) and the bytes are pinned in a
  [`blossom_cache`](https://pub.dev/packages/blossom_cache) before any network
  attempt. `upload()` returns once persistence is durable;
  delivery happens in the background and survives restarts.
- **100 % delivery guarantee.** An entry is only marked `delivered` once
  *every* targeted server has returned `success: true`. Partial success keeps
  the entry pending and retries the missing servers.
- **Monotonic ack history.** A server that has acked never un-acks. A
  delivered entry never silently flips back to pending due to a transient
  server outage.
- **No auto-deletion.** Delivered entries stay in the store and can be
  re-uploaded later, for instance to a freshly discovered server.

## Install

```yaml
dependencies:
  blossom_upload_queue_shim_for_ndk: ^0.10.0
  blossom_cache: ^0.5.0
  ndk: ^0.10.0-dev.7
  idb_shim: ^2.9.2
  sembast: ^3.8.7 # only to use SembastUploadQueueStore
```

## Quick start

```dart
import 'dart:typed_data';

import 'package:blossom_cache/blossom_cache.dart';
import 'package:blossom_upload_queue_shim_for_ndk/blossom_upload_queue_shim_for_ndk.dart';
import 'package:blossom_upload_queue_shim_for_ndk/sembast_store.dart';
import 'package:idb_shim/idb_io.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_io.dart';

Future<void> main() async {
  final db = await databaseFactoryIo.openDatabase('blossom_uploads.db');
  final store = SembastUploadQueueStore(db);
  final cache = await IdbBlossomCache.open(factory: idbFactorySembastIo);

  final ndk = Ndk(
    NdkConfig(eventVerifier: Bip340EventVerifier(), cache: MemCacheManager()),
  );

  final outbox = OfflineBlossomUpload.withNdk(ndk, cache: cache, store: store);
  outbox.start();

  // Bytes go in the cache first, with a caller-supplied sha256.
  await cache.put(mySha256, myBytes, type: 'image/png');

  // Returns as soon as the queue entry is persisted. Delivery is now the
  // shim's responsibility.
  await outbox.upload(
    sha256: mySha256,
    servers: const ['https://blossom.primal.net', 'https://cdn.satellite.earth'],
  );
}
```

## Queue storage

The shim keeps its queue metadata in an `UploadQueueStore`. Blob bytes stay in
the `BlossomCache`. Two stores ship with the package:

- `SembastUploadQueueStore(db, storeName: 'blob_uploads')`, from
  `package:blossom_upload_queue_shim_for_ndk/sembast_store.dart`. Open `db`
  with `sembast_io` on native targets, `sembast_web` in the browser, or
  `sembast_sqflite` on top of SQLite. Several queues can share one database
  under different store names.
- `InMemoryUploadQueueStore()`, which loses everything on restart. Use it in
  tests, or as a starting point for your own store.

To keep the queue in your app's own database, implement `UploadQueueStore`.
`QueuedBlobUpload.toMap()` and `QueuedBlobUpload.fromMap()` give a
JSON-compatible form of each record. The shim relies on three guarantees:

- `update()` is atomic: no other write to the same key may interleave between
  the read and the write.
- `watch()` and `watchPending()` emit the current state on listen, then after
  every change.
- `name` stays stable across restarts. The shim pins pending blobs under a
  holder derived from it, so renaming a queue orphans those pins, and two
  queues sharing a cache need distinct names.

## Semantics

### `upload(sha256:, servers:, contentType:, pubkey:)`

Persists a queue entry for the blob identified by `sha256` and schedules an
immediate attempt to push it to every URL in `servers`. The blob bytes must
already live in the `BlossomCache` passed to the constructor. `upload()`
looks them up via `cache.head(sha256)` and throws `StateError` if absent. The
`servers` list is **required**; no automatic server-list lookup is performed.
URLs are normalized (lowercased, trailing `/` stripped) before storage.

If a record with the same `sha256` **and** `pubkey` already exists, the server
lists are merged. `deliveredAt` is preserved if every server in the merged list
is already in the entry's ack set; otherwise the entry is demoted to pending so
the missing servers get pushed.

If no `contentType` is provided, the shim reads it from the cache descriptor.

`pubkey` is optional and binds the entry to a nostr account: see
[Account binding](#account-binding). Leave it out and the entry behaves exactly
as it did before 0.5.0.

### `enqueue(sha256:, servers:, contentType:, pubkey:)`

Same as `upload()`, without the immediate attempt. The entry is due at once, so
the next periodic tick, `retryNow()` or `sync()` sends it. Without `start()`,
only `retryNow()` and `sync()` do: see [Sending on demand](#sending-on-demand).

### `retryNow()`

Forces an immediate scan of due entries, bypassing the online check. Use it as
an explicit override (e.g. when the user pulls to refresh). Entries waiting out
their backoff are left for later; use `sync()` to attempt them too.

### `sync({String? pubkey, int concurrency = 4})`

Attempts every undelivered entry once, plus those awaiting a `reupload()`,
ignoring both the backoff and the online check. Unlike `retryNow()`, it
completes only when every attempt has finished, and returns a `SyncReport`:

- `delivered`: every targeted server has acked.
- `failed`: attempted, but at least one server still owes an ack. See
  `lastErrors`.
- `skipped`: not attempted, because the entry's account cannot sign right now.

At most `concurrency` entries are attempted at once, since each attempt loads
its blob into memory. An attempt already in flight is awaited, not duplicated.
Pass `pubkey` to sync one account's entries only.

### Sending on demand

An app that only sends when asked, such as a CLI with `add` and `sync`
commands, never calls `start()`. Each command is a short-lived process:

```dart
// mycli add photo.png
await cache.put(bytes, sha256: sha, type: 'image/png');
await outbox.enqueue(sha256: sha, servers: servers, pubkey: pubkey);
await outbox.dispose();

// mycli sync
final report = await outbox.sync();
for (final entry in report.failed) {
  print('${entry.sha256}: ${entry.lastErrors}');
}
await outbox.dispose();
```

With `withNdk`, an account-bound entry ends up in `skipped` unless its account
is logged in to `ndk` with a signer when `sync()` runs.

### Connectivity awareness

`OfflineBlossomUpload.withNdk()` subscribes to
`ndk.connectivity.relayConnectivityChanges` and pauses the periodic retry
timer while no public relay is connected. Blossom has no dedicated
connectivity stream, so the shim treats "a relay is reachable" as a proxy for
"the device has internet." Loopback addresses, RFC1918 IPv4, ULA/link-local
IPv6, and mDNS `.local` names are excluded from the "is online" computation
so a local dev relay cannot mask a real outage.

For non-NDK setups, pass any `Stream<bool> onlineSignal` to the default
constructor:

```dart
OfflineBlossomUpload(
  uploadFn: ...,
  cache: cache,
  store: store,
  onlineSignal: yourConnectivityStream, // true while online, false otherwise
);
```

If you don't pass anything, the shim assumes it is always online and the
periodic timer runs unconditionally.

### `reupload(sha256, {String? server, String? pubkey})`

`ackedServers` and `deliveredAt` are monotonic. `reupload` never rewrites the
past; it queues a one-shot push via a transient `forcedServers` override that
the next attempt consumes.

- `reupload(sha256)`: schedules an immediate push to **every** server in the
  entry's `servers` list, including those that already acked. Useful when you
  suspect a server dropped your blob. Acks and `deliveredAt` are preserved
  regardless of the new attempt's outcome.
- `reupload(sha256, server: s)`: pushes to that single server. If `s` is new
  to the entry, it joins the target list and the entry is demoted to pending
  until `s` acks. If `s` was already there, the historical state is preserved.

`pubkey` must match the one the entry was queued with.

### Account binding

Pass `pubkey` to `upload()` to bind an entry to a nostr account:

- Records are keyed by `(pubkey, sha256)`, so the same blob queued by two
  accounts yields two independent entries. Account-less entries keep the bare
  `sha256` key, so existing databases keep working. `get()`, `watch()` and
  `reupload()` take an optional `pubkey`, `watchPending()` too.
- Every attempt is signed by that account, passed to NDK as
  `AuthPolicy.require`, which wins over the logged-in one. A retry firing
  hours later still uploads under the queueing identity.
- If that account can no longer sign (logged out, or read-only), the entry is
  deferred untouched rather than counted as a failed attempt: NDK refuses to
  upload with `BlossomAuthUnavailableException`.

With your own `uploadFn`, it receives the entry's `pubkey` and must sign with
it; pass `canSignFor` to get the deferral.

### Removing entries

- `remove(sha256, pubkey:)` deletes that one record, delivered or not. An
  attempt already in flight may still reach its servers, but the record stays
  gone.
- `clearLocalAccountData(pubkey:)` deletes every record bound to that account,
  delivered or not. Other accounts and account-less records are untouched.
- `clearAllLocalData()` deletes every record.

Blob bytes stay in the cache either way; only shim-owned pins are released.

### Pin ownership

The shim pins the blob in the `BlossomCache` while an upload is pending and
releases the pin on `delivered`. It pins under its own holder,
`blossom_upload_queue_shim:<name>` where `<name>` is the store's `name`,
suffixed with `:<pubkey>` for account-bound entries, so each record owns
exactly one pin. **It never touches pins held by anyone else**: pin the blob
yourself, before or after `upload()`, and your pin survives delivery.

A blob whose bytes are deleted from the cache while still pending will have
its next attempt fail with `lastErrors[server] = 'blob bytes missing from
cache'`. The entry stays in the store; the network call is not made.

### What "success" means

The full target set must ack. The shim uses `UploadStrategy.allSimultaneous`
internally so each server is attempted independently; per-server failures do
not affect the others. `firstSuccess` and `mirrorAfterSuccess` are
deliberately not exposed: they would either fail to cover every target or
introduce server-to-server dependencies that complicate the model.

### What the shim does NOT do

- **It never signs.** Whatever bytes you pass are forwarded as-is to
  `ndk.blossom.uploadBlob`, which signs the BUD-01 authorization event. The
  shim only picks *which* signer NDK uses, for entries queued with a `pubkey`.
- **It never hashes.** The caller provides the sha256. The cache stores by
  that key. The shim queues by that key.
- **It never requests server-side media optimisation.** `serverMediaOptimisation`
  would re-encode the blob server-side and change the resulting sha256,
  breaking content-addressing. Hardcoded `false`.
- **It never deletes records on its own.** Even after full delivery, the entry
  stays in the store until you call `remove()` or one of the `clear*` methods.
- **It does not give up.** Without a `maxAttempts` knob, a deterministically
  rejected upload (size too big for the server, blob type not allowed, etc.)
  will retry forever with exponential backoff. Inspect
  `QueuedBlobUpload.lastErrors` and call `remove()` if needed.

## Tuning

```dart
OfflineBlossomUpload.withNdk(
  ndk,
  cache: cache,
  store: SembastUploadQueueStore(db, storeName: 'blob_uploads'),
  tickInterval: const Duration(seconds: 30),     // periodic retry scan
  initialBackoff: const Duration(seconds: 5),    // backoff floor
  maxBackoff: const Duration(minutes: 30),       // backoff ceiling
  perAttemptTimeout: const Duration(minutes: 5), // give up on a single NDK call after this
);
```

## Testing your integration

`OfflineBlossomUpload` is fully unit-testable without NDK or a database. Pass
a custom `BlobUploadFn` to the default constructor:

```dart
final outbox = OfflineBlossomUpload(
  uploadFn:
      ({
        required data,
        required serverUrls,
        required precomputedSha256,
        contentType,
        pubkey,
      }) async => [
        for (final s in serverUrls)
          BlobUploadResult(serverUrl: s, success: true),
      ],
  cache: await IdbBlossomCache.open(factory: newIdbFactoryMemory()),
  store: InMemoryUploadQueueStore(),
);
```

The package's own test suite uses this approach; see
[`test/offline_blossom_upload_test.dart`](test/offline_blossom_upload_test.dart).
[`test/upload_queue_store_test.dart`](test/upload_queue_store_test.dart) checks
both stores against the contract above, and is a good template to test your
own.

## License

MIT
