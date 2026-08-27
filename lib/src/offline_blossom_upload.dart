import 'dart:async';
import 'dart:math';
import 'dart:typed_data';

import 'package:blossom_cache/blossom_cache.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast.dart';

import 'backoff.dart';
import 'public_host_filter.dart';
import 'queue_store.dart';
import 'queued_blob_upload.dart';

/// Function that hands a blob off to the network. Matches the call pattern of
/// `Ndk.blossom.uploadBlob` with `serverUrls` always provided. The shim always
/// uses [UploadStrategy.allSimultaneous] under the hood and never asks for
/// server-side media optimisation (which would alter the resulting sha256).
///
/// [precomputedSha256] is the hex sha256 of [data]; the shim already knows it
/// (it is part of the queue key) so it forwards it on every attempt to skip
/// re-hashing.
///
/// [pubkey] is the account the queued entry belongs to, or null for
/// account-less entries. Implementations must sign the Blossom authorization
/// with *that* account, not with whichever one is logged in when the retry
/// fires.
typedef BlobUploadFn =
    Future<List<BlobUploadResult>> Function({
      required Uint8List data,
      required List<String> serverUrls,
      required String precomputedSha256,
      String? contentType,
      String? pubkey,
    });

/// Whether [pubkey]'s signer is currently available, so an account-bound entry
/// can be signed by its owner. Returning false defers the entry untouched
/// rather than signing it with the wrong identity.
typedef CanSignForFn = bool Function(String pubkey);

/// Offline-first wrapper around NDK's Blossom upload.
///
/// Contract:
///  - `upload(sha256: ..., servers: [...])` persists the queue entry before
///    returning. The blob bytes must already live in the caller-provided
///    [BlossomCache]; the shim pins them while delivery is pending.
///  - Delivery is guaranteed in the eventual sense: the shim keeps retrying
///    each `pending` entry until every server in [QueuedBlobUpload.servers]
///    has acknowledged it.
///  - Records are never auto-deleted. A delivered entry stays in the store
///    for manual `reupload` or inspection; `clearLocalAccountData` and
///    `clearAllLocalData` are the only ways to remove entries.
///  - An entry queued with a `pubkey` is signed by that account on every
///    attempt, and is deferred rather than signed by anyone else when that
///    account cannot sign.
class OfflineBlossomUpload {
  final BlobUploadFn _uploadFn;
  final BlossomCache _cache;
  final QueueStore _store;
  final Duration _tickInterval;
  final Duration _initialBackoff;
  final Duration _maxBackoff;
  final Duration _perAttemptTimeout;
  final Random _random;
  final int Function() _now;
  final Stream<bool>? _onlineSignal;
  final CanSignForFn? _canSignFor;

  Timer? _tickTimer;
  StreamSubscription<bool>? _onlineSub;
  bool _isOnline = true;
  final Map<String, Future<void>> _inFlight = <String, Future<void>>{};
  bool _disposed = false;

  OfflineBlossomUpload._({
    required BlobUploadFn uploadFn,
    required BlossomCache cache,
    required Database db,
    required String storeName,
    required Duration tickInterval,
    required Duration initialBackoff,
    required Duration maxBackoff,
    required Duration perAttemptTimeout,
    Stream<bool>? onlineSignal,
    CanSignForFn? canSignFor,
    Random? random,
    int Function()? now,
  }) : _uploadFn = uploadFn,
       _canSignFor = canSignFor,
       _cache = cache,
       _store = QueueStore(db: db, storeName: storeName),
       _tickInterval = tickInterval,
       _initialBackoff = initialBackoff,
       _maxBackoff = maxBackoff,
       _perAttemptTimeout = perAttemptTimeout,
       _onlineSignal = onlineSignal,
       _random = random ?? Random(),
       _now = now ?? (() => DateTime.now().millisecondsSinceEpoch);

  /// Default constructor: inject the upload function explicitly. Useful for
  /// tests or for callers who already wrap NDK.
  ///
  /// Pass [onlineSignal] to make the periodic retry loop connectivity-aware:
  /// while the latest emission is `false`, periodic ticks are no-ops, and the
  /// `false -> true` edge triggers an immediate retry pass. `retryNow()`
  /// always runs regardless of this signal. If [onlineSignal] is null the
  /// shim assumes it is always online.
  ///
  /// Pass [canSignFor] when queueing account-bound entries: it gates whether
  /// an entry whose `pubkey` is set may be attempted right now. Without it the
  /// shim attempts every due entry and leaves the identity question entirely
  /// to [uploadFn].
  factory OfflineBlossomUpload({
    required BlobUploadFn uploadFn,
    required BlossomCache cache,
    required Database db,
    String storeName = 'blob_uploads',
    Duration tickInterval = const Duration(seconds: 30),
    Duration initialBackoff = const Duration(seconds: 5),
    Duration maxBackoff = const Duration(minutes: 30),
    Duration perAttemptTimeout = const Duration(minutes: 5),
    Stream<bool>? onlineSignal,
    CanSignForFn? canSignFor,
    Random? random,
    int Function()? now,
  }) {
    return OfflineBlossomUpload._(
      uploadFn: uploadFn,
      cache: cache,
      db: db,
      storeName: storeName,
      tickInterval: tickInterval,
      initialBackoff: initialBackoff,
      maxBackoff: maxBackoff,
      perAttemptTimeout: perAttemptTimeout,
      onlineSignal: onlineSignal,
      canSignFor: canSignFor,
      random: random,
      now: now,
    );
  }

  /// Convenience constructor wired to an [Ndk] instance.
  ///
  /// Blossom has no dedicated connectivity stream, so the shim reuses
  /// `ndk.connectivity.relayConnectivityChanges` as a proxy for "the device
  /// has internet": as long as one connected relay sits on a public-internet
  /// host, retries are allowed to run. Loopback, private IPv4/IPv6, and
  /// `.local` names are filtered out so a connected dev relay on localhost
  /// will not mask a real outage.
  ///
  /// Entries queued with a `pubkey` are signed by *that* account on every
  /// attempt, resolved from `ndk.accounts` and passed as `customSigner`, which
  /// takes priority over the logged-in account. NDK keeps a signer per added
  /// account, so a retry fired while another account is active still uploads
  /// under the queueing identity. If the account can no longer sign (logged
  /// out, or read-only), the entry is deferred rather than signed by the wrong
  /// key: NDK would otherwise fall back to a throwaway keypair.
  factory OfflineBlossomUpload.withNdk(
    Ndk ndk, {
    required BlossomCache cache,
    required Database db,
    String storeName = 'blob_uploads',
    Duration tickInterval = const Duration(seconds: 30),
    Duration initialBackoff = const Duration(seconds: 5),
    Duration maxBackoff = const Duration(minutes: 30),
    Duration perAttemptTimeout = const Duration(minutes: 5),
  }) {
    final onlineSignal = ndk.connectivity.relayConnectivityChanges
        .map(
          (relays) =>
              relays.any((rc) => rc.isConnected && isPublicHost(rc.url)),
        )
        .distinct();
    return OfflineBlossomUpload(
      uploadFn:
          ({
            required Uint8List data,
            required List<String> serverUrls,
            required String precomputedSha256,
            String? contentType,
            String? pubkey,
          }) => ndk.blossom.uploadBlob(
            data: data,
            serverUrls: serverUrls,
            contentType: contentType,
            strategy: UploadStrategy.allSimultaneous,
            precomputedSha256: precomputedSha256,
            customSigner: pubkey == null
                ? null
                : ndk.accounts.accounts[pubkey]?.signer,
          ),
      canSignFor: (pubkey) =>
          ndk.accounts.accounts[pubkey]?.signer.canSign() ?? false,
      cache: cache,
      db: db,
      storeName: storeName,
      tickInterval: tickInterval,
      initialBackoff: initialBackoff,
      maxBackoff: maxBackoff,
      perAttemptTimeout: perAttemptTimeout,
      onlineSignal: onlineSignal,
    );
  }

  /// Persists a queue entry for the blob identified by [sha256], to be
  /// delivered to every URL in [servers], then fires the first attempt in
  /// the background. The returned [QueuedBlobUpload] reflects the persisted
  /// state, not the attempt outcome.
  ///
  /// The blob must already live in the [BlossomCache] passed to the
  /// constructor; the shim looks it up via `cache.head(sha256)` and throws
  /// [StateError] if it is absent. While the entry is pending, the shim pins
  /// the blob to protect it from auto-eviction. The pin is released on
  /// delivery, but only if the shim was the one that applied it.
  ///
  /// Pass [pubkey] to bind the entry to a nostr account: every attempt is then
  /// signed by that account rather than by whoever is logged in when the retry
  /// fires, and the entry can be purged with [clearLocalAccountData]. Entries
  /// are keyed by (pubkey, sha256), so the same blob queued by two accounts
  /// yields two independent records. Leave it null to keep the pre-0.5.0
  /// behaviour, where the signer is resolved at attempt time.
  ///
  /// If a record with the same [sha256] and [pubkey] already exists, its target
  /// servers are merged with [servers] and it is rescheduled for an immediate
  /// attempt.
  Future<QueuedBlobUpload> upload({
    required String sha256,
    required List<String> servers,
    String? contentType,
    String? pubkey,
  }) async {
    _ensureNotDisposed();
    if (servers.isEmpty) {
      throw ArgumentError.value(servers, 'servers', 'must not be empty');
    }
    final normalizedServers = _dedupNormalized(servers);
    final now = _now();

    final descriptor = await _cache.head(sha256);
    if (descriptor == null) {
      throw StateError(
        'Blob $sha256 is not in the cache. Call cache.put(...) before upload().',
      );
    }
    final effectiveContentType = contentType ?? descriptor.type;

    final key = QueuedBlobUpload.keyFor(sha256: sha256, pubkey: pubkey);
    final existing = await _store.get(key);
    final QueuedBlobUpload record;
    if (existing != null) {
      final mergedServers = _dedupNormalized([
        ...existing.servers,
        ...normalizedServers,
      ]);
      final fullyAcked = mergedServers.every(existing.ackedServers.contains);
      // We may need to re-pin if the merge demotes the entry back to pending.
      final shouldPin = !fullyAcked && !existing.pinnedByShim;
      final didPin = shouldPin
          ? await _acquirePin(sha256, exceptKey: key)
          : false;
      record = existing.copyWith(
        servers: mergedServers,
        contentType: existing.contentType ?? effectiveContentType,
        nextAttemptAt: now,
        clearDelivered: !fullyAcked,
        pinnedByShim: existing.pinnedByShim || didPin,
      );
    } else {
      final didPin = await _acquirePin(sha256);
      record = QueuedBlobUpload(
        sha256: sha256,
        pubkey: pubkey,
        contentType: effectiveContentType,
        servers: normalizedServers,
        ackedServers: const [],
        lastErrors: const {},
        attempts: 0,
        firstAttemptAt: null,
        lastAttemptAt: null,
        nextAttemptAt: now,
        deliveredAt: null,
        createdAt: now,
        pinnedByShim: didPin,
      );
    }
    await _store.put(record);

    unawaited(_attempt(record.key));
    return record;
  }

  /// Re-pushes a queued blob without rewriting its delivery history.
  ///
  /// `ackedServers` is monotonic and append-only over an entry's lifetime: a
  /// server that has confirmed receipt stays confirmed forever. `reupload`
  /// never clears acks; it sets a one-shot `forcedServers` override that the
  /// next attempt consumes.
  ///
  /// - `server == null`: schedules an immediate attempt that pushes to every
  ///   server in the entry's `servers` list, including those already acked.
  ///   `deliveredAt` is preserved.
  /// - `server != null`: adds [server] to the entry's `servers` list if
  ///   absent, schedules an immediate one-shot push to that single server.
  ///   If the server is new, `deliveredAt` is cleared (the entry can no
  ///   longer claim 100% delivery until the new server acks); otherwise it
  ///   is preserved.
  Future<QueuedBlobUpload?> reupload(
    String sha256, {
    String? server,
    String? pubkey,
  }) async {
    _ensureNotDisposed();
    final now = _now();
    final key = QueuedBlobUpload.keyFor(sha256: sha256, pubkey: pubkey);
    final updated = await _store.update(key, (current) {
      if (server == null) {
        return current.copyWith(
          forcedServers: List<String>.from(current.servers),
          nextAttemptAt: now,
        );
      }
      final normalized = _normalizeServer(server);
      final isNew = !current.servers.contains(normalized);
      final servers = isNew
          ? [...current.servers, normalized]
          : current.servers;
      return current.copyWith(
        servers: servers,
        forcedServers: [normalized],
        nextAttemptAt: now,
        clearDelivered: isNew,
      );
    });
    if (updated == null) return null;
    // Demoted back to pending and no shim-owned pin? Try to take ownership.
    if (updated.deliveredAt == null && !updated.pinnedByShim) {
      final didPin = await _acquirePin(sha256, exceptKey: key);
      if (didPin) {
        await _store.update(key, (current) {
          return current.copyWith(pinnedByShim: true);
        });
      }
    }
    unawaited(_attempt(key));
    return updated;
  }

  /// Triggers an immediate scan for due entries. Safe to call repeatedly;
  /// in-flight attempts are not duplicated.
  Future<void> retryNow() async {
    _ensureNotDisposed();
    await _tick();
  }

  /// Returns the currently persisted record for [sha256] under [pubkey], or
  /// `null` if none exists.
  Future<QueuedBlobUpload?> get(String sha256, {String? pubkey}) =>
      _store.get(QueuedBlobUpload.keyFor(sha256: sha256, pubkey: pubkey));

  /// Live snapshot of the record for [sha256] under [pubkey]. Emits `null`
  /// if/while the record is absent.
  Stream<QueuedBlobUpload?> watch(String sha256, {String? pubkey}) =>
      _store.watch(QueuedBlobUpload.keyFor(sha256: sha256, pubkey: pubkey));

  /// Live snapshot of every record that has not been delivered yet, across all
  /// accounts unless [pubkey] narrows it to one.
  Stream<List<QueuedBlobUpload>> watchPending({String? pubkey}) =>
      _store.watchPending(pubkey: pubkey);

  /// One-shot read of every record in the store, delivered or not.
  Future<List<QueuedBlobUpload>> listAll() => _store.findAll();

  /// Deletes every queue record bound to [pubkey], delivered or not.
  ///
  /// Records belonging to other accounts, and account-less records, are left
  /// alone. A blob still needed by another account keeps its cache pin; a pin
  /// the caller applied themselves is never released. The blob bytes stay in
  /// the cache either way: they belong to the caller, not to the shim.
  Future<void> clearLocalAccountData({required String pubkey}) async {
    _ensureNotDisposed();
    final doomed = await _store.findByPubkey(pubkey);
    if (doomed.isEmpty) return;
    await _store.deleteKeys(doomed.map((r) => r.key).toList(growable: false));
    for (final record in doomed) {
      if (!record.pinnedByShim) continue;
      await _releasePinIfOrphaned(record.sha256);
    }
  }

  /// Deletes every queue record, for all accounts. Blob bytes stay in the
  /// cache; only pins the shim itself applied are released.
  Future<void> clearAllLocalData() async {
    _ensureNotDisposed();
    final all = await _store.findAll();
    await _store.deleteAll();
    for (final sha256
        in all.where((r) => r.pinnedByShim).map((r) => r.sha256).toSet()) {
      await _cache.unpin(sha256);
    }
  }

  /// Starts the periodic retry timer and replays anything already due. Also
  /// subscribes to `onlineSignal` if one was provided. Idempotent: calling it
  /// more than once is a no-op.
  void start() {
    _ensureNotDisposed();
    if (_tickTimer != null) return;
    _tickTimer = Timer.periodic(_tickInterval, (_) => _periodicTick());
    if (_onlineSignal != null && _onlineSub == null) {
      _onlineSub = _onlineSignal.listen(_handleOnlineChange);
    }
    _periodicTick();
  }

  /// Stops the retry timer, cancels the connectivity subscription, and waits
  /// for any in-flight attempt to finish so the caller can safely close the
  /// underlying sembast database and Blossom cache.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _tickTimer?.cancel();
    _tickTimer = null;
    await _onlineSub?.cancel();
    _onlineSub = null;
    if (_inFlight.isNotEmpty) {
      await Future.wait(_inFlight.values);
    }
  }

  void _handleOnlineChange(bool online) {
    if (_disposed) return;
    final wasOnline = _isOnline;
    _isOnline = online;
    if (!wasOnline && online) {
      unawaited(_tick());
    }
  }

  void _periodicTick() {
    if (_disposed) return;
    if (!_isOnline) return;
    unawaited(_tick());
  }

  // ---------------------------------------------------------------------------
  // internals
  // ---------------------------------------------------------------------------

  Future<void> _tick() async {
    if (_disposed) return;
    final due = await _store.findDue(now: _now());
    for (final record in due) {
      if (_disposed) return;
      unawaited(_attempt(record.key));
    }
  }

  Future<void> _attempt(String key) async {
    if (_disposed) return;
    if (_inFlight.containsKey(key)) return;
    final completer = Completer<void>();
    _inFlight[key] = completer.future;

    try {
      final record = await _store.get(key);
      if (record == null) return;
      if (record.deliveredAt != null && record.forcedServers == null) return;

      final sha256 = record.sha256;
      final pubkey = record.pubkey;
      // Signing as the wrong account is worse than not uploading at all: NDK
      // would silently fall back to a throwaway keypair. Leave the record
      // untouched so it is retried once the account can sign again.
      if (pubkey != null && !(_canSignFor?.call(pubkey) ?? true)) return;

      final targets = record.forcedServers ?? record.remainingServers;
      if (targets.isEmpty) {
        await _store.update(key, (current) {
          if (current.servers.every(current.ackedServers.contains) &&
              current.deliveredAt == null) {
            return current.copyWith(deliveredAt: _now());
          }
          return null;
        });
        await _maybeReleasePin(sha256);
        return;
      }

      final bytes = await _cache.get(sha256);
      if (bytes == null) {
        await _store.update(key, (current) {
          final newErrors = Map<String, String>.from(current.lastErrors);
          for (final s in targets) {
            if (!current.ackedServers.contains(s)) {
              newErrors[s] = 'blob bytes missing from cache';
            }
          }
          final attempts = current.attempts + 1;
          final nextDelay = computeBackoff(
            attempts: attempts,
            initial: _initialBackoff,
            max: _maxBackoff,
            random: _random,
          );
          final nowMs = _now();
          return current.copyWith(
            lastErrors: newErrors,
            attempts: attempts,
            firstAttemptAt: current.firstAttemptAt ?? nowMs,
            lastAttemptAt: nowMs,
            nextAttemptAt: nowMs + nextDelay.inMilliseconds,
            clearForcedServers: true,
          );
        });
        return;
      }

      final attemptStart = _now();
      List<BlobUploadResult> results;
      String? syncError;
      try {
        results = await _uploadFn(
          data: bytes,
          serverUrls: targets,
          precomputedSha256: sha256,
          contentType: record.contentType,
          pubkey: pubkey,
        ).timeout(_perAttemptTimeout, onTimeout: () => const []);
      } catch (e) {
        syncError = e.toString();
        results = const [];
      }

      await _store.update(key, (current) {
        final newAcked = Set<String>.from(current.ackedServers);
        final newErrors = Map<String, String>.from(current.lastErrors);

        final byUrl = <String, BlobUploadResult>{};
        for (final r in results) {
          byUrl[_normalizeServer(r.serverUrl)] = r;
        }

        for (final target in targets) {
          final r = byUrl[target];
          final alreadyAcked = current.ackedServers.contains(target);
          if (r != null && r.success) {
            newAcked.add(target);
            newErrors.remove(target);
          } else if (!alreadyAcked) {
            final msg = r == null
                ? (syncError ?? 'no response (timeout or server unreachable)')
                : (r.error ?? 'rejected');
            newErrors[target] = msg;
          }
        }
        for (final ok in newAcked) {
          newErrors.remove(ok);
        }

        final delivered = current.servers.every(newAcked.contains);
        final attempts = current.attempts + 1;
        final nextDelay = computeBackoff(
          attempts: attempts,
          initial: _initialBackoff,
          max: _maxBackoff,
          random: _random,
        );

        return current.copyWith(
          ackedServers: newAcked.toList(growable: false),
          lastErrors: newErrors,
          attempts: attempts,
          firstAttemptAt: current.firstAttemptAt ?? attemptStart,
          lastAttemptAt: attemptStart,
          nextAttemptAt: delivered
              ? attemptStart
              : _now() + nextDelay.inMilliseconds,
          deliveredAt: delivered
              ? (current.deliveredAt ?? _now())
              : current.deliveredAt,
          clearForcedServers: true,
        );
      });
      await _maybeReleasePin(sha256);
    } finally {
      _inFlight.remove(key);
      completer.complete();
    }
  }

  /// Pins [sha256] for a record that is about to become pending, and reports
  /// whether the shim owns the resulting pin.
  ///
  /// The cache pin is a per-blob boolean, not a refcount, while records are
  /// per (account, blob). Ownership is therefore shared: if a sibling record
  /// already holds a shim-owned pin, this record inherits that ownership
  /// instead of pinning again, so the last sibling standing still protects the
  /// bytes. A pin that was already there and is not ours stays foreign.
  Future<bool> _acquirePin(String sha256, {String? exceptKey}) async {
    final siblings = await _store.findBySha256(sha256);
    for (final sibling in siblings) {
      if (sibling.key == exceptKey) continue;
      if (sibling.pinnedByShim) return true;
    }
    return _cache.pin(sha256);
  }

  /// Releases the shim-owned pin on [sha256] once *every* record for that blob
  /// is delivered. No-op while any account still owes an upload, or if the pin
  /// was not applied by the shim.
  Future<void> _maybeReleasePin(String sha256) async {
    final siblings = await _store.findBySha256(sha256);
    if (!siblings.any((r) => r.pinnedByShim)) return;
    if (siblings.any((r) => r.deliveredAt == null)) return;
    await _cache.unpin(sha256);
    for (final sibling in siblings.where((r) => r.pinnedByShim)) {
      await _store.update(sibling.key, (current) {
        return current.copyWith(pinnedByShim: false);
      });
    }
  }

  /// Releases the shim-owned pin on [sha256] after its last record was
  /// deleted. Keeps the pin while another account still has a record for the
  /// same blob.
  Future<void> _releasePinIfOrphaned(String sha256) async {
    final remaining = await _store.findBySha256(sha256);
    if (remaining.isNotEmpty) return;
    await _cache.unpin(sha256);
  }

  void _ensureNotDisposed() {
    if (_disposed) {
      throw StateError('OfflineBlossomUpload has been disposed');
    }
  }

  String _normalizeServer(String url) {
    var u = url.trim().toLowerCase();
    while (u.endsWith('/')) {
      u = u.substring(0, u.length - 1);
    }
    return u;
  }

  List<String> _dedupNormalized(Iterable<String> servers) {
    final seen = <String>{};
    final out = <String>[];
    for (final s in servers) {
      final n = _normalizeServer(s);
      if (n.isEmpty) continue;
      if (seen.add(n)) out.add(n);
    }
    return out;
  }
}
