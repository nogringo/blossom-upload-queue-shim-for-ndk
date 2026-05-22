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
/// (it is the queue key) so it forwards it on every attempt to skip re-hashing.
typedef BlobUploadFn =
    Future<List<BlobUploadResult>> Function({
      required Uint8List data,
      required List<String> serverUrls,
      required String precomputedSha256,
      String? contentType,
    });

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
///    for manual `reupload` or inspection.
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
    Random? random,
    int Function()? now,
  }) : _uploadFn = uploadFn,
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
              relays.values.any((rc) => rc.isConnected && isPublicHost(rc.url)),
        )
        .distinct();
    return OfflineBlossomUpload(
      uploadFn:
          ({
            required Uint8List data,
            required List<String> serverUrls,
            required String precomputedSha256,
            String? contentType,
          }) => ndk.blossom.uploadBlob(
            data: data,
            serverUrls: serverUrls,
            contentType: contentType,
            strategy: UploadStrategy.allSimultaneous,
            precomputedSha256: precomputedSha256,
          ),
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
  /// If a record with the same [sha256] already exists, its target servers
  /// are merged with [servers] and it is rescheduled for an immediate
  /// attempt.
  Future<QueuedBlobUpload> upload({
    required String sha256,
    required List<String> servers,
    String? contentType,
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

    final existing = await _store.get(sha256);
    final QueuedBlobUpload record;
    if (existing != null) {
      final mergedServers = _dedupNormalized([
        ...existing.servers,
        ...normalizedServers,
      ]);
      final fullyAcked = mergedServers.every(existing.ackedServers.contains);
      // We may need to re-pin if the merge demotes the entry back to pending.
      final shouldPin = !fullyAcked && !existing.pinnedByShim;
      final didPin = shouldPin ? await _cache.pin(sha256) : false;
      record = existing.copyWith(
        servers: mergedServers,
        contentType: existing.contentType ?? effectiveContentType,
        nextAttemptAt: now,
        clearDelivered: !fullyAcked,
        pinnedByShim: existing.pinnedByShim || didPin,
      );
    } else {
      final didPin = await _cache.pin(sha256);
      record = QueuedBlobUpload(
        sha256: sha256,
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

    unawaited(_attempt(record.sha256));
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
  Future<QueuedBlobUpload?> reupload(String sha256, {String? server}) async {
    _ensureNotDisposed();
    final now = _now();
    final updated = await _store.update(sha256, (current) {
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
      final didPin = await _cache.pin(sha256);
      if (didPin) {
        await _store.update(sha256, (current) {
          return current.copyWith(pinnedByShim: true);
        });
      }
    }
    unawaited(_attempt(sha256));
    return updated;
  }

  /// Triggers an immediate scan for due entries. Safe to call repeatedly;
  /// in-flight attempts are not duplicated.
  Future<void> retryNow() async {
    _ensureNotDisposed();
    await _tick();
  }

  /// Returns the currently persisted record for [sha256], or `null` if none
  /// exists.
  Future<QueuedBlobUpload?> get(String sha256) => _store.get(sha256);

  /// Live snapshot of the record for [sha256]. Emits `null` if/while the
  /// record is absent.
  Stream<QueuedBlobUpload?> watch(String sha256) => _store.watch(sha256);

  /// Live snapshot of every record that has not been delivered yet.
  Stream<List<QueuedBlobUpload>> watchPending() => _store.watchPending();

  /// One-shot read of every record in the store, delivered or not.
  Future<List<QueuedBlobUpload>> listAll() => _store.findAll();

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
      unawaited(_attempt(record.sha256));
    }
  }

  Future<void> _attempt(String sha256) async {
    if (_disposed) return;
    if (_inFlight.containsKey(sha256)) return;
    final completer = Completer<void>();
    _inFlight[sha256] = completer.future;

    try {
      final record = await _store.get(sha256);
      if (record == null) return;
      if (record.deliveredAt != null && record.forcedServers == null) return;

      final targets = record.forcedServers ?? record.remainingServers;
      if (targets.isEmpty) {
        await _store.update(sha256, (current) {
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
        await _store.update(sha256, (current) {
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
        ).timeout(_perAttemptTimeout, onTimeout: () => const []);
      } catch (e) {
        syncError = e.toString();
        results = const [];
      }

      await _store.update(sha256, (current) {
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
      _inFlight.remove(sha256);
      completer.complete();
    }
  }

  /// Releases the shim-owned pin on [sha256] once the record is delivered.
  /// No-op if the pin was not applied by the shim or the record is still
  /// pending.
  Future<void> _maybeReleasePin(String sha256) async {
    final record = await _store.get(sha256);
    if (record == null) return;
    if (record.deliveredAt == null) return;
    if (!record.pinnedByShim) return;
    await _cache.unpin(sha256);
    await _store.update(sha256, (current) {
      return current.copyWith(pinnedByShim: false);
    });
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
