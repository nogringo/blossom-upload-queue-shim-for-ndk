import 'package:url_set_for_ndk/url_set_for_ndk.dart';

/// Status of a queued blob upload.
enum BlobUploadStatus {
  /// At least one targeted server has not acknowledged the upload yet.
  pending,

  /// Every server in `servers` has acknowledged the upload at least once.
  /// Monotonic: once a record reaches this state the shim never demotes it
  /// back to `pending` on its own.
  delivered,
}

/// A single blob upload tracked by `OfflineBlossomUpload`.
///
/// Immutable from the caller's perspective: every mutation goes through the
/// store and yields a fresh instance. The blob bytes themselves live in the
/// caller-provided `BlossomCache`, keyed by [sha256]; this record only holds
/// the queue metadata.
class QueuedBlobUpload {
  /// The blob's sha256 (lowercase hex).
  final String sha256;

  /// Nostr pubkey (hex) this upload belongs to, or null when the entry is not
  /// bound to an account. Part of the record key, so the same blob queued by
  /// two accounts yields two independent entries.
  ///
  /// When non-null the shim signs every attempt with this account's signer,
  /// not with whichever account happens to be logged in when the retry fires.
  final String? pubkey;

  /// MIME type to forward to the Blossom server, if any. Looked up from the
  /// cache descriptor at `upload()` time when the caller does not provide one.
  final String? contentType;

  /// The list of servers this blob must reach. Grows when [pendingServerSet]
  /// resolves, when `enqueue` merges more servers, or when
  /// `reupload(sha, server: ...)` introduces a new server.
  final List<String> servers;

  /// Server set still to resolve, or `null` once its servers are frozen into
  /// [servers]. While non-null the record is [BlobUploadStatus.pending].
  final UrlSet? pendingServerSet;

  /// Resolutions of [pendingServerSet] that did not conclude since it was set.
  /// Drives the backoff between them.
  final int resolutionAttempts;

  /// Why the last resolution of [pendingServerSet] did not produce servers:
  /// a lookup was unavailable, or the set resolved to no server.
  final String? resolutionError;

  /// Subset of [servers] that have returned `success: true` at least once
  /// across all attempts.
  final List<String> ackedServers;

  /// Last error message seen per server still pending. Cleared on ack.
  final Map<String, String> lastErrors;

  /// Number of delivery attempts that have completed (success or failure)
  /// since the record was created.
  final int attempts;

  /// Wall-clock millis (since epoch) of the first attempt for this record,
  /// or null if none has run yet.
  final int? firstAttemptAt;

  /// Wall-clock millis (since epoch) of the most recent attempt, or null if
  /// none has run yet.
  final int? lastAttemptAt;

  /// Wall-clock millis (since epoch) at which the worker should attempt this
  /// record next. The periodic tick picks up records whose value is <= now.
  final int nextAttemptAt;

  /// Wall-clock millis (since epoch) of the first time every server in
  /// [servers] had acknowledged the blob. Monotonic: once set, never cleared
  /// by an attempt; only `reupload(sha, server: r)` with a brand-new server
  /// (or `upload()` merging in an unacked server) clears it.
  final int? deliveredAt;

  /// Wall-clock millis (since epoch) when this record was first persisted.
  final int createdAt;

  /// Override for the *next* attempt only: when non-null, the worker pushes
  /// the blob to exactly this list of servers, even if some of them already
  /// have acks. Set by `OfflineBlossomUpload.reupload`; cleared by the
  /// attempt itself once it runs. Existence is what makes a delivered entry
  /// eligible for one more push without rewriting its history.
  final List<String>? forcedServers;

  /// Creates a record. Most callers should not invoke this directly; records
  /// are produced by `OfflineBlossomUpload`.
  QueuedBlobUpload({
    required this.sha256,
    required this.contentType,
    required this.servers,
    required this.ackedServers,
    required this.lastErrors,
    required this.attempts,
    required this.firstAttemptAt,
    required this.lastAttemptAt,
    required this.nextAttemptAt,
    required this.deliveredAt,
    required this.createdAt,
    this.pubkey,
    this.forcedServers,
    this.pendingServerSet,
    this.resolutionAttempts = 0,
    this.resolutionError,
  });

  /// The store key for this entry.
  String get key => keyFor(sha256: sha256, pubkey: pubkey);

  /// Builds the store key for a (blob, account) pair: `pubkey|sha256`
  /// when bound to an account, the bare sha256 otherwise. Account-less entries
  /// keep their pre-0.5.0 key so existing databases keep working.
  static String keyFor({required String sha256, String? pubkey}) =>
      pubkey == null ? sha256 : '$pubkey|$sha256';

  /// `pending` while any server still owes an ack, otherwise `delivered`.
  BlobUploadStatus get status => deliveredAt != null
      ? BlobUploadStatus.delivered
      : BlobUploadStatus.pending;

  /// Servers still owed an ack, i.e. [servers] minus [ackedServers].
  List<String> get remainingServers {
    final acked = ackedServers.toSet();
    return servers.where((s) => !acked.contains(s)).toList(growable: false);
  }

  /// Returns a copy of this record with the given fields replaced.
  ///
  /// Use `clearDelivered: true` to force-clear [deliveredAt] (`null` arg
  /// alone is ambiguous with "leave as-is" for nullable fields). Same idea
  /// for `clearForcedServers`, `clearContentType`, `clearPendingServerSet`
  /// and `clearResolutionError`.
  QueuedBlobUpload copyWith({
    String? contentType,
    List<String>? servers,
    List<String>? ackedServers,
    Map<String, String>? lastErrors,
    int? attempts,
    int? firstAttemptAt,
    int? lastAttemptAt,
    int? nextAttemptAt,
    int? deliveredAt,
    List<String>? forcedServers,
    UrlSet? pendingServerSet,
    int? resolutionAttempts,
    String? resolutionError,
    bool clearDelivered = false,
    bool clearForcedServers = false,
    bool clearContentType = false,
    bool clearPendingServerSet = false,
    bool clearResolutionError = false,
  }) {
    return QueuedBlobUpload(
      sha256: sha256,
      pubkey: pubkey,
      contentType: clearContentType ? null : (contentType ?? this.contentType),
      servers: servers ?? this.servers,
      ackedServers: ackedServers ?? this.ackedServers,
      lastErrors: lastErrors ?? this.lastErrors,
      attempts: attempts ?? this.attempts,
      firstAttemptAt: firstAttemptAt ?? this.firstAttemptAt,
      lastAttemptAt: lastAttemptAt ?? this.lastAttemptAt,
      nextAttemptAt: nextAttemptAt ?? this.nextAttemptAt,
      deliveredAt: clearDelivered ? null : (deliveredAt ?? this.deliveredAt),
      createdAt: createdAt,
      forcedServers: clearForcedServers
          ? null
          : (forcedServers ?? this.forcedServers),
      pendingServerSet: clearPendingServerSet
          ? null
          : (pendingServerSet ?? this.pendingServerSet),
      resolutionAttempts: resolutionAttempts ?? this.resolutionAttempts,
      resolutionError: clearResolutionError
          ? null
          : (resolutionError ?? this.resolutionError),
    );
  }

  /// Serializes the record to a JSON-compatible map.
  Map<String, dynamic> toMap() {
    return {
      'sha256': sha256,
      'pubkey': pubkey,
      'contentType': contentType,
      'servers': servers,
      'ackedServers': ackedServers,
      'lastErrors': lastErrors,
      'attempts': attempts,
      'firstAttemptAt': firstAttemptAt,
      'lastAttemptAt': lastAttemptAt,
      'nextAttemptAt': nextAttemptAt,
      'deliveredAt': deliveredAt,
      'createdAt': createdAt,
      'forcedServers': forcedServers,
      'pendingServerSet': pendingServerSet?.toMap(),
      'resolutionAttempts': resolutionAttempts,
      'resolutionError': resolutionError,
    };
  }

  /// Inverse of [toMap].
  static QueuedBlobUpload fromMap(Map<String, dynamic> map) {
    return QueuedBlobUpload(
      sha256: map['sha256'] as String,
      pubkey: map['pubkey'] as String?,
      contentType: map['contentType'] as String?,
      servers: (map['servers'] as List).cast<String>(),
      ackedServers: (map['ackedServers'] as List).cast<String>(),
      lastErrors: (map['lastErrors'] as Map).map(
        (k, v) => MapEntry(k as String, v as String),
      ),
      attempts: map['attempts'] as int,
      firstAttemptAt: map['firstAttemptAt'] as int?,
      lastAttemptAt: map['lastAttemptAt'] as int?,
      nextAttemptAt: map['nextAttemptAt'] as int,
      deliveredAt: map['deliveredAt'] as int?,
      createdAt: map['createdAt'] as int,
      forcedServers: (map['forcedServers'] as List?)?.cast<String>(),
      pendingServerSet: switch (map['pendingServerSet']) {
        final Map set => UrlSet.fromMap(set),
        _ => null,
      },
      resolutionAttempts: map['resolutionAttempts'] as int? ?? 0,
      resolutionError: map['resolutionError'] as String?,
    );
  }
}
