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
  /// The blob's sha256 (lowercase hex). Doubles as the sembast record key.
  final String sha256;

  /// MIME type to forward to the Blossom server, if any. Looked up from the
  /// cache descriptor at `upload()` time when the caller does not provide one.
  final String? contentType;

  /// The list of servers this blob must reach. Fixed at creation, but may grow
  /// if `reupload(sha, server: ...)` introduces a new server.
  final List<String> servers;

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

  /// Whether the cache pin on [sha256] was applied by the shim itself (as
  /// opposed to a pre-existing pin owned by the caller). The shim only
  /// releases the pin when this flag is `true`, so a caller-owned pin is
  /// never accidentally cleared.
  final bool pinnedByShim;

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
    this.forcedServers,
    this.pinnedByShim = false,
  });

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
  /// for `clearForcedServers` and `clearContentType`.
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
    bool? pinnedByShim,
    bool clearDelivered = false,
    bool clearForcedServers = false,
    bool clearContentType = false,
  }) {
    return QueuedBlobUpload(
      sha256: sha256,
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
      pinnedByShim: pinnedByShim ?? this.pinnedByShim,
    );
  }

  /// Serializes the record for sembast storage.
  Map<String, dynamic> toMap() {
    return {
      'sha256': sha256,
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
      'pinnedByShim': pinnedByShim,
    };
  }

  /// Inverse of [toMap].
  static QueuedBlobUpload fromMap(Map<String, dynamic> map) {
    return QueuedBlobUpload(
      sha256: map['sha256'] as String,
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
      pinnedByShim: map['pinnedByShim'] as bool? ?? false,
    );
  }
}
