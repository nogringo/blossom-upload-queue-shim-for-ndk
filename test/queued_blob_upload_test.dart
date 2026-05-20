import 'package:blossom_upload_queue_shim_for_ndk/blossom_upload_queue_shim_for_ndk.dart';
import 'package:test/test.dart';

QueuedBlobUpload _base({
  List<String> servers = const ['https://a'],
  List<String> ackedServers = const [],
  int? deliveredAt,
  bool pinnedByShim = false,
}) {
  return QueuedBlobUpload(
    sha256: 'a' * 64,
    contentType: 'image/png',
    servers: servers,
    ackedServers: ackedServers,
    lastErrors: const {},
    attempts: 0,
    firstAttemptAt: null,
    lastAttemptAt: null,
    nextAttemptAt: 0,
    deliveredAt: deliveredAt,
    createdAt: 0,
    pinnedByShim: pinnedByShim,
  );
}

void main() {
  group('QueuedBlobUpload', () {
    test('remainingServers excludes acked', () {
      final r = _base(
        servers: const ['https://a', 'https://b', 'https://c'],
        ackedServers: const ['https://b'],
      );
      expect(r.remainingServers, ['https://a', 'https://c']);
    });

    test('status reflects deliveredAt', () {
      final pending = _base();
      expect(pending.status, BlobUploadStatus.pending);
      expect(
        pending.copyWith(deliveredAt: 1).status,
        BlobUploadStatus.delivered,
      );
    });

    test('toMap → fromMap roundtrip preserves shape', () {
      final original = QueuedBlobUpload(
        sha256: 'b' * 64,
        contentType: 'application/pdf',
        servers: const ['https://cdn.example'],
        ackedServers: const [],
        lastErrors: const {'https://cdn.example': 'timeout'},
        attempts: 2,
        firstAttemptAt: 100,
        lastAttemptAt: 200,
        nextAttemptAt: 300,
        deliveredAt: null,
        createdAt: 50,
        forcedServers: const ['https://cdn.example'],
        pinnedByShim: true,
      );
      final restored = QueuedBlobUpload.fromMap(original.toMap());
      expect(restored.sha256, original.sha256);
      expect(restored.contentType, original.contentType);
      expect(restored.servers, original.servers);
      expect(restored.lastErrors, original.lastErrors);
      expect(restored.attempts, original.attempts);
      expect(restored.forcedServers, original.forcedServers);
      expect(restored.pinnedByShim, isTrue);
    });

    test('copyWith clearDelivered forces null even when arg is non-null', () {
      final delivered = _base(deliveredAt: 42);
      final cleared = delivered.copyWith(clearDelivered: true);
      expect(cleared.deliveredAt, isNull);
    });

    test('copyWith preserves pinnedByShim when not specified', () {
      final pinned = _base(pinnedByShim: true);
      final touched = pinned.copyWith(attempts: 1);
      expect(touched.pinnedByShim, isTrue);
    });
  });
}
