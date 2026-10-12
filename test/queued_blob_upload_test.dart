import 'package:blossom_upload_queue_shim_for_ndk/blossom_upload_queue_shim_for_ndk.dart';
import 'package:test/test.dart';

QueuedBlobUpload _base({
  List<String> servers = const ['https://a'],
  List<String> ackedServers = const [],
  int? deliveredAt,
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
      );
      final restored = QueuedBlobUpload.fromMap(original.toMap());
      expect(restored.sha256, original.sha256);
      expect(restored.contentType, original.contentType);
      expect(restored.servers, original.servers);
      expect(restored.lastErrors, original.lastErrors);
      expect(restored.attempts, original.attempts);
      expect(restored.forcedServers, original.forcedServers);
    });

    test('toMap → fromMap roundtrip keeps the pending server set', () {
      final original = _base(servers: const []).copyWith(
        pendingServerSet: const UrlSet.fallback([
          UrlSet.blossom('p'),
          UrlSet.explicit(['https://default']),
        ]),
        resolutionAttempts: 3,
        resolutionError: 'offline',
      );
      final restored = QueuedBlobUpload.fromMap(original.toMap());
      expect(
        restored.pendingServerSet!.toMap(),
        original.pendingServerSet!.toMap(),
      );
      expect(restored.resolutionAttempts, 3);
      expect(restored.resolutionError, 'offline');
    });

    test('fromMap reads records stored before server sets', () {
      final map = _base().toMap()
        ..remove('pendingServerSet')
        ..remove('resolutionAttempts')
        ..remove('resolutionError');
      final restored = QueuedBlobUpload.fromMap(map);
      expect(restored.pendingServerSet, isNull);
      expect(restored.resolutionAttempts, 0);
      expect(restored.resolutionError, isNull);
    });

    test('copyWith clearDelivered forces null even when arg is non-null', () {
      final delivered = _base(deliveredAt: 42);
      final cleared = delivered.copyWith(clearDelivered: true);
      expect(cleared.deliveredAt, isNull);
    });
  });
}
