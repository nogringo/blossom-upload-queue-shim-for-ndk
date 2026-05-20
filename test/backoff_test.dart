import 'dart:math';

import 'package:test/test.dart';

// Reach into the package-private file for direct testing.
import 'package:blossom_upload_queue_shim_for_ndk/src/backoff.dart';

void main() {
  group('computeBackoff', () {
    test('attempts <= 0 returns the initial delay verbatim', () {
      final d = computeBackoff(
        attempts: 0,
        initial: const Duration(seconds: 5),
        max: const Duration(minutes: 30),
      );
      expect(d, const Duration(seconds: 5));
    });

    test('result is always within [initial, max]', () {
      final rng = Random(42);
      const initial = Duration(milliseconds: 100);
      const max = Duration(seconds: 5);
      for (var i = 1; i < 20; i++) {
        final d = computeBackoff(
          attempts: i,
          initial: initial,
          max: max,
          random: rng,
        );
        expect(d.inMilliseconds, greaterThanOrEqualTo(initial.inMilliseconds));
        expect(d.inMilliseconds, lessThanOrEqualTo(max.inMilliseconds));
      }
    });

    test('hits the max ceiling for large attempt counts', () {
      // With initial=1s, max=2s, attempts>=2 always saturates the cap.
      final d = computeBackoff(
        attempts: 30,
        initial: const Duration(seconds: 1),
        max: const Duration(seconds: 2),
        random: Random(1),
      );
      expect(d.inMilliseconds, lessThanOrEqualTo(2000));
      expect(d.inMilliseconds, greaterThanOrEqualTo(1000));
    });
  });
}
