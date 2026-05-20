import 'package:blossom_upload_queue_shim_for_ndk/src/public_host_filter.dart';
import 'package:test/test.dart';

void main() {
  group('isPublicHost', () {
    test('accepts real public hosts (https and wss)', () {
      expect(isPublicHost('https://cdn.example.com'), isTrue);
      expect(isPublicHost('https://blossom.primal.net/upload'), isTrue);
      expect(isPublicHost('wss://relay.damus.io'), isTrue);
    });

    test('rejects loopback and .local', () {
      expect(isPublicHost('http://localhost:3000'), isFalse);
      expect(isPublicHost('https://myserver.local'), isFalse);
      expect(isPublicHost('http://127.0.0.1'), isFalse);
      expect(isPublicHost('http://[::1]'), isFalse);
    });

    test('rejects RFC1918 IPv4', () {
      expect(isPublicHost('http://10.0.0.5'), isFalse);
      expect(isPublicHost('http://172.16.0.1'), isFalse);
      expect(isPublicHost('http://172.31.255.254'), isFalse);
      expect(isPublicHost('http://192.168.1.1'), isFalse);
      expect(isPublicHost('http://169.254.1.1'), isFalse);
    });

    test('accepts publicly routable IPv4 between private ranges', () {
      // 172.32.x.x is OUTSIDE 172.16/12, so public.
      expect(isPublicHost('http://172.32.0.1'), isTrue);
      expect(isPublicHost('http://8.8.8.8'), isTrue);
    });

    test('rejects IPv6 ULA and link-local', () {
      expect(isPublicHost('http://[fc00::1]'), isFalse);
      expect(isPublicHost('http://[fd00::1]'), isFalse);
      expect(isPublicHost('http://[fe80::1]'), isFalse);
    });

    test('rejects garbage / empty', () {
      expect(isPublicHost(''), isFalse);
      expect(isPublicHost('   '), isFalse);
      expect(isPublicHost('not a url'), isFalse);
    });
  });
}
