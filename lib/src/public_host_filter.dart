/// Returns `true` when the host portion of [url] is a public-internet
/// address, i.e. not loopback, not private IPv4, not private/link-local
/// IPv6, and not an mDNS `.local` name.
///
/// Used by the connectivity layer to decide whether a connected relay (or
/// reachable Blossom server) is evidence that the device can reach the
/// internet at large.
bool isPublicHost(String url) {
  final uri = Uri.tryParse(url.trim());
  if (uri == null) return false;
  final host = uri.host.toLowerCase();
  if (host.isEmpty) return false;
  if (host == 'localhost') return false;
  if (host.endsWith('.local')) return false;
  if (_isPrivateOrLoopbackIPv4(host)) return false;
  if (_isPrivateOrLoopbackIPv6(host)) return false;
  return true;
}

bool _isPrivateOrLoopbackIPv4(String host) {
  if (!RegExp(r'^\d{1,3}(\.\d{1,3}){3}$').hasMatch(host)) return false;
  final parts = host.split('.').map(int.parse).toList();
  if (parts.any((p) => p > 255)) return false;
  final a = parts[0];
  final b = parts[1];
  if (a == 127) return true;
  if (a == 10) return true;
  if (a == 172 && b >= 16 && b <= 31) return true;
  if (a == 192 && b == 168) return true;
  if (a == 169 && b == 254) return true;
  return false;
}

bool _isPrivateOrLoopbackIPv6(String host) {
  if (!host.contains(':')) return false;
  if (host == '::1' || host == '0:0:0:0:0:0:0:1') return true;
  if (host == '::' || host == '0:0:0:0:0:0:0:0') return true;
  final firstHextet = host.split(':').first;
  if (firstHextet.length >= 3) {
    final prefix = int.tryParse(firstHextet, radix: 16);
    if (prefix != null) {
      if (prefix >= 0xfe80 && prefix <= 0xfebf) return true;
      if (prefix >= 0xfc00 && prefix <= 0xfdff) return true;
    }
  }
  return false;
}
