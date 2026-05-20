import 'dart:typed_data';

import 'package:blossom_cache/blossom_cache.dart';
import 'package:blossom_upload_queue_shim_for_ndk/blossom_upload_queue_shim_for_ndk.dart';
import 'package:idb_shim/idb_io.dart';
import 'package:ndk/ndk.dart';
import 'package:sembast/sembast_io.dart';

Future<void> main() async {
  // 1. Open a sembast database for queue metadata. Use sembast_io on
  //    dart:io targets, or sembast_web for the browser.
  final db = await databaseFactoryIo.openDatabase('blossom_uploads.db');

  // 2. Open a Blossom cache for the bytes themselves. Web: idbFactoryBrowser.
  //    Native: idbFactorySembastIo. Tests: newIdbFactoryMemory().
  final cache = await IdbBlossomCache.open(factory: idbFactorySembastIo);

  // 3. Build NDK as usual. The signer is configured on NDK itself. The shim
  //    never signs.
  final ndk = Ndk(
    NdkConfig(eventVerifier: Bip340EventVerifier(), cache: MemCacheManager()),
  );

  // 4. Wrap the Blossom upload use case.
  final outbox = OfflineBlossomUpload.withNdk(ndk, cache: cache, db: db);
  outbox.start();

  // 5. Put the bytes in the cache first. The caller supplies the sha256 (the
  //    cache does not compute it; use any fast hash, including
  //    `crypto.subtle.digest` on web).
  final bytes = Uint8List.fromList('hello blossom'.codeUnits);
  const sha = 'replace-with-the-real-sha256-hex';
  await cache.put(sha, bytes, type: 'text/plain');

  // 6. Schedule the upload. Returns as soon as the queue entry is persisted;
  //    delivery happens in the background and survives restarts.
  await outbox.upload(
    sha256: sha,
    servers: const [
      'https://blossom.primal.net',
      'https://cdn.satellite.earth',
    ],
  );

  // 7. When connectivity comes back, ask for an immediate retry pass.
  await outbox.retryNow();

  // 8. Inspect what's still pending.
  for (final entry in await outbox.listAll()) {
    print(
      'blob ${entry.sha256.substring(0, 8)}… '
      'status=${entry.status.name} '
      'remaining=${entry.remainingServers} '
      'attempts=${entry.attempts}',
    );
  }

  await outbox.dispose();
  await db.close();
  await cache.close();
}
