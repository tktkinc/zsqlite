// Linked into Emscripten; Asyncify keeps the native call suspended during asynchronous reads.
addToLibrary({
  $zsqlite_page_key: function(manifest, page) {
    var hex = '';
    for (var i = 0; i < 32; i++) hex += HEAPU8[manifest + i].toString(16).padStart(2, '0');
    return 'p:' + hex + ':' + page;
  },
  zsqlite_page_cache_get__deps: ['$zsqlite_page_key'],
  zsqlite_page_cache_get: function(manifest, page, size, output) {
    var bytes = Module['diskCache']?.get(zsqlite_page_key(manifest, page));
    if (!bytes || bytes.length !== size) return -1;
    HEAPU8.set(bytes, output);
    return bytes.length;
  },
  zsqlite_page_cache_put__deps: ['$zsqlite_page_key'],
  zsqlite_page_cache_put: function(manifest, page, size, input) {
    Module['diskCache']?.put(zsqlite_page_key(manifest, page), HEAPU8.subarray(input, input + size));
  },
  zsqlite_page_cache_remove__deps: ['$zsqlite_page_key'],
  zsqlite_page_cache_remove: function(manifest, page) {
    Module['diskCache']?.delete(zsqlite_page_key(manifest, page));
  },
  zsqlite_page_cache_status: function(bytes, hit, pageSize) {
    Module['pageCacheBytes'] = bytes;
    Module['pageCacheHits'] = (Module['pageCacheHits'] || 0) + hit;
    Module['pageCacheReadBytes'] = (Module['pageCacheReadBytes'] || 0) + hit * pageSize;
  },
  zsqlite_snapshot_info: function(logical, sealed, pageSize, pages) {
    Module['snapshotStats'] = { logicalBytes: logical, sealedBytes: sealed, pageSize: pageSize, pages: pages };
  },
  zsqlite_http_read__deps: ['$Asyncify'],
  zsqlite_http_read: function(path, offset, length, output) {
    return Asyncify.handleSleep(function(wake) {
      var fail = function(error) {
        Module['bucketTransport'].lastError = error instanceof Error ? error.message : String(error);
        wake(-1);
      };
      var done = function(bytes) {
        try {
          if (!(bytes instanceof Uint8Array) || bytes.length > length) throw new Error('Invalid storage response');
          // The heap may have grown while the asynchronous read was pending.
          HEAPU8.set(bytes, output);
          wake(bytes.length);
        } catch (error) { fail(error); }
      };
      try {
        var result = Module['bucketTransport'].read(UTF8ToString(path), offset, length);
        if (result && typeof result.then === 'function') result.then(done, fail);
        else done(result);
      } catch (error) { fail(error); }
    });
  },
  zsqlite_http_stat__deps: ['$Asyncify'],
  zsqlite_http_read_many__deps: ['$Asyncify'],
  zsqlite_http_read_many: function(requests, length, output) {
    return Asyncify.handleSleep(function(wake) {
      var fail = function(error) {
        Module['bucketTransport'].lastError = error instanceof Error ? error.message : String(error);
        wake(-1);
      };
      try {
        var reads = JSON.parse(UTF8ToString(requests));
        var transport = Module['bucketTransport'];
        var result = transport.readMany ? transport.readMany(reads) : Promise.all(reads.map(function(read) { return transport.read(read.path, read.offset, read.length); }));
        Promise.resolve(result).then(function(values) {
          try {
            if (!Array.isArray(values) || values.length !== reads.length) throw new Error('Invalid storage batch response');
            var total = 0;
            for (var i = 0; i < values.length; i++) {
              if (!(values[i] instanceof Uint8Array) || values[i].length !== reads[i].length) throw new Error('Invalid storage batch range');
              total += values[i].length;
            }
            if (total !== length) throw new Error('Invalid storage batch size');
            var position = output;
            for (var i = 0; i < values.length; i++) {
              HEAPU8.set(values[i], position);
              position += values[i].length;
            }
            wake(total);
          } catch (error) { fail(error); }
        }, fail);
      } catch (error) { fail(error); }
    });
  },
  zsqlite_http_stat: function(path) {
    return Asyncify.handleSleep(function(wake) {
      var fail = function(error) {
        Module['bucketTransport'].lastError = error instanceof Error ? error.message : String(error);
        wake(-2);
      };
      try {
        var result = Module['bucketTransport'].stat(UTF8ToString(path));
        if (result && typeof result.then === 'function') result.then(wake, fail);
        else wake(result);
      } catch (error) { fail(error); }
    });
  },
});
