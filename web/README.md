# @zsqlite/browser

A read-only SQLite API written in TypeScript, backed by zsqlite's Rust decoder
and bundled SQLite in WASM. Applications provide storage; SQL runs in a dedicated
worker. The package includes generated declarations, worker modules and WASM,
with no runtime npm dependencies.

```ts
import { openDatabase, httpStorage } from '@zsqlite/browser';

const db = await openDatabase({
  storage: httpStorage({ url: 'https://example.com/archive/' }),
  diskCacheBytes: 2 * 1024 ** 3,
});
try {
  const stmt = await db.prepare('SELECT title FROM entries WHERE id=?');
  try {
    console.log(await stmt.get([42]));
    console.log(await stmt.get([43]));
  } finally { await stmt.finalize(); }
} finally { await db.close(); }
```

`query` returns `{columns, rows, metrics}` with rows in column order. `all` returns object
rows, and `get` returns the first row or `undefined`. Both the connection and
prepared statements expose these methods. `exec` and statement `run` discard
returned rows and return metrics. Metrics include elapsed milliseconds, downloaded
bytes, cached bytes read, requests and cache hits for that operation. Use `query`
with `{firstRow: true}` for one row plus metrics. Only one read-only statement is accepted; writes and PRAGMA
setters are rejected. FTS5 queries work when the database has an FTS5 index.
Arrays bind positional parameters; object keys include named prefixes (`:id`).
BigInts preserve SQLite int64 values outside the JavaScript safe integer range;
blobs are Uint8Arrays. Concurrent calls are serialized.

`httpStorage` supports byte ranges, automatic GET fallback and `httpMode: 'get'`
for ordinary HTTP. It reads the existing `catalog-head` and `objects/` layout.
The connection pins a finalized seal. RAM budgets and the optional persistent
OPFS disk budget are configurable; downloaded bytes and decoded pages are cached.
HTTP batches merge nearby ranges and fetch up to four independent groups at a
time. Nearby successive small pack reads use 64 KiB read-ahead; larger frames
stay exact to avoid padding their transfers. Set `readAheadBytes: 0`
in `httpStorage` to disable it. Cache spans serve enclosed reads, and downloaded
byte metrics include all gap/read-ahead bytes.
`stats()` reports database sizes, received bytes, cache sizes, hits and bytes read
from caches. `wasmMemoryBytes` reports allocated WASM linear memory, including
snapshot metadata and decoding workspace, separately from the cache budget.
Large manifests are read in 4 MiB chunks and view metadata is decoded incrementally.
`clearCache()` clears disk data.

For other backends, use `moduleStorage(moduleURL, options, cacheKey)`. That module
exports `createStorage(options, context)` and returns a `StorageAdapter` with
`read(path, offset, length)` and `stat(path)`. Both can return promises. Reads
return exact immutable ranges; the catalog-head read returns the complete root
record up to the requested limit. Missing objects return null/-1. Operations can also return
`{value, downloadedBytes, cacheReadBytes, requests}` to account for backend
caches and actual download sizes; the library includes these in query metrics.
An optional `readMany(requests)` accepts `{path, offset, length}` requests and
returns exact ranges in input order. It can return one aggregate operation
report for shared downloads; the WASM bridge preserves native batches and the
library removes its cache hits first. SQLite often requests dependent pages
sequentially, so not every query read can be collected into one batch.
The library
handles storage authentication, decompression and SQLite. Supply a stable cache
key for reuse across equivalent storage URLs or custom configurations.

Apps owning a dedicated worker can import `openInWorker` from
`@zsqlite/browser/worker` and pass a storage object directly. This uses the same
SQLite API without spawning another worker. Close the database before terminating
the app's worker.

For static hosting, serve all `dist/` files together and import `index.mjs` from
that directory. With a bundler, retain the worker modules and WASM asset, or
serve the built directory and set `workerUrl` explicitly. OPFS needs HTTPS or a
trusted loopback origin.

From the zsqlite repository, install dependencies with `npm ci` in `web/`, activate
Emscripten 4.0.23, then run `npm run build`. The build requires Rust 1.94 or later
and its wasm32-unknown-emscripten target. `npm test` checks TypeScript declarations
and unit tests; `npm run test:browser` checks actual WASM in Chromium. The repository's
`docs/browser.md` covers publishing, CORS, caching and the local search demo.
