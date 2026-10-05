# Browser WASM

The browser library is written in strict TypeScript and calls the existing Rust
decoder and bundled SQLite through WASM inside a
dedicated Web Worker. It queries a fixed snapshot of a named head's latest
**finalized seal**, fetching compressed frames as SQLite reads pages. HTTP byte
ranges keep transfers small; ordinary GET-only servers are also supported by
downloading and caching complete compressed objects. Opening authenticates
catalog indexes, manifest chains, dictionaries and pack metadata. Range mode
requests compressed spans and bounded read-ahead instead of whole-pack GETs.
Unsealed active pagefile changes and
WAL-only transactions are outside this snapshot.

Build with Rust 1.94 or later and [Emscripten 4.0.23](https://emscripten.org/docs/getting_started/downloads.html):

```sh
rustup target add wasm32-unknown-emscripten
source /path/to/emsdk/emsdk_env.sh
npm --prefix web ci
bash web/build.sh
```

Serve all files in `web/dist/` together over HTTP(S). Serve `.mjs` files as
JavaScript and `.wasm` as `application/wasm`. No SharedArrayBuffer or cross-origin
isolation headers are needed. SQL runs off the main thread. Emscripten Asyncify
lets the synchronous SQLite VFS suspend while an application storage adapter
awaits I/O. The built-in HTTP adapter uses asynchronous XHR for range batches;
catalog-head reads and object-size probes use worker-local synchronous XHR.
Each connection serializes operations, including concurrent promises from apps.

## Local knowledge search

After building, start the included Node HTTP server with a converted store:

```sh
node web/serve.mjs \
  --store wikivoyage=/path/to/wikivoyage.db \
  --store wikipedia=/path/to/wikipedia.db \
  --port 8080
```

Open <http://127.0.0.1:8080/>. Search opens a results view with 20 hits per page,
highlighted excerpts, source links and navigation. It ranks the entire matching
set before pagination: exact titles first, then title phrases, then other matches.
Within these groups FTS5 BM25 weights title hits 10 times body hits. Quoted
phrases and explicit word prefixes (`hike*`) are supported. If all terms find
nothing, search tries a final-word prefix and then related results matching any
term, clearly labeling the fallback. The imported Porter tokenizer handles
English stemming; Unicode tokenization and accent folding come from the index.
[FTS5 ranking reference](https://www.sqlite.org/fts5.html#the_bm25_function).

The index is contentless, so excerpts are extracted from stored HTML only for
the displayed hits. Excerpts favor windows covering distinct query terms;
highlights use case/accent-insensitive text matches, not FTS token offsets.
Results render as text and safe mark elements, with links to original sources.
There is no alphabetical article catalog or article reader. A read-only SQL
console remains available for other queries and database schemas.

Each search (including count, ranking, preview fetching and excerpt rendering)
shows elapsed time, newly received HTTP body bytes and cached bytes read, using
before/after counter snapshots. Cache reads count repeated reads at each cache
layer; SQLite cache hits are counted as one page-size read each. They are not a
measurement of unique cached data or wire bytes saved. Database statistics show
the logical SQLite size, referenced sealed storage size including the catalog
head, compression ratio, page count and page size. Historical/unreachable bucket
objects are excluded from the sealed size.

The server accepts a logical `.db` path, a direct `.zsqlite` path, or the sealed
sidecar directory. Repeat `--store NAME=PATH` for multiple stores. It exposes
only each selected store's `catalog-head` and immutable `objects/`, along with
the demo and WASM assets. SQL executes in the browser; the server streams files.
It listens on loopback by default and supports `--host` and `--port` overrides.

The page's **Ordinary GET only** option uses `/get/stores/NAME/`, an endpoint
that rejects HEAD and deliberately ignores Range. The normal `/stores/NAME/`
endpoint supports HEAD and byte ranges. Switch transport and click Reconnect
to compare transfers against the same store. Cache settings default to 64 MiB
of memory on mobile devices, 256 MiB on desktop, and 2 GiB of disk, with larger
selectable budgets. Explicit memory selections survive reloads. Disk capacity is
limited by the browser's available storage quota. Preferences survive reloads.
The footer shows current cache sizes and budgets, without splitting object types.
Opening shows aggregate downloaded bytes rather than individual HTTP requests.

The cache budget excludes snapshot metadata and decoder workspace.
`db.stats().wasmMemoryBytes` reports allocated WASM linear memory, including space
retained by its allocator after decoding. View metadata is decoded incrementally
and large manifest transfers use 4 MiB chunks in WASM to bound temporary buffers.
The current format still authenticates the complete manifest before running SQL;
large stores with one frame per page can have substantial opening costs.

For a large text store, the native conversion API can group pages into 64 KiB
frames, reducing manifest size and resolved frame metadata:

```rust
let layout = zsqlite::layout::LayoutPolicy::default()
    .fixed(zsqlite::domain::DecodedBytes::new(64 * 1024))?;
let policy = zsqlite::StoragePolicy::default().with_layout(layout);
zsqlite::convert_to_zsqlite_with_policy("archive.sqlite", "browser.db", policy)?;
```

The logical SQLite data and FTS index stay the same. A page miss decodes a larger
frame, whose verified live pages can then enter the decoded page cache.

## TypeScript SQLite API

The package is `@zsqlite/browser`; install the built `web/` directory locally or
package it with `npm pack` inside `web/`. Generated `.d.mts` declarations accompany
the compiled ES modules. It has no runtime npm dependencies. The demo uses this
same public API, without any knowledge-store logic inside the library.

```ts
import { openDatabase, httpStorage } from '@zsqlite/browser';

const db = await openDatabase({
  storage: httpStorage({
    url: 'https://bucket.example.com/archive/',
    httpMode: 'auto',              // auto, range, or get
    maxObjectBytes: 64 * 1024 ** 2, // largest whole-object HTTP response
    readAheadBytes: 64 * 1024,     // bounded read-ahead for clustered pack reads
    readConcurrency: 4,            // independent HTTP groups per batch
  }),
  head: 'main',                    // optional named head
  cacheBytes: 192 * 1024 ** 2,      // decoded pages in RAM; zero disables
  objectCacheBytes: 64 * 1024 ** 2, // compressed bytes in RAM
  diskCacheBytes: 2 * 1024 ** 3,    // persistent cache; zero disables
});

interface Entry { id: number; title: string }
try {
  const entries = await db.all<Entry>(
    'SELECT id, title FROM entries WHERE title LIKE ? LIMIT 20', ['Chicago%'],
  );
  const stmt = await db.prepare('SELECT id, title FROM entries WHERE id = :id');
  try {
    console.log(await stmt.get<Entry>({ ':id': 42 }));
    console.log(await stmt.get<Entry>({ ':id': 43 })); // reuses the prepared SQL
  } finally { await stmt.finalize(); }
  console.log(entries, await db.stats());
} finally { await db.close(); }
```

`all` returns objects keyed by column name. `get` returns the first object or
`undefined`, and stops stepping as soon as that row is available. Duplicate
column names keep the last value in object rows; `query` returns arrays to retain
all columns. `prepare` exposes `columns`, `query`, `all`, `get`, `run` and
`finalize`. Parameters are rebound for each execution. `exec` and statement
`run` execute one read-only statement, discard its rows and return operation
metrics. They cannot write.
Closing finalizes any remaining prepared statements. `db.readonly` is `true`.
Type parameters describe the expected row shape; they do not validate SQL results
at runtime. `openBucket({ url, ...options })` remains an HTTP convenience wrapper.

For static hosting, import from `/zsqlite/index.mjs` instead of the package name.
Serve the complete `dist/` directory together, including the worker and WASM
assets. Apps can override `workerUrl` when copying those assets to another URL.
Bundlers must retain the worker's ES modules and `zsqlite_browser.wasm`; serving
the built directory and specifying its worker URL also works independently of
the app's bundler. No SQL service or server-side decoder is required.

Each `query` (including a prepared statement's `query`) returns:

```ts
const { columns, rows, metrics } = await db.query(sql, parameters);
console.log(metrics.elapsedMs, metrics.downloadedBytes, metrics.cacheReadBytes);
// metrics also includes requests and cacheHits for this operation.
```

Metrics cover only that operation on the connection, including asynchronous
storage I/O; waiting in the queue and worker message transfer are outside
`elapsedMs`. Connection queries include preparation; prepared executions exclude
the earlier preparation. Counters include all library caches and adapter-reported
cache use. Cached bytes count repeated reads at each cache layer, and downloaded
bytes count newly received payload bytes. `all` and `get` keep their normal row
return values; use `query` for results with metrics. `{ firstRow: true }` stops a
query at the first row while retaining its metrics. `exec` and statement `run`
return the same metrics directly. No separate `stats()` snapshots are needed.

## Application storage

A backend needs just two operations, synchronously or asynchronously:

```ts
import type { StorageAdapter } from '@zsqlite/browser';

interface MyStorage extends StorageAdapter {
  stat(path: string): number | Promise<number>;
  read(path: string, offset: number, length: number):
    Uint8Array | null | Promise<Uint8Array | null>;
}
```

`stat` returns the object size, or `-1` for a missing object. `read` returns the
exact requested byte range, or `null` when missing. The exception is
`catalog-head`: a request starts at zero with an upper bound for `length`, and
returns the complete root record, which can be shorter. Object names are the
existing `catalog-head` and `objects/<digest>.<kind>` paths. They are relative to
the store; the adapter chooses how to retrieve them. Bytes must be the original
stored bytes. The library handles manifests, authentication, decompression,
SQLite, immutable-read caching and decoded-page caching.

Adapters can also implement `readMany(requests)`, where each request has
`{ path, offset, length }`. It returns one exact byte range (or `null` for a
missing object) per request, in input order. It can return an aggregate
`StorageOperation` with that array in `value`; count shared downloads and cache
reads once for the entire batch. Library cache hits are removed before calling
the adapter. Existing adapters with only `read` and `stat` remain supported.
The WASM bridge preserves native batches, with at most 4,096 ranges and 64 MiB
of requested data per batch.

The HTTP adapter sorts cold ranges by object and offset, merges overlaps and
gaps up to `mergeGapBytes` (8 KiB by default), and limits merged spans to
`maxMergedRangeBytes` (256 KiB by default). Independent groups use asynchronous
GETs with at most `readConcurrency` (four by default) in flight. Results retain
the caller's input order. In GET-only mode, each object is downloaded once per
batch. Cached spans serve any enclosed range, including after reopening the
disk cache, within the same total cache budgets.

SQLite often needs one page before it can identify the next, so a storage queue
alone cannot turn every SQL operation into a large batch. For that case,
nearby successive small pack reads trigger bounded, aligned read-ahead using
`readAheadBytes` (64 KiB by default). It needs a known object length and usable
cache capacity, clips reads to the object end, and does not enlarge isolated
reads. Requests larger than one eighth of the window stay exact, since large
frames already contain nearby pages. Set `readAheadBytes: 0` to disable it. Download counters include all
read-ahead and gap bytes, while cache counters record subsequent reuse.

With the default library-managed worker, put the adapter in an ES module that
exports `createStorage(options, context)`. Its TypeScript type is
`StorageFactory<YourOptions>`. The factory may be async, returns a
`StorageAdapter`, and can access the shared optional disk cache through
`context.cache`. Hook it up from the app:

```ts
import { openDatabase, moduleStorage } from '@zsqlite/browser';

const db = await openDatabase({
  storage: moduleStorage(
    new URL('./my-storage.mjs', location.href),
    { prefix: 'my-archive/' }, // structured-cloneable configuration for the factory
    'my-archive-v1',         // optional stable disk-cache scope
  ),
});
```

Serve/build that storage module as a browser ES module. A relative module URL
resolves against the application's document URL. The factory runs in the
library worker, so it can use fetch, a storage SDK, OPFS or application code.
Functions cannot be passed through worker messages; the module keeps those
functions with their implementation. Optional `close()` releases backend
resources when the connection closes. An adapter can report I/O with each
operation instead of maintaining global counters:

```ts
import type { StorageOperation } from '@zsqlite/browser';

// A read served partly by your backend cache and partly by the remote store.
const result: StorageOperation<Uint8Array> = {
  value: bytes,
  downloadedBytes: 1024,
  cacheReadBytes: 4096,
  requests: 1, // use zero for a read entirely from your own cache
};
return result;
```

Both `read` and `stat` accept this form; `stat` puts its numeric size in `value`.
Byte counts and request counts must be nonnegative safe integers. Count all newly
received bytes, including bytes beyond the requested range if the backend fetched
a whole object. Report bytes actually read from the adapter's own caches. The
library adds its RAM/disk/SQLite cache reads and propagates these totals into
per-query metrics. Requests default to one if omitted.

Plain bytes/numeric sizes remain supported. Without per-operation reports or
optional cumulative `stats()`, custom adapters report storage-call counts and
bytes returned by `read`, which can differ from actual network transfers. An
adapter's `stats()` may override its reported cumulative transport counters;
backend counts are included once, alongside the library's cache counters.

For an application that manages its own dedicated worker, supply the object
itself using the `@zsqlite/browser/worker` entry point:

```ts
import { openInWorker } from '@zsqlite/browser/worker';
import type { StorageAdapter } from '@zsqlite/browser';

// Inside your dedicated worker; implement these with your own storage client.
const storage: StorageAdapter = {
  stat: path => client.size(path),
  read: (path, offset, length) => client.readRange(path, offset, length),
};
const db = await openInWorker(storage, { cacheKey: 'my-archive-v1' });
const row = await db.get('SELECT count(*) AS count FROM entries');
```

`client` above is the application's storage client. This entry point does not
spawn another worker. Use a stable `cacheKey` for disk reuse between worker
lifetimes; otherwise a direct adapter gets an isolated random cache namespace.
Terminating an app-owned worker releases its WASM instance after closing SQLite.

`query(sql, parameters, { maxRows: 10000, firstRow: false })` accepts one read-only statement that
returns columns. Array parameters bind positionally; object keys must exactly
match named parameters, including prefixes (for example `{ ':id': 42 }`). Values
may be strings, finite numbers, signed 64-bit BigInts, booleans, null or
Uint8Arrays. Rows are arrays in column order, preserving duplicate column names.
Integers outside JavaScript's safe integer range return BigInt; blobs return
Uint8Array. Exceeding `maxRows` rejects the query; use SQL `LIMIT` for a partial
result. Write statements, ATTACH, PRAGMA setters and multiple statements are rejected.
Read-only introspection such as `PRAGMA table_info(items)` is supported. SQL errors
release transient statements and leave the connection usable. A prepared
statement can be reused after a binding or execution error.

`cacheBytes` covers decoded pages, rounded down to a whole number of pages.
`objectCacheBytes` independently bounds the LRU cache of whole compressed objects
received in GET mode or automatic fallback; zero disables this cache. Objects
larger than its budget are usable up to `maxObjectBytes`, but later reads may
read them again. GET-only mode can transfer most of a store during opening
or full-text search because metadata reads also touch pack files. Prefer ranges
for large stores. The whole-object limit is checked after an unknown-size GET
finishes, so it is not a hard limit on transient download memory.

Cache memory statistics include the decoded-page cache, compressed-object cache
and SQLite's internal page cache. Authenticated metadata and dictionaries,
transient decoding, sorting and query results use additional RAM. Closing
terminates the worker and releases its memory and VFS registration. Open a new
connection to see a newer seal.
Serve `catalog-head` with `Cache-Control: no-cache` (and invalidate any CDN copy
when publishing); immutable objects can use long-lived cache headers.

## Persistent browser cache

The worker uses the origin private file system (OPFS) with synchronous access
handles for a bounded disk cache of downloaded bytes and decoded SQLite pages.
It reuses immutable data between queries, reconnects and browser visits. Root
records are always requested afresh on open. Decoded pages are keyed by manifest
digest and page number; cache hits verify the native page checksum against the
authenticated manifest. A decoded frame admits all live neighbor pages after
verification, as the native cache does. Corrupt records are cache misses.

The disk file is an append log with checksummed records, LRU eviction and bounded
in-place compaction. Flushes occur after queries when stats are collected and on
close. Quota errors, unavailable OPFS, or another tab holding the same cache lock
disable disk caching for that worker; SQL remains usable. Cache namespaces include
the URL (or explicit cacheKey), supplied headers and credential mode. Changing
ports or origins uses a separate browser storage area. `clearCache()` clears the
current disk cache; the memory caches remain usable until the worker closes.

OPFS requires HTTPS or a trusted loopback origin. Normal OPFS storage survives
reloads but remains subject to browser eviction. The demo's Keep on disk button
requests `navigator.storage.persist()` and reports whether the browser grants it.
See [OPFS](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system).

## Bucket layout and publishing

The URL names a directory with the existing filesystem backend layout:

```text
archive/
  catalog-head
  objects/<digest>.index
  objects/<digest>.segment
  objects/<digest>.dict
  objects/<digest>.blob
```

For a local bundle, this is the contents of `archive.db.zsqlite.d/` (or
`archive.zsqlite.d/` for a direct storage path). The browser reads only
`catalog-head` and `objects/`; it does not need the notice database, active file,
backend-id, locks, samples or reader leases. Use `zsqlite flush archive.db` to
seal a closed/checkpointed database, upload immutable objects first, then publish
the matching `catalog-head` last. Copy from a stable snapshot so local maintenance
cannot remove objects while the copy is running.

Browser readers have no remote lease protocol. Keep every object referenced by
a published snapshot available while its browser connections are open. A frozen,
versioned bucket prefix is the straightforward deployment. Local reader leases
do not protect browser readers from remote GC or bucket lifecycle deletion. If
an object disappears or fails authentication, the query fails. A connection
never silently switches to a newer root.

## HTTP and authentication

`httpMode: 'auto'` (default) prefers HEAD and byte ranges. It falls back to a
whole-object GET when HEAD returns 405/501, omits Content-Length, or the server
returns 200 for a range request. `httpMode: 'get'` sends ordinary GETs without
HEAD or Range. Complete objects return 200 and must match Content-Length when
the header is present. `httpMode: 'range'` requires HEAD with Content-Length and
206 responses with the exact requested bytes and accurate Content-Range;
ignored ranges are rejected in this mode.

In every mode, serve stored bytes without HTTP compression (Content-Encoding
absent or identity). The worker validates response lengths and the native
decoder authenticates the stored data.

For a cross-origin bucket, configure CORS with your application's origin:

```json
[
  {
    "AllowedOrigins": ["https://app.example.com"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["Range", "Authorization"],
    "ExposeHeaders": ["Content-Length", "Content-Range", "Content-Encoding"]
  }
]
```

For GET-only mode, only GET needs to be allowed; Range does not need to be an
allowed header. An Authorization header still requires a CORS preflight.

`openBucket` accepts `headers` (for example an Authorization bearer token) and
`withCredentials` for a same-site/cross-origin authenticated HTTP proxy. Credential
requests require explicit origins and credential-enabled CORS. URL prefixes must
be HTTP(S) URLs without query strings or fragments. This release supports public
S3-compatible bucket URLs and authenticated HTTP endpoints; it does not implement
AWS SigV4 or per-object presigned URL generation. Keep AWS secrets on your server.

## Live browser diagnostics

The demo has an optional WebSocket bridge for debugging a browser on another
device. A classic script starts before the app modules, so it captures module
syntax/import errors as well as console warnings, unhandled rejections, app
loading stages and worker HTTP activity. The browser displays its connection ID.

```sh
node web/serve.mjs --store wikivoyage=/path/archive.db --port 8080 \
  --debug-file /private/tmp/zsqlite-debug.json
node web/debug-cli.mjs /private/tmp/zsqlite-debug.json sessions
node web/debug-cli.mjs /private/tmp/zsqlite-debug.json events SESSION_ID
node web/debug-cli.mjs /private/tmp/zsqlite-debug.json snapshot SESSION_ID
node web/debug-cli.mjs /private/tmp/zsqlite-debug.json probe SESSION_ID
```

`stats` retrieves database/cache counters, `query SESSION_ID SQL` runs one
read-only query with a 20-row limit, and `reload` refreshes the connected app.
There is no arbitrary JavaScript evaluation command. The controller token stays
in the local file; it is never delivered to browser clients. Controller endpoints
require that token and WebSocket connections require the page's origin. Event
history is bounded in server memory. Diagnostics are disabled unless the server
is started with `--debug-file`. Stop the server to end the session.

Apps can supply `onProgress(progress)` to `openDatabase` or `openBucket` to observe
disk-cache initialization, WASM loading, snapshot opening and HTTP request/response
stages. It runs on the app thread and does not change the SQL result API.

## Verification

```sh
cd web
npm ci
npm test
npx playwright install chromium
npm run test:browser
```

The browser test creates a real SQLite database, converts it with the native
CLI, serves its compressed objects on another origin, and queries through the
WASM worker in Chromium. It checks lazy range reads, indexed queries, parameter
and result types, write rejection, cache-disabled reads, corruption, missing
objects, strict range failures, GET-only caching, automatic fallback and closing.
It also checks prepared-statement reuse, typed object rows, first-row queries,
concurrent requests, per-operation downloaded/cached byte metrics, adapter I/O
reports, asynchronous application adapters, an application-owned
worker, storage-error recovery, and persistent reuse with custom storage.
Generated declarations are checked against a TypeScript app consumer. It checks
decoded-page disk reuse, native rejection of a tampered cached
page, globally ranked search results, excerpts, highlighting, phrase/prefix
matching, pagination, history navigation, reload metrics, database sizes, SQL
and GET-only reconnect. Unit tests cover disk eviction, torn records, corrupt
records and quota failure. Set `CHROME_BIN` to use an installed Chromium instead
of Playwright's downloaded browser.
