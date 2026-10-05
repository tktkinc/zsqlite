// Real browser + real compressed bucket. No cloud account or JS SQLite substitute.
import assert from 'node:assert/strict';
import { chromium } from 'playwright';
import { createServer } from 'node:http';
import { mkdtemp, readFile, stat, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { startServer } from '../serve.mjs';

const repo = fileURLToPath(new URL('../../', import.meta.url));
const scratch = await mkdtemp(join(tmpdir(), 'zsqlite-browser-'));
const source = join(scratch, 'source.db');
const database = join(scratch, 'archive.zsqlite');
function run(command, args) {
  const result = spawnSync(command, args, { cwd: repo, encoding: 'utf8' });
  if (result.status !== 0) throw new Error(`${command} failed: ${result.stderr}\n${result.stdout}`);
}
run('python3', ['-c', `
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
c.execute('CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT, payload BLOB)')
c.executemany('INSERT INTO items VALUES(?,?,?)', ((i, 'item-%04d' % i, (bytes([i % 251]) + b'compressible payload ' * 80)) for i in range(1, 4001)))
c.execute('CREATE INDEX items_label ON items(label)')
c.execute('CREATE VIRTUAL TABLE search USING fts5(body)')
c.executemany('INSERT INTO search(rowid,body) VALUES(?,?)', ((i, 'zsqlite browser entry%d' % i) for i in range(1,4001)))
c.execute('CREATE TABLE archive(title TEXT, description TEXT, date TEXT, entry_count INTEGER)')
c.execute("INSERT INTO archive VALUES('Travel library', 'A small knowledge store', '2026-09-13', 3)")
c.execute('CREATE TABLE entries(id INTEGER PRIMARY KEY, namespace INTEGER, path TEXT, title TEXT, canonical_url TEXT, redirect_to INTEGER, present INTEGER)')
c.executemany('INSERT INTO entries VALUES(?,67,?,?,?,NULL,1)', [(1, 'Chicago', 'Chicago', 'https://en.wikivoyage.org/wiki/Chicago'), (2, 'Jazz', 'Jazz', 'https://en.wikivoyage.org/wiki/Jazz')])
c.execute("INSERT INTO entries VALUES(3,67,'Windy_City','Windy City',NULL,1,1)")
c.execute('CREATE INDEX entries_title ON entries(title COLLATE NOCASE)')
c.execute('CREATE TABLE blobs(id INTEGER PRIMARY KEY, content BLOB)')
c.executemany('INSERT INTO blobs VALUES(?,?)', [(1, b'<main><p>Chicago has blues music.</p><a title="Jazz">Read about jazz</a><script>parent.articleScriptExecuted = true</script><p onclick="parent.articleScriptExecuted = true">Safe text</p></main>'), (2, b'<main><p>Jazz is an American musical tradition.</p></main>')])
c.execute("CREATE VIRTUAL TABLE entry_fts USING fts5(title,body,content='',contentless_delete=1)")
c.executemany('INSERT INTO entry_fts(rowid,title,body) VALUES(?,?,?)', [(1, 'Chicago', 'Chicago has blues music'), (2, 'Jazz', 'An American musical tradition')])
for i in range(4,39):
 title = 'Chicago guide' if i==38 else 'Destination %02d' % i
 body = 'Travel to Chicago and enjoy music. ' * 6
 c.execute('INSERT INTO entries VALUES(?,67,?,?,?,NULL,1)', (i,title.replace(' ','_'),title,'https://example.com/'+str(i)))
 c.execute('INSERT INTO blobs VALUES(?,?)', (i,('<p>'+body+'</p>').encode()))
 c.execute('INSERT INTO entry_fts(rowid,title,body) VALUES(?,?,?)',(i,title,body))
c.commit()
c.close()
`, source]);
run('cargo', ['run', '--quiet', '--release', '--no-default-features', '--features', 'static', '--bin', 'zsqlite', '--', 'convert', source, database]);
const requests = [];
const serve = async (req, res, bucket) => {
  try {
    const path = new URL(req.url, 'http://localhost').pathname;
    if (bucket) {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Range, Authorization');
      res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Content-Encoding');
      requests.push({ method: req.method, path, range: req.headers.range });
      if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
      if (!['GET', 'HEAD'].includes(req.method)) { res.writeHead(405); res.end(); return; }
    }
    if (!bucket && path === '/test-storage.mjs') { res.setHeader('Content-Type', 'text/javascript'); res.end(await readFile(join(repo, 'web/tests/async-storage.mjs'))); return; }
    if (!bucket && path === '/') { res.setHeader('Content-Type', 'text/html'); res.end('<!doctype html><title>zsqlite browser test</title>'); return; }
    const prefix = path.split('/')[1];
    if (prefix === 'missing') { res.writeHead(404); res.end(); return; }
    if (['get-only', 'headless'].includes(prefix) && req.method === 'HEAD') { res.writeHead(405); res.end(); return; }
    const suffix = path.slice(prefix.length + 2);
    const root = bucket ? `${database}.d` : resolve(repo, 'web/dist');
    const filename = resolve(root, bucket ? suffix : path.slice(1));
    if (!filename.startsWith(`${resolve(root)}/`)) { res.writeHead(403); res.end(); return; }
    const info = await stat(filename);
    res.setHeader('Content-Type', filename.endsWith('.wasm') ? 'application/wasm' : filename.endsWith('.mjs') ? 'text/javascript' : 'application/octet-stream');
    if (req.method === 'HEAD') { res.setHeader('Content-Length', info.size); res.end(); return; }
    const bytes = await readFile(filename);
    if (prefix === 'corrupt' && filename.endsWith('.blob')) bytes.fill(0);
    if (prefix === 'bad-root' && filename.endsWith('catalog-head')) bytes[0] ^= 1;
    const range = /^bytes=(\d+)-(\d+)$/.exec(req.headers.range ?? '');
    if (range && !['no-range', 'get-only', 'headless'].includes(prefix)) {
      const start = Number(range[1]);
      const end = Number(range[2]);
      if (end >= bytes.length || start > end) { res.writeHead(416); res.end(); return; }
      const slice = bytes.subarray(start, end + 1);
      res.setHeader('Content-Range', `bytes ${start}-${end}/${bytes.length}`);
      res.setHeader('Content-Length', slice.length);
      res.writeHead(206);
      res.end(slice);
    } else { res.setHeader('Content-Length', bytes.length); res.end(bytes); }
  } catch (error) { res.writeHead(error.code === 'ENOENT' ? 404 : 500); res.end(); }
};
const app = createServer((req, res) => { void serve(req, res, false); });
const bucket = createServer((req, res) => { void serve(req, res, true); });
await Promise.all([new Promise(r => app.listen(0, '127.0.0.1', r)), new Promise(r => bucket.listen(0, '127.0.0.1', r))]);
const demo = await startServer({ stores: [{ id: 'knowledge', path: database }], port: 0 });
let browser;
try {
  browser = await chromium.launch({ headless: true, ...(process.env.CHROME_BIN ? { executablePath: process.env.CHROME_BIN } : {}) });
  const page = await browser.newPage();
  page.on('pageerror', error => console.error('Browser error:', error));
  page.on('console', message => { if (message.type() === 'error') console.error('Browser console:', message.text()); });
  await page.goto(`http://127.0.0.1:${app.address().port}/`);
  const bucketUrl = `http://127.0.0.1:${bucket.address().port}/bucket/`;
  const result = await page.evaluate(async (url) => {
    const { openBucket } = await import('/index.mjs');
    const db = await openBucket({ url, cacheBytes: 32768, headers: { Authorization: 'Bearer browser-test' } });
    const opening = await db.stats();
    const point = await db.query('SELECT label FROM items WHERE id = ?', [3999]);
    const schema = await db.query("SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'items'");
    const count = await db.query('SELECT count(*) AS n FROM items');
    const info = await db.query('PRAGMA table_info(items)');
    const fts = await db.query('SELECT rowid FROM search WHERE search MATCH ?', ['entry3999']);
    const types = await db.query("SELECT ? AS big, ? AS blob, ? AS text, ? AS nil, ? AS real", [9223372036854775807n, new Uint8Array([0, 1, 255]), 'a\0雪', null, 1.25]);
    const named = await db.query('SELECT label FROM items WHERE id = :id', { ':id': 42 });
    const empty = await db.query('SELECT ? AS empty', [new Uint8Array()]);
    const explain = await db.query('EXPLAIN QUERY PLAN SELECT * FROM items WHERE label = ?', ['item-0042']);
    const failures = [];
    for (const sql of ['INSERT INTO items VALUES(5000,\'x\',NULL)', 'DELETE FROM items', 'CREATE TABLE nope(x)', 'ATTACH \'elsewhere\' AS other', 'PRAGMA query_only=OFF', 'PRAGMA temp_store=FILE', 'SELECT 1; SELECT 2', 'SELECT * FROM items LIMIT 3']) {
      try { await db.query(sql, [], { maxRows: 2 }); failures.push(null); }
      catch (error) { failures.push(error.message); }
    }
    // Failed queries must release every statement and leave the worker usable.
    const after = await db.query('SELECT label FROM items WHERE id = ?', [1]);
    const queryOnly = await db.query('PRAGMA query_only');
    const tempStore = await db.query('PRAGMA temp_store');
    const stats = await db.stats();
    await db.close();
    await db.close();
    let closed;
    try { await db.query('SELECT 1'); } catch (error) { closed = error.message; }
    const corrupt = [];
    for (const prefix of ['corrupt', 'bad-root', 'no-range', 'missing']) {
      let bad;
      try { bad = await openBucket({ url: url.replace('/bucket/', `/${prefix}/`), httpMode: 'range' }); await bad.query('SELECT * FROM items LIMIT 1'); corrupt.push(null); }
      catch (error) { corrupt.push(error.message); }
      finally { if (bad) await bad.close(); }
    }
    try { await openBucket({ url, head: 'missing-head' }); corrupt.push(null); }
    catch (error) { corrupt.push(error.message); }
    const uncached = await openBucket({ url, cacheBytes: 0, objectCacheBytes: 0, diskCacheBytes: 0 });
    const uncachedRow = await uncached.query('SELECT label FROM items WHERE id = 123');
    await uncached.close();
    const transports = [];
    for (const [prefix, httpMode] of [['get-only', 'get'], ['no-range', 'auto'], ['headless', 'auto']]) {
      const connection = await openBucket({ url: url.replace('/bucket/', `/${prefix}/`), httpMode, cacheBytes: 0 });
      try {
        const row = await connection.query('SELECT label FROM items WHERE id = ?', [3999]);
        await connection.query('SELECT label FROM items WHERE id = ?', [3999]);
        transports.push({ row, stats: await connection.stats() });
      } finally { await connection.close(); }
    }
    return { opening, point, schema, count, info, fts, types, named, empty, explain, failures, after, queryOnly, tempStore, stats, closed, corrupt, uncachedRow, transports };
  }, bucketUrl);
  assert.deepEqual(result.point.rows, [['item-3999']]);
  assert.ok(result.point.metrics.downloadedBytes > 0 && result.point.metrics.elapsedMs >= 0);
  assert.deepEqual(result.schema.rows, [['items']]);
  assert.deepEqual(result.count.rows, [[4000]]);
  assert.deepEqual(result.info.rows.map(row => row[1]), ['id', 'label', 'payload']);
  assert.deepEqual(result.fts.rows, [[3999]]);
  assert.deepEqual(result.types.rows, [[9223372036854775807n, Uint8Array.of(0, 1, 255), 'a\0雪', null, 1.25]]);
  assert.deepEqual(result.named.rows, [['item-0042']]);
  assert.deepEqual(result.empty.rows, [[new Uint8Array()]]);
  assert.ok(result.explain.rows.some(row => row.some(value => typeof value === 'string' && value.includes('items_label'))));
  assert.ok(result.failures.every(Boolean), JSON.stringify(result.failures));
  assert.ok(result.corrupt.every(Boolean), JSON.stringify(result.corrupt));
  assert.deepEqual(result.after.rows, [['item-0001']]);
  assert.deepEqual(result.queryOnly.rows, [[1]]);
  assert.deepEqual(result.tempStore.rows, [[2]]);
  assert.deepEqual(result.uncachedRow.rows, [['item-0123']]);
  assert.match(result.closed, /closed/);
  assert.ok(requests.every(r => ['GET', 'HEAD', 'OPTIONS'].includes(r.method)));
  assert.ok(requests.some(r => r.range && r.path.endsWith('.blob')));
  assert.ok(requests.filter(r => r.path.startsWith('/get-only/')).every(r => r.method !== 'HEAD' && !r.range));
  for (const { row, stats } of result.transports) {
    assert.deepEqual(row.rows, [['item-3999']]);
    assert.ok(stats.fullObjectRequests > 0 && stats.objectCacheHits > 0 && stats.cachedObjectBytes > 0);
  }
  assert.equal(result.transports[0].stats.rangeRequests, 0);
  const totalBlobBytes = (await import('node:fs/promises')).readdir(`${database}.d/objects`).then(async names =>
    (await Promise.all(names.filter(name => name.endsWith('.blob')).map(async name => (await stat(`${database}.d/objects/${name}`)).size))).reduce((a, b) => a + b, 0));
  assert.ok(result.opening.bytes < await totalBlobBytes, 'opening must not download all blobs');
  console.log(`Browser WASM passed: 4,000 rows, CORS ranges, GET-only, automatic fallback, values, write rejection, corruption and lifecycle (${result.stats.requests} requests, ${result.stats.bytes} bytes).`);

  const api = await page.evaluate(async (url) => {
    const { openDatabase, httpStorage, moduleStorage } = await import('/index.mjs');
    const db = await openDatabase({ storage: httpStorage({ url }), diskCacheBytes: 0 });
    const stmt = await db.prepare('SELECT id, label FROM items WHERE id = :id');
    const first = await stmt.get({ ':id': 42 });
    const second = await stmt.all({ ':id': 43 });
    const tuples = await stmt.query({ ':id': 44 });
    const runMetrics = await stmt.run({ ':id': 45 });
    let bindingError;
    try { await stmt.all({ id: 1 }); } catch (error) { bindingError = error.message; }
    const recovered = await stmt.get({ ':id': 46 });
    const one = await db.get('SELECT id FROM items ORDER BY id');
    const noRow = await db.get('SELECT id FROM items WHERE id=-1');
    const duplicate = await db.get('SELECT 1 AS x, 2 AS x');
    const large = await db.get('SELECT ? AS large, ? AS bytes', [9223372036854775807n, Uint8Array.of(0,255)]);
    const execMetrics = await db.exec('SELECT id FROM items');
    const firstWithMetrics = await db.query('SELECT id FROM items ORDER BY id', [], { firstRow: true, maxRows: 1 });
    const concurrent = await Promise.all(Array.from({ length: 8 }, (_, i) => db.get('SELECT label FROM items WHERE id=?', [i+1])));
    const rejected = [];
    for (const sql of ['DELETE FROM items', 'PRAGMA cache_size=1', 'SELECT 1; SELECT 2']) {
      try { await db.prepare(sql); rejected.push(false); } catch { rejected.push(true); }
    }
    await stmt.finalize(); await stmt.finalize();
    let finalized;
    try { await stmt.get({ ':id': 1 }); } catch (error) { finalized = error.message; }
    await db.prepare('SELECT * FROM items'); // close must release unfinalized statements.
    await db.close();
    const asyncDb = await openDatabase({ storage: moduleStorage(new URL('/test-storage.mjs', location.href), { url, batch: true }),
      cacheBytes: 0, objectCacheBytes: 0, diskCacheBytes: 0 });
    const asyncRows = await Promise.all([asyncDb.get('SELECT label FROM items WHERE id=3999'), asyncDb.get('SELECT count(*) AS n FROM items'), asyncDb.get('SELECT rowid FROM search WHERE search MATCH ?', ['entry3999'])]);
    const asyncStats = await asyncDb.stats();
    await asyncDb.close();
    return { readonly: db.readonly, first, second, tuples, bindingError, recovered, one, noRow, duplicate, large, concurrent, rejected, finalized, asyncRows, asyncStats, runMetrics, execMetrics, firstWithMetrics };
  }, bucketUrl);
  assert.equal(api.readonly, true);
  assert.deepEqual(api.first, { id: 42, label: 'item-0042' });
  assert.deepEqual(api.second, [{ id: 43, label: 'item-0043' }]);
  assert.deepEqual(api.tuples.columns, ['id','label']);
  assert.deepEqual(api.tuples.rows, [[44,'item-0044']]);
  for (const metrics of [api.tuples.metrics, api.runMetrics, api.execMetrics, api.firstWithMetrics.metrics]) {
    assert.ok(metrics.elapsedMs >= 0 && metrics.downloadedBytes >= 0 && metrics.cacheReadBytes >= 0);
  }
  assert.deepEqual(api.firstWithMetrics.rows, [[1]]);
  assert.ok(api.execMetrics.cacheReadBytes > 0);
  assert.match(api.bindingError, /Named parameters/);
  assert.deepEqual(api.recovered, { id: 46, label: 'item-0046' });
  assert.deepEqual(api.one, { id: 1 });
  assert.equal(api.noRow, undefined);
  assert.deepEqual(api.duplicate, { x: 2 });
  assert.deepEqual(api.large, { large: 9223372036854775807n, bytes: Uint8Array.of(0,255) });
  assert.deepEqual(api.concurrent.map(row=>row.label), Array.from({length:8},(_,i)=>`item-${String(i+1).padStart(4,'0')}`));
  assert.ok(api.rejected.every(Boolean));
  assert.match(api.finalized, /finalized/);
  assert.deepEqual(api.asyncRows, [{label:'item-3999'}, {n:4000}, {rowid:3999}]);
  assert.ok(api.asyncStats.requests>0 && api.asyncStats.bytes>0 && api.asyncStats.cacheReadBytes>0);
  console.log('TypeScript SQLite API passed: prepared reuse, object rows, get/all/exec, concurrent requests, lifecycle and asynchronous application storage through WASM.');

  const ownedWorker = await page.evaluate(async (url) => {
    const code = `import {openInWorker} from ${JSON.stringify(new URL('/engine.mjs', location.href).href)};
      import {createStorage} from ${JSON.stringify(new URL('/test-storage.mjs', location.href).href)};
      self.onmessage=async ({data:url})=>{
        let db;
        try {
          const adapter=createStorage({url});let failNext=false;const read=adapter.read;
          adapter.read=async (...args)=>{if(failNext){failNext=false;throw new Error('simulated asynchronous storage failure');}return read(...args);};
          const options={cacheBytes:0,objectCacheBytes:0,diskCacheBytes:16*1024**2,cacheKey:'owned-worker-test'};
          db=await openInWorker(adapter,options);failNext=true;
          let error;try{await db.get('SELECT label FROM items WHERE id=3999');}catch(e){error=e.message;}
          const row=await db.get('SELECT label FROM items WHERE id=3999');
          const stmt=await db.prepare('SELECT id FROM items ORDER BY id');const first=await stmt.get();
          await db.close();await stmt.finalize();
          db=await openInWorker(createStorage({url}),options);
          const opening=await db.stats();const warmRow=await db.get('SELECT label FROM items WHERE id=3999');const warm=await db.stats();
          await db.close();db=undefined;self.postMessage({row,first,error,warmRow,opening,warm});
        }catch(e){await db?.close();self.postMessage({failure:e.message});}
      };`;
    const workerUrl = URL.createObjectURL(new Blob([code], { type: 'text/javascript' }));
    const worker = new Worker(workerUrl, { type: 'module' });
    try {
      return await new Promise((resolve, reject) => {
        worker.onmessage = event => resolve(event.data);
        worker.onerror = event => reject(new Error(event.message));
        worker.postMessage(url);
      });
    } finally { worker.terminate(); URL.revokeObjectURL(workerUrl); }
  }, bucketUrl);
  assert.equal(ownedWorker.failure, undefined);
  assert.match(ownedWorker.error, /simulated asynchronous storage failure/);
  assert.deepEqual(ownedWorker.row, { label: 'item-3999' });
  assert.deepEqual(ownedWorker.first, { id: 1 });
  assert.deepEqual(ownedWorker.warmRow, { label: 'item-3999' });
  assert.equal(ownedWorker.warm.bytes, ownedWorker.opening.bytes);
  assert.ok(ownedWorker.warm.diskCacheAvailable && ownedWorker.warm.diskCacheHits > 0);
  console.log('Application-owned worker passed: direct async adapter, storage-error recovery, statement cleanup and persistent cache reuse.');

  const persistence = await page.evaluate(async (url) => {
    const { openBucket } = await import('/index.mjs');
    const options = { url, cacheBytes: 0, objectCacheBytes: 0, diskCacheBytes: 16 * 1024 ** 2 };
    let db = await openBucket(options);
    await db.query('SELECT label FROM items WHERE id=3999');
    const cold = await db.stats();
    await db.close();
    // Alter a decoded page and repair its local CRC. The native page checksum
    // must still reject it and fetch/redecode authenticated source bytes.
    const utility = `import {openDiskCache,checksum} from ${JSON.stringify(new URL('/disk-cache.mjs', location.href).href)};
      self.onmessage=async ({data:url})=>{
        const {cache}=await openDiskCache({url,diskCacheBytes:16*1024**2});
        try {
          const [key,entry]=[...cache.entries].find(([key])=>key.startsWith('p:')&&key.endsWith(':1'));
          const bytes=cache.get(key); bytes[0]^=1;
          cache.write(bytes,entry.at+20+entry.keySize);
          const crc=new Uint8Array(4);new DataView(crc.buffer).setUint32(0,checksum(bytes),true);
          cache.write(crc,entry.at+12);cache.flush();self.postMessage(key);
        }finally{cache.close();}
      };`;
    const workerUrl = URL.createObjectURL(new Blob([utility], { type: 'text/javascript' }));
    const worker = new Worker(workerUrl, { type: 'module' });
    const changed = await new Promise((resolve, reject) => {
      worker.onmessage = event => resolve(event.data);
      worker.onerror = event => reject(new Error(event.message));
      worker.postMessage(url);
    });
    worker.terminate(); URL.revokeObjectURL(workerUrl);
    db = await openBucket(options);
    const opening = await db.stats();
    const row = await db.query('SELECT label FROM items WHERE id=3999');
    const warm = await db.stats();
    await db.close();
    return { cold, opening, warm, row, changed };
  }, bucketUrl);
  assert.ok(persistence.cold.diskCacheAvailable && persistence.cold.diskCacheBytes > 0);
  assert.match(persistence.changed, /^p:/);
  assert.deepEqual(persistence.row.rows, [['item-3999']]);
  assert.equal(persistence.warm.bytes, persistence.opening.bytes);
  assert.equal(persistence.row.metrics.downloadedBytes, 0);
  assert.ok(persistence.row.metrics.cacheReadBytes > 0);
  assert.ok(persistence.warm.diskCacheHits > 0 && persistence.warm.cacheReadBytes > 0);
  console.log('Persistent decoded cache passed: reload reuse and native rejection of a tampered page with a valid local CRC.');

  // Exercise the user-facing page against the same real sealed database.
  await page.goto(`http://127.0.0.1:${demo.address().port}/`);
  const idle = () => page.waitForFunction(() => !document.getElementById('connect').disabled);
  await idle();
  assert.match(await page.locator('#connection-state').textContent(), /Connected/);
  assert.equal(await page.locator('#error').isHidden(), true);
  assert.equal(await page.locator('#store-title').textContent(), 'Travel library');
  await page.locator('#search').fill('blues');
  await page.locator('#search-button').click();
  await idle();
  assert.equal(await page.locator('.search-result').count(), 1);
  assert.equal(await page.locator('.search-result h3').textContent(), 'Chicago');
  assert.match(await page.locator('.result-snippet').textContent(), /Chicago has blues music/);
  assert.equal(await page.locator('.result-snippet mark').textContent(), 'blues');
  assert.equal(await page.locator('iframe, .article-list').count(), 0);
  assert.equal(await page.locator('.search-result script').count(), 0);
  assert.equal(await page.evaluate(() => window.articleScriptExecuted), undefined);
  await page.locator('#search').fill('Chicago');
  await page.locator('#search-button').click();
  await idle();
  assert.equal(await page.locator('.search-result h3').first().textContent(), 'Chicago');
  assert.equal(await page.locator('.search-result h3').nth(1).textContent(), 'Chicago guide');
  assert.equal(await page.locator('.search-result').count(), 20);
  assert.match(await page.locator('#results-summary').textContent(), /36 results/);
  assert.match(await page.locator('#search-time').textContent(), /ms search time/);
  assert.match(await page.locator('#search-downloaded').textContent(), /downloaded/);
  assert.match(await page.locator('#search-cached').textContent(), /cached data read/);
  assert.match(await page.locator('.search-result h3 a').first().getAttribute('href'), /wiki\/Chicago/);
  await page.locator('#next-page').click();
  await idle();
  assert.equal(await page.locator('.search-result').count(), 16);
  assert.match(await page.locator('#page-label').textContent(), /21–36/);
  await page.goBack();
  await idle();
  await page.waitForFunction(() => document.getElementById('page-label').textContent.startsWith('1–20'));
  await page.reload();
  await idle();
  assert.equal(await page.locator('#error').isHidden(), true);
  assert.equal(await page.locator('#search-downloaded').textContent(), '0 B downloaded');
  assert.match(await page.locator('#disk-cache').textContent(), /disk/);
  assert.match(await page.locator('#db-compressed').textContent(), /[KM]iB/);
  assert.match(await page.locator('#db-logical').textContent(), /[KM]iB/);
  await page.locator('#sql-console > summary').click();
  await page.locator('#sql').fill('SELECT title FROM archive');
  await page.locator('#run').click();
  await idle();
  assert.equal(await page.locator('#query-result td').textContent(), 'Travel library');
  await page.locator('#sql').fill('DELETE FROM entries');
  await page.locator('#run').click();
  await idle();
  assert.equal(await page.locator('#error').isVisible(), true);
  await page.locator('#sql').fill('SELECT title FROM entries WHERE id=3');
  await page.locator('#run').click();
  await idle();
  assert.equal(await page.locator('#query-result td').textContent(), 'Windy City');
  await page.locator('#transport').selectOption('get');
  await page.locator('#connect').click();
  await idle();
  assert.equal(await page.locator('#error').isHidden(), true);
  assert.match(await page.locator('#connection-state').textContent(), /Connected · GET only/);
  await page.locator('#search').fill('blues');
  await page.locator('#search-button').click();
  await idle();
  assert.equal(await page.locator('.search-result h3').textContent(), 'Chicago');
  assert.equal(await page.locator('#error').isHidden(), true);
  await page.locator('#search').fill('chicag');
  await page.locator('#search-button').click();
  await idle();
  assert.match(await page.locator('#results-summary').textContent(), /prefix matches/);
  await page.locator('#search').fill('"blues music"');
  await page.locator('#search-button').click();
  await idle();
  assert.equal(await page.locator('.search-result h3').textContent(), 'Chicago');
  await page.locator('#search').fill('jazz nonexistentterm');
  await page.locator('#search-button').click();
  await idle();
  assert.match(await page.locator('#results-summary').textContent(), /Related results/);
  await page.locator('#search').fill('nonexistentterm');
  await page.locator('#search-button').click();
  await idle();
  assert.match(await page.locator('.no-results').textContent(), /No matches/);
  assert.equal(await page.locator('#error').isHidden(), true);
  console.log('Knowledge search passed: global ranking, snippets, highlighting, phrase/prefix search, pagination, navigation, reload, cache metrics, database sizes, SQL and GET-only reconnect.');
} finally {
  if (browser) await browser.close();
  for (const server of [app, bucket, demo]) server.closeAllConnections();
  await Promise.all([app, bucket, demo].map(server => new Promise(r => server.close(r))));
  await rm(scratch, { recursive: true, force: true });
}
