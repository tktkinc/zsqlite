#!/usr/bin/env node
/** Local browser demo and read-only HTTP exposure of selected sealed stores. */
import { createServer } from 'node:http';
import { createReadStream } from 'node:fs';
import { access, realpath, stat, writeFile } from 'node:fs/promises';
import { dirname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { pipeline } from 'node:stream/promises';
import { attachDebug } from './debug-server.mjs';

const web = dirname(fileURLToPath(import.meta.url));
const objectName = /^[a-f0-9]{64}\.(blob|segment|dict|index)$/;
const assets = new Map([
  ['/', [join(web, 'demo/index.html'), 'text/html; charset=utf-8']],
  ['/app.mjs', [join(web, 'demo/app.mjs'), 'text/javascript; charset=utf-8']],
  ['/style.css', [join(web, 'demo/style.css'), 'text/css; charset=utf-8']],
  ['/debug.js', [join(web, 'demo/debug.js'), 'text/javascript; charset=utf-8']],
  ['/search.mjs', [join(web, 'demo/search.mjs'), 'text/javascript; charset=utf-8']],
]);
for (const name of ['index.mjs', 'worker.mjs', 'http.mjs', 'ranges.mjs', 'query.mjs', 'disk-cache.mjs', 'storage.mjs', 'engine.mjs', 'api.mjs', 'options.mjs', 'zsqlite-browser.mjs', 'zsqlite_browser.wasm']) {
  assets.set(`/wasm/${name}`, [join(web, 'dist', name), name.endsWith('.wasm') ? 'application/wasm' : 'text/javascript; charset=utf-8']);
}

export async function storeDirectory(path) {
  for (const candidate of [path, `${path}.zsqlite.d`, `${path}.d`]) {
    try {
      if (!(await stat(candidate)).isDirectory()) continue;
      await access(join(candidate, 'catalog-head'));
      await access(join(candidate, 'objects'));
      return await realpath(candidate);
    } catch (error) { if (!['ENOENT', 'ENOTDIR'].includes(error.code)) throw error; }
  }
  throw new Error(`No sealed store found at ${path}; provide a .db, .zsqlite or sidecar directory`);
}

export async function startServer({ stores, port = 8080, host = '127.0.0.1', debug = false }) {
  const selected = new Map();
  for (const { id, name = id, path } of stores) {
    if (!/^[a-zA-Z0-9_-]{1,80}$/.test(id) || selected.has(id)) throw new Error(`Invalid or duplicate store ID: ${id}`);
    selected.set(id, { id, name, root: await storeDirectory(resolve(path)) });
  }
  if (selected.size === 0) throw new Error('Select at least one store with --store NAME=PATH');
  await access(join(web, 'dist/zsqlite_browser.wasm'));
  let diagnostics;
  const server = createServer(async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Range, Authorization');
    res.setHeader('Access-Control-Expose-Headers', 'Content-Length, Content-Range, Content-Encoding');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    const fail = (status, message) => {
      if (res.headersSent) { res.destroy(); return; }
      res.writeHead(status, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
      res.end(message);
    };
    try {
      if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
      const path = new URL(req.url, 'http://localhost').pathname;
      if (diagnostics && await diagnostics.handle(req, res, path)) return;
      if (path === '/debug/config') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify({ enabled: false })); return; }
      if (!['GET', 'HEAD'].includes(req.method)) { res.setHeader('Allow', 'GET, HEAD, OPTIONS'); fail(405, 'Read-only server'); return; }
      if (path === '/favicon.ico') { res.writeHead(204); res.end(); return; }
      if (path === '/api/stores') {
        const body = JSON.stringify([...selected.values()].map(({ id, name }) => ({
          id, name, url: `/stores/${id}/`, getUrl: `/get/stores/${id}/`,
        })));
        res.writeHead(200, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body), 'Cache-Control': 'no-store' });
        res.end(req.method === 'HEAD' ? undefined : body);
        return;
      }
      let filename;
      let type;
      let plain = false;
      let immutable = false;
      const asset = assets.get(path);
      if (asset) [filename, type] = asset;
      else {
        const match = /^\/(get\/)?stores\/([a-zA-Z0-9_-]+)\/(catalog-head|objects\/[^/]+)$/.exec(path);
        const store = match && selected.get(match[2]);
        if (!store || (match[3] !== 'catalog-head' && !objectName.test(match[3].slice(8)))) { fail(404, 'Not found'); return; }
        plain = Boolean(match[1]);
        if (plain && req.method === 'HEAD') { res.setHeader('Allow', 'GET, OPTIONS'); fail(405, 'This endpoint supports ordinary GET only'); return; }
        filename = await realpath(join(store.root, match[3]));
        if (!filename.startsWith(`${store.root}${sep}`)) { fail(404, 'Not found'); return; }
        type = 'application/octet-stream';
        immutable = match[3] !== 'catalog-head';
      }
      const info = await stat(filename);
      if (!info.isFile()) { fail(404, 'Not found'); return; }
      res.setHeader('Content-Type', type);
      res.setHeader('Cache-Control', immutable ? 'public, max-age=31536000, immutable' : 'no-store');
      res.setHeader('Accept-Ranges', plain ? 'none' : 'bytes');
      let start = 0;
      let end = info.size - 1;
      let status = 200;
      if (req.headers.range && !plain && req.method === 'GET') {
        const range = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range);
        if (!range || (!range[1] && !range[2])) {
          res.setHeader('Content-Range', `bytes */${info.size}`); fail(416, 'Invalid range'); return;
        }
        if (!range[1]) start = Math.max(0, info.size - Number(range[2]));
        else { start = Number(range[1]); end = range[2] ? Math.min(Number(range[2]), end) : end; }
        if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start > end || end >= info.size) {
          res.setHeader('Content-Range', `bytes */${info.size}`); fail(416, 'Unsatisfiable range'); return;
        }
        res.setHeader('Content-Range', `bytes ${start}-${end}/${info.size}`);
        status = 206;
      }
      res.setHeader('Content-Length', info.size === 0 ? 0 : end - start + 1);
      res.writeHead(status);
      if (req.method === 'HEAD' || info.size === 0) res.end();
      else await pipeline(createReadStream(filename, { start, end }), res);
    } catch (error) {
      if (['ENOENT', 'ENOTDIR'].includes(error.code)) fail(404, 'Not found');
      else if (error.code !== 'ERR_STREAM_PREMATURE_CLOSE') { console.error(error.message); fail(500, 'Could not read store object'); }
    }
  });
  if (debug) diagnostics = server.debug = attachDebug(server);
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => { server.off('error', reject); resolve(); });
  });
  return server;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const stores = [];
    let port = 8080;
    let host = '127.0.0.1';
    let debugFile;
    for (let i = 2; i < process.argv.length; i++) {
      const option = process.argv[i];
      const value = process.argv[++i];
      if (option === '--store' && value) {
        const equals = value.indexOf('=');
        if (equals < 1) throw new Error('--store expects NAME=PATH');
        stores.push({ id: value.slice(0, equals), path: value.slice(equals + 1) });
      } else if (option === '--port' && /^\d+$/.test(value ?? '')) port = Number(value);
      else if (option === '--host' && value) host = value;
      else if (option === '--debug-file' && value) debugFile = value;
      else throw new Error('Usage: node web/serve.mjs --store wikivoyage=/path/archive.db [--port 8080]');
    }
    const server = await startServer({ stores, port, host, debug: Boolean(debugFile) });
    if (debugFile) {
      await writeFile(debugFile, JSON.stringify({ url: `http://${host}:${server.address().port}/`, token: server.debug.token }), { mode: 0o600 });
      console.log(`Browser diagnostics controller: ${debugFile}`);
    }
    console.log(`Knowledge store browser: http://${host}:${server.address().port}/`);
    console.log(`Serving ${stores.map(store => store.id).join(', ')} through /stores/ (ranges) and /get/stores/ (GET only).`);
    for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { server.debug?.close(); server.close(); server.closeAllConnections(); });
  } catch (error) { console.error(error.message); process.exitCode = 1; }
}
