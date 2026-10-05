import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../serve.mjs';

test('server streams selected store objects, supports GET-only and refuses unrelated paths and writes', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'zsqlite-http-'));
  const root = join(directory, 'sample.db.zsqlite.d');
  await mkdir(join(root, 'objects'), { recursive: true });
  await writeFile(join(root, 'catalog-head'), 'root');
  await writeFile(join(root, 'backend-id'), 'private');
  const name = `${'a'.repeat(64)}.blob`;
  await writeFile(join(root, 'objects', name), Uint8Array.from([0, 1, 2, 3, 4, 5]));
  const server = await startServer({ stores: [{ id: 'sample', path: join(directory, 'sample.db') }], port: 0 });
  const base = `http://127.0.0.1:${server.address().port}`;
  try {
    const list = await (await fetch(`${base}/api/stores`)).json();
    assert.deepEqual(list, [{ id: 'sample', name: 'sample', url: '/stores/sample/', getUrl: '/get/stores/sample/' }]);
    const response = await fetch(`${base}/stores/sample/objects/${name}`, { headers: { Range: 'bytes=2-4' } });
    assert.equal(response.status, 206);
    assert.equal(response.headers.get('Content-Range'), 'bytes 2-4/6');
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()), Uint8Array.of(2, 3, 4));
    const head = await fetch(`${base}/stores/sample/objects/${name}`, { method: 'HEAD' });
    assert.equal(head.status, 200);
    assert.equal(head.headers.get('Content-Length'), '6');
    const plain = await fetch(`${base}/get/stores/sample/objects/${name}`, { headers: { Range: 'bytes=2-4' } });
    assert.equal(plain.status, 200);
    assert.deepEqual(new Uint8Array(await plain.arrayBuffer()), Uint8Array.of(0, 1, 2, 3, 4, 5));
    assert.equal((await fetch(`${base}/get/stores/sample/objects/${name}`, { method: 'HEAD' })).status, 405);
    assert.equal((await fetch(`${base}/stores/sample/catalog-head`)).headers.get('Cache-Control'), 'no-store');
    for (const path of ['/stores/sample/backend-id', '/stores/sample/objects/../../backend-id', '/wasm/../../Cargo.toml', '/stores/unknown/catalog-head']) {
      assert.equal((await fetch(`${base}${path}`)).status, 404);
    }
    assert.equal((await fetch(`${base}/stores/sample/catalog-head`, { method: 'PUT', body: 'changed' })).status, 405);
    assert.equal((await fetch(`${base}/stores/sample/objects/${name}`, { headers: { Range: 'bytes=6-7' } })).status, 416);
    assert.match(await (await fetch(`${base}/`)).text(), /Knowledge store/);
    assert.equal((await fetch(`${base}/wasm/zsqlite_browser.wasm`, { method: 'HEAD' })).headers.get('Content-Type'), 'application/wasm');
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});
