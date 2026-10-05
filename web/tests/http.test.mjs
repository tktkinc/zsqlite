import assert from 'node:assert/strict';
import test from 'node:test';
import { HttpBucket } from '../dist/http.mjs';
import { DiskCache } from '../dist/disk-cache.mjs';

const object = `objects/${'a'.repeat(64)}.blob`;
function fixture(responses, options = {}) {
  const requests = [];
  const bucket = new HttpBucket({ url: 'https://bucket.example/snapshot', headers: { Authorization: 'Bearer token' }, ...options }, () => {
    const response = responses.shift();
    assert.ok(response, 'unexpected request');
    return {
      status: response.status ?? 200,
      response: Uint8Array.from(response.bytes ?? []).buffer,
      open(method, url, async) { requests.push({ method, url, async, headers: {} }); },
      setRequestHeader(name, value) { requests.at(-1).headers[name] = value; },
      getResponseHeader(name) { return response.headers?.[name] ?? null; },
      send() { if (requests.at(-1).async) queueMicrotask(() => this.onload()); },
    };
  });
  return { bucket, requests };
}
test('uses HEAD and exact ranges, forwarding auth and caching lengths', () => {
  const { bucket, requests } = fixture([
    { headers: { 'Content-Length': '100' } },
    { status: 206, bytes: [7, 8, 9], headers: { 'Content-Range': 'bytes 12-14/100' } },
  ]);
  assert.equal(bucket.stat(object), 100);
  assert.deepEqual(bucket.read(object, 12, 3), Uint8Array.of(7, 8, 9));
  assert.equal(bucket.stat(object), 100);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].url, `https://bucket.example/snapshot/${object}`);
  assert.equal(requests[1].headers.Range, 'bytes=12-14');
  assert.equal(requests[1].headers.Authorization, 'Bearer token');
  assert.ok(requests.every(r => r.async === false));
});
test('batch reads coalesce nearby spans, preserve input order and reuse enclosing cached ranges', async () => {
  const second = `objects/${'b'.repeat(64)}.blob`;
  const { bucket, requests } = fixture([
    { status: 206, bytes: Array.from({length:14},(_,i)=>i+1), headers: { 'Content-Range': 'bytes 1-14/100' } },
    { status: 206, bytes: [52,53], headers: { 'Content-Range': 'bytes 2-3/50' } },
  ], { mergeGapBytes: 5, maxMergedRangeBytes: 20, readAheadBytes: 0 });
  const values = await bucket.readMany([
    { path: object, offset: 12, length: 3 },
    { path: second, offset: 2, length: 2 },
    { path: object, offset: 1, length: 2 },
    { path: object, offset: 5, length: 2 },
  ]);
  assert.deepEqual(values.map(bytes=>[...bytes]), [[12,13,14],[52,53],[1,2],[5,6]]);
  assert.deepEqual(requests.map(request=>request.headers.Range), ['bytes=1-14','bytes=2-3']);
  assert.ok(requests.every(request=>request.async));
  assert.equal(bucket.stats().bytes,16);
  const cached = await bucket.readMany([{path:object,offset:8,length:4}]);
  assert.deepEqual([...cached[0]],[8,9,10,11]);
  assert.equal(requests.length,2);
  assert.equal(bucket.stats().cacheReadBytes,4);
});
test('clustered single reads trigger bounded read-ahead and clip it to the pack end', async () => {
  const { bucket, requests } = fixture([
    { headers: { 'Content-Length': '30' } },
    { status: 206, bytes: [18,19], headers: { 'Content-Range': 'bytes 18-19/30' } },
    { status: 206, bytes: Array.from({length:14},(_,i)=>i+16), headers: { 'Content-Range': 'bytes 16-29/30' } },
  ], { readAheadBytes: 16, objectCacheBytes:32 });
  assert.equal(bucket.stat(object),30);
  assert.deepEqual([...(await bucket.readMany([{path:object,offset:18,length:2}]))[0]],[18,19]);
  assert.deepEqual([...(await bucket.readMany([{path:object,offset:22,length:2}]))[0]],[22,23]);
  assert.deepEqual([...(await bucket.readMany([{path:object,offset:26,length:2}]))[0]],[26,27]);
  assert.deepEqual(requests.slice(1).map(request=>request.headers.Range),['bytes=18-19','bytes=16-29']);
  assert.equal(bucket.stats().bytes,16);
  assert.ok(bucket.stats().cachedObjectBytes<=32);
});
test('GET-only batches download each object once, and malformed batch replies remain errors', async () => {
  const { bucket, requests } = fixture([{bytes:[0,1,2,3,4,5]}],{httpMode:'get'});
  const values=await bucket.readMany([{path:object,offset:4,length:2},{path:object,offset:1,length:2}]);
  assert.deepEqual(values.map(bytes=>[...bytes]),[[4,5],[1,2]]);
  assert.equal(requests.length,1);
  assert.equal(requests[0].headers.Range,undefined);
  assert.equal(bucket.stats().bytes,6);
  const bad=fixture([{status:206,bytes:[1],headers:{'Content-Range':'bytes 0-1/10'}}]);
  await assert.rejects(bad.bucket.readMany([{path:object,offset:0,length:2}]),/truncated/);
  await assert.rejects(bucket.readMany([{path:object,offset:Number.MAX_SAFE_INTEGER,length:2}]),/Invalid storage read/);
});
test('large neighboring frames stay exact instead of adding read-ahead padding', async () => {
  const length = 24 * 1024;
  const { bucket, requests } = fixture([
    { headers: { 'Content-Length': '262144' } },
    { status: 206, bytes: new Uint8Array(length).fill(1), headers: { 'Content-Range': 'bytes 1000-25575/262144' } },
    { status: 206, bytes: new Uint8Array(length).fill(2), headers: { 'Content-Range': 'bytes 26000-50575/262144' } },
  ], { readAheadBytes: 65536, objectCacheBytes: 131072 });
  assert.equal(bucket.stat(object), 262144);
  assert.equal((await bucket.readMany([{path: object, offset: 1000, length}]))[0][0], 1);
  assert.equal((await bucket.readMany([{path: object, offset: 26000, length}]))[0][0], 2);
  assert.deepEqual(requests.slice(1).map(request => request.headers.Range), ['bytes=1000-25575', 'bytes=26000-50575']);
  assert.equal(bucket.stats().bytes, length * 2);
});
test('batch concurrency is bounded and active I/O drains before a failure returns', async () => {
  let active=0,peak=0,fail=false;
  const bucket=new HttpBucket({url:'https://bucket.example/',readConcurrency:2,objectCacheBytes:0},()=>({
    status:206,response:Uint8Array.of(1,2).buffer,
    open(){},setRequestHeader(){},getResponseHeader(name){return name==='Content-Range'?'bytes 0-1/10':null;},
    send(){active++;peak=Math.max(peak,active);queueMicrotask(()=>{if(fail){this.status=500;fail=false;}active--;this.onload();});},
  }));
  const input=Array.from({length:8},(_,i)=>({path:`objects/${i.toString(16).padStart(64,'0')}.blob`,offset:0,length:2}));
  assert.equal((await bucket.readMany(input)).length,8);
  assert.equal(peak,2);
  fail=true;
  await assert.rejects(bucket.readMany(input),/HTTP 500/);
  assert.equal(active,0);
  assert.deepEqual([...(await bucket.readMany([input[0]]))[0]],[1,2]);
});
test('coalesced ranges persist and a later connection can reuse bytes between the original requests', async () => {
  const file={
    bytes:new Uint8Array(),
    read(out,{at}){const bytes=this.bytes.subarray(at,at+out.length);out.set(bytes);return bytes.length;},
    write(input,{at}){if(at+input.length>this.bytes.length)this.truncate(at+input.length);this.bytes.set(input,at);return input.length;},
    truncate(size){const bytes=new Uint8Array(size);bytes.set(this.bytes.subarray(0,size));this.bytes=bytes;},
    getSize(){return this.bytes.length;},flush(){},close(){},
  };
  const disk=new DiskCache(file,2048);
  const first=fixture([{status:206,bytes:Array.from({length:11},(_,i)=>i+10),headers:{'Content-Range':'bytes 10-20/100'}}],{diskCache:disk,mergeGapBytes:4,readAheadBytes:0});
  await first.bucket.readMany([{path:object,offset:10,length:4},{path:object,offset:18,length:3}]);
  disk.close();
  const second=fixture([],{diskCache:new DiskCache(file,2048),objectCacheBytes:0});
  assert.deepEqual([...(await second.bucket.readMany([{path:object,offset:15,length:2}]))[0]],[15,16]);
  assert.equal(second.requests.length,0);
  assert.equal(second.bucket.stats().bytes,0);
  assert.ok(second.bucket.diskCache.readBytes>=11);
});
test('fixes the catalog root for the lifetime of the connection', () => {
  const { bucket, requests } = fixture([{ bytes: [1, 2, 3] }]);
  assert.deepEqual(bucket.read('catalog-head', 0, 65608), Uint8Array.of(1, 2, 3));
  assert.deepEqual(bucket.read('catalog-head', 0, 65608), Uint8Array.of(1, 2, 3));
  assert.equal(requests.length, 1);
});
test('fails closed on ignored, shifted, truncated or changed ranges', () => {
  for (const response of [
    { bytes: [1, 2, 3] },
    { status: 206, bytes: [1, 2, 3], headers: { 'Content-Range': 'bytes 1-3/100' } },
    { status: 206, bytes: [1, 2], headers: { 'Content-Range': 'bytes 0-2/100' } },
    { status: 206, bytes: [1, 2, 3], headers: { 'Content-Range': 'bytes 0-2/101' } },
    { status: 206, bytes: [1, 2, 3], headers: { 'Content-Range': 'bytes 0-2/100', 'Content-Encoding': 'gzip' } },
    { status: 206, bytes: [1, 2, 3] },
    { status: 404 },
  ]) {
    const { bucket } = fixture([{ headers: { 'Content-Length': '100' } }, response], { httpMode: 'range' });
    bucket.stat(object);
    assert.throws(() => bucket.read(object, 0, 3));
  }
});
test('GET-only mode uses no HEAD or Range and caches whole objects for later reads', () => {
  const { bucket, requests } = fixture([{ bytes: [0, 1, 2, 3, 4, 5], headers: { 'Content-Length': '6' } }], { httpMode: 'get' });
  assert.equal(bucket.stat(object), 6);
  assert.deepEqual(bucket.read(object, 2, 3), Uint8Array.of(2, 3, 4));
  assert.deepEqual(bucket.read(object, 4, 2), Uint8Array.of(4, 5));
  assert.equal(requests.length, 1);
  assert.equal(requests[0].method, 'GET');
  assert.equal(requests[0].headers.Range, undefined);
  assert.equal(bucket.stats().bytes, 6);
  assert.equal(bucket.stats().cachedObjectBytes, 6);
  assert.equal(bucket.stats().objectCacheHits, 2);
});
test('auto falls back when HEAD is unsupported or ranges return whole objects', () => {
  for (const head of [{ status: 405 }, { status: 501 }, { headers: {} }]) {
    const { bucket, requests } = fixture([head, { bytes: [1, 2, 3, 4] }]);
    assert.equal(bucket.stat(object), 4);
    assert.deepEqual(bucket.read(object, 1, 2), Uint8Array.of(2, 3));
    assert.equal(requests.length, 2);
    assert.equal(requests[1].headers.Range, undefined);
  }
  const { bucket, requests } = fixture([{ headers: { 'Content-Length': '4' } }, { bytes: [1, 2, 3, 4] }]);
  bucket.stat(object);
  assert.deepEqual(bucket.read(object, 1, 2), Uint8Array.of(2, 3));
  assert.deepEqual(bucket.read(object, 2, 2), Uint8Array.of(3, 4));
  assert.equal(requests.length, 2);
});
test('whole-object cache evicts old objects within its byte budget', () => {
  const second = `objects/${'b'.repeat(64)}.blob`;
  const { bucket, requests } = fixture([{ bytes: [1, 2, 3] }, { bytes: [4, 5, 6] }, { bytes: [1, 2, 3] }], { httpMode: 'get', objectCacheBytes: 5 });
  assert.deepEqual(bucket.read(object, 0, 1), Uint8Array.of(1));
  assert.deepEqual(bucket.read(second, 0, 1), Uint8Array.of(4));
  assert.deepEqual(bucket.read(object, 0, 1), Uint8Array.of(1));
  assert.equal(requests.length, 3);
  assert.equal(bucket.stats().cachedObjectBytes, 3);
});
test('rejects truncated, oversized or partial whole-object responses', () => {
  for (const response of [
    { bytes: [1, 2, 3], headers: { 'Content-Length': '4' } },
    { bytes: [1, 2, 3], headers: { 'Content-Range': 'bytes 0-2/100' } },
    { bytes: [1, 2, 3, 4, 5] },
    { status: 206, bytes: [1, 2, 3], headers: { 'Content-Range': 'bytes 0-2/100' } },
  ]) {
    const { bucket } = fixture([response], { httpMode: 'get', maxObjectBytes: 4 });
    assert.throws(() => bucket.read(object, 0, 1));
  }
  const { bucket } = fixture([{ headers: { 'Content-Length': '4' } }, { bytes: [1, 2, 3] }]);
  bucket.stat(object);
  assert.throws(() => bucket.read(object, 0, 1), /truncated/);
});
test('rejects oversized roots, unsafe sizes, and traversal before requesting', () => {
  const { bucket, requests } = fixture([]);
  for (const path of ['../secret', 'objects/../../secret', 'https://other.example/object']) {
    assert.throws(() => bucket.read(path, 0, 1));
  }
  assert.throws(() => bucket.read(object, Number.MAX_SAFE_INTEGER, 1));
  assert.throws(() => bucket.read(object, 0, 64 * 1024 * 1024 + 1));
  assert.equal(requests.length, 0);
  const large = fixture([{ bytes: new Array(65609).fill(0) }]);
  assert.throws(() => large.bucket.read('catalog-head', 0, 65608));
});
