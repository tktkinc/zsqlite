import assert from 'node:assert/strict';
import { test } from 'node:test';
import { AdapterStorage } from '../dist/storage.mjs';
const path = `objects/${'a'.repeat(64)}.blob`;
test('application operation reports count cold and cached bytes without counting backend cache hits as requests', async () => {
  let reads = 0;
  const storage = new AdapterStorage({
    async stat() { return { value: 16, downloadedBytes: 50, cacheReadBytes: 0, requests: 1 }; },
    async read() { return { value: Uint8Array.of(1,2,3,4), downloadedBytes: reads++ ? 0 : 12, cacheReadBytes: reads === 1 ? 0 : 4, requests: reads === 1 ? 1 : 0 }; },
  }, null, 0);
  assert.equal(await storage.stat(path), 16);
  await storage.read(path, 0, 4);
  await storage.read(path, 4, 4);
  assert.equal(storage.stats().bytes, 62);
  assert.equal(storage.stats().cacheReadBytes, 4);
  assert.equal(storage.stats().requests, 2);
  assert.equal(storage.stats().objectCacheHits, 1);
});
test('cumulative backend counters override operation reports while library cache reads still count', async () => {
  const storage = new AdapterStorage({
    stat: () => 4,
    read: () => ({ value: Uint8Array.of(1,2,3,4), downloadedBytes: 4, cacheReadBytes: 10 }),
    stats: () => ({ bytes: 4, requests: 1, cacheReadBytes: 10 }),
  }, null, 4);
  await storage.read(path, 0, 4);
  await storage.read(path, 0, 4);
  assert.equal(storage.stats().bytes, 4);
  assert.equal(storage.stats().cacheReadBytes, 14);
  assert.equal(storage.stats().requests, 1);
});
test('application batches receive only misses in order and report shared download/cache work once', async () => {
  const batches=[];
  const storage=new AdapterStorage({
    stat:()=>100,
    read:()=>{throw new Error('batch expected');},
    async readMany(requests) {
      batches.push(requests);
      return {value:requests.map(request=>Uint8Array.from({length:request.length},(_,i)=>request.offset+i)),downloadedBytes:10,cacheReadBytes:3,requests:1};
    },
  },null,64);
  const input=[{path,offset:10,length:2},{path,offset:1,length:3}];
  assert.deepEqual((await storage.readMany(input)).map(bytes=>[...bytes]),[[10,11],[1,2,3]]);
  await storage.readMany([input[0],{path,offset:20,length:2}]);
  assert.deepEqual(batches,[input,[{path,offset:20,length:2}]]);
  assert.equal(storage.stats().requests,2);
  assert.equal(storage.stats().bytes,20);
  assert.equal(storage.stats().cacheReadBytes,8);
  assert.equal(storage.stats().cachedObjectBytes,7);
});
test('malformed adapter batches never populate the cache and allow a retry', async () => {
  let fail=true;
  const storage=new AdapterStorage({stat:()=>100,read:()=>null,readMany:()=>fail?[Uint8Array.of(1)]:[Uint8Array.of(1,2)]},null,32);
  const input=[{path,offset:0,length:2}];
  await assert.rejects(storage.readMany(input),/one exact range/);
  assert.equal(storage.stats().cachedObjectBytes,0);
  fail=false;
  assert.deepEqual([...(await storage.readMany(input))[0]],[1,2]);
});
