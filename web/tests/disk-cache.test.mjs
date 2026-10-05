import assert from 'node:assert/strict';
import test from 'node:test';
import { DiskCache, checksum } from '../dist/disk-cache.mjs';

class File {
  bytes = new Uint8Array();
  read(output, { at }) { const data = this.bytes.subarray(at, at + output.length); output.set(data); return data.length; }
  write(input, { at }) { if (at + input.length > this.bytes.length) this.truncate(at + input.length); this.bytes.set(input, at); return input.length; }
  truncate(size) { const bytes = new Uint8Array(size); bytes.set(this.bytes.subarray(0, size)); this.bytes = bytes; }
  getSize() { return this.bytes.length; }
  flush() {}
  close() {}
}

test('disk records survive reopening and budget eviction retains recently used entries', () => {
  const file = new File();
  const cache = new DiskCache(file, 150);
  cache.put('old', new Uint8Array(30).fill(1));
  cache.put('recent', new Uint8Array(30).fill(2));
  assert.deepEqual(cache.get('recent'), new Uint8Array(30).fill(2));
  cache.put('new', new Uint8Array(30).fill(3));
  assert.equal(cache.get('old'), null);
  assert.ok(file.getSize() <= 150);
  const reopened = new DiskCache(file, 150);
  assert.deepEqual(reopened.get('recent'), new Uint8Array(30).fill(2));
  assert.deepEqual(reopened.get('new'), new Uint8Array(30).fill(3));
  assert.equal(reopened.get('old'), null);
});

test('corrupt payloads and interrupted records become misses without breaking existing entries', () => {
  const file = new File();
  const cache = new DiskCache(file, 1024);
  cache.put('good', Uint8Array.of(1, 2, 3));
  cache.put('bad', Uint8Array.of(4, 5, 6));
  const entry = cache.entries.get('bad');
  file.bytes[entry.at + 20 + entry.keySize] ^= 1;
  assert.equal(cache.get('bad'), null);
  cache.put('bad', Uint8Array.of(7, 8, 9));
  const complete = file.getSize();
  file.write(Uint8Array.of(1, 2, 3, 4, 5), { at: complete });
  const reopened = new DiskCache(file, 1024);
  assert.equal(file.getSize(), complete);
  assert.deepEqual(reopened.get('good'), Uint8Array.of(1, 2, 3));
  assert.deepEqual(reopened.get('bad'), Uint8Array.of(7, 8, 9));
  assert.equal(checksum(new TextEncoder().encode('123456789')), 0xcbf43926);
});

test('storage failures disable the cache and zero reads remain misses', () => {
  const file = new File();
  const cache = new DiskCache(file, 1024);
  file.write = () => { throw new Error('Quota exceeded'); };
  cache.put('key', Uint8Array.of(1));
  assert.match(cache.error, /Quota/);
  assert.equal(cache.get('key'), null);
  cache.put('other', Uint8Array.of(2));
});
