import { HttpBucket } from './http.mjs';
import { checkReads } from './ranges.mjs';
import type { DiskCache } from './disk-cache.mjs';
import type { HttpStorage, HttpStorageOptions, ModuleStorage, ResolvedOptions, StorageAdapter, StorageFactory, StorageSource, StorageOperation, StorageRead, StorageReads, OpenProgress, TransportStats } from './types.mjs';
import type { WasmStorage } from './wasm.mjs';

export function httpStorage({ url, headers, withCredentials, httpMode, maxObjectBytes, mergeGapBytes, maxMergedRangeBytes, readAheadBytes, readConcurrency }: HttpStorageOptions): HttpStorage {
  return { kind: 'http', url, headers, withCredentials, httpMode, maxObjectBytes, mergeGapBytes, maxMergedRangeBytes, readAheadBytes, readConcurrency };
}
/** Module exporting createStorage(options, { cache }); loaded inside the database worker. */
export function moduleStorage(module: string | URL, options?: unknown, cacheKey?: string): ModuleStorage {
  return { kind: 'module', module: String(module), options, cacheKey };
}
export function storageScope(source: StorageSource): string {
  if (source.kind === 'http') return source.url;
  if (source.cacheKey !== undefined) return source.cacheKey;
  try { return JSON.stringify([source.module, source.options]); }
  catch { return `${source.module}:${crypto.randomUUID()}`; }
}
const emptyStats = (): TransportStats => ({ requests: 0, bytes: 0, cachedObjectBytes: 0,
  objectCacheHits: 0, rangeRequests: 0, fullObjectRequests: 0, cacheReadBytes: 0 });
function validPath(path: string) {
  if (path !== 'catalog-head' && !/^objects\/[a-f0-9]{64}\.(blob|segment|dict|index)$/.test(path)) throw new Error('Invalid storage path');
}
/** Caches immutable reads around an application adapter, including async backends. */
export class AdapterStorage implements WasmStorage {
  lastError: string | null = null;
  root?: Uint8Array;
  private lengths = new Map<string, number>();
  private ranges = new Map<string, Uint8Array>();
  private counters = emptyStats();
  private backendCacheReadBytes = 0;
  private backendCacheHits = 0;
  constructor(private adapter: StorageAdapter, private cache: DiskCache | null, private budget: number) {}
  stats(): TransportStats {
    const custom = this.adapter.stats?.();
    return { ...this.counters, ...custom, cachedObjectBytes: this.counters.cachedObjectBytes,
      objectCacheHits: this.counters.objectCacheHits + (custom?.objectCacheHits ?? this.backendCacheHits),
      cacheReadBytes: this.counters.cacheReadBytes + (custom?.cacheReadBytes ?? this.backendCacheReadBytes) };
  }
  private unwrap<T>(result: T | StorageOperation<T>, defaultBytes: (value: T) => number): T {
    if (result !== null && typeof result === 'object' && 'value' in result) {
      const report = result as StorageOperation<T>;
      for (const value of [report.downloadedBytes, report.cacheReadBytes, report.requests ?? 1]) {
        if (!Number.isSafeInteger(value) || value < 0) throw new Error('Invalid storage operation counters');
      }
      this.counters.bytes += report.downloadedBytes;
      this.counters.requests += (report.requests ?? 1) - 1;
      this.backendCacheReadBytes += report.cacheReadBytes;
      this.backendCacheHits += report.cacheReadBytes > 0 ? 1 : 0;
      return report.value;
    }
    const value = result as T;
    this.counters.bytes += defaultBytes(value);
    return value;
  }
  async stat(path: string): Promise<number> {
    validPath(path);
    const known = this.lengths.get(path);
    if (known !== undefined) return known;
    const stored = this.cache?.get(`s:${path}`);
    if (stored?.length === 8) {
      const size = Number(new DataView(stored.buffer, stored.byteOffset, 8).getBigUint64(0, true));
      if (Number.isSafeInteger(size) && size > 0) { this.lengths.set(path, size); return size; }
    }
    this.counters.requests++;
    const size = this.unwrap<number>(await this.adapter.stat(path), () => 0);
    if (!Number.isSafeInteger(size) || size < -1) throw new Error('Invalid storage object size');
    if (size >= 0) {
      this.lengths.set(path, size);
      const bytes = new Uint8Array(8);
      new DataView(bytes.buffer).setBigUint64(0, BigInt(size), true);
      this.cache?.put(`s:${path}`, bytes);
    }
    return size;
  }
  async read(path: string, offset: number, length: number): Promise<Uint8Array> {
    validPath(path);
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(length) || length < 1
      || length > 64 * 1024 ** 2 || !Number.isSafeInteger(offset + length)) throw new Error('Invalid storage read');
    if (path === 'catalog-head' && this.root) return this.root;
    const key = `r:${path}:${offset}:${length}`;
    if (path !== 'catalog-head') {
      const memory = this.ranges.get(key);
      if (memory) {
        this.ranges.delete(key); this.ranges.set(key, memory);
        this.counters.objectCacheHits++; this.counters.cacheReadBytes += memory.length;
        return memory;
      }
      const stored = this.cache?.get(key);
      if (stored?.length === length) { this.keep(key, stored); return stored; }
    }
    this.counters.requests++;
    const result = this.unwrap<Uint8Array | null>(await this.adapter.read(path, offset, length), value => value instanceof Uint8Array ? value.length : 0);
    if (result === null && path === 'catalog-head') return this.root = new Uint8Array();
    if (!(result instanceof Uint8Array)) throw new Error(`Missing storage object: ${path}`);
    if (path === 'catalog-head') {
      if (offset !== 0 || result.length > length) throw new Error('Invalid catalog-head response');
      return this.root = result.slice();
    }
    if (result.length !== length) throw new Error('Storage must return the exact requested range');
    const bytes = result.slice();
    this.cache?.put(key, bytes); this.keep(key, bytes);
    return bytes;
  }
  private keep(key: string, bytes: Uint8Array) {
    if (this.ranges.has(key)) return;
    if (bytes.length > this.budget) return;
    while (this.counters.cachedObjectBytes + bytes.length > this.budget) {
      const oldest = this.ranges.keys().next().value!;
      this.counters.cachedObjectBytes -= this.ranges.get(oldest)!.length;
      this.ranges.delete(oldest);
    }
    this.ranges.set(key, bytes); this.counters.cachedObjectBytes += bytes.length;
  }
  async readMany(requests: readonly StorageRead[]): Promise<Uint8Array[]> {
    checkReads(requests);
    if (!this.adapter.readMany || requests.some(request => request.path === 'catalog-head')) {
      return Promise.all(requests.map(request => this.read(request.path, request.offset, request.length)));
    }
    const output: Uint8Array[] = new Array(requests.length);
    const misses: StorageRead[] = [], slots: number[] = [];
    for (const [index, request] of requests.entries()) {
      const key = `r:${request.path}:${request.offset}:${request.length}`;
      const memory = this.ranges.get(key);
      if (memory) {
        this.ranges.delete(key); this.ranges.set(key, memory);
        this.counters.objectCacheHits++; this.counters.cacheReadBytes += memory.length;
        output[index] = memory;
      } else {
        const stored = this.cache?.get(key);
        if (stored?.length === request.length) { this.keep(key, stored); output[index] = stored; }
        else { misses.push(request); slots.push(index); }
      }
    }
    if (!misses.length) return output;
    this.counters.requests++;
    const values = this.unwrap<StorageReads>(await this.adapter.readMany(misses), data => Array.isArray(data) ? data.reduce((sum, value) => sum + (value?.length ?? 0), 0) : 0);
    if (!Array.isArray(values) || values.length !== misses.length
        || values.some((value, index) => !(value instanceof Uint8Array) || value.length !== misses[index]!.length)) {
      throw new Error('Storage batch must return one exact range per request in input order');
    }
    for (const [index, request] of misses.entries()) {
      const bytes = values[index]!.slice();
      const key = `r:${request.path}:${request.offset}:${request.length}`;
      this.cache?.put(key, bytes); this.keep(key, bytes); output[slots[index]!] = bytes;
    }
    return output;
  }
  async close(): Promise<void> { await this.adapter.close?.(); }
}
export async function createStorage(source: StorageSource, options: ResolvedOptions, cache: DiskCache | null, report?: (progress: OpenProgress) => void): Promise<WasmStorage> {
  if (source.kind === 'http') return new HttpBucket({ ...source, objectCacheBytes: options.objectCacheBytes, diskCache: cache, report });
  const factory = (await import(/* @vite-ignore */ source.module) as { createStorage?: StorageFactory }).createStorage;
  if (typeof factory !== 'function') throw new Error('Storage module must export createStorage');
  const adapter = await factory(source.options, { cache });
  if (!adapter || typeof adapter.read !== 'function' || typeof adapter.stat !== 'function') throw new Error('Storage adapter requires read and stat');
  return new AdapterStorage(adapter, cache, options.objectCacheBytes);
}
