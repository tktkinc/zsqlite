import type { HttpStorageOptions, OpenProgress, StorageRead, TransportStats } from './types.mjs';
import type { DiskCache } from './disk-cache.mjs';
import { checkReads, groupReads } from './ranges.mjs';
interface CachedRange { path: string; offset: number; bytes: Uint8Array<ArrayBuffer> }
/** Coalesced range batches or cached whole-object GETs, inside a dedicated worker. */
export class HttpBucket {
  base: URL;
  headers: Record<string, string>;
  withCredentials: boolean;
  httpMode: 'auto' | 'range' | 'get';
  diskCache: DiskCache | null;
  objectCacheBytes: number;
  maxObjectBytes: number;
  xhrFactory: () => XMLHttpRequest;
  report?: (progress: OpenProgress) => void;
  lengths = new Map<string, number>();
  root: Uint8Array<ArrayBuffer> | undefined;
  lastError: string | null = null;
  requests = 0;
  bytes = 0;
  objects = new Map<string, Uint8Array<ArrayBuffer>>();
  cachedObjectBytes = 0;
  objectCacheHits = 0;
  rangeRequests = 0;
  fullObjectRequests = 0;
  cacheReadBytes = 0;
  private mergeGapBytes: number;
  private maxMergedRangeBytes: number;
  private readAheadBytes: number;
  private readConcurrency: number;
  private ranges = new Map<string, CachedRange>();
  private rangeKeys = new Map<string, Set<string>>();
  private cacheOrder = new Map<string, boolean>();
  private diskRanges = new Map<string, { key: string; offset: number; length: number }[]>();
  private recent = new Map<string, { offset: number; length: number; time: number }>();
  constructor({ url, headers = {}, withCredentials = false, httpMode = 'auto',
    objectCacheBytes = 64 * 1024 * 1024, maxObjectBytes = 64 * 1024 * 1024, diskCache = null, report,
    mergeGapBytes = 8192, maxMergedRangeBytes = 256 * 1024, readAheadBytes = 64 * 1024, readConcurrency = 4 }: HttpStorageOptions & { objectCacheBytes?: number; diskCache?: DiskCache | null; report?: (progress: OpenProgress) => void }, xhrFactory = () => new XMLHttpRequest()) {
    this.base = new URL(url);
    if (!['http:', 'https:'].includes(this.base.protocol) || this.base.search || this.base.hash) {
      throw new Error('Bucket URL must be an HTTP(S) directory URL without a query or fragment');
    }
    if (!this.base.pathname.endsWith('/')) this.base.pathname += '/';
    this.headers = headers;
    this.withCredentials = withCredentials;
    if (!['auto', 'range', 'get'].includes(httpMode)) throw new Error('httpMode must be auto, range or get');
    for (const [name, value] of Object.entries({ objectCacheBytes, maxObjectBytes })) {
      if (!Number.isSafeInteger(value) || value < 0 || (name === 'maxObjectBytes' && value === 0)) throw new Error(`Invalid ${name}`);
    }
    this.httpMode = httpMode;
    this.diskCache = diskCache;
    this.objectCacheBytes = objectCacheBytes;
    this.maxObjectBytes = maxObjectBytes;
    this.xhrFactory = xhrFactory;
    this.report = report;
    for (const [name, value] of Object.entries({ mergeGapBytes, maxMergedRangeBytes, readAheadBytes, readConcurrency })) {
      if (!Number.isSafeInteger(value) || value < 0 || value > 64 * 1024 ** 2) throw new Error(`Invalid ${name}`);
    }
    if (maxMergedRangeBytes < 1 || readConcurrency < 1 || readConcurrency > 32) throw new Error('Invalid HTTP batch limits');
    this.mergeGapBytes = mergeGapBytes;
    this.maxMergedRangeBytes = maxMergedRangeBytes;
    this.readAheadBytes = readAheadBytes;
    this.readConcurrency = readConcurrency;
    for (const key of diskCache?.entries.keys() ?? []) {
      const match = /^r:(objects\/[a-f0-9]{64}\.(?:blob|segment|dict|index)):(\d+):(\d+)$/.exec(key);
      if (match) this.indexDiskRange(match[1]!, key, Number(match[2]), Number(match[3]));
    }
  }

  stats(): TransportStats {
    return { requests: this.requests, bytes: this.bytes, cachedObjectBytes: this.cachedObjectBytes,
      objectCacheHits: this.objectCacheHits, rangeRequests: this.rangeRequests, fullObjectRequests: this.fullObjectRequests,
      cacheReadBytes: this.cacheReadBytes };
  }

  cached(path: string, length = 0) {
    const bytes = this.objects.get(path);
    if (bytes) {
      this.objects.delete(path);
      this.objects.set(path, bytes);
      this.touch(`o:${path}`);
      this.objectCacheHits++;
      this.cacheReadBytes += Math.min(length, bytes.length);
    }
    return bytes;
  }

  rememberLength(path: string, length: number) {
    this.lengths.set(path, length);
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setBigUint64(0, BigInt(length), true);
    this.diskCache?.put(`s:${path}`, bytes);
  }

  keepWhole(path: string, bytes: Uint8Array<ArrayBuffer>) {
    if (bytes.length <= this.objectCacheBytes) {
      const old = this.objects.get(path);
      if (old) { this.cachedObjectBytes -= old.length; this.objects.delete(path); this.cacheOrder.delete(`o:${path}`); }
      this.evict(bytes.length);
      this.objects.set(path, bytes);
      this.cachedObjectBytes += bytes.length;
      this.touch(`o:${path}`);
    }
    return bytes;
  }

  private touch(key: string) { this.cacheOrder.delete(key); this.cacheOrder.set(key, true); }
  private evict(incoming: number) {
    while (this.cachedObjectBytes + incoming > this.objectCacheBytes) {
      const key = this.cacheOrder.keys().next().value!;
      this.cacheOrder.delete(key);
      if (key.startsWith('o:')) {
        this.cachedObjectBytes -= this.objects.get(key.slice(2))!.length;
        this.objects.delete(key.slice(2));
      } else {
        const range = this.ranges.get(key)!;
        this.cachedObjectBytes -= range.bytes.length;
        this.ranges.delete(key);
        const keys = this.rangeKeys.get(range.path)!;
        keys.delete(key); if (!keys.size) this.rangeKeys.delete(range.path);
      }
    }
  }
  private keepRange(path: string, offset: number, bytes: Uint8Array<ArrayBuffer>) {
    const key = `r:${path}:${offset}:${bytes.length}`;
    if (bytes.length > this.objectCacheBytes || this.ranges.has(key)) return;
    this.evict(bytes.length);
    this.ranges.set(key, { path, offset, bytes });
    const keys = this.rangeKeys.get(path) ?? new Set<string>();
    keys.add(key); this.rangeKeys.set(path, keys);
    this.cachedObjectBytes += bytes.length; this.touch(key);
  }
  private indexDiskRange(path: string, key: string, offset: number, length: number) {
    if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(length) || offset < 0 || length < 1) return;
    const entries = (this.diskRanges.get(path) ?? []).filter(entry => this.diskCache?.entries.has(entry.key));
    if (!entries.some(entry => entry.key === key)) entries.push({ key, offset, length });
    this.diskRanges.set(path, entries);
  }
  private rememberRange(path: string, offset: number, bytes: Uint8Array<ArrayBuffer>) {
    this.keepRange(path, offset, bytes);
    const key = `r:${path}:${offset}:${bytes.length}`;
    this.diskCache?.put(key, bytes);
    if (this.diskCache?.entries.has(key)) this.indexDiskRange(path, key, offset, bytes.length);
  }
  private cachedRange(path: string, offset: number, length: number) {
    for (const key of this.rangeKeys.get(path) ?? []) {
      const range = this.ranges.get(key)!;
      if (range.offset <= offset && range.offset + range.bytes.length >= offset + length) {
        this.touch(key); this.objectCacheHits++; this.cacheReadBytes += length;
        return range.bytes.subarray(offset - range.offset, offset - range.offset + length);
      }
    }
    const candidates = this.diskRanges.get(path);
    const range = candidates?.filter(entry => entry.offset <= offset && entry.offset + entry.length >= offset + length
      && this.diskCache?.entries.has(entry.key)).sort((a, b) => a.length - b.length)[0];
    if (range) {
      const bytes = this.diskCache?.get(range.key);
      if (bytes?.length === range.length) {
        this.keepRange(path, range.offset, bytes);
        return bytes.subarray(offset - range.offset, offset - range.offset + length);
      }
    }
    return null;
  }

  diskWhole(path: string) {
    const bytes = this.diskCache?.get(`o:${path}`);
    if (!bytes) return null;
    if (bytes.length > this.maxObjectBytes || (this.lengths.has(path) && this.lengths.get(path) !== bytes.length)) {
      this.diskCache?.delete(`o:${path}`);
      return null;
    }
    this.rememberLength(path, bytes.length);
    return this.keepWhole(path, bytes);
  }

  complete(path: string, xhr: XMLHttpRequest) {
    if (xhr.status !== 200 || xhr.getResponseHeader('Content-Range')) throw new Error('Expected a complete HTTP GET response');
    const bytes = new Uint8Array(xhr.response);
    this.bytes += bytes.length;
    if (bytes.length === 0 || bytes.length > this.maxObjectBytes) throw new Error(`Whole object exceeds maxObjectBytes (${this.maxObjectBytes}) or is empty; use HTTP ranges`);
    const header = xhr.getResponseHeader('Content-Length');
    if ((header !== null && (!/^\d+$/.test(header) || Number(header) !== bytes.length))
        || (this.lengths.has(path) && this.lengths.get(path) !== bytes.length)) {
      throw new Error('Invalid or truncated whole-object response');
    }
    this.rememberLength(path, bytes.length);
    this.diskCache?.put(`o:${path}`, bytes);
    return this.keepWhole(path, bytes);
  }

  whole(path: string) {
    const cached = this.cached(path) ?? this.diskWhole(path);
    if (cached) return cached;
    if ((this.lengths.get(path) ?? 0) > this.maxObjectBytes) throw new Error('Object is too large for whole-object GET; use HTTP ranges');
    this.fullObjectRequests++;
    const xhr = this.request('GET', path);
    return xhr ? this.complete(path, xhr) : null;
  }

  private startRequest(method: 'GET' | 'HEAD', path: string, range: readonly [number, number] | undefined, asynchronous: boolean) {
    if (path !== 'catalog-head' && !/^objects\/[a-f0-9]{64}\.(blob|segment|dict|index)$/.test(path)) {
      throw new Error('Invalid bucket object path');
    }
    const xhr = this.xhrFactory();
    xhr.open(method, new URL(path, this.base).href, asynchronous);
    xhr.responseType = 'arraybuffer';
    xhr.withCredentials = this.withCredentials;
    for (const [name, value] of Object.entries(this.headers)) {
      if (/^(range|accept-encoding)$/i.test(name)) throw new Error(`Reserved HTTP header: ${name}`);
      xhr.setRequestHeader(name, value);
    }
    if (range) xhr.setRequestHeader('Range', `bytes=${range[0]}-${range[1]}`);
    this.requests++;
    const started = performance.now();
    this.report?.({ stage: 'http-request', message: `${method} ${path}`, method, path, offset: range?.[0], length: range ? range[1] - range[0] + 1 : undefined });
    return { xhr, started };
  }
  private finishRequest(method: 'GET' | 'HEAD', path: string, range: readonly [number, number] | undefined, xhr: XMLHttpRequest, started: number) {
    this.report?.({ stage: 'http-response', message: `${method} ${path}: ${xhr.status}`, method, path, status: xhr.status, durationMs: performance.now() - started, responseBytes: xhr.response?.byteLength ?? 0,
      offset: range?.[0], length: range ? range[1] - range[0] + 1 : undefined });
    if (xhr.status === 404) return null;
    if (method === 'HEAD' && [405, 501].includes(xhr.status)) return xhr;
    if (xhr.status !== 200 && xhr.status !== 206) throw new Error(`Bucket ${method} ${path}: HTTP ${xhr.status}`);
    const encoding = xhr.getResponseHeader('Content-Encoding');
    if (encoding && encoding !== 'identity') throw new Error('Bucket objects must use identity Content-Encoding');
    return xhr;
  }
  request(method: 'GET' | 'HEAD', path: string, range?: readonly [number, number]) {
    const { xhr, started } = this.startRequest(method, path, range, false);
    xhr.send();
    return this.finishRequest(method, path, range, xhr, started);
  }
  private asyncRequest(path: string, range?: readonly [number, number]): Promise<XMLHttpRequest | null> {
    return new Promise((resolve, reject) => {
      const { xhr, started } = this.startRequest('GET', path, range, true);
      xhr.onload = () => { try { resolve(this.finishRequest('GET', path, range, xhr, started)); } catch (error) { reject(error); } };
      xhr.onerror = () => reject(new Error(`Bucket GET ${path}: network error`));
      xhr.onabort = () => reject(new Error(`Bucket GET ${path}: aborted`));
      xhr.ontimeout = () => reject(new Error(`Bucket GET ${path}: timed out`));
      xhr.send();
    });
  }

  stat(path: string): number {
    if (this.lengths.has(path)) return this.lengths.get(path)!;
    const stored = this.diskCache?.get(`s:${path}`);
    if (stored?.length === 8) {
      const length = Number(new DataView(stored.buffer, stored.byteOffset, 8).getBigUint64(0, true));
      if (Number.isSafeInteger(length) && length > 0) { this.lengths.set(path, length); return length; }
    }
    if (this.httpMode === 'get') return this.whole(path)?.length ?? -1;
    const xhr = this.request('HEAD', path);
    if (!xhr) return -1;
    const header = xhr.getResponseHeader('Content-Length');
    if (xhr.status !== 200 || header === null) {
      if (this.httpMode === 'range') throw new Error('HEAD must expose Content-Length in range mode');
      return this.whole(path)?.length ?? -1;
    }
    if (!/^\d+$/.test(header)) throw new Error('Invalid Content-Length');
    const length = Number(header);
    if (!Number.isSafeInteger(length) || length <= 0) throw new Error('Invalid object length');
    this.rememberLength(path, length);
    return length;
  }

  read(path: string, offset: number, length: number): Uint8Array<ArrayBuffer> {
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(length)
        || length <= 0 || length > 64 * 1024 * 1024 || !Number.isSafeInteger(offset + length)) {
      throw new Error('Invalid or excessive HTTP range');
    }
    if (path === 'catalog-head') {
      // A single connection keeps its original root even if the bucket advances.
      if (this.root === undefined) {
        const xhr = this.request('GET', path);
        if (!xhr) this.root = new Uint8Array();
        else {
          if (xhr.status !== 200) throw new Error('catalog-head requires a complete response');
          this.root = new Uint8Array(xhr.response);
          this.bytes += this.root.length;
          if (this.root.length > 65536 + 72) throw new Error('Excessive catalog-head length');
        }
      }
      if (offset !== 0 || this.root.length > length) throw new Error('Invalid catalog-head read');
      return this.root;
    }
    const whole = this.cached(path, length) ?? this.diskWhole(path) ?? (this.httpMode === 'get' ? this.whole(path) : undefined);
    if (whole) {
      if (offset + length > whole.length) throw new Error('HTTP range exceeds object length');
      return whole.subarray(offset, offset + length);
    }
    if (this.httpMode === 'get') throw new Error(`Missing bucket object: ${path}`);
    const stored = this.cachedRange(path, offset, length);
    if (stored) return stored;
    this.rangeRequests++;
    const xhr = this.request('GET', path, [offset, offset + length - 1]);
    if (!xhr) throw new Error(`Missing bucket object: ${path}`);
    if (xhr.status === 200) {
      if (this.httpMode === 'range') throw new Error('Bucket must support HTTP Range requests (206)');
      const whole = this.complete(path, xhr);
      this.fullObjectRequests++;
      if (offset + length > whole.length) throw new Error('HTTP range exceeds object length');
      return whole.subarray(offset, offset + length);
    }
    return this.acceptRange(path, offset, length, xhr);
  }
  private acceptRange(path: string, offset: number, length: number, xhr: XMLHttpRequest) {
    const bytes = new Uint8Array(xhr.response);
    this.bytes += bytes.length;
    const match = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(xhr.getResponseHeader('Content-Range') ?? '');
    if (!match) throw new Error('Range responses must expose Content-Range');
    const [start, end, total] = match.slice(1).map(Number) as [number, number, number];
    if (![start, end, total].every(Number.isSafeInteger) || start !== offset
        || end !== offset + length - 1 || bytes.length !== length || total <= end
        || (this.lengths.has(path) && this.lengths.get(path) !== total)) {
      throw new Error('Invalid or truncated bucket range response');
    }
    this.rememberLength(path, total);
    this.rememberRange(path, offset, bytes);
    return bytes;
  }
  private ahead(request: StorageRead): StorageRead {
    const previous = this.recent.get(request.path);
    this.recent.set(request.path, { offset: request.offset, length: request.length, time: performance.now() });
    const size = this.lengths.get(request.path);
    const window = this.readAheadBytes;
    // Read-ahead helps small page frames amortize request latency. Large frames
    // already contain nearby pages; padding them mostly increases cold traffic.
    if (!window || !request.path.endsWith('.blob') || request.length > window / 8 || size === undefined
        || request.offset + request.length > size || (!this.objectCacheBytes && !this.diskCache?.handle)
        || !previous || performance.now() - previous.time > 2000) return request;
    const distance = Math.max(0, Math.max(previous.offset, request.offset) - Math.min(previous.offset + previous.length, request.offset + request.length));
    if (distance > window) return request;
    const offset = Math.floor(request.offset / window) * window;
    const end = Math.min(size, Math.ceil((request.offset + request.length) / window) * window);
    if (end - offset > this.objectCacheBytes && end - offset > (this.diskCache?.handle ? this.diskCache.budget : 0)) return request;
    return { path: request.path, offset, length: end - offset };
  }
  /** Plan cold ranges together, then fetch independent groups concurrently. */
  async readMany(requests: readonly StorageRead[]): Promise<Uint8Array[]> {
    checkReads(requests);
    const output: Uint8Array[] = new Array(requests.length);
    const misses: { request: StorageRead; index: number; fetch: StorageRead }[] = [];
    for (const [index, request] of requests.entries()) {
      if (request.path === 'catalog-head') { output[index] = this.read(request.path, request.offset, request.length); continue; }
      const expanded = this.ahead(request);
      const whole = this.cached(request.path, request.length) ?? this.diskWhole(request.path);
      if (whole) {
        if (request.offset + request.length > whole.length) throw new Error('HTTP range exceeds object length');
        output[index] = whole.slice(request.offset, request.offset + request.length);
      } else {
        const stored = this.cachedRange(request.path, request.offset, request.length);
        if (stored) output[index] = stored.slice();
        else misses.push({ request, index, fetch: expanded });
      }
    }
    const groups = this.httpMode === 'get'
      ? [...new Set(misses.map(item => item.request.path))].map(path => ({ path, offset: 0, length: 0, indices: misses.flatMap((item, index) => item.request.path === path ? [index] : []) }))
      : groupReads(misses.map(item => item.fetch), this.mergeGapBytes, this.maxMergedRangeBytes);
    let next = 0;
    const errors: unknown[] = [];
    const fetchGroup = async (group: typeof groups[number]) => {
      let bytes: Uint8Array<ArrayBuffer>, start = group.offset;
      if (this.httpMode === 'get') {
        if ((this.lengths.get(group.path) ?? 0) > this.maxObjectBytes) throw new Error('Object is too large for whole-object GET; use HTTP ranges');
        this.fullObjectRequests++;
        const xhr = await this.asyncRequest(group.path);
        if (!xhr) throw new Error(`Missing bucket object: ${group.path}`);
        bytes = this.complete(group.path, xhr);
      } else {
        const whole = this.cached(group.path, group.length) ?? this.diskWhole(group.path);
        const stored = whole ? null : this.cachedRange(group.path, group.offset, group.length);
        if (whole) { bytes = whole; start = 0; }
        else if (stored) bytes = stored;
        else {
          this.rangeRequests++;
          const xhr = await this.asyncRequest(group.path, [group.offset, group.offset + group.length - 1]);
          if (!xhr) throw new Error(`Missing bucket object: ${group.path}`);
          if (xhr.status === 200) {
            if (this.httpMode === 'range') throw new Error('Bucket must support HTTP Range requests (206)');
            this.fullObjectRequests++;
            bytes = this.complete(group.path, xhr); start = 0;
          } else bytes = this.acceptRange(group.path, group.offset, group.length, xhr);
        }
      }
      for (const slot of group.indices) {
        const { request, index } = misses[slot]!;
        const offset = request.offset - start;
        if (offset < 0 || offset + request.length > bytes.length) throw new Error('HTTP range exceeds object length');
        // A result must not retain a large read-ahead/object buffer after that
        // buffer has been evicted from the bounded cache.
        output[index] = bytes.slice(offset, offset + request.length);
      }
    };
    // Drain active groups before reporting a failure, so no old I/O overlaps
    // the next serialized SQLite operation or changes its metric baseline.
    await Promise.all(Array.from({ length: Math.min(groups.length, this.readConcurrency) }, async () => {
      while (next < groups.length && !errors.length) {
        const group = groups[next++]!;
        try { await fetchGroup(group); } catch (error) { errors.push(error); }
      }
    }));
    if (errors.length) throw errors[0];
    return output;
  }
}
