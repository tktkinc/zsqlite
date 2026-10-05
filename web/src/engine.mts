import createModule from './zsqlite-browser.mjs';
import { openDiskCache } from './disk-cache.mjs';
import type { DiskCacheResult } from './disk-cache.mjs';
import { createStorage, AdapterStorage, storageScope } from './storage.mjs';
import { prepare, NativeStatement } from './query.mjs';
import { objectRows, statementApi } from './api.mjs';
import { resolveOptions } from './options.mjs';
import type { Database, DatabaseStats, OpenOptions, Parameters, QueryOptions, QueryResult, QueryMetrics, RowsResult, Row, Statement, StorageAdapter, WorkerOptions, ResolvedOptions, OpenProgress } from './types.mjs';
import type { WasmModule, WasmStorage } from './wasm.mjs';
export type { StorageAdapter, StorageFactory, Database, Statement, WorkerOptions } from './types.mjs';

/** For applications managing their own dedicated worker. Direct adapters may return promises. */
export async function openInWorker(storage: StorageAdapter, options: WorkerOptions = {}): Promise<Database> {
  const configuration = resolveOptions(options);
  const disk = await openDiskCache({ url: configuration.cacheKey ?? crypto.randomUUID(), ...configuration });
  return open(new AdapterStorage(storage, disk.cache, configuration.objectCacheBytes), disk, configuration);
}
export async function openFromSource(options: OpenOptions, report: (progress: OpenProgress) => void = () => {}): Promise<Database> {
  const configuration = resolveOptions(options);
  const source = options.storage;
  report({ stage: 'disk-cache', message: 'Opening disk cache' });
  const disk = await openDiskCache({ url: storageScope(source), headers: source.kind === 'http' ? source.headers : undefined,
    withCredentials: source.kind === 'http' ? source.withCredentials : false, ...configuration });
  report({ stage: 'disk-cache-ready', message: disk.cache ? 'Disk cache ready' : `Using memory cache: ${disk.reason}` });
  let transport: WasmStorage;
  try { transport = await createStorage(source, configuration, disk.cache, report); }
  catch (error) { disk.cache?.close(); throw error; }
  return open(transport, disk, configuration, report);
}
async function open(transport: WasmStorage, disk: DiskCacheResult, options: ResolvedOptions, report: (progress: OpenProgress) => void = () => {}): Promise<Database> {
  try {
    report({ stage: 'wasm-loading', message: 'Loading SQLite WASM' });
    const module = await createModule({ bucketTransport: transport, diskCache: disk.cache });
    report({ stage: 'wasm-ready', message: 'SQLite WASM ready; opening sealed snapshot' });
    const pointer = await module.ccall('zsqlite_browser_open', 'number', ['string', 'number'], [options.head, options.cacheBytes], { async: true });
    if (!pointer) throw new Error(transport.lastError || module.UTF8ToString(module._zsqlite_browser_error()));
    report({ stage: 'database-ready', message: 'Sealed database open', wasmMemoryBytes: module.HEAPU8.length });
    return new Connection(module, pointer, disk, options);
  } catch (error) { disk.cache?.close(); await transport.close?.(); throw error; }
}
class Connection implements Database {
  readonly readonly = true;
  private queue: Promise<unknown> = Promise.resolve();
  private closed = false;
  private closing?: Promise<void>;
  private statements = new Set<NativeStatement>();
  constructor(private module: WasmModule, private pointer: number, private disk: DiskCacheResult, private configuration: ResolvedOptions) {}
  private enqueue<T>(operation: () => Promise<T> | T): Promise<T> {
    const result = this.queue.then(() => { if (this.closed) throw new Error('Database is closed'); return operation(); });
    this.queue = result.catch(() => {});
    return result;
  }
  prepare(sql: string): Promise<Statement> {
    return this.enqueue(async () => {
      const native = await prepare(this.module, this.pointer, sql);
      this.statements.add(native);
      return statementApi(native.columns, (parameters, maxRows, mode) => this.enqueue(() => this.measure(() => native.execute(parameters, maxRows, mode))),
        () => this.closing ?? this.enqueue(() => { native.finalize(); this.statements.delete(native); }));
    });
  }
  private execute(sql: string, parameters: Parameters, maxRows: number, mode: 'all' | 'first' | 'discard') {
    return this.enqueue(() => this.measure(async () => {
      const native = await prepare(this.module, this.pointer, sql);
      try { return await native.execute(parameters, maxRows, mode); }
      finally { native.finalize(); }
    }));
  }
  query(sql: string, parameters: Parameters = [], { maxRows = 10000, firstRow = false }: QueryOptions = {}) { return this.execute(sql, parameters, maxRows, firstRow ? 'first' : 'all'); }
  async all<T extends object = Row>(sql: string, parameters: Parameters = [], options: QueryOptions = {}): Promise<T[]> { return objectRows<T>(await this.query(sql, parameters, options)); }
  async get<T extends object = Row>(sql: string, parameters: Parameters = []): Promise<T | undefined> { return objectRows<T>(await this.execute(sql, parameters, 1, 'first'))[0]; }
  async exec(sql: string, parameters: Parameters = []): Promise<QueryMetrics> { return (await this.execute(sql, parameters, 0, 'discard')).metrics; }
  private async measure(operation: () => Promise<RowsResult>): Promise<QueryResult> {
    const before = this.collectStats();
    const start = performance.now();
    const result = await operation();
    const elapsedMs = performance.now() - start;
    const after = this.collectStats();
    return { ...result, metrics: { elapsedMs, downloadedBytes: after.bytes - before.bytes,
      cacheReadBytes: after.cacheReadBytes - before.cacheReadBytes, requests: after.requests - before.requests,
      cacheHits: after.cacheHits - before.cacheHits } };
  }
  stats(): Promise<DatabaseStats> { return this.enqueue(() => { this.disk.cache?.flush(); return this.collectStats(); }); }
  private collectStats(): DatabaseStats {
    const m = this.module, disk = this.disk;
    const transport = m.bucketTransport.stats();
    const buffer = m._malloc(8);
    if (!buffer) throw new Error('WASM allocation failed');
    let sqliteBytes = 0, sqliteHits = 0;
    try {
      if (m._sqlite3_db_status(this.pointer, 1, buffer, buffer + 4, 0) === 0) sqliteBytes = m.HEAPU32[buffer >>> 2]!;
      if (m._sqlite3_db_status(this.pointer, 7, buffer, buffer + 4, 0) === 0) sqliteHits = m.HEAPU32[buffer >>> 2]!;
    } finally { m._free(buffer); }
    return { ...transport, ...(m.snapshotStats ?? { logicalBytes: 0, sealedBytes: 0, pageSize: 0, pages: 0 }),
      sealedBytes: (m.snapshotStats?.sealedBytes ?? 0) + (m.bucketTransport.root?.length ?? 0),
      memoryCacheBytes: transport.cachedObjectBytes + (m.pageCacheBytes ?? 0) + sqliteBytes,
      memoryCacheBudget: this.configuration.cacheBytes + this.configuration.objectCacheBytes + 2 * 1024 ** 2,
      diskCacheBytes: disk.cache?.size ?? 0, diskCacheBudget: disk.cache?.budget ?? 0,
      diskCacheAvailable: Boolean(disk.cache?.handle), diskCacheError: disk.cache?.error ?? disk.reason,
      diskCacheHits: disk.cache?.hits ?? 0,
      wasmMemoryBytes: m.HEAPU8.length,
      cacheHits: transport.objectCacheHits + (m.pageCacheHits ?? 0) + (disk.cache?.hits ?? 0) + sqliteHits,
      cacheReadBytes: transport.cacheReadBytes + (m.pageCacheReadBytes ?? 0) + (disk.cache?.readBytes ?? 0) + sqliteHits * (m.snapshotStats?.pageSize ?? 0),
    };
  }
  clearCache(): Promise<void> { return this.enqueue(() => { this.disk.cache?.clear(); }); }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closing = this.enqueue(async () => {
      for (const stmt of this.statements) stmt.finalize();
      this.statements.clear();
      if (this.module._sqlite3_close(this.pointer) !== 0) throw new Error('Cannot close SQLite connection');
      this.closed = true;
      try { await this.module.bucketTransport.close?.(); } finally { this.disk.cache?.close(); }
    });
    return this.closing;
  }
}
