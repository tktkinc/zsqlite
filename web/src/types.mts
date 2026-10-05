export type Parameter = string | number | bigint | boolean | null | Uint8Array;
export type Value = string | number | bigint | null | Uint8Array;
export type Parameters = readonly Parameter[] | Readonly<Record<string, Parameter>>;
export type Row = Record<string, Value>;
export type Awaitable<T> = T | Promise<T>;
export interface RowsResult { columns: string[]; rows: Value[][] }
export interface QueryMetrics { elapsedMs: number; downloadedBytes: number; cacheReadBytes: number; requests: number; cacheHits: number }
export interface QueryResult extends RowsResult { metrics: QueryMetrics }
export interface QueryOptions { maxRows?: number; firstRow?: boolean }
export interface Statement {
  readonly columns: readonly string[];
  query(parameters?: Parameters, options?: QueryOptions): Promise<QueryResult>;
  all<T extends object = Row>(parameters?: Parameters, options?: QueryOptions): Promise<T[]>;
  get<T extends object = Row>(parameters?: Parameters): Promise<T | undefined>;
  run(parameters?: Parameters): Promise<QueryMetrics>;
  finalize(): Promise<void>;
}
export interface Database {
  readonly readonly: true;
  prepare(sql: string): Promise<Statement>;
  query(sql: string, parameters?: Parameters, options?: QueryOptions): Promise<QueryResult>;
  all<T extends object = Row>(sql: string, parameters?: Parameters, options?: QueryOptions): Promise<T[]>;
  get<T extends object = Row>(sql: string, parameters?: Parameters): Promise<T | undefined>;
  /** Execute a single read-only statement, discarding returned rows. */
  exec(sql: string, parameters?: Parameters): Promise<QueryMetrics>;
  stats(): Promise<DatabaseStats>;
  clearCache(): Promise<void>;
  close(): Promise<void>;
}
export interface TransportStats {
  requests: number; bytes: number; cachedObjectBytes: number;
  objectCacheHits: number; rangeRequests: number; fullObjectRequests: number; cacheReadBytes: number;
}
export interface SnapshotStats { logicalBytes: number; sealedBytes: number; pageSize: number; pages: number }
export interface DatabaseStats extends TransportStats, SnapshotStats {
  memoryCacheBytes: number; memoryCacheBudget: number; diskCacheBytes: number; diskCacheBudget: number;
  diskCacheAvailable: boolean; diskCacheError: string | null; diskCacheHits: number; cacheHits: number;
  /** Allocated WASM linear memory, including metadata and decoder workspace. */
  wasmMemoryBytes: number;
}
/** Optional per-operation I/O counts, including caches inside the application backend. */
export interface StorageOperation<T> {
  value: T;
  downloadedBytes: number;
  cacheReadBytes: number;
  /** Actual backend requests; defaults to one when omitted. */
  requests?: number;
}
/** Immutable zsqlite object reads. Missing objects use null/-1. */
export interface StorageRead { path: string; offset: number; length: number }
export type StorageReads = readonly (Uint8Array | null)[];
export interface StorageAdapter {
  read(path: string, offset: number, length: number): Awaitable<Uint8Array | null | StorageOperation<Uint8Array | null>>;
  /** Exact ranges in input order. Optional; read() remains sufficient. */
  readMany?(requests: readonly StorageRead[]): Awaitable<StorageReads | StorageOperation<StorageReads>>;
  stat(path: string): Awaitable<number | StorageOperation<number>>;
  stats?(): Partial<TransportStats>;
  close?(): Awaitable<void>;
}
export interface HttpStorageOptions {
  url: string; headers?: Record<string, string>; withCredentials?: boolean;
  httpMode?: 'auto' | 'range' | 'get'; maxObjectBytes?: number;
  /** Merge nearby requests within a batch; defaults to 8 KiB gaps / 256 KiB spans. */
  mergeGapBytes?: number; maxMergedRangeBytes?: number;
  /** Bounded read-ahead for nearby small reads (at most 1/8 of the window); defaults to 64 KiB. Zero disables. */
  readAheadBytes?: number;
  /** Independent HTTP groups in flight per batch; defaults to four. */
  readConcurrency?: number;
}
export interface HttpStorage extends HttpStorageOptions { kind: 'http' }
export interface ModuleStorage {
  kind: 'module'; module: string; options?: unknown;
  /** Stable scope for reuse of this adapter's immutable objects on disk. */
  cacheKey?: string;
}
export type StorageSource = HttpStorage | ModuleStorage;
export interface CacheOptions {
  cacheBytes?: number; objectCacheBytes?: number; diskCacheBytes?: number; cacheKey?: string;
}
export interface OpenProgress {
  stage: string; message: string; path?: string; method?: string; status?: number; durationMs?: number; responseBytes?: number;
  offset?: number; length?: number;
  wasmMemoryBytes?: number;
}
export interface OpenOptions extends CacheOptions {
  storage: StorageSource; head?: string; workerUrl?: string | URL; onProgress?: (progress: OpenProgress) => void;
}
export interface WorkerOptions extends CacheOptions { head?: string }
export interface ResolvedOptions {
  head: string; cacheBytes: number; objectCacheBytes: number; diskCacheBytes: number; cacheKey?: string;
}
export interface PersistentCache {
  get(key: string): Uint8Array | null;
  put(key: string, bytes: Uint8Array): void;
  delete(key: string): void;
}
export interface StorageContext { cache: PersistentCache | null }
export type StorageFactory<T = unknown> = (options: T, context: StorageContext) => Awaitable<StorageAdapter>;
export interface BucketOptions extends HttpStorageOptions, CacheOptions { head?: string; workerUrl?: string | URL; onProgress?: (progress: OpenProgress) => void }
export type Bucket = Database;
