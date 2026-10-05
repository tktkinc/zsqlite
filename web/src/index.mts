import { httpStorage, moduleStorage } from './storage.mjs';
import { resolveOptions } from './options.mjs';
import { objectRows, statementApi } from './api.mjs';
import type { BucketOptions, Database, DatabaseStats, OpenOptions, Parameters, QueryOptions, QueryResult, QueryMetrics, Row, Statement, OpenProgress } from './types.mjs';
import type { Reply, Request } from './protocol.mjs';
export { httpStorage, moduleStorage };
export type * from './types.mjs';
type Command = Request extends infer T ? T extends Request ? Omit<T, 'id'> : never : never;
interface Pending { resolve(value: unknown): void; reject(error: unknown): void }
class RemoteDatabase implements Database {
  readonly readonly = true;
  private sequence = 0;
  private closed = false;
  private closing?: Promise<void>;
  private pending = new Map<number, Pending>();
  constructor(private worker: Worker, onProgress?: (progress: OpenProgress) => void) {
    worker.onmessage = ({ data }: MessageEvent<Reply | { kind: 'progress'; progress: OpenProgress }>) => {
      if ('kind' in data) { onProgress?.(data.progress); return; }
      const request = this.pending.get(data.id);
      if (!request) return;
      this.pending.delete(data.id);
      if (data.error !== undefined) request.reject(new Error(data.error)); else request.resolve(data.result);
    };
    worker.onerror = (event: ErrorEvent) => this.terminate(new Error(event.message || 'zsqlite worker failed'));
    worker.onmessageerror = () => this.terminate(new Error('Could not decode zsqlite worker response'));
  }
  private terminate(error = new Error('Database is closed')) {
    this.closed = true; this.worker.terminate();
    for (const request of this.pending.values()) request.reject(error);
    this.pending.clear();
  }
  private call<T>(command: Command): Promise<T> {
    if (this.closed || this.closing) return Promise.reject(new Error('Database is closed'));
    return new Promise<T>((resolve, reject) => {
      const id = ++this.sequence;
      this.pending.set(id, { resolve: value => resolve(value as T), reject });
      try { this.worker.postMessage({ id, ...command }); }
      catch (error) { this.pending.delete(id); reject(error); }
    });
  }
  async open(options: OpenOptions): Promise<void> {
    try { await this.call({ action: 'open', options }); }
    catch (error) { this.terminate(); throw error; }
  }
  async prepare(sql: string): Promise<Statement> {
    const prepared = await this.call<{ id: number; columns: string[] }>({ action: 'prepare', sql });
    return statementApi(prepared.columns,
      (parameters, maxRows, mode) => this.call({ action: 'statement', statement: prepared.id, parameters, maxRows, mode }),
      () => this.closing ?? (this.closed ? Promise.resolve() : this.call({ action: 'finalize', statement: prepared.id })));
  }
  query(sql: string, parameters: Parameters = [], { maxRows = 10000, firstRow = false }: QueryOptions = {}): Promise<QueryResult> {
    return this.call({ action: 'query', sql, parameters, maxRows, firstRow });
  }
  async all<T extends object = Row>(sql: string, parameters: Parameters = [], options: QueryOptions = {}): Promise<T[]> { return objectRows<T>(await this.query(sql, parameters, options)); }
  async get<T extends object = Row>(sql: string, parameters: Parameters = []): Promise<T | undefined> {
    return objectRows<T>(await this.query(sql, parameters, { maxRows: 1, firstRow: true }))[0];
  }
  exec(sql: string, parameters: Parameters = []): Promise<QueryMetrics> { return this.call({ action: 'exec', sql, parameters }); }
  stats(): Promise<DatabaseStats> { return this.call({ action: 'stats' }); }
  clearCache(): Promise<void> { return this.call({ action: 'clearCache' }); }
  close(): Promise<void> {
    if (this.closing) return this.closing;
    if (this.closed) return Promise.resolve();
    this.closing = this.call<void>({ action: 'close' }).finally(() => this.terminate());
    return this.closing;
  }
}
/** Opens one immutable SQLite connection in its own worker, backed by supplied storage. */
export async function openDatabase(options: OpenOptions): Promise<Database> {
  const configuration = resolveOptions(options);
  if (!options?.storage || !['http', 'module'].includes(options.storage.kind)) throw new Error('storage is required');
  const source = options.storage.kind === 'http' ? httpStorage({ ...options.storage, url: new URL(options.storage.url, globalThis.location.href).href }) : { ...options.storage, module: new URL(options.storage.module, globalThis.location.href).href };
  const worker = options.workerUrl
    ? new Worker(options.workerUrl, { type: 'module' })
    : new Worker(new URL('./worker.mjs', import.meta.url), { type: 'module' });
  const db = new RemoteDatabase(worker, options.onProgress);
  await db.open({ storage: source, ...configuration });
  return db;
}
/** Convenience wrapper for an HTTP/S3-compatible bucket prefix. */
export function openBucket(options: BucketOptions): Promise<Database> {
  return openDatabase({ ...options, storage: httpStorage(options) });
}
