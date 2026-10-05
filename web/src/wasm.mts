import type { TransportStats, SnapshotStats, StorageAdapter, StorageRead } from './types.mjs';
import type { DiskCache } from './disk-cache.mjs';

export interface WasmStorage extends StorageAdapter {
  lastError: string | null;
  root?: Uint8Array;
  read(path: string, offset: number, length: number): Uint8Array | Promise<Uint8Array>;
  readMany(requests: readonly StorageRead[]): Promise<Uint8Array[]>;
  stat(path: string): number | Promise<number>;
  stats(): TransportStats;
}
export interface WasmModule {
  bucketTransport: WasmStorage;
  diskCache: DiskCache | null;
  pageCacheBytes?: number; pageCacheHits?: number; pageCacheReadBytes?: number; snapshotStats?: SnapshotStats;
  HEAPU8: Uint8Array<ArrayBuffer>; HEAPU32: Uint32Array<ArrayBuffer>;
  ccall(name: string, type: 'number', argumentTypes: string[], args: unknown[], options: { async: true }): Promise<number>;
  _malloc(size: number): number; _free(pointer: number): void;
  UTF8ToString(pointer: number): string;
  lengthBytesUTF8(text: string): number;
  stringToUTF8(text: string, pointer: number, size: number): void;
  _zsqlite_browser_error(): number;
  _sqlite3_close(database: number): number;
  _sqlite3_db_status(database: number, operation: number, current: number, highwater: number, reset: number): number;
  _sqlite3_errmsg(database: number): number;
  _sqlite3_stmt_readonly(statement: number): number;
  _sqlite3_finalize(statement: number): number;
  _sqlite3_reset(statement: number): number;
  _sqlite3_clear_bindings(statement: number): number;
  _sqlite3_bind_parameter_count(statement: number): number;
  _sqlite3_bind_parameter_name(statement: number, index: number): number;
  _sqlite3_bind_null(statement: number, index: number): number;
  _sqlite3_bind_int64(statement: number, index: number, value: bigint): number;
  _sqlite3_bind_double(statement: number, index: number, value: number): number;
  _sqlite3_bind_text(statement: number, index: number, pointer: number, size: number, destructor: number): number;
  _sqlite3_bind_blob(statement: number, index: number, pointer: number, size: number, destructor: number): number;
  _sqlite3_column_count(statement: number): number;
  _sqlite3_column_name(statement: number, column: number): number;
  _sqlite3_column_type(statement: number, column: number): number;
  _sqlite3_column_int64(statement: number, column: number): bigint;
  _sqlite3_column_double(statement: number, column: number): number;
  _sqlite3_column_blob(statement: number, column: number): number;
  _sqlite3_column_text(statement: number, column: number): number;
  _sqlite3_column_bytes(statement: number, column: number): number;
}
