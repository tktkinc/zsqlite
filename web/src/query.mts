import type { Parameters, Parameter, RowsResult, Value } from './types.mjs';
import type { WasmModule } from './wasm.mjs';

export function sqliteError(module: WasmModule, database: number): Error {
  return new Error(module.bucketTransport.lastError || module.UTF8ToString(module._sqlite3_errmsg(database)));
}
export async function prepare(module: WasmModule, database: number, sql: string): Promise<NativeStatement> {
  if (typeof sql !== 'string' || sql.includes('\0')) throw new Error('SQL must be a string without NUL bytes');
  const size = module.lengthBytesUTF8(sql) + 1;
  const text = module._malloc(size);
  const output = module._malloc(8);
  if (!text || !output) { if (text) module._free(text); if (output) module._free(output); throw new Error('WASM allocation failed'); }
  let statement = 0, extra = 0;
  try {
    module.bucketTransport.lastError = null;
    module.stringToUTF8(sql, text, size);
    module.HEAPU32[output >>> 2] = 0;
    const code = await module.ccall('sqlite3_prepare_v2', 'number', ['number', 'number', 'number', 'number', 'number'], [database, text, -1, output, output + 4], { async: true });
    statement = module.HEAPU32[output >>> 2]!;
    if (code !== 0) throw sqliteError(module, database);
    if (!statement) throw new Error('Expected one read-only SQL query');
    const remaining = module.HEAPU32[(output + 4) >>> 2]!;
    module.HEAPU32[output >>> 2] = 0;
    const extraCode = await module.ccall('sqlite3_prepare_v2', 'number', ['number', 'number', 'number', 'number', 'number'], [database, remaining, -1, output, 0], { async: true });
    extra = module.HEAPU32[output >>> 2]!;
    if (extraCode !== 0) throw sqliteError(module, database);
    if (extra) throw new Error('Only one SQL statement is allowed per query');
    if (!module._sqlite3_stmt_readonly(statement) || module._sqlite3_column_count(statement) === 0) throw new Error('Only read-only queries are allowed');
    const result = new NativeStatement(module, database, statement);
    statement = 0;
    return result;
  } finally {
    if (extra) module._sqlite3_finalize(extra);
    if (statement) module._sqlite3_finalize(statement);
    module._free(text); module._free(output);
  }
}
export class NativeStatement {
  readonly columns: string[];
  constructor(private module: WasmModule, private database: number, private pointer: number) {
    this.columns = Array.from({ length: module._sqlite3_column_count(pointer) }, (_, i) => module.UTF8ToString(module._sqlite3_column_name(pointer, i)));
  }
  private bind(parameters: Parameters) {
    const m = this.module, stmt = this.pointer;
    const count = m._sqlite3_bind_parameter_count(stmt);
    let values: readonly Parameter[];
    if (Array.isArray(parameters)) {
      if (parameters.length !== count) throw new Error(`Expected ${count} parameters`);
      values = parameters;
    } else if (parameters && typeof parameters === 'object') {
      const object = parameters as Readonly<Record<string, Parameter>>;
      const names = Array.from({ length: count }, (_, i) => {
        const name = m._sqlite3_bind_parameter_name(stmt, i + 1);
        if (!name) throw new Error('Use an array for positional parameters');
        return m.UTF8ToString(name);
      });
      if (Object.keys(object).length !== count || names.some(name => !Object.hasOwn(object, name))) throw new Error('Named parameters must match the SQL parameter names, including their prefixes');
      values = names.map(name => object[name]!);
    } else throw new Error('Parameters must be an array or an object');
    const check = (code: number) => { if (code !== 0) throw sqliteError(m, this.database); };
    values.forEach((value, i) => {
      const slot = i + 1;
      if (value === null) check(m._sqlite3_bind_null(stmt, slot));
      else if (typeof value === 'bigint' || typeof value === 'boolean' || (typeof value === 'number' && Number.isSafeInteger(value))) {
        const integer = BigInt(value);
        if (integer < -(1n << 63n) || integer >= 1n << 63n) throw new Error('Integer parameter is outside SQLite int64');
        check(m._sqlite3_bind_int64(stmt, slot, integer));
      } else if (typeof value === 'number' && Number.isFinite(value)) check(m._sqlite3_bind_double(stmt, slot, value));
      else if (typeof value === 'string' || value instanceof Uint8Array) {
        const bytes = typeof value === 'string' ? new TextEncoder().encode(value) : value;
        const pointer = m._malloc(Math.max(1, bytes.length));
        if (!pointer) throw new Error('WASM allocation failed');
        try {
          m.HEAPU8.set(bytes, pointer);
          check(typeof value === 'string' ? m._sqlite3_bind_text(stmt, slot, pointer, bytes.length, -1) : m._sqlite3_bind_blob(stmt, slot, pointer, bytes.length, -1));
        } finally { m._free(pointer); }
      } else throw new Error(`Unsupported parameter at index ${i}`);
    });
  }
  async execute(parameters: Parameters = [], maxRows = 10000, mode: 'all' | 'first' | 'discard' = 'all'): Promise<RowsResult> {
    if (!this.pointer) throw new Error('Statement is finalized');
    if (!Number.isSafeInteger(maxRows) || maxRows < 0) throw new Error('maxRows must be a nonnegative integer');
    const m = this.module;
    m.bucketTransport.lastError = null;
    m._sqlite3_reset(this.pointer); m._sqlite3_clear_bindings(this.pointer);
    const rows: Value[][] = [];
    try {
      this.bind(parameters);
      for (;;) {
        const code = await m.ccall('sqlite3_step', 'number', ['number'], [this.pointer], { async: true });
        if (code === 101) break;
        if (code !== 100) throw sqliteError(m, this.database);
        if (mode === 'discard') continue;
        if (rows.length >= maxRows) throw new Error(`Query exceeded maxRows (${maxRows}); use LIMIT or increase maxRows`);
        rows.push(this.columns.map((_, i) => {
          const type = m._sqlite3_column_type(this.pointer, i);
          if (type === 5) return null;
          if (type === 1) {
            const value = m._sqlite3_column_int64(this.pointer, i);
            return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value;
          }
          if (type === 2) return m._sqlite3_column_double(this.pointer, i);
          const pointer = type === 4 ? m._sqlite3_column_blob(this.pointer, i) : m._sqlite3_column_text(this.pointer, i);
          const size = m._sqlite3_column_bytes(this.pointer, i);
          const bytes = m.HEAPU8.slice(pointer, pointer + size);
          return type === 4 ? bytes : new TextDecoder().decode(bytes);
        }));
        if (mode === 'first') break;
      }
      return { columns: [...this.columns], rows };
    } finally { m._sqlite3_reset(this.pointer); m._sqlite3_clear_bindings(this.pointer); }
  }
  finalize() { if (this.pointer) this.module._sqlite3_finalize(this.pointer); this.pointer = 0; }
}
