import type { Parameters, QueryResult, RowsResult, Row, Statement } from './types.mjs';
export function objectRows<T extends object = Row>(result: RowsResult): T[] {
  return result.rows.map(row => Object.fromEntries(result.columns.map((name, i) => [name, row[i]!])) as T);
}
export function statementApi(columns: readonly string[], execute: (parameters: Parameters, maxRows: number, mode: 'all' | 'first' | 'discard') => Promise<QueryResult>, finalize: () => Promise<void>): Statement {
  let finalized = false;
  const run = (parameters: Parameters, maxRows: number, mode: 'all' | 'first' | 'discard') => finalized ? Promise.reject(new Error('Statement is finalized')) : execute(parameters, maxRows, mode);
  return {
    columns: Object.freeze([...columns]),
    query: (parameters = [], { maxRows = 10000, firstRow = false } = {}) => run(parameters, maxRows, firstRow ? 'first' : 'all'),
    all: async <T extends object = Row>(parameters: Parameters = [], { maxRows = 10000, firstRow = false } = {}) => objectRows<T>(await run(parameters, maxRows, firstRow ? 'first' : 'all')),
    get: async <T extends object = Row>(parameters: Parameters = []) => objectRows<T>(await run(parameters, 1, 'first'))[0],
    run: async (parameters = []) => (await run(parameters, 0, 'discard')).metrics,
    finalize: async () => { if (finalized) return; finalized = true; await finalize(); },
  };
}
