import { openFromSource } from './engine.mjs';
import type { Database, Statement } from './types.mjs';
import type { Request, Reply } from './protocol.mjs';
const worker = self as unknown as DedicatedWorkerGlobalScope;
let database: Database | undefined;
let opening = false;
let sequence = 0;
const statements = new Map<number, Statement>();
let queue: Promise<unknown> = Promise.resolve();
async function dispatch(request: Request): Promise<unknown> {
  if (request.action === 'open') {
    if (opening) throw new Error('This worker already opened a database');
    opening = true;
    database = await openFromSource(request.options, progress => worker.postMessage({ kind: 'progress', progress }));
    return;
  }
  if (!database) throw new Error('Database is not open');
  switch (request.action) {
    case 'prepare': {
      const stmt = await database.prepare(request.sql);
      const id = ++sequence;
      statements.set(id, stmt);
      return { id, columns: stmt.columns };
    }
    case 'statement': {
      const stmt = statements.get(request.statement);
      if (!stmt) throw new Error('Statement is finalized');
      if (request.mode === 'discard') { const metrics = await stmt.run(request.parameters); return { columns: [...stmt.columns], rows: [], metrics }; }
      // First-row queries stop stepping as soon as a row is available.
      if (request.mode === 'first') {
        return stmt.query(request.parameters, { maxRows: request.maxRows, firstRow: true });
      }
      return stmt.query(request.parameters, { maxRows: request.maxRows });
    }
    case 'finalize': { await statements.get(request.statement)?.finalize(); statements.delete(request.statement); return; }
    case 'query': return database.query(request.sql, request.parameters, { maxRows: request.maxRows, firstRow: request.firstRow });
    case 'exec': return database.exec(request.sql, request.parameters);
    case 'stats': return database.stats();
    case 'clearCache': return database.clearCache();
    case 'close': { await database.close(); database = undefined; statements.clear(); return; }
  }
}
worker.onmessage = ({ data }: MessageEvent<Request>) => {
  queue = queue.then(async () => {
    let reply: Reply;
    try { reply = { id: data.id, result: await dispatch(data) }; }
    catch (error) { reply = { id: data.id, error: error instanceof Error ? error.message : String(error) }; }
    worker.postMessage(reply);
  });
};
