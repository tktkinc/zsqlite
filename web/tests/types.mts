import { openDatabase, httpStorage, moduleStorage, type StorageAdapter, type StorageFactory, type Database, type Value, type QueryMetrics, type StorageOperation } from '@zsqlite/browser';
import { openInWorker } from '@zsqlite/browser/worker';
interface Item { id: number; label: string }
const adapter: StorageAdapter = {
  async read(_path, _offset, _length) { return Uint8Array.of(1); },
  async readMany(requests) { return { value: requests.map(request => new Uint8Array(request.length)), downloadedBytes: 10, cacheReadBytes: 0, requests: 1 }; },
  async stat(_path): Promise<StorageOperation<number>> { return { value: 1, downloadedBytes: 0, cacheReadBytes: 0, requests: 0 }; },
};
const factory: StorageFactory<{ url: string }> = (_config, context) => {
  context.cache?.put('example', Uint8Array.of(1));
  return adapter;
};
async function consumer(): Promise<void> {
  const db: Database = await openDatabase({ storage: httpStorage({ url: 'https://example.com/store/', httpMode: 'get' }) });
  const rows: Item[] = await db.all<Item>('SELECT id, label FROM items');
  const row: Item | undefined = await db.get<Item>('SELECT id, label FROM items WHERE id=?', [42]);
  const stmt = await db.prepare('SELECT id, label FROM items WHERE id=:id');
  const prepared: Item[] = await stmt.all<Item>({ ':id': 42 });
  const tuple: Value | undefined = (await stmt.query({ ':id': 42 })).rows[0]?.[0];
  const metrics: QueryMetrics = await stmt.run({ ':id': 42 });
  void metrics; await stmt.finalize();
  await db.exec('SELECT 1'); await db.stats(); await db.clearCache(); await db.close();
  await openDatabase({ storage: moduleStorage(new URL('./app-storage.mjs', import.meta.url), { url: 'https://example.com/store/' }) });
  await openInWorker(adapter, { diskCacheBytes: 0 });
  void rows; void row; void prepared; void tuple; void factory;
}
void consumer;
