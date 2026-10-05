import type { ResolvedOptions, WorkerOptions } from './types.mjs';
export function resolveOptions(options: WorkerOptions = {}): ResolvedOptions {
  const result: ResolvedOptions = { head: options.head ?? 'main', cacheBytes: options.cacheBytes ?? 192 * 1024 ** 2,
    objectCacheBytes: options.objectCacheBytes ?? 64 * 1024 ** 2, diskCacheBytes: options.diskCacheBytes ?? 2 * 1024 ** 3,
    cacheKey: options.cacheKey };
  if (typeof result.head !== 'string' || !/^[a-zA-Z0-9._-]{1,128}$/.test(result.head)) throw new Error('Invalid head name');
  for (const name of ['cacheBytes', 'objectCacheBytes', 'diskCacheBytes'] as const) {
    if (!Number.isSafeInteger(result[name]) || result[name] < 0 || (name === 'cacheBytes' && result[name] > 0xffffffff)) throw new Error(`Invalid ${name}`);
  }
  if (result.cacheKey !== undefined && typeof result.cacheKey !== 'string') throw new Error('cacheKey must be a string');
  return result;
}
