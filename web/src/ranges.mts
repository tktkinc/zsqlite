import type { StorageRead } from './types.mjs';

export function checkReads(requests: readonly StorageRead[]): void {
  if (!Array.isArray(requests) || !requests.length || requests.length > 4096) throw new Error('Invalid storage batch');
  let total = 0;
  for (const request of requests) {
    if (request.path !== 'catalog-head' && !/^objects\/[a-f0-9]{64}\.(blob|segment|dict|index)$/.test(request.path)) throw new Error('Invalid storage path');
    if (!Number.isSafeInteger(request.offset) || request.offset < 0 || !Number.isSafeInteger(request.length) || request.length < 1
        || !Number.isSafeInteger(request.offset + request.length)) throw new Error('Invalid storage read');
    total += request.length;
    if (total > 64 * 1024 ** 2) throw new Error('Storage batch exceeds 64 MiB');
  }
}

export interface RangeGroup extends StorageRead { indices: number[] }
/** Group overlapping or nearby ranges from the same object; retain result slots. */
export function groupReads(reads: readonly StorageRead[], gap: number, limit: number): RangeGroup[] {
  const order = reads.map((read, index) => ({ ...read, index })).sort((a, b) => a.path.localeCompare(b.path) || a.offset - b.offset);
  const groups: RangeGroup[] = [];
  for (const read of order) {
    const previous = groups.at(-1);
    const end = read.offset + read.length;
    if (previous?.path === read.path && read.offset <= previous.offset + previous.length + gap
        && Math.max(end, previous.offset + previous.length) - previous.offset <= limit) {
      previous.length = Math.max(end, previous.offset + previous.length) - previous.offset;
      previous.indices.push(read.index);
    } else groups.push({ path: read.path, offset: read.offset, length: read.length, indices: [read.index] });
  }
  return groups;
}
