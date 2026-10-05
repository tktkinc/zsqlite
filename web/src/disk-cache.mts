import type { PersistentCache } from './types.mjs';
// Disposable OPFS cache. One locked append log, bounded by its physical size.
// Records validate their key and payload; damaged cache entries become misses.
const HEADER = 20;
const MAGIC = 0x3153435a; // ZCS1
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });
const crcTable = Uint32Array.from({ length: 256 }, (_, i) => {
  for (let bit = 0; bit < 8; bit++) i = (i >>> 1) ^ ((i & 1) ? 0xedb88320 : 0);
  return i >>> 0;
});
export function checksum(bytes: Uint8Array) {
  let crc = 0xffffffff;
  for (const byte of bytes) crc = (crc >>> 8) ^ crcTable[(crc ^ byte) & 255]!;
  return (crc ^ 0xffffffff) >>> 0;
}

interface Entry { at: number; keySize: number; bytes: number; total: number; crc: number }
export interface DiskHandle {
  read(buffer: Uint8Array<ArrayBuffer>, options: { at: number }): number;
  write(buffer: Uint8Array<ArrayBuffer>, options: { at: number }): number;
  getSize(): number; truncate(size: number): void; flush(): void; close(): void;
}
export interface DiskCacheOptions { url: string; diskCacheBytes?: number; cacheKey?: string; headers?: Record<string, string>; withCredentials?: boolean }
export interface DiskCacheResult { cache: DiskCache | null; reason: string | null }
export class DiskCache implements PersistentCache {
  handle: DiskHandle | null;
  budget: number;
  entries = new Map<string, Entry>();
  liveBytes = 0; size = 0; hits = 0; readBytes = 0;
  error: string | null = null;
  constructor(handle: DiskHandle, budget: number) {
    this.handle = handle;
    this.budget = budget;
    try { this.scan(); } catch (error) { this.disable(error); }
  }
  read(bytes: Uint8Array<ArrayBuffer>, at: number) {
    if (this.handle!.read(bytes, { at }) !== bytes.length) throw new Error('Short disk-cache read');
  }
  write(bytes: Uint8Array, at: number) {
    if (this.handle!.write(bytes.buffer instanceof ArrayBuffer ? bytes as Uint8Array<ArrayBuffer> : new Uint8Array(bytes), { at }) !== bytes.length) throw new Error('Short disk-cache write');
  }
  scan() {
    const size = this.handle!.getSize();
    const header = new Uint8Array(HEADER);
    while (this.size + HEADER <= size) {
      this.read(header, this.size);
      const view = new DataView(header.buffer);
      const keySize = view.getUint32(4, true);
      const bytes = view.getUint32(8, true);
      const total = HEADER + keySize + bytes;
      if (view.getUint32(0, true) !== MAGIC || keySize < 1 || keySize > 1024
          || bytes < 1 || total > this.budget || this.size + total > size) break;
      const keyBytes = new Uint8Array(keySize);
      this.read(keyBytes, this.size + HEADER);
      if (checksum(keyBytes) !== view.getUint32(16, true)) break;
      const key = decoder.decode(keyBytes);
      this.delete(key);
      this.entries.set(key, { at: this.size, keySize, bytes, total, crc: view.getUint32(12, true) });
      this.liveBytes += total;
      this.size += total;
    }
    // An interrupted append is disposable. Keep only the complete prefix.
    this.handle!.truncate(this.size);
    if (this.size > this.budget) this.compact(0);
  }
  delete(key: string) {
    const entry = this.entries.get(key);
    if (entry) { this.liveBytes -= entry.total; this.entries.delete(key); }
  }
  get(key: string): Uint8Array<ArrayBuffer> | null {
    if (!this.handle) return null;
    const entry = this.entries.get(key);
    if (!entry) return null;
    try {
      const bytes = new Uint8Array(entry.bytes);
      this.read(bytes, entry.at + HEADER + entry.keySize);
      if (checksum(bytes) !== entry.crc) { this.delete(key); return null; }
      this.entries.delete(key);
      this.entries.set(key, entry);
      this.hits++;
      this.readBytes += bytes.length;
      return bytes;
    } catch (error) { this.disable(error); return null; }
  }
  compact(incoming: number) {
    const target = Math.max(0, Math.min(this.budget - incoming, Math.floor(this.budget * 0.8)));
    while (this.liveBytes > target) this.delete(this.entries.keys().next().value!);
    // Copy retained records forward in physical order. This never overwrites an
    // unread record; a crash during compaction merely invalidates cached bytes.
    const ordered = [...this.entries.values()].sort((a, b) => a.at - b.at);
    const buffer = new Uint8Array(64 * 1024);
    let position = 0;
    for (const entry of ordered) {
      if (entry.at !== position) {
        for (let copied = 0; copied < entry.total; copied += buffer.length) {
          const chunk = buffer.subarray(0, Math.min(buffer.length, entry.total - copied));
          this.read(chunk, entry.at + copied);
          this.write(chunk, position + copied);
        }
      }
      entry.at = position;
      position += entry.total;
    }
    this.size = position;
    this.handle!.truncate(position);
  }
  put(key: string, bytes: Uint8Array) {
    if (!this.handle || this.entries.has(key) || !bytes.length) return;
    const keyBytes = encoder.encode(key);
    const total = HEADER + keyBytes.length + bytes.length;
    if (keyBytes.length > 1024 || bytes.length > 0xffffffff || total > this.budget) return;
    try {
      if (this.size + total > this.budget) this.compact(total);
      const header = new Uint8Array(HEADER);
      const view = new DataView(header.buffer);
      const crc = checksum(bytes);
      [MAGIC, keyBytes.length, bytes.length, crc, checksum(keyBytes)].forEach((value, i) => view.setUint32(i * 4, value, true));
      // Commit the header last. A torn record cannot claim uninitialized data.
      this.write(keyBytes, this.size + HEADER);
      this.write(bytes, this.size + HEADER + keyBytes.length);
      this.write(header, this.size);
      this.entries.set(key, { at: this.size, keySize: keyBytes.length, bytes: bytes.length, total, crc });
      this.size += total;
      this.liveBytes += total;
    } catch (error) { this.disable(error); }
  }
  flush() {
    if (this.handle) {
      try { this.handle.flush(); } catch (error) { this.disable(error); }
    }
  }
  clear() {
    if (this.handle) {
      try { this.handle!.truncate(0); this.handle.flush(); this.entries.clear(); this.size = this.liveBytes = 0; }
      catch (error) { this.disable(error); }
    }
  }
  disable(error: unknown) {
    this.error = error instanceof Error ? error.message : String(error);
    try { this.handle?.close(); } catch { /* Best-effort cache only. */ }
    this.handle = null;
  }
  close() { this.flush(); this.handle?.close(); this.handle = null; }
}

export async function openDiskCache(options: DiskCacheOptions): Promise<DiskCacheResult> {
  const requested = options.diskCacheBytes ?? 2 * 1024 ** 3;
  if (!requested) return { cache: null, reason: 'Disabled' };
  let handle: FileSystemSyncAccessHandle | undefined;
  try {
    if (!navigator.storage?.getDirectory) throw new Error('This browser does not support disk caching');
    const root = await navigator.storage.getDirectory();
    const directory = await root.getDirectoryHandle('zsqlite-cache-v1', { create: true });
    const scope = JSON.stringify([options.cacheKey ?? options.url, options.headers ?? {}, Boolean(options.withCredentials)]);
    const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', encoder.encode(scope)));
    const name = [...digest].map(byte => byte.toString(16).padStart(2, '0')).join('');
    const file = await directory.getFileHandle(`${name}.cache`, { create: true });
    handle = await file.createSyncAccessHandle();
    const { quota, usage } = await navigator.storage.estimate();
    const available = Number.isFinite(quota) ? Math.max(0, (quota ?? requested) - (usage ?? 0) + handle.getSize()) : requested;
    const budget = Math.min(requested, Math.floor(available * 0.8));
    if (budget < 4096) throw new Error('Insufficient browser storage quota');
    const cache = new DiskCache(handle, budget);
    return { cache, reason: cache.error };
  } catch (error) {
    try { handle?.close(); } catch { /* Failed/locked cache does not block SQL. */ }
    return { cache: null, reason: error instanceof Error && error.name === 'NoModificationAllowedError' ? 'Disk cache is in use by another tab' : error instanceof Error ? error.message : String(error) };
  }
}
