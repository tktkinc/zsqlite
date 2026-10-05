import type { WasmModule, WasmStorage } from './wasm.mjs';
import type { DiskCache } from './disk-cache.mjs';
export default function createModule(options: { bucketTransport: WasmStorage; diskCache: DiskCache | null }): Promise<WasmModule>;
