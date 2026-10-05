// Application-owned backend, with its own cache and per-operation I/O reporting.
export function createStorage({ url, batch = false }) {
  const objects = new Map();
  async function download(path) {
    const cached = objects.get(path);
    if (cached) return { bytes: cached, cached: true, downloadedBytes: 0, requests: 0 };
    const response = await fetch(new URL(path, url));
    if (response.status === 404) return { bytes: null, cached: false, downloadedBytes: 0, requests: 1 };
    if (!response.ok) throw new Error(`Storage HTTP ${response.status}`);
    const bytes = new Uint8Array(await response.arrayBuffer());
    objects.set(path, bytes);
    return { bytes, cached: false, downloadedBytes: bytes.length, requests: 1 };
  }
  const adapter = {
    async stat(path) {
      const result = await download(path);
      return { value: result.bytes?.length ?? -1, downloadedBytes: result.downloadedBytes, cacheReadBytes: 0, requests: result.requests };
    },
    async read(path, offset, length) {
      const result = await download(path);
      const value = result.bytes?.subarray(offset, offset + length) ?? null;
      return { value, downloadedBytes: result.downloadedBytes, cacheReadBytes: result.cached ? value?.length ?? 0 : 0, requests: result.requests };
    },
  };
  if (batch) {
    const read = adapter.read;
    adapter.read = (path, offset, length) => {
      if (path !== 'catalog-head') throw new Error('Object reads must arrive as batches');
      return read(path, offset, length);
    };
    adapter.readMany = async requests => {
      const results = [];
      for (const request of requests) results.push(await read(request.path, request.offset, request.length));
      return { value: results.map(result => result.value), downloadedBytes: results.reduce((sum,result)=>sum+result.downloadedBytes,0),
        cacheReadBytes: results.reduce((sum,result)=>sum+result.cacheReadBytes,0), requests: results.reduce((sum,result)=>sum+result.requests,0) };
    };
  }
  return adapter;
}
