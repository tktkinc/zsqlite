# Garbage collection with local disk and object storage

`gc_object_storage` measures request counts and elapsed time for pack maintenance,
unreachable-object collection, a no-op maintenance pass, and a second collection
after overwriting all rows. It uses real local files and a simulated remote
`MemoryBackend`, **not S3**. The intended durability model is documented in
[tiered storage](tiered-storage.md).

The default workload has 8,417,280 logical bytes: four initial seals, followed by
an overwrite of 75% of rows. It leaves four partially live source packs and 518
live page frames to copy, containing approximately 1.57 MB of compressed payload.
`fragmented` preserves every fourth row; `clustered` preserves the last quarter
of each seal. Data is generated deterministically. Dictionary training is
disabled, and page frames require zero decompression during these passes.

## Models and measurements

`local_async` is the primary model. A `FilesystemBackend` makes local objects
and catalog publication durable. Metadata remains local; only uploaded payload
blobs are eligible for cache eviction. A cache miss downloads the complete blob.
The benchmark maintains an in-memory inventory of known object sizes, so reads
do not require remote HEADs. `cold`, `warm`, and `mixed` reset the uploaded-blob
cache before each measured operation; pending uploads always remain local.
`mixed` keeps every second object in inventory order. Cache preparation and SQL
workload setup are excluded from timings.

Maintenance and collection complete before an explicit `sync_gc` stage uploads
the pending objects and publishes one remote catalog root. A later full overwrite
and collection has its own `sync_dead` stage. The remote copy retains deleted
objects: the harness counts deferred deletion candidates but does not implement
remote GC. The final remote checkpoint is bootstrapped into a separate local
database and fully verified outside the timed region.

`write_through` measures a conservative direct-remote backend: immutable reads,
HEAD, root reads, inventory, writes, deletion, and root CAS all contact the
simulated remote. `GC_CACHE_MODES=none` exposes the core's remote request count.
This is a comparison model, not the intended local commit durability contract.

The simulated RTT is 1 ms per request. Range batches use at most 16 concurrent
requests; each range remains a separate GET unless the core coalesces it. There
is no bandwidth limit. Writes count one PUT per completed object, without
multipart overhead. OS sleep granularity adds overhead to the injected delay.
Uploaded bytes and downloaded immutable-object bytes are counted separately;
small root-record transfer bytes are excluded from those byte totals.

## Results

Results are recorded on Darwin arm64 with Rust 1.94.1, using optimized builds.
The main comparison uses the median of three serial trials, with other test and
build jobs stopped. The CSV files preserve individual observations and request
counts. Request counts are the more portable result; absolute timings depend on
the local filesystem and machine.

For the fragmented 8 MiB `local_async` workload, elapsed milliseconds are:

| Cache | Operation | Before | After | Remote GETs before → after |
| --- | --- | ---: | ---: | ---: |
| Cold | Maintain | 1,051.6 | 183.5 | 6 → 4 |
| Warm | Maintain | 833.9 | 121.5 | 0 → 0 |
| Cold | Collect | 238.3 | 61.7 | 2 → 0 |
| Warm | Collect | 242.7 | 73.3 | 0 → 0 |
| Cold | No-op maintain | 180.3 | 1.7 | 3 → 0 |
| Warm | No-op maintain | 150.3 | 1.7 | 0 → 0 |
| Cold | Fully dead collect | 239.8 | 55.6 | 1 → 0 |
| Warm | Fully dead collect | 234.6 | 63.9 | 0 → 0 |

Cold maintenance downloads 6.59 MB rather than 11.51 MB. The warm case makes no
remote requests during maintenance or collection. Across both cache states,
maintenance's local root publications remain three, with one more for collection.
The subsequent `sync_gc` uploads eight objects totaling approximately 1.884 MB
and makes **one remote root publication** for the entire pass; its median is
approximately 12.4 ms before and after. Payload compression work remains zero.

The direct-remote comparison exposes request amplification without cache fills:

| Operation | All GETs before → after | Blob GETs before → after |
| --- | ---: | ---: |
| Maintain | 28,396 → 84 | 28,316 → 4 |
| Collect | 6,229 → 61 | 6,168 → 0 |
| No-op maintain | 6,195 → 27 | 6,168 → 0 |
| Fully dead collect | 7,784 → 80 | 7,704 → 0 |

Coalescing fragmented survivors trades larger reads for fewer requests: direct
maintenance downloads 8.16 MB after the change, versus 4.69 MB before. Clustered
survivors need 3.27 MB after the change. A cold whole-blob cache downloads the
same four source blobs in either case. These numbers include metadata reads.

A single larger cold-cache run used 33,636,352 logical bytes and eight sparse
source packs. Maintenance copied 2,067 frames without decoding, made eight blob
GETs totaling 26.29 MB, and took 388.9 ms. Collection took 104.8 ms with no remote
GETs; no-op maintenance took 1.8 ms with no remote GETs. The separate GC sync
uploaded nine objects totaling 7.65 MB, published one root, and took 15.1 ms.
This confirms request growth follows the selected source packs in this workload.

Raw data:

- [Three serial timing trials](benchmarks/gc-local-async.csv).
- [Both survivor patterns and mixed caches](benchmarks/gc-local-async-exploratory.csv).
- [Direct remote request accounting](benchmarks/gc-direct-object-exploratory.csv).
- [Single 32 MiB cold-cache run](benchmarks/gc-local-async-32mib.csv).

The exploratory files include single timings collected while other work was
running; they support request-count comparisons, not the median timing table.

## Reproduce

```sh
cargo bench --bench gc_object_storage --no-default-features --features static --no-run

# Intended local-first model; default 8 MiB workload, both survivor patterns.
GC_MODEL=local_async GC_LATENCY_US=1000 \
  cargo bench --bench gc_object_storage --no-default-features --features static

# Main timing comparison: run this three times, with no concurrent test/build jobs.
GC_MODEL=local_async GC_CACHE_MODES=cold,warm GC_PATTERNS=fragmented \
  GC_MIB=8 GC_LATENCY_US=1000 GC_READ_CONCURRENCY=16 \
  cargo bench --bench gc_object_storage --no-default-features --features static

# Direct object-storage request accounting, without a local cache.
GC_MODEL=write_through GC_CACHE_MODES=none GC_PATTERNS=fragmented \
  GC_LATENCY_US=1000 \
  cargo bench --bench gc_object_storage --no-default-features --features static

# Larger local-first case.
GC_MODEL=local_async GC_CACHE_MODES=cold GC_PATTERNS=fragmented \
  GC_MIB=32 GC_LATENCY_US=1000 \
  cargo bench --bench gc_object_storage --no-default-features --features static
```

The before measurements used an isolated copy of the source immediately before
the GC read changes, compiled with the same benchmark harness and release
settings. The after measurements use the current implementation. No real cloud
requests or pricing assumptions are involved.

## Limits

This benchmark does not implement a restart-safe upload outbox, independent
remote retention/readers, concurrent uploads and eviction, or remote deletion
authority. Its in-memory inventory and upload queue are not a production tier
implementation. See [the remaining work](tiered-storage.md) before treating the
simulated sync as a deployable uploader.

`deleted_bytes` is the logical size of collected objects, including any payloads
already evicted from local disk. It is not a measurement of physical disk space
freed, and no remote bytes are physically deleted by `local_async`.

CPU usage, peak memory, real network throughput, throttling, multipart uploads,
and remote error/retry behavior were not measured. Collection still scans the
namespace inventory; these workloads do not establish behavior for millions of
objects. Page-frame results do not predict compression cost for partially live
multi-page frames.
