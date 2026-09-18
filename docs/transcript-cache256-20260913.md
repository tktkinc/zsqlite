# Transcript queries with equal RAM pagers and a larger disk cache

Run date: 2026-09-13. Each configuration uses a 10 MiB SQLite RAM page-cache budget. VFS configurations additionally use a 256 MiB extracted-page disk cache, up from 8 MiB in the preceding replay. mmap is disabled.

The run reuses the closed, compacted databases verified by the [full transcript replay](transcript-perf-20260913.md). Its native snapshot is 638,750,720 bytes (609.16 MiB), reflecting the same completed mutate/restore workload as the managed layouts. The original source was `transcripts-v1-latest-20260912-1805-checkpointed.sqlite` (608.88 MiB). No import, sealing, or write workload was repeated.

The native input is a hard link named `input.sqlite` to the preceding run’s `native.sqlite`. The query benchmark updates the managed cache policies in those existing bundles; their sealed payloads and query results are unchanged.

Command:

```sh
cargo bench --offline --no-default-features --features static --bench transcript_queries -- \
  --bundles experiments/transcript-perf-20260913 \
  --profiles page,64k,1m --sqlite-cache-mib 10 --page-cache-mib 256
```

Each profile executes 480 deterministic indexed SQL cases in three fresh worker processes, with one shuffled stream per process. Every case runs once and then immediately repeats. Each total below sums 1,440 executions. The operating-system cache is not flushed, and the VFS extracted-page cache starts empty in each process.

Query timing includes statement preparation, execution, and owned result collection. Fingerprinting and validation happen afterward. This differs from the earlier replay, which includes fingerprinting in its query timer and uses a different shuffle phase; the earlier milliseconds are not a controlled before/after comparison. Open timings include opening SQLite and applying benchmark PRAGMAs, without the source ATTACH used by the full replay. Bootstrap is not measured.

| Configuration | First total (ms) | First p50 (ms) | First p95 (ms) | Repeat total (ms) | Median open (ms) |
|---|---:|---:|---:|---:|---:|
| Native SQLite | 27.565 | 0.008 | 0.049 | 18.543 | 0.653 |
| VFS, 4 KiB frames | 230.141 | 0.035 | 0.620 | 31.252 | 369.969 |
| VFS, 64 KiB frames | 332.649 | 0.021 | 1.060 | 29.431 | 114.659 |
| VFS, 1 MiB frames | 752.817 | 0.020 | 4.895 | 29.858 | 98.654 |

The requested pages fit within the common RAM pager budget in this workload: observed peak SQLite cache usage was 4.19 MiB natively and 4.88 MiB with VFS. In the 4 KiB layout, all 3,492 VFS page requests were first-time misses. Multi-page frames produced extracted-page cache hits for pages decoded by earlier requests.

| VFS frames | Requested (MiB) | Fetched (MiB) | Inflated (MiB) | Disk page cache hits | Disk page cache misses |
|---|---:|---:|---:|---:|---:|
| page | 13.641 | 3.449 | 13.641 | 0 | 3,492 |
| 64k | 13.641 | 10.359 | 45.691 | 2,757 | 735 |
| 1m | 13.641 | 25.110 | 120.480 | 3,369 | 123 |

All 11,520 query executions matched native row counts, result lengths, and fingerprints. Query plans passed the indexed-access checks. All four storage records confirm a 10 MiB RAM pager; each managed record confirms a 256 MiB disk cache.

The query benchmark now separates `--sqlite-cache-mib` from `--page-cache-mib`; `--frame-cache-mib` remains an alias for the disk budget. Both transcript benchmarks default to a 10 MiB RAM pager and a 256 MiB VFS disk cache. These are benchmark defaults; library storage-policy defaults are unchanged.

Raw results are local artifacts under `experiments/transcript-cache256-20260913/`,
excluded from the repository: `queries.csv`, `connections.csv`, `storage.csv`,
`plans.txt`, and `benchmark.log`.
