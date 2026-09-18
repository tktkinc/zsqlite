# Transcript performance run — 2026-09-13

Source: `transcripts-v1-latest-20260912-1805-checkpointed.sqlite` (608.88 MiB).
Source BLAKE3: `29b78db6bbd26fe3242beaae94e17b3e5a2777d14bc3d55449bdf34935677315`.

Equivalent command (replace the source path with your local copy):

```sh
cargo bench --offline --no-default-features --features static --bench transcript_replay -- \
  --source /path/to/transcripts-v1-latest-20260912-1805-checkpointed.sqlite \
  --output experiments/transcript-perf-20260913
```

macOS 26.4.1, arm64; Rust 1.94.1; optimized static SQLite build. Release compilation took 54.39 s and is excluded from all benchmark timings.

480 deterministic indexed SQL cases, three shuffled rounds per read phase. Each case has a first execution and an immediate repeat. Timings include SQL preparation, execution, and row fingerprinting; transaction BEGIN/COMMIT and statistics collection are outside the query timer. Connections are fresh handles in one process. The operating-system cache is not flushed. Open timing includes benchmark PRAGMAs and attaching the read-only source; it does not measure storage bootstrap.

Native uses a 10 MiB SQLite RAM pager. VFS profiles use a 2 MiB SQLite RAM pager plus an 8 MiB extracted-page disk cache. These are not equal RAM budgets. mmap is disabled. All VFS profiles use Zstandard level 3 and the same dictionary policy.

## Initial sealed snapshot

| Profile | First total (ms) | First p50 (ms) | First p95 (ms) | Repeat total (ms) | Median open (ms) |
|---|---:|---:|---:|---:|---:|
| native | 116.960 | 0.013 | 0.297 | 40.881 | 1.373 |
| page | 325.218 | 0.064 | 0.944 | 49.398 | 592.567 |
| 64k | 419.974 | 0.070 | 1.125 | 46.358 | 180.830 |
| 1m | 2344.640 | 0.085 | 5.635 | 48.091 | 134.880 |

## After eight seals, before compaction

| Profile | First total (ms) | First p50 (ms) | First p95 (ms) | Repeat total (ms) | Median open (ms) |
|---|---:|---:|---:|---:|---:|
| native | 48.298 | 0.012 | 0.122 | 40.089 | 1.762 |
| page | 303.426 | 0.057 | 0.855 | 50.069 | 1342.042 |
| 64k | 401.244 | 0.058 | 1.037 | 44.424 | 537.342 |
| 1m | 2571.483 | 0.085 | 6.717 | 53.281 | 469.208 |

## After the first compaction

| Profile | First total (ms) | First p50 (ms) | First p95 (ms) | Repeat total (ms) | Median open (ms) |
|---|---:|---:|---:|---:|---:|
| native | 54.979 | 0.014 | 0.134 | 44.265 | 1.631 |
| page | 306.821 | 0.060 | 0.939 | 46.427 | 388.830 |
| 64k | 397.161 | 0.071 | 1.097 | 44.497 | 119.626 |
| 1m | 3041.780 | 0.152 | 7.663 | 61.175 | 134.342 |

## After churn, compaction, and collection

| Profile | First total (ms) | First p50 (ms) | First p95 (ms) | Repeat total (ms) | Median open (ms) |
|---|---:|---:|---:|---:|---:|
| native | 50.571 | 0.013 | 0.127 | 43.025 | 1.507 |
| page | 347.169 | 0.065 | 1.055 | 49.610 | 384.717 |
| 64k | 457.238 | 0.085 | 1.239 | 48.512 | 133.225 |
| 1m | 2470.604 | 0.144 | 6.339 | 48.373 | 105.000 |

## Setup and storage

| Profile | Build/setup (s) | Initial complete (MiB) | Final complete (MiB) | Entire replay (s) |
|---|---:|---:|---:|---:|
| native | 0.233 | 608.88 | 609.16 | 10.376 |
| page | 155.055 | 104.28 | 113.16 | 1420.227 |
| 64k | 39.258 | 57.76 | 65.22 | 233.407 |
| 1m | 30.199 | 40.93 | 48.06 | 167.161 |

Build includes snapshot conversion/copying, initial WAL setup, checkpoint, and initial flush. VFS conversion writes the active pagefile, trains dictionaries, compresses/seals, and runs full verification twice. It is not the latency of opening an already sealed database or bootstrapping from storage.

The full replay includes eight mutate/restore epochs, metadata compaction, four additional epochs with a retained root, retention release, bounded/full collection, export and integrity verification. Total wall time includes those operations and validation, so it is not a query-throughput metric.

A two-second stack sample was taken during page-profile sealing to investigate the setup question; maintenance timing for that run includes sampling overhead. The sample caught GC root tracing reopening manifests and computing full page-map content roots. The release executable is stripped; sampled instructions were matched to the retained Rust object and its symbol table to identify `ViewMetadata::content_root`, `PinnedView::open_inner`, and `CatalogGuard::trace_logical`. The sample is not a phase-by-phase breakdown of the earlier import.

## Validation

Completed profiles: native, page, 64k, 1m.
Recorded query executions: 69,120. Recorded table/FTS verification fingerprints: 156.

The harness compares every query result with native SQLite, compares exported table and FTS fingerprints with the source, checks integrity and foreign keys, and confirms the original source hash after all profiles finish.

## Seal operation timings

| Frame layout | Seals | Median (s) | Total (s) |
|---|---:|---:|---:|
| page | 12 | 42.286 | 526.164 |
| 64k | 12 | 6.318 | 80.032 |
| 1m | 12 | 3.426 | 48.674 |

Sealing includes automatic collection. Benchmark occupancy snapshots and query verification are outside these operation timers.

All four profiles completed successfully, including the final source-hash check. The CSVs were independently checked for matching row counts, result byte counts, and fingerprints across profiles.

Raw results are local artifacts under `experiments/transcript-perf-20260913/`,
excluded from the repository: `run.txt`, `query-summary.csv`, `reads.csv`,
`connections.csv`, `storage.csv`, and `verification.csv`.
