# Transcript replay benchmark

The transcript benchmark compares native SQLite with page-sized, fixed 64 KiB,
and fixed 1 MiB frames. It runs real parameterized SQL against every usable
primary-key, rowid, secondary-index, and FTS access path discovered in the
source schema. Results are fingerprinted against native SQLite.

Every profile uses a 10 MiB SQLite RAM page cache. Managed profiles additionally
use a 256 MiB extracted-page disk cache. The query-only benchmark exposes these
independently as `--sqlite-cache-mib` and `--page-cache-mib`.

The source must be a closed SQLite database with no nonempty WAL or journal.
The benchmark copies it into a disposable output directory and never mutates the
original.

## Workload

Ordinary application tables are discovered through `pragma_table_list`; SQLite
and FTS shadow tables are excluded. Empty tables, NULL-only keys, partial
indexes, and expression indexes are reported rather than silently sampled.
`EXPLAIN QUERY PLAN` must show an indexed search for ordinary tables or an FTS5
virtual-table index. Real key values are sampled at deterministic positions and
executed in seeded shuffled order.

The current transcript schema also gets a bounded mutation plan spanning session,
event, message, tool, agent-work, conversation, ordinary-index, partial-index,
and trigger-maintained FTS data. Alternate epochs mutate and restore the exact
typed source values without changing primary or foreign keys.

After the initial reads, the benchmark runs churn/seal epochs, compacts metadata,
retains a pre-churn root, runs four more churn/seal epochs, releases the root,
and measures bounded and full collection. Frame and pack occupancy distinguish
fully live, partially obsolete, and fully obsolete units, so fixed frame sizes
can be compared for both random-read amplification and GC readiness.

## Run it

First run the bounded, read-only preflight:

```sh
cargo bench --offline --no-default-features --features static --bench transcript_replay -- \
  --source /path/to/closed-transcripts.sqlite --preflight
```

Then run the full benchmark:

```sh
cargo bench --offline --no-default-features --features static --bench transcript_replay -- \
  --source /path/to/closed-transcripts.sqlite
```

Useful controls are `--profiles`, `--random-samples`, `--mutation-rows`, and
`--churn-epochs`. Profiles must begin with `native`; managed choices are `page`,
`64k`, and `1m`. Churn epochs must be an even count from 4 through 32 so the
last epoch restores source values exactly. The legacy `rec/raw/ft/sess/dim`
incremental replay additionally accepts `--batch-rows`, `--seal-batches`, and
`--limit`.

For a fixed-layout query-only matrix, first create bundles and then run fresh
worker processes:

```sh
cargo bench --offline --no-default-features --features static --bench frame_layout -- \
  --source /path/to/closed-transcripts.sqlite --build-only
cargo bench --offline --no-default-features --features static --bench transcript_queries -- \
  --bundles /path/printed/by/the/first/command
```

## Output

- `reads.csv`: first/repeat query timings, fingerprints, and read/decode/cache counters.
- `connections.csv`: connection-open latency per profile, phase, and round.
- `storage.csv`: object, dictionary, and manifest summaries.
- `frames.csv`: decoded frame-size and compression-mode distribution.
- `gc-layout.csv`: page, frame, pack, retention, and collection readiness.
- `index-coverage.csv`: included tables and skipped access paths.
- `mutation-coverage.csv`: mutated columns, indexes/FTS structures, and row counts.
- `query-plans.txt`: validated query plans for every profile.
- `verification.csv`: source/export/integrity comparisons.
- `run.txt`: source hash, policies, limits, and timing metadata.

Summarize a completed directory with:

```sh
cd /path/to/replay-output
sqlite3 :memory: < /path/to/zsqlite/benches/replay_summary.sql
```

Generated databases and result directories are Git-ignored. Keep the immutable
source snapshot; disposable copies and reports can be deleted after analysis.
