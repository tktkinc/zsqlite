# zsqlite

`zsqlite` is a SQLite VFS that stores the main database image as independently
readable page frames in transactional, LTX-style segments. SQLite journals and
WAL files continue to use the host VFS.

The current format is V5 and intentionally has no compatibility path for older
zsqlite formats.

## Storage model

A database is a small control file and a sibling object directory:

```text
database.zsqlite
database.zsqlite.d/
  active/
    <writer-id>.zactive
  segments/
    <start-txid>-<end-txid>-<history>-<physical>.zseg
  roots/
    <head-txid>-<history>-<catalog>.zroot
  locks/
```

The `.zsqlite` file has an identity header and two alternating 4 KiB root
sectors. A root selects an immutable catalog and, while writes are accumulating,
the exact committed position in one active segment. The catalog is an exact,
ordered list of immutable segments, not a directory-listing hint.

Each active or sealed segment contains:

- its database identity, TXID range, and parent history hash;
- every Zstandard dictionary required by its page frames;
- raw or independently decompressible per-page frames;
- commit records in an active segment;
- a sorted live-page index and complete page-to-last-TXID map when sealed;
- a logical content root and physical BLAKE3 digest.

TXID zero in a full page map represents a zero-filled sparse page. This
matches native file semantics when a WAL checkpoint extends the main image
before copying every newly addressable page.

Fixed-width hexadecimal TXID prefixes preserve LTX-style lexical range order.
The ending history hash remains unchanged by compaction; only the physical
digest changes.

The detailed format and design invariants are in
[`ltx-style-segments.md`](ltx-style-segments.md).

## Commit and durability behavior

`xWrite` immediately writes the final version of each dirty SQLite page to the
active segment. Repeated writes to the same page before publication overwrite
or replace that uncommitted frame. A publication appends one commit record,
optionally syncs the active file, and then switches one alternating root sector.
The root switch is the visibility point.

With `synchronous=OFF`, the records and root are still structurally complete,
but zsqlite does not ask the operating system to make them power-loss durable.
With SQLite sync enabled, the active data is synced before the root that names
it. Rollback discards unreferenced staged frames.

In WAL mode, WAL writes are passed through. A zsqlite TXID therefore describes
an atomic main-database checkpoint batch, not an individual SQL transaction in
the WAL. `SQLITE_FCNTL_CKPT_DONE` and checkpoint-lock release publish those main
image writes.

Committed frames needed by another process or by the previous root are not
reclaimed. Once unpinned, dead active ranges become reusable and aligned
interiors can be hole-punched. Recovery truncates an uncommitted tail only while
holding the publication lock.

## Dictionaries and compaction

An empty database starts with raw page frames. zsqlite keeps a discardable,
bounded reservoir of recently changed pages and trains a 64 KiB dictionary from
up to 32 MiB of samples. A candidate is promoted at a segment boundary only
when held-out samples improve by at least 5%, repay the dictionary bytes, meet
the configured page-churn threshold, and satisfy the promotion cooldown.

Offline conversion performs the same kind of training pass before writing page
frames when the source is large enough.

Sealing does not decompress or recompress pages. Compaction copies live raw or
Zstandard frame payloads byte-for-byte, rewrites only structural offsets and
dictionary selectors, and publishes a new snapshot segment beginning at TXID
1. The compacted segment keeps the existing ending history hash.

## Rust API

```rust,no_run
let info = zsqlite::inspect("app.zsqlite")?;
zsqlite::verify("app.zsqlite")?;
zsqlite::flush("app.zsqlite")?;
zsqlite::compact("app.zsqlite")?;

zsqlite::convert_to_zsqlite("app.db", "app.zsqlite")?;
zsqlite::export_to_sqlite("app.zsqlite", "restored.db")?;

# Ok::<(), zsqlite::StoreError>(())
```

`configure()` persists settle, maximum-staleness, local heat, GC, and adaptive
dictionary policy. Defaults are a 5 minute settle interval, 1 hour maximum
active age, 24 hour hot horizon, 64 KiB dictionaries, a 32 MiB reservoir, 5%
minimum held-out improvement, 25% churn, and a 24 hour promotion cooldown.

## CLI

```text
zsqlite inspect database.zsqlite
zsqlite verify database.zsqlite
zsqlite flush database.zsqlite
zsqlite compact database.zsqlite
zsqlite convert database.db database.zsqlite
zsqlite export database.zsqlite database.db
```

## Performance checks

The VFS benchmark compares native SQLite and zsqlite under both SQLite page
cache pressure and a cache-resident profile. Random IDs are generated
deterministically but are not a single repeated page; the sequential warmup is
reported explicitly.

```sh
cargo bench --no-default-features --features static \
  --bench vfs_performance -- --quick
```

Use `--pressure-cache-mib` and `--resident-cache-mib` to change the two SQLite
cache profiles. `--rows`, `--reads`, `--updates`, `--payload-bytes`,
`--page-size`, and `--samples` control workload size.

The separate dictionary experiment accepts an ordinary, checkpointed SQLite
database and compares 64 KiB frames with per-page frames with and without a
shared dictionary:

```sh
cargo bench --bench page_dictionary -- test.db
```

## Validation

```sh
cargo test --no-default-features --features static
cargo clippy --all-targets --no-default-features --features static -- -D warnings
```

The suite covers every SQLite page size, rollback modes, WAL checkpoints,
online backup, concurrent readers and writers, multi-process creation, stale
writers, sync-off structural commits, and subprocess crash recovery.
