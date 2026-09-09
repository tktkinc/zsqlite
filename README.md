# zsqlite

`zsqlite` is a SQLite VFS that stores the main database image as independently
readable page frames in transactional, LTX-style segments. SQLite journals and
WAL files continue to use the host VFS.

The current format is V6 and intentionally has no compatibility path for older
zsqlite formats.

## Storage model

A database is an append-only active segment and a sibling directory of sealed
predecessors:

```text
database.zsqlite
database.zsqlite.d/
  segments/
    <start-txid>-<end-txid>-<history>-<physical>.zseg
  locks/
    publication.lock
    lifecycle.lock
    sqlite.lock
```

The `.zsqlite` file is the active segment. Its immutable header contains the
database identity and the physical digest of its immediate sealed predecessor.
Opening follows that digest chain through `segments/`; unrelated files are not
authoritative. Transactions are recovered by scanning the active file to its
last complete, checksummed commit record.

The host VFS uses `sqlite.lock` as the native SQLite byte-lock and WAL-SHM
carrier. It deliberately does not use the `.zsqlite` inode for SQLite byte
locks: on POSIX, closing any independently opened database descriptor could otherwise release a
live connection's process-owned `fcntl` locks.

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
The current durability, recovery, and concurrency review is in
[`correctness-audit.md`](correctness-audit.md).

## Commit and durability behavior

`xWrite` appends the resulting bytes of each affected SQLite page to the active
`.zsqlite` segment. Repeated writes to the same page before publication may
reuse that transaction's uncommitted frame; otherwise they append a replacement
and mark the earlier uncommitted frame free. A publication writes one
sector-aligned commit body and writes its checksummed header last. The valid commit record is the
visibility point. Committed frames are append-only for the lifetime of that
active segment, including frames later
superseded by another commit. Sealing preserves those bytes; compaction can
omit dead frames by writing a new immutable segment.

With `synchronous=OFF`, the records are still structurally complete,
but zsqlite does not ask the operating system to make them power-loss durable.
With SQLite sync enabled, the complete active prefix is synced before success is
reported. `synchronous=FULL` also propagates macOS `F_FULLFSYNC` to those Store
files. Recovery ignores an incomplete or invalid tail and the next writer
truncates it before appending. Rollback discards unreferenced staged frames. A commit that shrinks the
database records an authenticated truncate low-water mark as well as its final
size, so shrinking and re-extending in one transaction cannot resurrect old
pages during replay.

In WAL mode, WAL writes are passed through. When SQLite acquires its exclusive
checkpoint SHM lock, zsqlite holds one publication lease for the whole
checkpoint. Each successful checkpoint `xWrite` publishes its completed
main-image change nondurably before returning, because SQLite ignores errors
from later checkpoint-done and unlock notifications. A checkpoint `xTruncate`
is published the same way, and a subsequent `xSync` syncs the active data and
commit record. Consequently a zsqlite TXID describes an
atomic main-image publication, often one checkpoint write, not an individual
SQL transaction in the WAL.

SQLite suppresses SHM-lock callbacks in `locking_mode=EXCLUSIVE` and during
its last-close checkpoint. For those paths, checkpoint start/done file controls
provide the equivalent bounded publication state; partial checkpoints release
at DONE, while an immediately following truncate remains attributable to the
completed checkpoint.

Only uncommitted frames superseded within the current publication may be reused
or have aligned payload interiors hole-punched. Published frames are not
overwritten or punched while their active segment remains mutable. Sealing and
later compaction omit dead versions from the live index; whole obsolete files
become collectible after neither the active lineage nor any reader generation needs them.
Recovery truncates an uncommitted tail only while holding the publication lock.

At rollover, zsqlite seals and syncs the current `.zsqlite`, hardlinks that
inode into `segments/`, syncs the segment directory, writes and syncs a fresh
active segment that names the sealed digest, atomically renames it over
`.zsqlite`, and syncs the parent directory. Existing readers keep a valid old
inode and reopen the new active generation at the next transaction boundary.

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

A raw bundle copy is not a SQLite online backup. In WAL mode, acknowledged SQL
transactions can exist only in the host `-wal` file while the `.zsqlite` active
still names an older main image. Copy a closed or coordinately checkpointed
database, or use SQLite's online-backup protocol. The `.zsqlite` file and its
sidecar directory must be captured from one pinned point in time; copying only
one of them is not sufficient.

`configure()` persists settle, maximum-staleness, and adaptive dictionary
policy. Defaults are a 5 minute settle interval, 1 hour maximum active age,
64 KiB dictionaries, a 32 MiB reservoir, 5% minimum held-out improvement,
25% churn, and a 24 hour promotion cooldown.

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
