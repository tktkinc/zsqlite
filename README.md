# zsqlite

`zsqlite` is a SQLite VFS that stores the main database image as independently
readable page frames in transactional, LTX-style segments. SQLite journals and
WAL files continue to use the host VFS.

The current format is V6 and intentionally has no compatibility path for older
zsqlite formats.

## Storage model

A logical `.db` name is a small ordinary SQLite notice database. Selecting the
`zsqlite` VFS maps that name to an append-only active segment and a sibling
directory of sealed predecessors:

```text
database.db                           read-only SQLite notice
database.db.zsqlite                   mutable active segment
database.db.zsqlite.d/
  segments/
    <start-txid>-<end-txid>-<history>-<physical>.zseg
  locks/
    publication.lock
    lifecycle.lock
    sqlite.lock
```

Without the VFS, `sqlite3 database.db` can read the
`zsqlite_extension_required` view, which explains how to reopen the database.
The notice is made read-only to reject accidental native writes. With the VFS,
`file:database.db?vfs=zsqlite` opens `database.db.zsqlite`; SQLite derives its
journal and WAL paths from that storage name, so native notice-file recovery
state cannot collide with the real database. An existing ordinary
`database.db` is never overwritten or implicitly converted.

Direct `.zsqlite` paths remain supported for existing bundles and low-level
maintenance, but they do not create a separate notice database.

The `.db.zsqlite` file is the active segment. Its immutable header contains the
database identity and the physical digest of its immediate sealed predecessor.
Opening follows that digest chain through `segments/`; unrelated files are not
authoritative. Transactions are recovered by scanning the active file to its
last complete, checksummed commit record.

The host VFS uses `sqlite.lock` as the native SQLite byte-lock and WAL-SHM
carrier. It deliberately does not use the active segment inode for SQLite byte
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

## Commit and durability behavior

`xWrite` appends the resulting bytes of each affected SQLite page to the active
`.db.zsqlite` segment. Repeated writes to the same page before publication may
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

At rollover, zsqlite seals and syncs the current `.db.zsqlite`, hardlinks that
inode into `segments/`, syncs the segment directory, writes and syncs a fresh
active segment that names the sealed digest, atomically renames it over
`.db.zsqlite`, and syncs the parent directory. Existing readers keep a valid old
inode and reopen the new active generation at the next transaction boundary.

## Dictionaries and compaction

An empty database starts with raw page frames. zsqlite captures up to 8 MiB of
unique committed page images directly from SQLite's write buffers. Once enough
samples exist, a background worker trains one 64 KiB dictionary and installs it
at the next ordinary segment rollover. The dictionary applies only to future
writes; existing frames are never read back, decompressed, or recompressed for
training. A future page is stored raw whenever one dictionary-compression
attempt does not save at least 64 bytes.

Offline conversion is likewise single-pass. Converted pages remain raw, while
the dictionary learned from that stream is available for later writes.

Sealing does not decompress or recompress pages. Compaction copies live raw or
Zstandard frame payloads byte-for-byte, rewrites only structural offsets and
dictionary selectors, and publishes a new snapshot segment beginning at TXID
1. The compacted segment keeps the existing ending history hash.

## Rust API

```rust,no_run
let info = zsqlite::inspect("app.db")?;
zsqlite::verify("app.db")?;
zsqlite::flush("app.db")?;
zsqlite::compact("app.db")?;

zsqlite::convert_to_zsqlite("legacy.db", "app.db")?;
zsqlite::export_to_sqlite("app.db", "restored.db")?;

# Ok::<(), zsqlite::StoreError>(())
```

A raw bundle copy is not a SQLite online backup. In WAL mode, acknowledged SQL
transactions can exist only in the host `-wal` file while the `.db.zsqlite` active
still names an older main image. Copy a closed or coordinately checkpointed
database, or use SQLite's online-backup protocol. The notice, active segment,
sidecar directory, and any live SQLite auxiliary files must be captured from
one pinned point in time; `database.db` by itself contains no application data.

`configure()` persists settle, maximum-staleness, and dictionary sizing policy.
Defaults are a 5 minute settle interval, 1 hour maximum active age, 64 KiB
dictionaries, and an 8 MiB in-memory sample budget. V6 retains its former
adaptive-policy fields on disk for format compatibility, but forward-only
dictionary training does not use them.

## CLI

```text
zsqlite inspect database.db
zsqlite verify database.db
zsqlite flush database.db
zsqlite compact database.db
zsqlite convert legacy.db database.db
zsqlite export database.db restored.db
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
`--page-size`, and `--samples` control workload size. The report includes
SQLite's post-warmup page-cache usage, hit/miss, write, and spill counters.

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
