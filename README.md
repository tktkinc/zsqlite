# zsqlite

`zsqlite` is a SQLite VFS that keeps changed live pages in overwriteable raw
page records and turns those records into independently readable compressed
page records when the generation is sealed. SQLite journals and WAL files
continue to use the host VFS.

The current format is V6 and intentionally has no compatibility path for older
zsqlite formats.

## Storage model

A logical `.db` name is a small ordinary SQLite notice database. Selecting the
`zsqlite` VFS maps that name to a mutable active segment and a sibling
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

The `.db.zsqlite` file is the active generation. Its immutable header contains
the database identity and the physical digest of its sealed predecessor. Two
alternating checksummed state sectors publish its logical size, transaction
identity, page-record count, and truncate boundary. Active and sealed files use
the same page-record header. The active page map associates each logical SQLite
page with a record offset; the first update appends a raw record and later
updates overwrite that record in place. Pages absent from the active map
resolve through the sealed digest chain in `segments/`; unrelated files are
not authoritative.

The host VFS uses `sqlite.lock` as the native SQLite byte-lock and WAL-SHM
carrier. It deliberately does not use the active segment inode for SQLite byte
locks: on POSIX, closing any independently opened database descriptor could otherwise release a
live connection's process-owned `fcntl` locks.

Each sealed segment contains:

- its database identity, TXID range, and parent history hash;
- any Zstandard dictionaries added by that segment;
- raw or individually decompressible page frames using lineage dictionaries;
- a sorted live-page index and complete page-to-last-TXID map;
- a logical content root and physical BLAKE3 digest.

TXID zero in a full page map represents a zero-filled sparse page. This
matches native file semantics when a WAL checkpoint extends the main image
before copying every newly addressable page.

Fixed-width hexadecimal TXID prefixes preserve LTX-style lexical range order.
The ending history hash remains unchanged by compaction; only the physical
digest changes.

## Commit and durability behavior

`xWrite` writes affected SQLite pages directly into raw active records.
Within a process, zsqlite retains original page images until SQLite publishes or
rolls back the operation. Publication syncs the raw pages as requested and then
writes the alternate checksummed state sector. The state sequence is the Store
visibility point; SQLite's rollback journal or WAL remains responsible for its
normal crash-recovery protocol around in-place main-file updates.

With `synchronous=OFF`, the records are still structurally complete,
but zsqlite does not ask the operating system to make them power-loss durable.
With SQLite sync enabled, the raw records are synced before the state sector is
published. `synchronous=FULL` also propagates macOS `F_FULLFSYNC` to those Store
files. A corrupt newest state sector falls back to the preceding sector.
Truncate-and-regrow operations zero discarded pages so old contents cannot be
resurrected.

In WAL mode, WAL writes are passed through. When SQLite acquires its exclusive
checkpoint SHM lock, zsqlite holds one publication lease for the whole
checkpoint. Each successful checkpoint `xWrite` publishes its completed
main-image change nondurably before returning, because SQLite ignores errors
from later checkpoint-done and unlock notifications. A checkpoint `xTruncate`
is published the same way, and a subsequent `xSync` syncs the active data and
state sector. Consequently a zsqlite TXID describes an
atomic main-image publication, often one checkpoint write, not an individual
SQL transaction in the WAL.

SQLite suppresses SHM-lock callbacks in `locking_mode=EXCLUSIVE` and during
its last-close checkpoint. For those paths, checkpoint start/done file controls
provide the equivalent bounded publication state; partial checkpoints release
at DONE, while an immediately following truncate remains attributable to the
completed checkpoint.

Ordinary commits retain the active inode. A page's first update in an active
generation appends one raw record; later updates overwrite that record without
growing the file. Sealing writes the same record format into a staged segment,
optionally with compressed payloads, and appends its final index, page map, and
trailer. Later seals write only pages represented by the active records. Each
segment's authoritative ending page map removes zeroed or truncated pages from
older mappings. Sealing then replaces `.db.zsqlite` with an empty active
generation naming the new head. Existing readers keep the old generation; new
readers follow the new chain.

## Dictionaries and compaction

The live generation has no compressed frames or dictionary. At seal, zsqlite
samples up to 8 MiB from its changed raw page records, trains one 64 KiB
dictionary when at least 256 pages and 1 MiB are available, and compresses those
pages into the staged segment. A page remains raw when dictionary compression
would not save at least 64 bytes. Zero pages lose their entry in the
authoritative ending map and therefore need no stored payload. Dictionary
selectors address the cumulative dictionary set inherited through the segment
lineage. Small delta segments reuse the newest inherited dictionary without
copying its bytes into each segment; a segment header stores only newly trained
dictionaries.

Compaction resolves the current page map and rewrites one complete compressed
base segment, allowing older incremental segments to be collected after their
reader leases end.

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

`configure()` persists settle, maximum-staleness, target segment size, and
dictionary sizing policy. Active generations seal at a configured byte target,
on either time trigger, or on an explicit `flush`. Defaults are a 5 minute
settle interval, 1 hour maximum active age, no byte target, 64 KiB dictionaries,
and an 8 MiB sample budget.

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
