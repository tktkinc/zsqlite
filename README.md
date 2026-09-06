# zsqlite

`zsqlite` is a SQLite VFS that stores main-database pages in
Zstandard-compressed extents. SQL, schemas, rollback journals, WAL files, and
shared-memory files remain standard SQLite structures.

It is intended for databases dominated by compressible JSON and text. SQLite
still sees ordinary fixed-size pages; by default the VFS batches adjacent dirty
pages into logical extents of at most 1 MiB and encodes each extent as
independently decompressible 64 KiB Zstandard frames plus a seek table.

> **Status:** V3 has broad automated correctness coverage on macOS and Linux,
> including real multi-process WAL and rollback-journal contention. It has not
> had an independent security review, Android device testing, broad filesystem
> qualification, or hardware power-cut testing. Keep recoverable standard
> SQLite exports of important data.

## Build and test

The crate uses Rust 2024 and tracks the current stable Rust toolchain. The
manifest currently requires Rust 1.94 or newer.

```console
cargo build --release
cargo test --no-default-features --features static
cargo clippy --all-targets --no-default-features --features static -- -D warnings
python3 tests/loadable_smoke.py target/release/libzsqlite.dylib --cli target/release/zsqlite
```

Use `libzsqlite.so` instead of `libzsqlite.dylib` on Linux.

The static suite covers all SQLite page sizes; partial and cross-page I/O;
bounded staging with page 1 arriving last; configurable extent and seek-frame
sizes; rewrites of already-spilled extents; seekable and legacy extent data;
range-index reconstruction;
every single-bit metadata mutation; arbitrary malformed metadata; every byte
truncation of a newly appended generation; missing, swapped, and corrupt
companions; non-destructive unpublished tails; rollback, savepoints, VACUUM,
incremental vacuum, ATTACH, and backup; 60 journal/synchronous/page-size
combinations; forced-process termination; simultaneous first creation; and
independent multi-process readers, writers, and WAL checkpoints. High-contention
CI cases are repeated to explore different schedules.

### Performance comparison

The custom benchmark compares the native SQLite VFS with zsqlite using fresh,
alternating-order samples of the same deterministic WAL workload. It reports
median insert/commit, checkpoint, close/reopen, random-read, update/commit, and
scan timings, along with final logical and allocated storage. Benchmark results
are informational and are deliberately not used as noisy CI pass/fail gates.
Each run measures both an 8 MiB pressure-cache profile and a 64 MiB
cache-resident profile, applying the same SQLite page-cache size to both VFSes.
The timed random-read phase follows an untimed sequential table scan: the small
cache still churns, while the large default cache retains the full working set.

```console
cargo bench --no-default-features --features static --bench vfs_performance
```

The defaults use five samples with 20,000 rows and 1 KiB compressible payloads.
For a quick smoke run or workload sizing, pass options after `--`:

```console
cargo bench --no-default-features --features static --bench vfs_performance -- \
  --samples 1 --rows 1000 --reads 2000 --updates 500 \
  --extent-size 1048576 --seek-size 65536
```

Run the benchmark on an otherwise idle machine and compare results from the
same filesystem and build. Use `--help` to see controls for page size, payload
size, transaction batch size, operation counts, and both cache-profile sizes.

To compare compression and format overhead for 1 MiB/1 MiB,
1 MiB/64 KiB, and 64 KiB/64 KiB extent/seek-frame layouts on existing closed
SQLite databases, run:

```console
cargo bench --bench extent_compression -- test-dbs/path/to/one.sqlite \
  test-dbs/path/to/another.sqlite
```

This corpus benchmark uses the production Zstandard level, official seek-table
layout, strong-digest metadata, packed extent records, and persistent-index
encoding. It reports stored extent payload (compressed frames plus seek/digest
metadata) separately from an estimated compacted V3 sidecar size so that lost
compression and format overhead are both visible.
Databases with nonempty `-wal` or `-journal` companions are rejected; snapshot
or cleanly close them first so uncheckpointed data is not silently omitted.

On Unix, `sidecar_allocated_bytes` in the inspection output comes from
`st_blocks * 512`, rather than from the sidecar's logical file length.

## Using the VFS

Loading or statically registering the extension creates a **named** VFS called
`zsqlite`. It deliberately does not replace SQLite's default VFS. Select it for
each database connection:

```text
sqlite3 :memory:
.load ./target/release/libzsqlite sqlite3_zsqlite_init
.open file:transcripts.db?vfs=zsqlite
PRAGMA journal_mode=WAL;
```

An embedding application should register the library once, then use
`sqlite3_open_v2(..., "zsqlite")` or the `vfs=zsqlite` URI parameter. Opening a
V3 anchor with stock SQLite, without this VFS, correctly fails as “not a
database”.

New writes default to 1 MiB extents and 64 KiB seek frames. Both sizes can be
selected per connection with URI parameters:

```text
file:transcripts.db?vfs=zsqlite&zsqlite_extent_size=1048576&zsqlite_seek_size=65536
```

Both values must be powers of two, the seek size must divide the extent size,
and both must contain a whole number of SQLite pages. Connections sharing an
open database must use the same values. The choice is not a permanent database
setting: each extent describes its own frames, so a later connection or
compaction can choose different valid sizes while retaining read compatibility.

Existing ordinary SQLite files pass through to the parent VFS unchanged and do
not gain zsqlite sidecars. New databases created through the `zsqlite` VFS use
the compressed V3 format. Use the offline conversion command described below
to compress an existing database. A recognized V3 anchor with missing or
mismatched sidecars fails closed instead of falling back to ordinary I/O.

## Page and extent sizes

All SQLite page sizes from 512 bytes through 64 KiB are supported. The extent
size controls batching and page-run granularity; the seek size controls how
much compressed data a cache miss must read and decompress. The defaults are
1 MiB and 64 KiB respectively.
Larger pages reduce index and pager metadata but amplify small updates and use
more cache memory. Start with SQLite's 4 KiB default, or benchmark 8–16 KiB for
large transcript records. Set `PRAGMA page_size` before creating the first
table; changing it in place later is rejected.

V3 writes the
[Zstandard seekable format](https://github.com/facebook/zstd/blob/dev/contrib/seekable_format/zstd_seekable_compression_format.md):
independent standard Zstandard frames followed by a skippable seek table.
zsqlite adds a skippable, authenticated metadata frame containing full BLAKE3
digests for the data frames and seek table. A standard streaming Zstandard
decoder ignores both skippable frames and produces the original extent bytes.
No compression dictionary is used.

## Files on disk

For `transcripts.db`, the durable bundle is:

- `transcripts.db`: a 4096-byte V3 anchor containing a random database UUID;
- `transcripts.db-zsqlite`: the compressed extent log and persistent indexes;
- `transcripts.db-zsqlite-lock`: the cross-process lifecycle/initialization lock;
- `transcripts.db-zsqlite-publish`: the cross-process generation-publication lock;
- `transcripts.db-wal`, `transcripts.db-shm`, or `transcripts.db-journal` when
  SQLite's selected journal mode needs them.

The anchor, sidecar header, and lock files all carry the same 128-bit UUID.
Missing or swapped companions fail closed instead of appearing as an empty or
different database. Deleting through the VFS uses an identity-bearing deletion
marker and the lifecycle lock so companion failures cannot silently resurrect
an old database.

The sidecar layout is append-oriented:

```text
0 KiB       4 KiB immutable header (magic, V3, database UUID, CRC)
4 KiB       4 KiB superblock A (sequence, generation, commit/index offsets, CRC)
8 KiB       4 KiB superblock B
12 KiB...   packed EXT3 headers + seekable Zstd extent payloads
            CMT3 generation records linked backward to the previous commit
            optional IDX3 compressed page-run indexes
```

Each seek frame has a full BLAKE3 digest, and the extent header authenticates
the digest metadata and seek table. Each commit authenticates its ordered
extent headers. The persistent index stores page runs, not one entry per
database page, so contiguous ranges remain compact. All data access uses
positional `pread`/`pwrite`-style I/O. Older V3 raw and single-frame Zstandard
extents remain readable.

Extent records are packed within a generation; zsqlite no longer rounds every
record up to 4 KiB or writes zero padding after it. Generation starts and
persistent indexes remain 4 KiB aligned for publication/recovery and filesystem
allocation behavior.

Obsolete payload blocks are hole-punched live on supported Linux and macOS
filesystems, but only after both recoverable superblock slots have advanced past
the old extent. Failure or lack of hole-punch support affects space use, never
recovery. Offline compaction is still needed to reduce the sidecar's logical
length. Reclamation candidates lost in a process crash may also wait for
compaction.

## Publication, durability, and recovery

- SQLite owns SQL commit and rollback semantics. zsqlite does not add another
  transaction layer; it translates the main-file writes SQLite issues into an
  unpublished compressed layer and reacts to SQLite's VFS durability and lock
  protocol.
- Main-file `xWrite` calls are coalesced into configured logical ranges (1 MiB
  by default). At most eight incomplete ranges are buffered in memory. Full or
  least-recently-used ranges are compressed into an unlinked temporary file;
  reads in the same transaction see that staged data. Publication copies
  complete compressed records with positional I/O and only recompresses a
  bounded fragment when a later write superseded part of a staged extent.
- If SQLite writes pages before page 1 reveals the database page size (notably
  during online backup), those bytes use a bounded-memory, unlinked positional
  spool. Once page 1 arrives, it is converted to normal compressed extents in
  configured-extent pieces.
- A clean commit publishes the staged layer at the SQLite-provided boundary.
  Rollback or unlock without publication drops the unlinked staging files.
- For rollback journals, `SQLITE_FCNTL_SYNC` marks the publication boundary.
  A following `xSync` performs data-before-superblock durable publication. If
  `PRAGMA synchronous=OFF` suppresses `xSync`, `SQLITE_FCNTL_COMMIT_PHASETWO`
  publishes the clean commit non-durably before SQLite unlocks the database.
- WAL checkpoints publish at `SQLITE_FCNTL_CKPT_DONE`, before SQLite advances
  the shared `nBackfill` value. This is required for correct partial PASSIVE
  checkpoints in other processes. `xSync` then makes the sidecar durable when
  the configured synchronous mode requests it.
- Publication appends extents and a commit record, optionally syncs them, then
  updates the older of two sector-separated superblocks and optionally syncs
  again.
- Writers serialize through a cross-process publication lock and refresh the
  newest committed generation after acquiring it. SQLite byte-range and WAL
  shared-memory locks continue to come from the parent VFS.
- A physically truncated newest generation that was explicitly published as
  non-durable falls back to the prior valid superblock. A truncated durable
  generation, or in-bounds checksum/digest corruption, is reported and is
  never silently treated as a torn tail.
- Unpublished trailing bytes are retained as evidence and ignored. A later
  append starts after the physical end; opening never performs a destructive
  scan-and-truncate repair.

`synchronous=OFF` retains SQLite's normal weak crash-durability guarantee. It is
supported for correct clean commits and rollbacks, not made power-loss safe by
the compression layer.

## Multi-process behavior

V3 follows normal SQLite multi-process semantics on supported Unix platforms.
The parent VFS still owns database byte locks and WAL shared memory. zsqlite's
additional publication lock prevents two processes with stale cached indexes
from appending from the same generation, and read/WAL lock transitions refresh
committed sidecar state. Maintenance takes an exclusive lifecycle lock and
returns `SQLITE_BUSY`/`StoreError::Busy` while any SQLite process is open.

## Conversion, export, and maintenance

All maintenance commands are offline. Close connections and ensure no
`-journal`, `-wal`, or `-shm` file remains. Inputs are never modified and output
paths must not already exist.

```console
zsqlite convert ordinary.db transcripts.db
zsqlite convert --extent-size 1MiB --seek-size 64KiB ordinary.db transcripts.db
zsqlite inspect transcripts.db
zsqlite verify transcripts.db
zsqlite compact --extent-size 1MiB --seek-size 64KiB transcripts.db
zsqlite export transcripts.db ordinary-export.db
```

Conversion stages a complete V3 bundle beside the destination, verifies it,
installs UUID-bearing companions without overwriting existing paths, syncs the
directory, and exposes the anchor last. Export reconstructs a byte-for-byte
ordinary SQLite main database into a new file while holding the exclusive
lifecycle lock. `Store` itself is private; public Rust callers use `inspect`,
`verify`, `compact`, `compact_with_config`, `convert_to_zsqlite`,
`convert_to_zsqlite_with_config`, and `export_to_sqlite` so they cannot bypass
locking.

Run `PRAGMA integrity_check` on an ordinary source before conversion and on an
export before treating it as a backup. The converter validates SQLite's file
header, page size, alignment, stable header, and absence of journal companions;
it does not implement SQLite's full B-tree integrity checker itself.

## Litestream

Direct Litestream replication of the V3 anchor/sidecar bundle is unsupported.
Litestream expects the main file to be ordinary SQLite, whereas the V3 main file
is an identity anchor. The safe bridge is:

1. checkpoint and close the zsqlite database;
2. run `zsqlite export transcripts.db replica-source.db`;
3. point Litestream at `replica-source.db`;
4. after a restore, validate the standard database and use `zsqlite convert` to
create a new V3 bundle if desired.

Do not point Litestream only at `transcripts.db` or copy just the anchor.

## Android

Android/Kotlin packaging is deliberately deferred. The Rust storage core uses
the required positional I/O and Unix locking primitives, and Android remains a
target in the low-level hole-punch code, but no AndroidX `SQLiteDriver` adapter
or device test matrix is currently shipped. A future adapter must register the
named VFS before opening the application database.

## Compatibility

The format magic is `ZSQLPG03` and the format version is 3. V3 intentionally
does not open or migrate the earlier development formats. Convert from an
ordinary SQLite export instead. The format is little-endian and unknown future
versions must be rejected rather than guessed.
