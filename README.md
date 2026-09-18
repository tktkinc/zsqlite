# zsqlite

`zsqlite` is a SQLite VFS with overwriteable raw active pages and immutable,
compressed sealed storage. SQLite journals and WAL files use the host VFS.

All active files, sealed metadata, payload packs, frames, dictionaries, catalogs,
blobs, and indexes use the V1 format. Logical pins live in the catalog.
See [storage invariants and interfaces](docs/storage.md) and
[transcript replay methodology](docs/transcript-replay.md). The adapter contract
and lazy recovery are documented in [pluggable sealed storage](docs/storage-backends.md).

## Storage model

A logical `.db` name is a small read-only SQLite notice database. Selecting the
`zsqlite` VFS resolves it to the active file and sibling catalogue:

```text
database.db                           SQLite notice, not application data
database.db.zsqlite                   raw active pagefile
database.db.zsqlite.d/
  objects/<digest>.segment            metadata run or checkpoint only
  objects/<digest>.blob               complete logical packs + extent index
  objects/<digest>.dict               shared dictionary
  objects/<digest>.index              immutable catalog index run/checkpoint
  catalog-head                        CAS root referencing indexes and named heads
  active-location                     local attachment reservation
  readers/<manifest-digest>            manifest-specific OS reader lease
  object-readers/<logical-object>      exact reader metadata dependencies
  physical-readers/<blob-or-index>     exact placement/blob leases
  dictionary.samples                 advisory bounded sample reservoir
  locks/{publication,catalog,lifecycle,sqlite}.lock
```

Without the VFS, opening `database.db` exposes the
`zsqlite_extension_required` notice. With `file:database.db?vfs=zsqlite`,
journals and WAL paths derive from `database.db.zsqlite`. Existing ordinary
SQLite files are never implicitly converted. Direct `.zsqlite` paths remain
supported.

A metadata run records its own page changes and one parent hash. Missing
pages fall back to that parent, recursively; explicit zero entries and logical
truncation stop fallback. A checkpoint manifest ends the chain. There is no
page-range delegation. Recovery resolves the chain into a complete in-memory map
and verifies its content root before serving reads.

Each segment has a 128-byte header with full-view txid begin/end,
represented transaction range, logical endpoint hash and logical parent hash.
The compressed metadata run or checkpoint is a footer in its own segment file.
All frame payloads live in immutable logical packs inside backend blobs.
Segment headers carry no ordering counter. The txid span
preserves the base checkpoint's starting endpoint through the current
endpoint, not a promise that every intermediate snapshot is retained. An explicit
authenticated endpoint index records represented coverage. Parent lookup selects the widest range ending at the exact logical endpoint hash;
ties prefer checkpoints, then a deterministic physical checksum order, never
rewrite recency. Typed manifest IDs keep equivalent encodings distinct. Logical hashes bind database/lineage, size,
txid, history and page versions/checksums, but exclude physical layout. Rollups
preserve them; only authenticated, fully resolved metadata can satisfy a parent.
Invalid segment encodings, level-prefixed names, standalone manifest objects,
and physical-manifest retention records are rejected.

Active records contain one raw page and can be overwritten in place.
Sealed records share their envelope but may contain multiple pages in a raw or
Zstandard frame. Frames are independently authenticated fetch/decode
units; segment/pack files are physical collection/upload units. Only absence
from the fully resolved map means zero.

The stable `sqlite.lock` inode carries SQLite's native byte locks and WAL SHM,
separately from the active pathname that sealing replaces.

## Publication and durability

Ordinary writes and commits keep the active inode. The first changed version
of a page appends a raw record; subsequent changes overwrite that record.
SQLite retains responsibility for rollback and WAL recovery. Only successful
main-image publications advance the active endpoint. A WAL checkpoint may
publish many individual main-file writes under one publication lock.

Sealing durably installs dictionaries, payload blobs, metadata and index deltas.
It CAS-publishes a pending candidate protecting both heads, replaces and syncs
an empty active file, then finalizes the published sealed head.
Unchanged pages are inherited. The first seal
uses the same path as later seals; it never expands a sealed database into the
active file.

Two checksummed active state sectors record publication sequence, logical size,
history and truncate state. They are **not** historical copies of overwritten
page bytes: damaged state metadata fails closed. SQLite sync strength is
propagated, including macOS full sync. If visibility may have preceded an I/O
failure, `PublicationUncertain` preserves possibly referenced objects and the
Store reloads rather than deleting them. `synchronous=OFF` does not promise
power-loss durability.

Readers pin their selected manifest and its exact physical dependencies.
New readers may resolve a logical parent through a wider rollup without changing
the child; existing pins keep their old files alive. The local implementation still
serializes sealing/repacking with Store publication and its process mutex;
compression can delay same-process readers. It is not an asynchronous
object-storage uploader.

## Dictionaries, layouts, and collection

An up-to-96 MiB committed-page reservoir survives seals and reopening. Conversion
samples across the whole database, not just its first pages. Fresh databases
accumulate samples across seals; dictionary candidates grow from 8 KiB to 768 KiB
as enough distinct training pages become available. The full pool supports
comparison of 512 KiB and 768 KiB candidates, with about 100 training bytes per
dictionary byte plus separate held-out pages. After 1 MiB of new distinct samples,
candidates are evaluated on held-out pages, including the new dictionary's
storage cost, and promoted only for at least 5% improvement over existing choices.
Up to four preferred dictionaries are carried
forward, including a fallback; live frames retain any additional dictionaries
they require. Tiny seals can reuse dictionaries without copying them into
each pack. Training failure is advisory. Compression competes with raw storage.

Page frames at Zstandard level 3 remain the default baseline. Validated policy
types enable page-sized or fixed 512-byte–8-MiB frames, plus explicit pack,
cache, codec, and maintenance limits.

Decoded pages are cached in a private, unlinked temporary file, not retained as
RAM frames. By default its cap is 20% of the database's logical size, limited to
half the free space reported by the temporary-file filesystem. The file grows
only as pages are cached. Callers can instead set a fixed cap, including zero to
disable it. Filesystem-aligned slots and an in-memory page index/LRU support
reads from active pages, then this plaintext cache, then compressed frames.
Writes invalidate the old cached page immediately and attempt to hole-punch its
slot; punching is best-effort only.
Unsupported/failed punches leave the rest of the cache intact and slots remain
reusable. Active writes stay in the authoritative active file, separate from
this disposable cache. The OS may cache file data; dictionary training and
transient decoding still use RAM.
GC traces the current manifest, named offline roots and reader leases.
Named roots pin a logical hash and endpoint txid, not a physical manifest file.
They resolve through the widest validated rollup ending at that same hash; a
newer endpoint cannot substitute merely because its txid range overlaps. GC keeps
the chosen representation and its dependencies, and may delete superseded
narrower files after their physical readers close. Creating a pin writes only a
small retained-root index delta, not a copy of the resolved page map.
Dropping a named-pin handle does **not** release its persistent root.
Wholly unreachable blobs need no copying; dead extents remain until the whole
blob is collectible. Explicit byte-budgeted relocation can group compatible
packs while exact reader leases retain old blobs. Missing/corrupt retained metadata
blocks destructive collection. Routine deletion is budgeted; bounded repacking
selects at most one pack below 50% live occupancy and at most 64 MiB of decoded
input and skipping packs retained by forks. `compact()` requires an explicitly
flushed head and merges its metadata LSM
runs into an equivalent checkpoint;
it never reads frame payloads, decompresses them, or rewrites payload packs. `maintain()` is the
separate bounded operation that can rewrite partially live payload.
Compression/layout choices are made when payload packs are first sealed or when
that bounded payload maintenance is justified by reclaimable garbage.
Use `convert_to_zsqlite_with_policy()` to select the intended layout and compression
when importing a SQLite file, without a second compression pass.

## Configured storage and lazy opening

The host can provide any `Arc<dyn StorageBackend>` and register a named VFS:

```rust,no_run
use std::sync::Arc;
use zsqlite::{FilesystemBackend, Storage};
let storage = Storage::new(
    Arc::new(FilesystemBackend::open("sealed-storage")?),
    "local-coordination",
)?;
# #[cfg(feature = "static")]
storage.register_vfs("archive")?;
// Now SQLite opens file:local.db?vfs=archive normally.
// A missing pagefile restores the latest finalized seal lazily.
# Ok::<(), Box<dyn std::error::Error>>(())
```

The adapter owns all sealed data and metadata. Active changes, the existing
extracted-page cache, SQLite WAL/journals and filesystem coordination stay local.
The filesystem layout above is the default backend; custom adapters do not need
local sealed objects. All processes share the namespace's coordination directory;
each named head has its own writable pagefile and local locks. A separate named VFS can configure another source;
callers do not pass a data source on each open.

`storage.bootstrap(destination)` explicitly restores the finalized sealed head
and returns a `Database`; `inspect()` exposes the restored logical endpoint.
Bootstrap starts with a 12 KiB active pagefile and an empty extracted-page cache.
It reads authenticated metadata, dictionaries and the SQLite header frame, then
fetches other frames on demand. It rejects existing destinations and open local
instances, and fences superseded pagefiles with a fresh attachment token.
Unsealed pagefile changes and transactions remaining only in WAL are outside
this recovery boundary. There is no S3 adapter, compressed-object cache, automatic
offloading, or self-contained read-only bundle implementation.

`storage.fork("experiment")` creates an independent writable head sharing the
source's latest finalized seal. Register the returned handle under a separate
VFS name and open a new local path normally. `storage.head("experiment")` selects
it in another process. Each head has its own attachment and pending/finalized
seal; GC protects all heads. Removing a closed fork with `remove_head()` fences
its old pagefiles and allows unreferenced objects to be collected.

Content and lineage hashes are computed at sealing, not on each mutable
publication. Packs derive identity from ordered frame IDs and layout metadata;
blobs derive identity from ordered pack extents. Adapter-owned staged writers
stream payloads and finalize under the completed key. Filesystem finalization
installs the same temporary file without copying it. Objects have no 512 MiB cap;
read, decoding, and metadata budgets remain bounded.

## Rust API

```rust,no_run
let info = zsqlite::inspect("app.db")?;
zsqlite::verify("app.db")?;
zsqlite::flush("app.db")?;
zsqlite::compact("app.db")?;
let pin = zsqlite::retain("app.db", zsqlite::RetentionName::new("offline-fork")?)?;
let report = zsqlite::collect("app.db", 0)?; // inspect without deleting
zsqlite::release_retention("app.db", pin)?;  // explicit durable-root release
let work = zsqlite::maintain("app.db")?;     // one bounded repack

zsqlite::convert_to_zsqlite("legacy.db", "app.db")?;
zsqlite::export_to_sqlite("app.db", "restored.db")?;

# Ok::<(), zsqlite::StoreError>(())
```

A raw bundle copy is not a SQLite online backup. In WAL mode, acknowledged SQL
transactions can exist only in the host `-wal` file while the `.db.zsqlite` active
still names an older main image. Copy a closed or coordinately checkpointed
database, or use SQLite's online-backup protocol. The notice, active pagefile,
sidecar directory, and any live SQLite auxiliary files must be captured from
one pinned point in time; `database.db` by itself contains no application data.

`configure()` persists settle, maximum-staleness, target segment size, and
dictionary sizing policy. Active files seal at a configured byte target,
on either time trigger, or on an explicit `flush`. Defaults are a 5 minute
settle interval, 1 hour maximum active age, no byte target, a 768 KiB dictionary
ceiling, and a 96 MiB sample budget. Existing bundles retain their persisted
limits until reconfigured. The reservoir is advisory and stored compressed;
training and serialization use additional transient memory beyond the sample budget.

## CLI

```text
zsqlite inspect database.db
zsqlite verify database.db
zsqlite flush database.db
zsqlite compact database.db
zsqlite convert legacy.db database.db
zsqlite export database.db restored.db
```

The optional ZIM importer is a separate local experiment under
`experiments/zim-import`, not a dependency of the library or CLI.

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

The frame-layout benchmark exports every profile byte-for-byte and measures
read amplification and fork-retained churn. It keeps inputs and CSV reports in
its printed scratch directory. `--source` requires a closed ordinary SQLite snapshot.

```sh
cargo bench --no-default-features --features static --bench frame_layout -- --source snapshot.sqlite
cargo bench --no-default-features --features static --bench frame_layout -- --churn --skip-reads
```

For application reads, prepare bundles **without page probes**, then run seeded
parameterized point/range reads through every usable primary/rowid/secondary
index on every ordinary application table, plus FTS virtual-index searches:

```sh
cargo bench --no-default-features --features static --bench frame_layout -- --source snapshot.sqlite --build-only
cargo bench --no-default-features --features static --bench transcript_queries -- --bundles /path/printed/by/first/command
```

The query benchmark discovers the schema, excludes SQLite/FTS shadow tables,
reports empty/NULL-only/unsupported access paths, and fails if an ordinary
indexed case devolves to a full scan. It validates every result against native
SQLite, records query plans, and runs three fresh-process shuffled streams per
layout. Every case gets a `stream-first` execution followed immediately by an
`immediate-repeat`; only the first query in each stream follows connection
startup. Connection-open latency is reported separately. Every profile uses a
10 MiB SQLite RAM page cache; managed profiles also get a 256 MiB disk page cache.
“Cold” does not flush the OS cache. Use `--sqlite-cache-mib` for the shared RAM
pager budget and `--page-cache-mib` for the separate VFS disk budget
(`--frame-cache-mib` remains an alias). Use `--profiles`, `--repeats`, and
`--random-samples` for paired experiments. Start from fresh
`frame_layout --build-only` bundles for each run.
Default profiles stop at 4 MiB.
The historical measurements used a RAM frame cache; current runs use a disk
page cache, so compare new runs with each other rather than those old timings.

Run the indexed-read, mutation, retained-root, and GC sequence
against a closed transcript snapshot:

```sh
cargo bench --offline --no-default-features --features static --bench transcript_replay -- \
  --source /path/to/closed-transcripts.sqlite
```

For the current V1 transcript schema this converts the complete snapshot into
private native/page/64-KiB/1-MiB copies. It repeatedly reads every
ordinary application table through real PK/rowid/secondary indexes and FTS,
using one connection per seeded round rather than one per case. Four final-read
phases and three rounds therefore open 12 connections per profile while still
recording two executions of every case in every round and phase.
The harness then deterministically mutates and exactly restores bounded rows from
`session`, event, message, tool, agent-work, and conversation tables. The
mutations churn populated ordinary/partial indexes and trigger-maintained FTS
indexes without changing keys or foreign-key columns. Eight churn seals are
followed by a metadata rollup and four retained-root GC-evaluation seals. The
report covers page/frame/pack occupancy before and after root release,
bounded/full GC, and final rollup. Use `--preflight` for a read-only bounded
schema/target check plus one-key-per-index workload and query-plan validation;
the sampled native queries are also executed. `fts5vocab` metadata is excluded
from base FTS searches, and vocabulary terms are phrase-escaped before binding.
Use `--mutation-rows` to set rows per mutation family. The legacy
`rec/raw/ft/sess/dim` incremental replay remains supported. See
[methodology and output](docs/transcript-replay.md).

## Validation

```sh
cargo test --no-default-features --features static
cargo clippy --all-targets --no-default-features --features static -- -D warnings
```

The suite covers every SQLite page size, rollback modes, WAL checkpoints,
online backup, concurrent readers and writers, multi-process creation, stale
writers, sync-off structural commits, and subprocess crash recovery.
