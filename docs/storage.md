# V1 storage invariants and interfaces

Sealed lineage now advances only at seals. Mutable publications preserve the
last sealed lineage digest while advancing transaction counters. Parent sealed
views are explicit metadata, independent of the manifest chain used to resolve
pages. Named fork heads share immutable objects and namespace-wide GC, while
local pagefiles and lifecycle locks are per head. Packs/blobs derive identities
from authenticated child hashes and canonical layout metadata, and stream through
adapter-owned staged writers. See [the current adapter and fork contract](storage-backends.md).

Sealed storage goes through `StorageBackend`, including every authoritative
metadata/index object. See [the adapter and bootstrap design](storage-backends.md) for transport, catalog,
placement, local attachment, and lazy VFS opening contracts. Active page changes
and the extracted-page disk cache stay local; distributed leases are deferred.

## Validation and capabilities

| Type / boundary | Established fact | Not implied |
| --- | --- | --- |
| `PageSize`, `PageNumber`, `LogicalBytes`, `FrameShape`, `StoredRange`, policy constructors | Valid domain, checked bounds/alignment and arithmetic | File existence or integrity |
| Separate manifest/pack/frame/dictionary IDs and byte units | IDs and units cannot be mixed accidentally | An ID alone authenticates nothing |
| `ViewMetadata` draft → `ValidatedMetadata` | Complete map, compatible page size, valid slots/references/versions/ranges/content root | Reachability or verified payloads |
| `FrameMetadata` → `VerifiedFrame` | Metadata identity, payload digest, exact decoded size and every page checksum | Current page version |
| Internal `Building` → `Finalized` → `Durable` | Consuming construction, file sync, installation and directory sync | Generic bytes are a valid manifest |
| `ManifestBuilder` → `DurableView` | Valid complete manifest whose dependencies are durable or pinned | Its head has been installed |
| `PinnedView::resolve` → borrowed `ResolvedPage` | Owning view, frame and slot stay bound to a manifest lease | An arbitrary cached slot is current |
| Publication owner enum | Owned OS guard for transaction, checkpoint, or maintenance publication | OS locking eliminates I/O failures |
| `DurablePin` | Persistent named logical endpoint plus a private compare-and-release token | Physical representation is fixed, or dropping the handle releases the root |
| Internal `ValidatedRoots` → `GcSession` → `DeletionPermit` | Fully traced root set; one exact, single-use deletion under catalogue exclusion | Low occupancy authorizes deletion |
| `RepackCandidate` | Candidate tied to its source manifest | It can publish without a fresh source comparison |

Serialized active/header records are wire representations, not domain proofs.
All disk decoders are bounded. The safe storage modules forbid unsafe code;
unsafe operations are confined to SQLite and OS adapters. SQLite borrows are
scoped to their callback/connection lifetime, not manufactured as `'static`.

GC takes an exclusive mutable catalogue borrow: a builder or durable receipt
that must still be used cannot overlap a collection call. Backend objects are deleted only through namespace-bound consumed permits.
Local staging and lease files belong to the coordination directory.
The ordinary GC-only pass does not reserve main-file publication; active writes
do not alter their immutable header's manifest dependency.

## Metadata LSM and immutable payload packs

Each seal writes changed frames into complete immutable logical packs in backend
blobs, then a metadata-only manifest. The manifest contains either an L0 page-map
run or a complete checkpoint; it never embeds payload. Typed object IDs replace
filesystem discovery. The explicit endpoint index records authenticated header
coverage and preserves the widest-equivalent-rollup preference.

The fixed 128-byte logical segment header stores represented and full-view txid
spans, logical endpoint hash, and logical parent hash/txid. The full-view span
runs from the base checkpoint's endpoint through the current endpoint. Neither
span promises intermediate retained snapshots. `SegmentCoverage` validates their
relationship. Physical blobs have a separate authenticated extent index.

An ordinary run contains changed pages plus explicit zero tombstones and
authoritative logical size. Missing pages inherit from one parent recursively.
A checkpoint has no parent. No logical page ranges are delegated. The resolved
map remains complete in memory: this change reduces metadata rewritten per seal,
not the memory required for that map. Recovery bounds chains at 64 runs and
512 MiB of decoded metadata, including nested envelopes; this is not an RSS cap.

Headers plus manifest footers determine physical segment identity. A separate
`ViewHash` binds database/lineage, size, txid, history and the complete page-version
content root. It excludes frame layout/codec/physical offsets and survives rollup.
Parent lookup matches this logical hash and endpoint, preferring the widest
represented range. Equal coverage prefers a checkpoint, then deterministic
physical-checksum order; creation time or a rewrite counter never ranks candidates.
A typed physical manifest ID lets equivalent encodings coexist.
The decoded, fully reconstructed parent must verify against the requested hash;
matching an endpoint entry alone is not validation. Frame metadata includes
independent payload hashes; reads validate headers, compressed bytes and page
checksums. V1 active files carry attachment tokens. All format decoders validate
their V1 identifiers, bounds, and checksums before accepting records.

The catalog authenticates a segment's header/footer once and passes an immutable
proof to the metadata parser. Disk decoding and in-memory run construction share
structural validation; building a run does not serialize and reparse its map.

GC retains every parent metadata file needed by a rooted descendant and the
resolved view's payload packs/dictionaries. An ancestor-only preferred dictionary
is not an implicit root. Metadata compaction can remove parent dependencies
without touching payload packs, after which obsolete runs are independently
collectible.
Named forks retain their requested views. Already-open readers additionally hold
object-specific leases on their exact physical resolution: changing parent lookup
must not let GC remove files their existing maps still reference. Old dependencies
become collectible only after all such readers release them.

`compact()` publishes a wider-coverage checkpoint without reading or rewriting
payloads and preserves every pack/frame reference; explicitly flush unsealed
writes first. Bounded payload repacking also publishes a metadata checkpoint,
replacing affected pack locations so the old packs can be collected once no
retained roots or readers need them.
Ordinary seals checkpoint at the chain bound or when inheritance is unhelpful.
GC may publish endpoint or representation retirements without changing the
logical head. An already-flat view is a `compact()` no-op, even if compression
policy changed. Database compaction does not run payload/placement collection.
A rollup earns preference by wider txid coverage, not by being newer. Whole
unreachable blobs are directly deleted; partial dead extents remain.
`maintain()` may rewrite one partially live pack under the configured
decoded-input budget.

## Layout policy

The default is page frames, 4 MiB target packs, level 3, and an automatically
sized disk page cache. Its target is 20% of the database's logical size, capped
at half the free space reported for the temporary-file filesystem. Fixed
multi-page frames and a different pack target are explicit:

```rust,no_run
use zsqlite::{StoragePolicy, configure};
use zsqlite::layout::LayoutPolicy;
use zsqlite::domain::{DecodedBytes, StoredBytes};
let fixed = LayoutPolicy::default()
    .fixed(DecodedBytes::new(256 * 1024))?
    .with_pack_target(StoredBytes::new(4 * 1024 * 1024))?
    .with_level(3)?;
configure("app.db", StoragePolicy::default().with_layout(fixed))?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Frame sizes must be powers of two from 512 bytes through 8 MiB; a frame is never
smaller than the SQLite page size. The pack target is soft because the last frame
may cross it. Metadata remains independently bounded.

The plaintext page cache uses a private, unlinked temporary file per Store (the
VFS shares a Store per process/database). Lookup/LRU metadata remains in RAM;
decoded frame payloads do not. Reads check authoritative raw/zero overrides
first, then page-number cache entries bound to the pinned manifest, before
resolving a compressed frame. Misses verify the frame and cache only live slots
not shadowed by active pages/truncation. Hits verify the cached page checksum.
A frame larger than the file budget can retain the requested page plus a bounded
subset of neighbors. Export and verification use a separate 9 MiB disk cache;
metadata compaction never decodes payload.

The cache is sparse and grows as pages are admitted; its capacity does not
preallocate the target size. `with_cache()` selects a fixed cap and zero disables
it; `with_automatic_cache()` restores the default sizing. The cap includes slot
padding. Slots are rounded up to filesystem allocation blocks so a small SQLite page can be punched without
affecting a neighbor. Local writes invalidate their cached page before the raw
write; truncate invalidates removed pages. The same slots are then reusable.
On refresh, another writer's active overrides invalidate matching cached pages;
switching manifests resets the private cache namespace. Old independent readers
keep their own pinned views/caches until they refresh or close.

Hole punching (macOS `F_PUNCHHOLE`, Linux/Android `FALLOC_FL_PUNCH_HOLE`) is attempted
immediately on invalidation, but is strictly an optimization: failure
does not discard entries for other pages, disable caching, or fail publication.
Invalidated slots remain reusable even when their old blocks stay allocated.
LRU eviction directly overwrites the reused slot without a redundant punch.
Actual cache read/write corruption or I/O failure bypasses the disposable cache
and falls back to verified source objects; it never blesses unverified bytes.
OS file caching, the bounded page index, dictionaries and transient frame decoding
remain separate memory costs. The 96 MiB dictionary training pool is unaffected.

## Dictionary growth

`DictionaryPolicy` separates enabled/disabled training, `DictionaryCapacity`
(8–768 KiB), and `SampleBudget` (1–96 MiB) into validated types. Its integer
constructor treats a zero capacity as disabled and validates every other value.
Defaults are 768 KiB / 96 MiB.

At seal, every eligible committed page is offered to a bounded, content-deduplicated,
digest-priority reservoir. Thus conversion samples across the full database in
an extra read pass, while fresh databases accumulate samples over many seals and
reopens. Sealing or evaluating a candidate does not empty the pool. At capacity,
the pool retains a deterministic content sample; rejected/duplicate content does
not trigger another evaluation. Missing/corrupt advisory state starts a new pool.
Unchanged pools are not rewritten. Changed pools are currently serialized as a
whole, compressed advisory file, not an incremental sample log.

Every fifth retained sample is held out of training. With another 1 MiB of
accepted distinct samples, evaluate the two largest supported size tiers with
at least 100 **training** bytes per dictionary byte (8, 16, 32, 64, 128, 256,
512, 768 KiB; custom ceilings also participate). A full default pool therefore
compares 512 and 768 KiB, rather than forcing the larger one. The held-out score
scales payload savings to the current logical database size, accounts for raw
fallback/reference overhead and charges the new dictionary once. Promotion
requires at least 5% improvement over the best retained dictionary or no dictionary.
This is a page-compression estimate, not a measurement of every configured frame
layout. Failed training/scoring/persistence does not prevent sealing.

The preferred pool retains up to four dictionaries, including a general fallback.
Old frames and fork pins retain additional required dictionaries independently.
Immutable dictionary objects up to 768 KiB are accepted by both readers and GC;
new dictionaries never reinterpret old frames. Compression contexts are reused
within each seal without making frames depend on one another. Pinned views keep
one owned reusable decoder per shared dictionary, protected by a mutex for
concurrent Rust readers; dictionary setup is not repeated on every frame miss.
The 96 MiB limit
is retained sample payload, not a process memory ceiling: metadata, trainer,
serialization, dictionaries and compression contexts consume additional memory.

## Retention and maintenance

`retain(path, name)`, `advance_retention(path, name)`, `retention(path, &name)`,
`open_retained(path, &name)` and `release_retention(path, pin)` expose named roots.
Each root pins a logical hash and end txid. Resolution selects the widest
available representation ending at **that exact hash**, validates its reconstructed
identity and decoding dependencies, and traces those dependencies for GC. A
wider rollup at the same endpoint can replace narrower representations; a later
endpoint with an overlapping txid range cannot. GC must keep a usable
representation of every pinned hash. It never issues deletion permits when a
retained endpoint is missing or corrupt.

The checksummed `ZROOT001` record is 144 bytes: 8-byte magic, 32-byte database ID,
32-byte logical hash, 8-byte end txid, 32-byte private retention token and a
32-byte checksum. Retaining an already sealed image writes no page-map snapshot.
The random token changes on every explicit replacement, including replacement
with the same logical endpoint, so stale release handles cannot match a recreated root.
Only valid logical root records are accepted. Invalid records stop collection
without modifying any objects.

Root replacement validates the representation logical lookup will select before
atomic rename. A stale release handle cannot delete an advanced or recreated root,
even at the same logical endpoint. A retained reader owns a manifest lease;
dropping the temporary reader releases only that lease. Named roots also prevent
whole-bundle deletion when their owners are offline.
Temporary readers retain their exact physical resolution, so they can continue
reading an older chain while subsequent readers adopt a wider rollup. Logical
pins promise contents and required decoding dictionaries, not the advisory
preferred-dictionary pool of a superseded physical representation.

Pins cover the sealed **main image**, not uncheckpointed WAL transactions. For an
application-consistent backup/fork, coordinate a closed/checkpointed SQLite
snapshot or use SQLite's backup protocol before retention. Do not treat a raw
bundle copy or pin as automatic capture of live WAL/journal state.

`collect(path, 0)` only reports. Other budgets limit deleted immutable objects.
Reports separate current-view bytes, additional fork-retained bytes,
reader-only-retained bytes, collectible bytes and estimated partially obsolete
bytes. Shared retained bytes are charged to forks first to avoid double counting.
Partial-obsolescence estimates use live-page occupancy; compressed pages need not
contribute equal physical bytes. Retained forks still prevent GC-driven copying
when their owners would keep the original pack anyway.

`maintain(path)` reports one bounded repack, decoded input and resulting GC work.
Fully live frames copy their encoded payload unchanged; partial frames drop dead
slots and recompress. Source comparison precedes head installation. The local
implementation currently builds while publication/catalogue locks remain held;
it does not promise nonblocking background compression. `compact()`
only merges metadata runs and preserves every payload location.
Idle maintenance first checks eligibility without reserving publication, then
selects again under publication exclusion if work exists. An idle reader with
fully live packs therefore does not compete with writers for that lock.

## Statistics

`statistics::connection_statistics` reads V1 VFS file-control counters
and SQLite's existing pager counters without resetting them. Its unsafe contract
requires a live, exclusively available SQLite pointer for the duration of the
call; no borrow is retained. Results have distinct per-handle I/O,
per-process/database plaintext-cache and per-connection SQLite scopes.

Snapshots count requested bytes, fetched frame payload/envelope bytes, inflated
bytes, decode-and-verification nanoseconds, cache hits/misses, useful extra pages
and unused extra pages on eviction. Manifest/dictionary open work is outside
frame-read counters, so the benchmark reports connection-open latency separately.
Compare cumulative snapshots for intervals. SQLite counters remain SQLite's
counters; the VFS does not pretend to observe pager cache hits directly.

The V1 `resident_bytes` field now means occupied disk-cache slot bytes, including
padding, not RAM/RSS or filesystem allocated blocks. Hole-punch failure can leave
free slots physically allocated. V1 fetched-byte counters measure source active/
frame reads, not additional plaintext-cache file I/O; query latency includes it.

`inspect()` includes frame-size/encoding distributions, pack occupancy,
preferred dictionary count, dictionary bytes and GC categories.

## Recovery tests

Compile-fail doctests cover ID/unit separation, validation bypass, immutable
objects, unfinished dependencies, consumed builders, escaping read pins and
forged/reused GC permits; positive domain and retention workflows also compile.
Runtime tests exercise hostile counts/references/slots, frame/cache obsolescence,
dictionary reuse, offline roots and leases, stale releases, whole-pack GC,
bounded repacking, all SQLite page sizes, truncate/regrow, rollback/WAL,
concurrency and exact export/reopen.

Test-only injection covers object sync/link/install, manifest finalization,
active sync/rename/directory sync, state publication and root replacement.
Subprocess abrupt exits cover every seal publication boundary. I/O-error tests
assert that possibly visible publication never triggers unsafe cleanup or an
old-page/new-history pairing. Corruption is rejected, not silently repaired into
an unauthenticated historical snapshot. These tests do not simulate every
filesystem, actual power loss or distributed storage.
