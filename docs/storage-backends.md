# Pluggable sealed storage and local bootstrap

The storage boundary is implemented without an S3 adapter. A backend owns the
complete sealed database: blobs, metadata manifests, dictionaries, endpoint and
placement indexes, retained roots, and the published sealed head. The local
active pagefile and the disk-backed extracted-page cache remain separate.

## Host configuration and opening

`Storage::new(Arc<dyn StorageBackend>, coordination_directory)` binds one backend
namespace to its shared host-local coordination directory. The host registers it
with `storage.register_vfs("archive")` (the `static` feature), then opens a local
path normally with SQLite's `sqlite3_open_v2(..., "archive")`, or selects that VFS
with `file:local.db?vfs=archive`. No per-open data-source selector is needed.
Use a different named VFS for another namespace. Registering an existing VFS name
with a different source fails, and bound paths cannot silently change sources.

Opening a missing local pagefile through that configured VFS bootstraps the
latest finalized seal automatically. With SQLite's CREATE flag, an entirely empty
namespace creates a new database. `storage.bootstrap(destination)` provides the
same restore explicitly and returns a `Database` with `inspect()` exposing its
restored transaction and history endpoint. `storage.create` requires an empty
namespace; `storage.open` opens or lazily restores a configured local instance.
Path-based library APIs continue to use filesystem storage by default.

All processes using one namespace share its coordination directory. Processes
opening the same named head share its writable pagefile; independent heads use
separate pagefiles and local lifecycle/publication/SQLite locks. Journals, WAL, SQLite locks/SHM, publication locks, lifecycle
leases remain local. This is a single-host lifecycle
protocol, not distributed leases. An adapter needs no filesystem for sealed data;
the memory backend exercises this boundary through library APIs and SQLite.

## Read and write paths

```text
SQLite page read
  -> authoritative active pagefile override
  -> valid extracted-page cache slot
  -> manifest: page -> frame -> PackId + PackRange
  -> pinned placement: PackId -> BlobId + BlobExtent
  -> backend range read -> authenticated frame -> extracted pages
```

The extracted-page cache remains a private, unlinked, disk-backed file with its
existing budget, page checksums, eviction, slot reuse, and invalidation behavior.
Writes and truncation invalidate affected slots. Refreshing another process's
publication invalidates active overrides; a changed manifest resets the cache
namespace. Cache residency neither retains nor authorizes deleting sealed data.
There is no persistent compressed-object cache or additional authoritative local
tier. The active pagefile remains authoritative for every unsealed write.

The normal read path deduplicates distinct frame requests and merges only
overlapping or adjacent ranges in the same blob. Input/output correspondence and exact lengths
are validated. Batches are bounded to 4096 requests and 64 MiB; the extracted-page
cache planner uses smaller 256-page / 16 MiB groups. Adapters may execute the
independent requests concurrently. Core code authenticates frame headers,
payloads, decoded lengths, and page checksums before caching.

## Transport contract

`StorageBackend: Send + Sync` is synchronous and object-safe. Another crate can
implement it and provide it through `Arc<dyn StorageBackend>`. Its capabilities
are:

- Adapter-owned staged writers: `begin_write()` returns an `ObjectWriter`;
  `finish(key, length)` consumes it and durably installs an immutable object.
  Dropping a writer aborts installation. Neither the final key nor length needs
  to be known before streaming. `put(key, length, Read)` is a convenience method
  for already named inputs, checking exact length and EOF.
- Batched bounded range reads, and separate existence/length inspection.
- Ordered paginated inventory with exclusive typed-key cursors.
- A small conditional root record: create-if-absent or compare with an opaque,
  never-reused revision token, returning applied, stale, or uncertain outcomes.
- Deletion consuming a namespace-bound, single-use core GC permit.

`ObjectKey` distinguishes blobs, manifests, dictionaries, and index objects.
Paths and future S3 keys are adapter details. Stat and inventory never authenticate
contents or establish liveness. Missing data, transport failures, corruption,
identity conflicts, stale publication, and uncertain publication are distinct.
Backend bytes are untrusted; core code constructs authentication proofs, durable
receipts, placement pins, and deletion authority.

`FilesystemBackend` uses synced immutable installation and a locked atomic root
replacement with a fresh revision. `MemoryBackend` implements the same contract
for its lifetime. `FaultBackend` wraps either transport with deterministic fault
injection and operation/byte accounting. There is no S3 implementation.

## Sealed identities and streaming

Mutable page writes and publication update the active pagefile and transaction
counter. They do not reread changed pages to compute a rolling write-history
hash. Sealing hashes the final page versions and encoded frames. The sealed
lineage digest binds the parent sealed view, database/lineage, transaction
endpoint, logical size/page size, and canonical page-version content root.
`Inspect::head_history` now describes sealed lineage and stays unchanged between
seals. The original sealed parent is recorded explicitly and exposed through
`ManifestStatistics::sealed_parent()`, including after metadata compaction.
Intermediate changes that end at identical page versions and the same endpoint
can share the same sealed representation.

A pack ID hashes its format header and ordered frame IDs, positions and lengths.
Frame IDs already authenticate codec/dictionary choices, page versions, and
encoded payload hashes. A blob ID hashes the canonical extent index and total
length. Hashing these small records avoids rereading whole payloads for naming;
IDs commit to the exact canonical representation through their child hashes.
Reads still authenticate fetched payload bytes before use.

Frames stream directly into adapter-owned blob staging. Only frame metadata and
the extent index remain in memory. The filesystem adapter syncs and atomically
installs that same temporary inode under the completed key; it does not copy the
completed payload into another staging file. Relocation copies and authenticates
source packs in bounded chunks while retaining exact source leases. Verification
likewise streams rather than collecting complete packs in memory. MemoryBackend
naturally retains its objects in RAM as its storage implementation.

Read batches retain the 64 MiB / 4096-request limits. Frame decoding and catalog
metadata decoding keep their own bounds. Pack targets may be large; writers also
rotate at a bounded metadata budget. Maintenance bounds the total decoded size of
source frames containing live pages across all selected packs, even when those
frames can be copied encoded. These are working-memory and metadata bounds,
not maximum physical object sizes.

## Writable forks in one object namespace

`storage.fork("experiment")` publishes a new head from the selected head's latest
finalized seal and returns a configured `Storage`. It initially shares the same
immutable manifests, packs, and dictionaries. Register that handle as a separate
named VFS, or call `fork.open(local_path)` to bootstrap its own writable pagefile.
Subsequent seals update only that fork's head. Identical object keys are idempotent
only for identical bytes; conflicting bytes are rejected. CAS prevents stale
catalog updates from overwriting another publication.

`storage.head("experiment")` selects a head in another process using the same
backend and namespace coordination directory; `storage.heads()` lists names.
Head names are bounded ASCII identifiers, defaulting to `main`. Restoring or
removing one head fences only its own attachment. `fork.remove_head()` requires
its readers/writers to close and preserves at least one head; old pagefiles then
fail attachment validation. Normal database deletion handles the last head and
refuses to destroy a namespace still containing other heads or retained roots.
GC protects every head, including forks whose local files have been discarded.
Retained snapshot names and object/placement leases remain namespace-wide.

## Catalog and publication

A checksummed `ZCAT0001` CAS root records database identity and refers to five
independent immutable indexes: placement, endpoints, retained roots, advisory
data, and named heads. Each head records its own attachment token, finalized
sealed descriptor, and optional pending seal descriptor. Updates preserve all
other heads under shared catalog exclusion and root CAS. Index runs are authenticated, sorted
additions/tombstones, with checkpoints at depth 16; decoding rejects cycles,
chains beyond 64, and more than 64 MiB decoded per index.

The explicit endpoint index replaces directory-name discovery. Parent resolution
requires an authenticated matching logical hash and transaction endpoint. It
prefers wider represented transaction coverage, then checkpoints, then a
deterministic manifest ID order. Filenames provide no discovery authority.

Seal publication is ordered:

1. Durably install payload blobs and required dictionaries.
2. Install metadata manifests and index deltas.
3. CAS-publish the pending candidate while retaining the previous sealed head.
4. Replace and sync the local active file.
5. Finalize the sealed head with another catalog CAS.

Recovery compares pending publication against the actual active header under
exclusion. If the candidate active file was installed, it finalizes that seal.
If the previous active head survived, recovery abandons the candidate through
a new catalog revision. Both dependency sets stay protected until that decision
or a successful bootstrap resolves the uncertainty. Pending candidates never become bootstrap
heads merely because their objects exist. A failed or uncertain publication
cannot authorize deletion using the writer's stale catalog state.

## Lazy bootstrap and recovery boundary

Each finalized head contains an authenticated descriptor with database/lineage
identity via its manifest, page size, logical size, transaction/history endpoint,
and persisted storage policy. Restoring needs no original header, sidecar, or
extracted-page cache.

Bootstrap pins the catalog state and physical placements, validates the resolved
manifest chain and required dictionaries, and fetches/authenticates the SQLite
header frame for a nonempty database. It stages and syncs a 12 KiB active file
containing the descriptor and initial active state, with no local page records.
The next transaction follows the restored endpoint. The extracted-page cache
starts empty; subsequent reads populate it and writes accumulate in the pagefile.
The already-validated metadata is transferred directly into the local Store.
Startup cost depends on metadata size and chain depth, local durability operations,
and adapter latency; it does not download the entire database payload.

Per-head lifecycle exclusion rejects restore while that head's local database or
retained reader is open. Another fork can remain open throughout the restore. A fresh attachment token is published by CAS and recorded in local
coordination metadata. Superseded pagefiles fail later opens. Multiple processes
can share the newly attached pagefile. Uncertain bootstrap CAS outcomes reread the
root and accept only the exact intended root. Installation never overwrites an
existing destination and uses an atomic no-replace rename on Linux and macOS.
An interrupted attempt is retryable before installation, or opens as a valid
installed database afterward. The optional `.db` notice is created normally.

Recovery reaches the finalized sealed endpoint. Unsealed pagefile writes and
transactions that exist only in WAL remain outside that boundary. Bootstrap does
not rewrite manifests or payloads. Forks can be created from a finalized head;
bootstrapping directly from a retained snapshot is not implemented.

## Physical placement and relocation

`PackId`, `BlobId`, `IndexId`, and the core's internal representation IDs are
distinct types. A
`PackRange` cannot be substituted for a blob-relative offset. `BlobExtent`
construction checks nonzero lengths, overflow, containment, object bounds, and
agreement with the logical pack length. Located ranges borrow their placement
pin; attempting to use another placement snapshot's range fails.

Blobs contain unchanged complete logical packs:

```text
ZBLOB001 header | complete pack A | complete pack B | ...
  | authenticated ZBINDEX1 extent index | footer offset + ZBEND001
```

The default is one pack per blob. `BlobIndex::authenticate` verifies a complete
blob, footer, contiguous extents, and every pack ID. Normal range reads use
pinned placement metadata and independent frame authentication, without fetching
the footer or whole blob.

`Database::relocate(packs, byte_budget)` explicitly groups compatible churn
cohorts. It pins and verifies source packs, installs a complete new blob, checks
catalog freshness/reachability under exclusion, then publishes preferred
representations while retaining old locations. Existing readers retain exact
blob leases; new readers choose the preferred representation. Relocation changes
no manifest, logical hash, frame ID, transaction range, or history endpoint.
Automatic grouping targets and consolidation are disabled.

## Collection and compaction

GC traces the active attachment, every fork's finalized and pending heads, retained logical
roots, reader manifests, and exact physical leases under catalog exclusion. It
revalidates required metadata before granting any deletion permit. Corrupt or
missing required metadata blocks deletion. Preferred representations protect
logical roots; old exact readers and pending uncertainty retain additional blobs.

Reports distinguish unreachable packs, wholly dead blobs, dead extents within
live blobs, fork/reader/uncertainty retention, and physical bytes copied by
relocation. Dead extents are not individually deleted. A blob is removed only
when all its extents and exact readers are dead. Permits are single-use and bound
to the backend namespace and catalog exclusion lifetime.

Database metadata compaction writes no payload or placement metadata. Flush
unsealed changes explicitly first; compaction returns Busy while they remain.
GC is explicit maintenance and can publish endpoint/representation retirements
before deleting objects.

`Database::maintain()` provides the same bounded repack pass as `maintain(path)`
for configured backends. It selects multiple packs below 50% estimated live-byte occupancy,
preferring estimated reclaimed bytes per unit of surviving frame work and
skipping packs retained by other roots. The total `maintenance_input` budget
counts each surviving frame's full decoded size, including obsolete slots in
partially live frames; fully dead frames cost no input. Packs exceeding the
remaining budget are skipped, while packs at least 50% live await further churn.
Selection and GC reporting use the resolved live-frame metadata and placement
lengths without fetching cold payload blobs. Obsolete complete frame spans are
counted exactly; partial-frame waste is estimated in proportion to obsolete
pages. The explicit inspection API still reads pack headers for exact inventory.

Fully live frames are copied encoded after record-header and payload-hash
authentication, preserving frame IDs, codecs and dictionary dependencies. Only
partially obsolete multi-page frames are decoded and recompressed; default page
frames require neither operation. Repacking reuses existing dictionaries and
does not load or retrain the advisory sample reservoir. Reports expose
`repacked_packs`, `copied_frames`,
`copied_bytes` (encoded payload only, excluding record headers), `decoded_input`
(actual partial-frame decoding), and `gc`.

Maintenance groups source records in pack/offset order into up to 8 MiB ranges.
It may bridge gaps up to 64 KiB while capping total fetched bytes at four times
the included complete frame records; a single large frame gets its own bounded
window up to 16 MiB. Only one window is retained at a time. Collection also
memoizes object lengths within its catalog exclusion, avoiding repeated stat
requests for the same blob. Inventory remains a full namespace sweep, so the
deletion budget bounds deletions rather than total scanning work.

The intended local-first asynchronous tiering contract and its separate local
and remote publication/collection boundaries are described in
[Tiered storage](tiered-storage.md).

The pass installs one metadata checkpoint and batches all placement updates in
one catalog registration CAS, followed by the pending and final publication CAS
operations. Collection can add one retirement CAS. Thus catalog publication work
is fixed per pass, independent of pack count; the catalog and local active file
still use the recoverable publication protocol above. The existing endpoint
index selects the widest checkpoint at the same logical endpoint. No additional
format or aggregated-pack lookup is required. Exact reader leases retain old
blobs until readers release them, and whole unreachable blobs remain directly
collectible without copying.

## Formats, verification, and deferred work

The active, catalog, blob, index, logical segment, and frame formats are V1.
Their decoders validate format identifiers, bounds, and authentication data.

Tests cover an external-crate transport through SQLite and bootstrap, lazy reads,
cache validation/invalidation/eviction, bootstrap publication boundaries and stale
attachments, exact-reader relocation, malformed metadata blocking collection,
range bounds/batching, and metadata/placement isolation. Default/static suites,
subprocess recovery/concurrency tests, doctests and CI Clippy checks remain the
validation commands.

Deferred: S3 transport, distributed coordination, additional authoritative local
tiers, compressed-object caching, asynchronous offloading, automatic grouping,
and automatic consolidation. A trained startup read profile could later live in
the advisory index: record pages touched by representative queries, resolve them
through the current manifest, and prefetch a bounded set of distinct frames into
the existing extracted-page cache. Training and eager fetching are not enabled.

A self-contained file for distributing a large read-only database is possible:
it could bundle multiple unchanged packs plus the catalog, manifests, indexes,
and dictionaries, serving reads through the same range interface. This would
need a read-only attachment mode that does not publish a replacement token.
Objects and ranges use checked 64-bit lengths and offsets without a 512 MiB
object cap. Metadata remains in separate objects, and the full page map is
resolved in memory at open. A self-contained read-only bundle and lazy page-map
lookup remain deferred.
