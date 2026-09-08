# LTX-style compressed page segments

Status: implemented as the breaking V5 storage format. V5 deliberately has no
reader, writer, or migration path for older zsqlite formats. The filesystem
backend is implemented now; the S3 backend and remote-backed local eviction
policies described below remain future work.

The implementation keeps the exact canonical segment list in an immutable
`.zroot` catalog selected by the alternating roots in the `.zsqlite` control
file. Every sealed `.zseg` contains its complete ending page-to-last-TXID map.
Keeping the dependency catalog separate avoids rewriting the newest immutable
segment when a compaction changes only physical reachability.

## Summary

Replace the raw fixed-slot working page tier and asynchronously rebuilt
compressed extents with a sequence of self-contained page segments. Each live
page version is either raw bootstrap data or an independent Zstandard frame
compressed with a shared dictionary embedded in its segment. An active segment
accepts transactions but retains only the newest committed version of each
page written since that segment began. Rotation makes the resulting end-state
delta immutable. Sealed segments can be copied, compacted, uploaded, cached,
and garbage-collected.

Segment size is policy, not format. A transaction boundary, idle flush,
durability request, administrative command, or storage-backend preference may
produce a small or large segment. No 4 MiB minimum or target is part of the
identity or correctness rules.

The stable `.zsqlite` file is a small control file. It identifies the database,
the current and previous roots, and the active segment's committed position.
The data lives in `.zsqlite.d/`:

```text
database.zsqlite
database.zsqlite.d/
  segments/
    0000000000000001-0000000000000080-<history-0080>-<physical>.zseg
    0000000000000081-00000000000000c0-<history-00c0>-<physical>.zseg
  active/
    <writer-id>.zactive
  roots/
    <head-txid>-<history>-<catalog>.zroot
  locks/
```

At any point, the current root selects one canonical, gap-free set of segments.
Other files may remain temporarily because they belong to the previous root,
an active reader or backup, a losing fork, or pending garbage collection.

## Goals

- Preserve SQLite transaction visibility and requested sync semantics.
- Permit the newest local segment to be the only durable copy temporarily.
- Make every committed page independently readable and independently movable.
- Eliminate 64 KiB read amplification for 4 KiB point reads.
- Avoid decompression and recompression during normal segment rotation and GC.
- Allow local durability first and S3 convergence later.
- Calculate a complete restore plan from object metadata before fetching page
  contents.
- Make a segment self-contained, including every Zstandard dictionary needed
  to decode it.
- Let every sealed segment describe its complete ending page-version map, and
  let the selected root catalog name the exact older segments needed to
  resolve those versions.
- Manage local storage according to physically allocated blocks and page heat,
  rather than apparent file length or slot reuse.
- Detect divergent histories and corrupted content cryptographically.

## Terms

### SQLite page number

SQLite pages are one-based logical page identifiers. For page size `P`, page
number `N` represents database byte range:

```text
[(N - 1) * P, N * P)
```

`page_number` identifies which part of the SQLite database a frame contains.
`last_txid` identifies the version of that page. One cannot replace the other.

In a dense global map, page number need not be stored explicitly: array element
`N - 1` is the entry for page `N`.

### TXID

SQLite does not provide the VFS with a durable, globally monotonic transaction
identifier. zsqlite assigns its own unsigned 64-bit TXID while holding the
single-writer/publication lock:

```text
next_txid = committed_head_txid + 1
```

The ID is assigned to an atomic published page set. It does not need to match
an internal SQLite identifier.

With rollback journaling, the VFS can infer boundaries from the journal,
database sync, journal invalidation, and lock sequence. With WAL, a SQL
transaction commits at a WAL frame whose database-size field is nonzero. The
current VFS passes SQLite WAL files through, so its present generations
describe main-image writes/checkpoint batches rather than individual WAL-mode
SQL transactions. Producing one zsqlite TXID per WAL transaction requires
intercepting and parsing WAL commits.

### Segment

A segment covers one or more committed TXIDs and materializes the database
delta at the end of that range. It contains independently encoded page frames,
an index, page-resolution metadata, any required embedded dictionaries, and
integrity metadata. If a page changes repeatedly within the range, only its
last version is required in the segment's ending state.

The active segment is mutable. `xWrite` supplies replacement bytes, so it does
not need page-level copy-on-write or an old page to construct a new one. It
stages one final value per dirty page until publication and keeps the previous
committed frame only until the replacement is committed. Once another active
segment is published, its predecessor is immutable. A segment does not
necessarily preserve page bodies needed to reconstruct an interior TXID; the
retained restore points are segment endpoints unless a reader or retention
policy caused an earlier endpoint to be sealed and kept.

### Root

A root selects the canonical segment set and committed active position for one
database state. The control file retains at least the current and previous
roots. Roots used by readers, backups, or uploads may also be pinned.

## LTX range naming

LTX names files by the transaction range represented by the file:

```text
{min_txid:016x}-{max_txid:016x}.ltx

0000000000000001-0000000000000001.ltx
0000000000000002-000000000000000a.ltx
000000000000000b-0000000000000014.ltx
```

The fixed-width lowercase hexadecimal numbers sort lexically in TXID order.
Files with `min_txid == max_txid` represent one transaction. Compaction creates
wider ranges. In LTX, a range beginning at TXID 1 is a complete snapshot and
must contain all database pages.

References:

- <https://github.com/superfly/ltx/blob/main/ltx.go#L428-L464>
- <https://github.com/superfly/ltx/blob/main/README.md>
- <https://github.com/benbjohnson/litestream/blob/main/replica.go#L1411-L1519>

zsqlite adds the running history BLAKE3 at the end of the represented range,
followed by a digest of this particular physical encoding:

```text
{min_txid:016x}-{max_txid:016x}-{end_history_hash}-{physical_digest}.zseg

0000000000000001-0000000000000080-<history-0080>-<physical>.zseg
0000000000000081-00000000000000c0-<history-00c0>-<physical>.zseg
```

The TXID fields use fixed-width lowercase hexadecimal, as LTX does, so normal
lexical ordering groups files by starting TXID and then ending TXID. The full
history hash keeps forks with the same TXID range distinct. It is the logical
history identity at `max_txid`, not a digest of the segment's current physical
encoding. The final physical digest lets old and new encodings coexist while
readers or roots still pin them.

Every segment header also contains:

```text
start_txid = min_txid
base_history_hash = history_hash[start_txid - 1]
end_txid = max_txid
end_history_hash = history_hash[end_txid]
```

This says exactly where a segment begins and which running history it derives
from. The filename's ending hash must match the header. A genesis segment uses
the format-defined genesis history hash as its base.

Range-and-history names make an object listing useful as a restore index.
Starting from a snapshot, a planner selects gap-free ranges that reach the
desired TXID and whose base and ending history hashes connect, preferring
compacted ranges that cover the needed pages with fewer objects. It can
determine every required segment before downloading page data. Dictionaries
do not add another fetch because they are embedded in their segment.

The root remains authoritative because old compactions and losing branches can
produce overlapping range files. A TXID identifies one segment only within the
canonical, nonoverlapping segment set selected by that root. The immutable
`.zroot` catalog carries that canonical range table; lexical listing and
hash-chain validation remain recovery tools.

Compaction changes `min_txid` and physical bytes but does not invent a new
history. Its output keeps the `end_history_hash` of the newest input verbatim,
and therefore keeps that latest BLAKE3 component in its name. For example:

```text
before:
  0000000000000001-0000000000000040-<history-0040>-<physical-a>.zseg
  0000000000000041-0000000000000080-<history-0080>-<physical-b>.zseg

after:
  0000000000000001-0000000000000080-<history-0080>-<physical-c>.zseg
```

The output gets a newly computed physical digest in its trailer because its
bytes changed. That physical digest is deliberately not the running history
name. Compaction preserves the `history-0080` component exactly and computes
only `physical-c`. This lets both encodings remain immutable and coexist until
the old root is no longer pinned.

## Segment contents

The V5 sealed-segment layout is:

```text
segment header
  format version
  database ID
  SQLite page size
  start and end TXID
  base and ending history hash

embedded dictionary table
  dictionary BLAKE3 and bytes
  ...

live page frame records

optional compact transaction-hash summaries

segment page index
ending full page map
segment trailer
  content-state root
  physical segment digest
```

The exact canonical range/dependency table lives in the immutable root catalog
rather than being duplicated inside the segment. The data format is canonical
and domain-separated. All sizes and
offsets must be checked for overflow and range validity while decoding.

### Page frame record

A page frame minimally needs:

```text
page number
last TXID
codec: raw or Zstandard
dictionary table index
stored length
raw length or implied SQLite page size
raw page digest
raw bytes or compressed Zstandard frame bytes
```

The dictionary index is absent for a raw frame and can be omitted when a
segment embeds only one dictionary. The full dictionary digest detects
corruption and binds the exact prepared dictionary to the encoded frames; do
not rely on Zstandard's shorter dictionary ID as a globally unique identity.

The raw page digest allows validation after decompression and moves with the
frame during byte-copy compaction. A physical segment digest separately covers
the exact encoded bytes and structural metadata.

### Commit publication record

The active segment uses a commit publication record to make exactly one ending
state current. It should include:

```text
TXID
resulting SQLite database size in pages
final frame locations for pages changed by this transaction
canonical root of the pages changed by this transaction
previous history hash
resulting history hash
commit-record checksum or digest
```

Only the final committed version of a page within a SQLite transaction is
written. Once that commit is durable and published, older frames for the same
page in this active segment leave the live index and their byte ranges become
reclaimable. A truncate-only transaction still records the new database size.

After a crash, recovery finds the last complete, authenticated transaction
publication and ignores unreferenced staged bytes or a torn publication
record. An alternating committed-end pointer in the segment or stable control
file can make this lookup constant time. This is transactional staging and
atomic metadata publication, not a general copy-on-write page tree.

### Segment page index

The segment index supports exact random frame reads and map reconstruction. A
straightforward entry is:

```text
page_number -> last_txid, frame_offset, record_length, raw_page_digest
```

Page numbers can be represented as sorted deltas or a bitmap. Frame offsets can
be prefix sums of compressed lengths rather than explicit 64-bit values. V5
duplicates the raw digest in the frame and index: the index can build the
content map without per-frame reads, while the frame remains independently
verifiable and byte-copyable.

If a page is updated more than once in a segment, the ending-state index points
only to its newest committed frame. After the replacement commits, the older
frame leaves the live set and its range is free. Retaining point-in-time reads
inside a segment would require keeping older page frames and is intentionally
not the default design.

The active index is updated transactionally with the commit publication. A
sealed segment may encode the same information more densely. The index can be
rebuilt by scanning self-describing live records, but it is used in the normal
random-read path.

### Ending page map and dependency table

Every V5 sealed segment embeds the complete page-version map at the segment
endpoint:

```text
database page count at end_txid
page 1 -> last_txid
page 2 -> last_txid
...
```

Array position implies page number. Normally the array is bounded by the
current database page count, and truncation shortens it. A zero TXID denotes
an implicit zero-filled sparse page. This is required because a WAL checkpoint
may extend the main database image before it copies every newly addressable
page out of the WAL.

The immutable `.zroot` catalog selected by the control root stores the compact
canonical range table. Each entry gives the logical filename, TXID range, base
history hash, ending history hash, physical digest, and file length of a
segment selected to resolve the endpoint. Given the current root, a reader can
therefore:

1. Decode the newest segment's full page map.
2. Collect the distinct `last_txid` values actually needed.
3. Resolve those TXIDs through the canonical range table.
4. Fetch exactly those older segment files and their indexes.

The current segment itself resolves page versions materialized within its
range. A compacted segment may resolve a wider range. The table is a direct
dependency plan, not another source of truth: every entry must agree with the
filename, embedded header hashes, and current root.

A raw dense map costs eight bytes per SQLite page—about 1 MiB for the
132,689-page test database, or roughly 0.2% of its 518 MiB logical size. Page
TXIDs have many repeated values and runs, so V5 uses run-length/varint encoding
followed by Zstandard when beneficial. Repeating a compressed full map in many
tiny segments can still dominate their payload; that cost is reported and can
motivate a later format revision, but V5 intentionally makes the full map
mandatory.

## Active segments and commit path

The active segment is a mutable encoded-page container with a transaction
write buffer. It has a live page index containing at most one committed frame
per page changed since `start_txid`. Its base map is stored in the active file;
the current ending map and canonical range table are maintained in memory from
the commit chain and selected root catalog.

The `xWrite` contract supplies a byte range and file offset. SQLite pager
writes are commonly a full aligned page, which can be accepted directly. The
VFS must still handle a partial or cross-page write by merging the supplied
bytes with the current page. The interface deliberately exposes `iAmt` and
`iOfst`, not a page object; see
<https://www.sqlite.org/c3ref/io_methods.html>. Repeated writes to the same page
in one transaction replace its pending value rather than creating historical
frames.

A commit proceeds as follows:

1. Buffer SQLite writes for the current atomic publication.
2. Retain only the final content of each changed page.
3. Encode each page as raw bytes during dictionary bootstrap, or as an
   independent Zstandard frame using the current dictionary embedded in the
   active segment.
4. Put each final frame in an unreferenced free range or at the end of the
   active file. Do not overwrite a frame belonging to the current committed
   state.
5. Append the authenticated commit publication and updated index/map entries.
6. Honor SQLite's requested sync boundary.
7. Publish the new committed TXID/offset for readers.
8. Remove superseded frames from the live index. Their ranges can now be
   reused or hole-punched once no protected previous publication needs them.

This is only a one-transaction old/new distinction. It does not copy unchanged
pages and does not maintain a persistent copy-on-write tree. The active file
may contain free or punched ranges, but it does not logically retain
intermediate page versions. Allocation should reuse suitable free ranges
before extending the file. Reclamation works at filesystem allocation
granularity, so adjacent dead ranges may need to accumulate before hole
punching releases blocks. Packing away remaining empty ranges is compaction
work, not part of every commit.

The running history hash advances for every committed TXID even though old
page bodies are discarded. Compact transaction-hash summaries may be retained
to audit the chain from `base_history_hash` to `end_history_hash`; they are not
used for page reads.

Compression therefore moves onto the commit path. The existing raw working
page design avoids this CPU work during commits but requires later compression
and a more complicated convergence state machine.

With `synchronous=FULL`, frame bytes and the commit publication must be synced
before the new committed head is durably advertised. With
`synchronous=OFF`, SQLite has declined crash-durability guarantees; the state
can become visible without forcing it to stable storage. The format must still
ensure recovery selects a complete commit rather than a partial tail.

### Rotation

Rotation occurs only after a complete transaction:

1. Stop accepting writes into the old active segment.
2. Sync it according to the required durability policy.
3. Freeze its live index, ending page map, embedded dictionaries, and exact
   committed state, then write a replacement immutable root catalog.
4. Finalize its physical digest. A sparse local representation may retain free
   ranges until later compaction.
5. Rename it to its immutable range-and-ending-history name and sync the
   directory.
6. Create a child active segment for the next TXID.
7. Atomically switch the stable control root.

This is logical sealing—the old file becomes immutable—but it does not require
rewriting or recompressing live page frames. A child starts at the
predecessor's ending TXID plus one and records the predecessor's ending history
hash as its base history. An explicit physical parent ID is unnecessary
because the page map, selected root catalog, and history hashes establish the
dependency set and continuity.

Rotation has no byte-size threshold in the format. Policy may rotate after one
transaction, many transactions, a time limit, an explicit flush, or a backend
request. If the frozen file is too sparse for efficient upload, a separate
compaction copies its live compressed frames into a packed segment.

The `.zsqlite` control file should remain stable. Renaming the pathname that
multiple VFS users consider the database creates inode and recovery problems:
existing descriptors continue to refer to the renamed inode while new openers
can see a different file.

## Read path and page mapping

The V5 direct read mapping is:

```text
dense global map:
  SQLite page number -> last TXID
  TXID 0 -> implicit sparse zero page

canonical range/dependency table:
  TXID range -> segment object

segment index:
  SQLite page number -> frame offset, length, and codec
```

For a 4 KiB database, a dense array of 64-bit last-TXID values costs eight
bytes per page. The 132,689-page test database would require about 1 MiB before
compression. The array position implies page number.

Each sealed segment stores the complete ending map; the selected `.zroot`
catalog stores the canonical range table. Opening the root catalog and newest
segment identifies the exact page versions and older objects needed without
downloading every older index.

Compaction changes which object resolves some versions but preserves each
page's `last_txid`. It copies the latest input's ending page map unchanged at
the logical level and rewrites the root catalog and physical encoding.

A demand read performs:

1. Convert the requested SQLite byte range to a page number.
2. Read its last TXID from the in-memory map.
3. Binary-search the canonical segment ranges for that TXID.
4. Use the segment index to find the frame range and codec.
5. `pread` only that frame.
6. Return zeros for TXID 0; otherwise return a raw frame directly, or load the
   embedded dictionary selected by a compressed frame and decompress one
   SQLite page.
7. Verify its raw digest.

An old on-disk segment has essentially the same data-path cost as the active
segment: a memory lookup, one small random read, and at most one-page
decompression. There is still filesystem block-granularity I/O, descriptor
caching, and Zstd CPU overhead, but no 64 KiB decompression requirement for a
4 KiB page.

For S3, the same layout permits a footer range request for the index followed
by exact range requests for frames. Network latency makes local caching and
request coalescing important even when byte amplification is small.

## Shared-dictionary page frames

Concatenated independent Zstandard frames form a valid multi-frame stream. They
can also be treated simply as individually addressable frame records. Copying
or concatenating already-compressed frames does not exploit new cross-page
redundancy, but a shared trained dictionary recovers much of the compression
lost when the frame size falls from 64 KiB to one SQLite page.

Once a page is compressed with an immutable shared dictionary, zsqlite can:

- Concatenate it into another segment without decompression.
- Rebuild only offsets, indexes, and segment authentication metadata.
- Copy it byte-for-byte during GC.
- Replace a modified page by compressing only its new frame.
- Leave unchanged pages in their existing segments.

The first commit of a new page version still performs one compression. Normal
segments should use one long-lived database dictionary, so every frame is
freely copyable and the per-frame dictionary selector is implicit.

### Dictionary storage

Dictionaries are immutable, BLAKE3-identified byte strings embedded in each
segment that needs them. A normal segment has one table entry. A compacted
segment may have several entries when its copied frames span dictionary
generations:

```text
dictionary table:
  0 -> dictionary digest A, dictionary bytes A
  1 -> dictionary digest B, dictionary bytes B

page frame/index entry:
  dictionary table index -> 0 or 1
```

Compaction copies each Zstandard frame byte-for-byte, adds its immutable
dictionary to the output table if not already present, and changes only the
small dictionary selector and frame offset in the new index. It does not run a
decompression loop. The page digest, `last_txid`, content-state root, and
history hash are defined over logical page identity/content rather than
compressed bytes, so they remain unchanged. The new segment physical digest is
recomputed because its container bytes and offsets changed.

The active writer has one current dictionary and embeds it when creating a new
active segment. A later dictionary rotation affects only newly compressed page
versions. Old frames remain movable because a destination can embed both old
and new dictionaries. Recompressing old frames into the newest dictionary is
an optional, explicitly measured space optimization, never a requirement for
rotation, compaction, upload, or GC.

Embedding a 64 KiB dictionary adds roughly 64 KiB of fixed overhead to every
segment that uses it, and multiple generations add one copy of each distinct
dictionary. This makes very small segments valid but potentially inefficient;
it is another reason to measure dictionary size and segment counts, not a
reason to impose a minimum segment payload.

### Dictionary lifecycle

Dictionary construction is asynchronous and generational. A segment never
changes dictionaries while it is accepting writes.

#### Bootstrap

`raw` is a valid self-describing page-frame codec. A new database can therefore
commit and seal segments before a useful dictionary exists:

1. Store the final page values as raw frames and collect a bounded,
   page-diverse sample of those values.
2. Seal and publish raw segments normally; dictionary training must not delay
   their durability.
3. Once the sample has enough useful page data, train a candidate dictionary
   off the commit path.
4. Compare the candidate against raw and any current dictionary on a separate
   held-out sample.
5. If it wins by enough to repay its embedded bytes and operational cost,
   install it only when creating the next active segment.

Use a page/sample-byte threshold rather than a segment count because segment
sizes are unconstrained. The existing experiment's 8,192 pages / 32 MiB of
training input is a sensible first benchmark point, not yet a format constant.
A small database may train from all available distinct pages; a database with
too little or poorly compressible data can remain raw.

Sealing a raw active segment and training can run independently. The writer can
continue producing raw segments until the candidate is ready, then switch at a
transaction and segment boundary. There is no multi-second training pause in
the commit path.

#### Keeping the dictionary current

While accepting writes, maintain a bounded reservoir of complete raw page
images already assembled from `xWrite`. Admit only final committed page values,
not intermediate or rolled-back writes. Sample by page identity as well as
write frequency so a handful of hot pages do not dominate training. Pages
already decompressed for demand reads may supplement the reservoir without
causing a scan solely for dictionary maintenance.

Periodically train a candidate after enough distinct-page churn, rather than
merely on a wall-clock timer. Evaluate current and candidate dictionaries on
the same held-out recent sample and include these costs in the promotion
decision:

- compressed frame bytes;
- dictionary bytes repeated in future segments;
- compression and decompression CPU;
- the extra dictionary-table entry retained by mixed-generation compactions;
  and
- the expected lifetime and write mix of the database.

Promote only a material improvement, and rate-limit promotions so dictionary
generations remain uncommon. Promotion rotates the active segment: the old
segment keeps its embedded dictionary, while the new one embeds and uses the
candidate. The control root records the current dictionary digest for the next
active segment; the writer obtains its bytes from the newest segment or its
prepared-dictionary cache. Decoding always follows the dictionary embedded in
the segment containing the frame.

#### Compaction across generations

The default compactor never normalizes codecs or dictionaries. It copies raw
frames and Zstandard frames byte-for-byte and embeds the union of dictionaries
required by the copied compressed frames. This guarantees there is no
decompression loop in ordinary compaction.

An optional rewrite mode may compress old raw frames with a chosen dictionary
or decompress and recompress old Zstandard frames when measured space savings
justify it. That creates a new physical object, preserves every page's
`last_txid` and the segment's ending history hash, and is never required for
correctness, upload, or GC.

### Compression experiment

The repeatable benchmark is `benches/page_dictionary.rs`:

```console
cargo bench --bench page_dictionary -- \
  test-dbs/files/tklink/c64f18c0-d9db-4d19-a058-287cea1fae8f/db.sqlite
```

It tested a 518.32 MiB database containing 132,689 4 KiB pages at Zstandard
level 3. A 64 KiB dictionary was trained from 8,192 evenly distributed pages
(32 MiB). Estimates use the implemented V5 frame-header and compact index-entry
sizes, raw fallback, dictionary-table and segment-container overhead, and a
conservative uncompressed upper bound for the full page map.

| Layout | Zstd frame bytes | Frame + index metadata | Estimated sidecar |
|---|---:|---:|---:|
| Independent 64 KiB frames | 142.16 MiB | 1.11 MiB | 145.81 MiB (28.132%) |
| Independent 4 KiB page frames | 172.62 MiB | 17.72 MiB | 192.88 MiB (37.212%) |
| 4 KiB page frames + shared dictionary | 146.42 MiB | 17.72 MiB | 166.74 MiB (32.169%) |

Dictionary-backed page frames were 2.993% larger in raw Zstd bytes and 14.352%
larger in estimated sidecar size than 64 KiB frames. The total estimated
difference was 4.037% of the original database. The shared dictionary reduced
the estimated per-page-frame sidecar by 13.552% relative to page frames without
a dictionary.

Compression took 1.44 seconds for all dictionary-backed page frames versus
0.86 seconds for 64 KiB frames. Dictionary training took 2.05 seconds once.
This microbenchmark does not include complete transaction latency, digest
calculation, locking, writes, or `fsync`; `benches/vfs_performance.rs` measures
the end-to-end VFS path under both SQLite cache pressure and a cache-resident
profile.

This result suggests compression CPU is unlikely to dominate small durable
transactions, where `fsync` normally dominates. Testing different transaction
sizes under SQLite `synchronous=OFF`, `NORMAL`, and `FULL` remains useful; the
`OFF` case exposes CPU and metadata overhead most clearly.

The dominant remaining space penalty is the 80-byte independently verifiable
frame header plus 60-byte segment index entry per stored page. The index keeps
the page digest so startup can build and verify the content map without one
range read per page; the frame keeps it so frames remain self-contained and
byte-copyable. Future
delta-coded offsets and page numbers could reduce the index portion without
weakening the frame's BLAKE3 integrity check.

## History and fork detection

Hash TXID content, not merely the integer TXID values. A canonical construction
can be:

```text
page_hash = BLAKE3(
  "zsqlite/page/v1" ||
  txid ||
  page_number ||
  raw_page_bytes
)

transaction_hash = BLAKE3(
  "zsqlite/transaction/v1" ||
  txid ||
  resulting_database_page_count ||
  sorted(page_hashes)
)

history_hash[n] = BLAKE3(
  "zsqlite/history/v1" ||
  history_hash[n - 1] ||
  transaction_hash
)
```

All encodings need unambiguous lengths, fixed endianness, and domain
separation. Truncation-only and empty transactions must bind their resulting
database size and other state changes even when they contain no page hashes.

If two writers begin from the same history at TXID 100 and publish different
transaction 101 contents, they produce different history hashes:

```text
history-100
  |-- transaction-101A -> history-101A
  `-- transaction-101B -> history-101B
```

Every descendant remains different. Range-and-hash filenames allow both fork
objects to coexist without overwriting one another. The hash makes a fork
evident; it does not choose or prevent one. Single-writer fencing and atomic or
conditional root publication select the canonical branch.

Keep three concepts separate:

- **Page digest:** authenticates the raw content and identity of one page
  version.
- **History hash:** commits to every transaction and its order. It remains
  stable when physical data is compacted.
- **Physical segment digest:** commits to exact segment bytes, index, dictionary
  references, and encoding. It changes during repacking.

A plain BLAKE3 chain provides corruption and fork evidence relative to a
trusted root. It does not authenticate which writer created the history. That
requires a keyed MAC, digital signature, or an externally trusted conditional
head update.

Compacted checkpoints can carry the existing history hash forward. To prove
the full discarded transaction sequence later, retain transaction hashes or a
compact append-only Merkle accumulator. If old history is not independently
auditable after GC, the ending history hash can instead be treated as an opaque
trusted checkpoint commitment.

## Segment compaction

### Rotation without compaction

The cheapest path is to rotate the active segment at any useful transaction
boundary and upload it directly. Rotation writes no page data twice and has no
minimum or target payload size.

### Local compaction before upload

Compaction is useful when an active file has reusable or punched ranges, when
sealed files contain avoidable empty ranges, or when policy wants fewer
objects:

1. Pin a root/generation.
2. Select sealed local segments.
3. Determine the newest live frame for each page materialized by the selected
   range.
4. Copy those raw or compressed frame bytes into a packed output without
   decoding them. Deduplicate and embed all dictionaries referenced by the
   compressed frames.
5. Copy the latest input's full ending page map; rebuild the root catalog,
   index, free-space-free physical layout, and physical digest.
6. Sync the new segment.
7. Atomically publish a root that substitutes it for the inputs.
8. Delete inputs after old roots and readers release them.

The compacted output takes the earliest input's `start_txid` and
`base_history_hash`, and the latest input's `end_txid` and
`end_history_hash`. The ending hash is copied verbatim, not recalculated from
the new physical bytes. Empty and punched ranges are omitted.

Do not mutate a sealed segment in place. Write a replacement, publish it, and
then collect the old objects.

### Full checkpoint compaction

Full GC can collapse all data ancestry at TXID `N` into one checkpoint:

```text
before:
  A -> B -> C -> D       (head at N)

after:
  checkpoint-D           (latest version of every page at N)
```

The checkpoint includes the latest version of every SQLite page from 1 through
the current database page count. SQLite freelist pages remain part of the
database image; pages removed by truncation do not. It records each page's
original `last_txid`, the content-state root at `N`, and the history hash at
`N`. It has no dependency on the replaced data segments.

The output filename ends in the existing history hash at `N`; that hash is
copied from the prior head rather than recomputed. The content-state root proves
the same database image, and the physical segment digest changes because the
encoding is new. Its dictionary table contains each distinct dictionary needed
by the copied frames, so building the checkpoint needs no page decompression.

If writes continue while compaction works, pin TXID `N`, build its checkpoint,
and retain later deltas:

```text
checkpoint at N -> segments N+1 through M
```

Publishing only needs a short root transition. Old ancestry at or before `N`
becomes collectible. A design with immutable physical parent pointers must
avoid rewriting post-`N` children; separating the root's canonical segment
catalog from history continuity makes this substitution easier.

## Segment sizing and upload

Segment size has no correctness significance and the format imposes no 4 MiB
goal. Upload policy may send any sealed segment as-is. It can consider object
count, request cost, dead or empty ranges, network conditions, database write
rate, and desired convergence age:

- Rotate and upload a clean segment of any size.
- Merge segments when reducing object count is worth copying their live
  compressed frames.
- Repack a sparse segment before upload when omitting empty ranges saves enough
  space to justify the work.
- Upload an idle tail after a maximum age or explicit flush, however small.
- Keep a large segment intact when it is already efficient to read and upload.

Local-only segments may be the sole durable copy. Before publishing a remote
root, upload and verify every segment it references. Each segment already
contains its decoding dictionaries. Publish the remote head last using
conditional update semantics.

Page heat must not decide whether a live local-only page is uploaded. All live
pages must eventually become remote-durable. Heat controls which local copies
remain after upload.

## Local cache and physical-block management

The storage target applies to physically allocated page-data blocks, not
apparent segment length, allocated slot count, or whether an address has been
reused.

A possible soft target is:

```text
target local page-data allocation = logical SQLite database bytes * 20%
```

Track allocation classes separately:

- Local-only committed frames: durable locally and not evictable.
- Uploaded clean frames: evictable according to heat.
- Punched or absent ranges: consume no page-data blocks.
- Headers, indexes, control records, and filesystem metadata: reported
  separately from page-cache occupancy.

Local-only data can temporarily exceed the target. It becomes eviction-eligible
only after another durable copy is confirmed. A 22% high-water mark with
eviction toward 18-20% avoids constant map changes and hole punching.

The VFS observes demand reads that reach it. SQLite pager-cache hits are
invisible, which is useful: a page currently served from SQLite's cache does
not also need immediate promotion into zsqlite's local cache. A two-hit
admission rule and 24-hour heat horizon remain reasonable. Under pressure, a
newly hot page should replace a colder resident page instead of temporarily
growing the allocation without bound.

Physical allocation matters for small SQLite page sizes. Multiple sub-4-KiB
frames may share one filesystem allocation block, so evicting only one may
release no space. Eviction should select groups that actually make complete
filesystem blocks reclaimable.

Content-addressed remote objects are logically immutable. There are three
possible local eviction granularities:

1. Delete an entire uploaded local segment. This is simplest but uses segment
   granularity.
2. Copy selected hot raw or compressed frames into a cache-only local segment,
   then delete the original local object. This incurs byte copying but no
   decoding or recompression.
3. Maintain a sparse local mirror, punch remote-backed cold frame ranges, and
   fetch holes from remote. This preserves the logical object identity but
   requires explicit knowledge of missing ranges because reading a filesystem
   hole normally returns zeroes.

The first implementation should prefer whole-segment deletion plus small
cache-only hot-frame segments unless measurements justify sparse mirrors.

An explicit shrink command overrides normal heat policy: upload or otherwise
make required data durable, then release every eligible local page-data block.

## Reachability garbage collection

All immutable files form a content-addressed object graph. GC is mark-and-sweep
from retained physical roots.

Root set:

- Current published root.
- Previous rollback root.
- Active reader generation leases.
- Backup and point-in-time retention roots.
- In-progress publication/upload roots.
- Any explicitly pinned administrative snapshot.

Traversed references include selected segments and any separate index nodes.
Embedded dictionaries live and die with their segment and add no graph edge. A
history hash is a commitment, not a physical object reference; carrying an old
history hash into a checkpoint does not retain the historical segment files.

Safe deletion order:

1. Write and sync or upload replacement objects.
2. Atomically publish the replacement root.
3. Mark everything reachable from all retained roots.
4. Wait for reader leases and the configured grace period.
5. Delete unmarked hashes.

Fork objects become ordinary unreachable objects after no retained root points
to their branch. They may be retained temporarily for diagnosis.

Local POSIX unlink semantics protect already-open file descriptors, but a
reader may have loaded a page map without opening every referenced segment.
Reader-generation pinning or a sufficient grace period is therefore still
required. Remote deletion likewise needs root retention and delayed sweep.

## Restore, backup, and startup

A backup consists of the `.zsqlite` control state plus every segment reachable
from the chosen root. Its dictionaries are embedded. A remote backup publishes
a root only after all segment objects are durable.

For full restore:

1. Read the current root and newest segment footer, or fall back to listing
   range-prefixed segment names.
2. Decode the newest available full page map and selected root catalog.
3. Select a suitable checkpoint and gap-free TXID ranges through the desired
   retained segment endpoint.
4. Verify base/ending history continuity and every expected physical digest.
5. Fetch complete segment objects or their embedded dictionary, index, and
   demanded page-frame ranges.
6. Materialize the dense in-memory `page -> last_txid` map.

Because filenames expose ranges and indexes identify pages, the complete
object plan can be known up front without opening and decoding every page.

## Core invariants

1. A visible TXID refers only to a complete commit publication.
2. TXIDs increase monotonically under one fenced writer.
3. Every current page resolves to exactly one frame in the canonical segment
   set or to an explicit TXID-0 sparse zero entry.
4. Canonical live segment ranges are sufficient to locate every current page's
   last TXID unambiguously.
5. A compressed frame is decoded only with the exact embedded dictionary whose
   full digest its table entry records; a raw frame needs no dictionary.
6. A local-only live frame is never removed before another durable
   representation exists.
7. A remote root is never published before all referenced objects exist.
8. Sealed objects are immutable; replacement uses write-publish-delete.
9. History identity is independent of physical compaction identity.
10. GC deletes only objects unreachable from every retained physical root.

## Suggested validation work

- Benchmark end-to-end compressed commits at 1, 10, 100, and 1,000 changed
  pages under `synchronous=OFF`, `NORMAL`, and `FULL`.
- Measure BLAKE3 page hashing, index construction, frame writes, and `fsync`
  separately from Zstandard CPU.
- Measure random point-read latency for one-page dictionary frames against the
  current 64 KiB frames under SQLite cache pressure.
- Compare delta-coded page numbers and frame offsets with the current 60-byte
  fixed segment index entries.
- Exercise crashes at every commit and rotation publication step.
- Exercise concurrent compaction at pinned TXID `N` while writes advance to
  `M`.
- Verify fork detection and conditional-head rejection with two writers
  attempting the same next TXID.
- Verify current/previous roots and reader pins prevent premature GC.
- Measure raw bootstrap duration and dictionary quality at several distinct
  page/sample-byte thresholds.
- Measure compressed full-page-map and dependency-table overhead for frequent
  small segments across every test database; compare periodic full maps plus
  deltas if repetition is material.
- Replay evolving workloads to tune dictionary candidate sampling, held-out
  scoring, promotion threshold, and rate limit.
- Verify ordinary rotation, mixed-generation compaction, and upload copy raw
  Zstandard frame bytes without invoking page decompression.
- Measure whole-segment deletion, hot-frame cache copying, and sparse local
  mirrors before choosing final local eviction granularity.
