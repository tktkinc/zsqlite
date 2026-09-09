# V6 active-segment storage

Status: implemented as the breaking V6 format. V6 has no compatibility path
for earlier zsqlite formats.

## The simplification

The file named `database.zsqlite` is the one mutable active segment. There is
no control-file anchor, alternating root sector, active-file directory, or
root-catalog directory.

```text
database.zsqlite                     mutable active segment
database.zsqlite.d/
  segments/
    <start>-<end>-<history>-<physical>.zseg
  locks/
    publication.lock
    lifecycle.lock
    sqlite.lock
```

The directory belongs to one database lineage. Its contents are nevertheless
not selected by lexical order: the active header contains the physical digest
of its immediate predecessor, and each sealed predecessor contains the digest
of the segment before it. Recovery follows that exact chain and ignores
unreferenced segment files.

This removes the torn-root ambiguity from normal commits. A transaction is
published by appending a commit record to the active file; no previously
committed metadata sector is overwritten.

## Active segment

The active file contains, in order:

1. one checksummed 4 KiB segment header;
2. its dictionary table;
3. a page-to-last-TXID map for the sealed base;
4. alignment padding;
5. append-only page frames and commit records.

The header is immutable for the lifetime of that active inode. It records:

- the database identity;
- page size, generation, and first TXID;
- the base history and logical size;
- the predecessor segment's physical digest, or zero at genesis;
- storage and dictionary policy;
- offsets of the dictionary, base map, and append area.

The empty genesis file can have page size zero. The first SQLite page fixes the
page size by creating and atomically renaming a new active inode before any
transaction is appended.

## Transaction publication

During one Store publication, page frames are appended first. A commit record
then authenticates:

- its TXID and previous commit offset;
- the previous and resulting history hashes;
- page size and resulting logical size;
- the transaction's truncate low-water mark, if any;
- the ordered set of page/frame references and page hashes;
- the commit timestamp.

The commit begins at a 4 KiB boundary and its complete record is padded to a
multiple of 4 KiB. Its header has a record checksum and the referenced entries
have a BLAKE3 digest. Page contents have transaction- and page-specific BLAKE3
hashes. The valid commit record is the transaction's visibility point.

Publication is:

```text
append frames
append zero alignment padding
append commit entries and zero padding behind a zero header area
truncate to the complete record end
write the checksummed commit header last
sync the .zsqlite file when SQLite requested durability
report success
```

No selector or root is switched. A later transaction begins after the prior
complete commit and never overwrites it.

`synchronous=OFF` omits the durability request but preserves the same
structural format. On macOS, a SQLite full-sync request propagates
`F_FULLFSYNC`.

## Recovery

Recovery validates the immutable header and sealed dependency chain, applies
the base page map, and scans the active append area from left to right.

A transaction is applied only after its complete commit record and every
referenced frame validate. An incomplete frame, padding region, commit record,
or referenced payload ends recovery at the preceding valid commit. Bytes after
that point are an uncommitted tail. A writer holding `publication.lock`
truncates the tail before appending new data.

A sealed segment is different: every byte through its index, map, trailer, and
whole-file physical digest must validate. There is no prefix fallback inside a
sealed segment.

As with SQLite's journal and WAL protocols, checksums distinguish a valid
committed record from a torn write with overwhelming probability; POSIX does
not make the 4 KiB write atomic. Also as with SQLite, bytes alone cannot prove
whether later storage corruption damaged a previously acknowledged final
commit. V6 treats an invalid active tail as uncommitted. Hardware and
filesystem durability guarantees still matter after `fsync` returns.

## Rollover

Rollover turns the current active inode into an immutable segment and publishes
a fresh active inode:

```text
1. truncate the active file to its last committed end
2. append the sorted live-page index, full page map, and trailer
3. write the whole-file physical digest and sync the file
4. hardlink that inode into segments/<content-addressed-name>.zseg
5. sync segments/
6. create a sibling temporary active file whose parent is that digest
7. write and sync the complete new active file
8. rename the temporary over database.zsqlite
9. sync the database's parent directory
```

Important crash states are recoverable without a selector:

| Crash point | Path state | Recovery |
|---|---|---|
| before step 3 | old active with absent/torn seal tail | ignore the seal tail and continue from its committed prefix |
| after step 3, before step 4 | sealed old active only at `.zsqlite` | validate it and finish the hardlink/rename rollover |
| after step 4, before step 8 | `.zsqlite` and `segments/` link the sealed inode | finish rollover; a valid sealed alias is permitted |
| after step 8 | new active names the durable predecessor digest | follow the chain normally |

A manually hardlinked mutable active file is rejected. The only permitted
multi-link active pathname is a fully sealed, physically verified inode left
at the legitimate rollover boundary.

Open readers may still hold the old inode after step 8. That inode is complete
and immutable. At SQLite transaction boundaries the Store compares the open
inode with the pathname, validates a same-database replacement, and reloads the
new lineage. SQLite byte locks and WAL shared memory use `locks/sqlite.lock`, a
stable inode unaffected by active-file replacement.

## Sealed segments

A sealed segment extends the active layout with:

1. a compressed sorted live-page index;
2. a compressed full page-to-last-TXID map;
3. a checksummed 4 KiB trailer.

The trailer records the TXID range, base and ending history, logical size,
page size, section offsets, logical content root, and whole-file physical
digest. The content-addressed filename repeats the TXID range, ending history,
and physical digest.

Each segment header's predecessor digest makes the lineage append-only and
authenticated. A compacted snapshot starts at TXID 1 with a zero predecessor;
the fresh active file then points at that snapshot.

## Compaction and garbage collection

Compaction first verifies every source segment's physical digest. It copies
the live frame payloads into one snapshot segment, remapping dictionary
selectors and offsets without recompressing pages. The snapshot retains the
same endpoint TXID, history, logical size, page map, and content root.

After the snapshot is synced and linked into `segments/`, the same active-file
rename protocol publishes a fresh active that points to it. Unreferenced old
segments are removed only while the lifecycle lock proves no reader still has
an older generation pinned.

## Locking and copies

- `publication.lock` serializes append, recovery truncation, rollover, policy
  replacement, and compaction.
- `lifecycle.lock` prevents deletion or garbage collection beneath live
  readers.
- `sqlite.lock` carries the parent VFS's SQLite byte locks and WAL SHM.

The `.zsqlite` file and `.zsqlite.d` directory form one bundle. A raw online
copy must pin a consistent generation; copying the two components at unrelated
times is not a backup protocol. In WAL mode the host `-wal` file may also hold
acknowledged transactions not yet checkpointed into `.zsqlite`.
