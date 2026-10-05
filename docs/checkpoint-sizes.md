# WAL checkpoint sizes across BLOB and ordinary workloads

Measured 2026-09-27 at commit `625869f875f87a74ec9d0e030043d32ec0e35fd4`, using the
real zsqlite VFS and bundled SQLite 3.53.2. All 62 workload/page-size combinations
passed: 458 checkpoint calls, including two empty controls.

Checkpoint boundaries provided an exact count of the bytes and pages copied to
the main database. Large BLOB inserts and replacements produced large batches,
but ordinary inserts and scattered updates produced similarly sized batches.
Deleting a large BLOB produced a small checkpoint and left most payload pages
untouched on the freelist. These results support using checkpoint boundaries to
group writes; reliable BLOB reclamation still requires SQLite page-liveness
information or database truncation.

## Method

The probe ran in an isolated, instrumented source copy with release compilation.
It recorded successful main/WAL xWrite calls, main xTruncate calls,
SQLITE_FCNTL_CKPT_START/DONE, and committed WAL frame counts. Production source
was unchanged.

Both 4 KiB and 16 KiB SQLite page sizes used WAL, synchronous=NORMAL, a 2 MiB
SQLite cache, auto_vacuum=NONE, secure_delete=OFF, and disabled automatic
checkpointing. Setup was checkpointed and excluded. Measured checkpoints used
wal_checkpoint(FULL), followed by an unmeasured TRUNCATE checkpoint to reset the
WAL before the next sample. No concurrent readers or writers held snapshots.

BLOBs used randomblob(). Each insert-size test started with an empty BLOB table.
The replacement/shrink/delete/VACUUM series began with one 100 MiB BLOB and
checkpointed between operations. A separate fixture tested deleting a 100 MiB
BLOB directly.

Each ordinary workload started with 50,000 rows: integer primary key, unique
email, indexed category, counter, title, and a 256-byte text body. Its initial
database was about 17.3 MiB. Contiguous updates selected adjacent row IDs;
scattered updates selected every 50th row for 1,000 updates or every fifth row
for 10,000 updates. Each batch used one transaction unless otherwise stated.

Reported bytes are uncompressed main-database xWrite bytes, before pack
compression. Runs are consecutive logical database page numbers, not pack or
physical disk ranges. The probe measured sizes, not throughput.

## BLOB results

| Operation | 4 KiB pages: checkpoint bytes | Pages / runs | 16 KiB pages: checkpoint bytes |
|---|---:|---:|---:|
| Insert 64 KiB BLOB | 72 KiB | 18 / 1 | 96 KiB |
| Insert 256 KiB BLOB | 264 KiB | 66 / 1 | 288 KiB |
| Insert 1 MiB BLOB | 1.008 MiB | 258 / 1 | 1.031 MiB |
| Insert 4 MiB BLOB | 4.012 MiB | 1,027 / 1 | 4.031 MiB |
| Insert 16 MiB BLOB | 16.023 MiB | 4,102 / 1 | 16.031 MiB |
| Insert 100 MiB BLOB | 100.105 MiB | 25,627 / 1 | 100.047 MiB |
| Insert sixteen 1 MiB BLOBs, one transaction | 16.031 MiB | 4,104 / 1 | 16.062 MiB |
| Insert sixteen 1 MiB BLOBs, sixteen transactions, one checkpoint | 16.031 MiB | 4,104 / 1 | 16.062 MiB |
| Replace 100 MiB BLOB with another 100 MiB BLOB | 100.102 MiB | 25,626 / 1 | 100.031 MiB |
| Shrink 100 MiB BLOB to 1 MiB | 1.109 MiB | 284 / 27 | 1.062 MiB |
| Delete remaining 1 MiB BLOB after shrink | 12 KiB | 3 / 2 | 48 KiB |
| VACUUM after deleting everything | 8 KiB | 2 / 1 | 32 KiB |
| Delete 100 MiB BLOB directly | 112 KiB | 28 / 26 | 64 KiB |

At 4 KiB, directly deleting the 100 MiB BLOB left the database at 25,627 pages
with 25,625 free pages. Only 28 pages were written at checkpoint. The empty
database after the separate shrink/delete/VACUUM series truncated to 2 pages.
At 16 KiB, direct deletion wrote only 4 pages and left 6,401 of 6,403 pages free.

Fresh BLOB inserts formed a single contiguous range that also included B-tree
and database-header pages. The probe did not classify overflow pages. It did
not test BLOB allocation in a fragmented database, so these ranges do not
establish a general BLOB-identification rule.

## Ordinary database activity

| Operation | 4 KiB pages: checkpoint bytes | Pages / runs | 16 KiB pages: checkpoint bytes |
|---|---:|---:|---:|
| No changes | 0 | 0 / 0 | 0 |
| Insert 1 row | 12 KiB | 3 / 3 | 48 KiB |
| Insert 100 rows | 472 KiB | 118 / 109 | 608 KiB |
| Insert 10,000 rows | 4.023 MiB | 1,030 / 133 | 4.078 MiB |
| Update 1 counter | 4 KiB | 1 / 1 | 16 KiB |
| Update 1,000 contiguous counters | 312 KiB | 78 / 11 | 320 KiB |
| Update 1,000 scattered counters | 3.906 MiB | 1,000 / 1,000 | 14.984 MiB |
| Update 10,000 contiguous counters | 3.008 MiB | 770 / 110 | 3.016 MiB |
| Update 10,000 scattered counters | 15.027 MiB | 3,847 / 565 | 14.984 MiB |
| Update indexed category on 1,000 scattered rows | 3.957 MiB | 1,013 / 1,005 | 15.359 MiB |
| Update indexed category on 10,000 scattered rows | 15.344 MiB | 3,928 / 514 | 16.047 MiB |
| Replace text body on 1,000 scattered rows | 3.906 MiB | 1,000 / 1,000 | 14.984 MiB |
| Delete 1,000 scattered rows | 5.719 MiB | 1,464 / 1,224 | 16.812 MiB |

The 10,000-row insert had a longest contiguous run of 892 pages (3.484 MiB)
at 4 KiB. Ordinary growth can therefore create substantial contiguous ranges.
At 16 KiB, selecting every 50th row touched the same 959 table pages as selecting
every fifth row: both batches wrote 14.984 MiB despite changing ten times as
many rows in the latter case.

## Transaction boundaries and checkpoint frequency

| Workload, 4 KiB pages | Commits | Checkpoints | WAL frames | Total main-page writes | Total checkpoint bytes |
|---|---:|---:|---:|---:|---:|
| Update the same row 100 times; checkpoint once | 100 | 1 | 100 | 1 | 4 KiB |
| Update the same row 100 times; checkpoint after every commit | 100 | 100 | 100 | 100 | 400 KiB |
| Update 100 scattered rows in one transaction | 1 | 1 | 100 | 100 | 400 KiB |
| Update those rows in 100 transactions; checkpoint once | 100 | 1 | 100 | 100 | 400 KiB |
| Update those rows in 100 transactions; checkpoint after every commit | 100 | 100 | 100 | 100 | 400 KiB |

At 16 KiB the page counts were identical, so byte totals were four times larger.
For sixteen 1 MiB BLOB inserts, separate commits increased the 4 KiB WAL frame
count from 4,104 to 4,134, but both checkpoints wrote the same 4,104 pages.
The checkpoint coalesced older versions of pages modified by multiple commits.

## Boundary detection and validation

Every measured main-database xWrite was exactly one aligned SQLite page. Within
each nonempty checkpoint, page numbers were strictly increasing, with no
duplicate page writes. All main writes occurred between one CKPT_START and one
CKPT_DONE; neither empty control produced these callbacks. No main writes
occurred during the measured SQL before its checkpoint.

This makes a byte/page counter at the existing checkpoint boundary sufficient
to measure completed batches in these conditions. Contiguous-run counts can
also be computed incrementally. The total is known when copying finishes;
pack-placement decisions needed earlier must buffer or use a running threshold.
CKPT_DONE is not a durability boundary: truncation and sync can follow it.

All checkpoint calls reported no busy condition and fully backfilled the WAL.
Assertions checked commit/frame accounting, alignment, callback boundaries,
page ordering, SQL results, and integrity_check after reopening each fixture.
These measurements cover explicit full checkpoints without reader interference;
partial checkpoints, automatic scheduling, and concurrency were not exercised.

## Local reproduction artifacts

`experiments/checkpoint-sizes/` contains `run.py`, `probe.rs`, `summarize.py`,
`results.csv` (458 individual samples), `summary.csv`, rendered `results.md`,
and `run.log`. This directory is ignored by Git under the repository's existing
policy for local experiment tools and raw results.

```sh
python3 experiments/checkpoint-sizes/run.py
python3 experiments/checkpoint-sizes/summarize.py
```
