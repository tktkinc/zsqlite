-- Run from a completed replay output directory:
-- sqlite3 :memory: < /path/to/zsqlite/benches/replay_summary.sql
.mode csv
.import reads.csv reads
.import connections.csv connections
.import batches.csv batches
.import storage.csv storage
.import frames.csv frames
.import gc-layout.csv gc_layout
.import index-coverage.csv index_coverage
.import mutation-coverage.csv mutation_coverage
.headers on
.mode column

SELECT profile,phase,count(*) AS queries,
       round(sum(CAST(ns AS REAL))/1e9,3) AS query_seconds,
       round(sum(CAST(fetched AS REAL))/max(1,sum(CAST(requested AS REAL))),3) AS fetch_per_request,
       round(sum(CAST(inflated AS REAL))/max(1,sum(CAST(requested AS REAL))),3) AS inflate_per_request,
       sum(CAST(prefetch_used AS INTEGER)) AS prefetch_used,
       sum(CAST(prefetch_unused AS INTEGER)) AS prefetch_evicted_unused
FROM reads GROUP BY profile,phase ORDER BY profile,phase;

WITH ranked AS (
  SELECT profile,phase,workload,CAST(ns AS REAL) ns,
         row_number() OVER(PARTITION BY profile,phase,workload ORDER BY CAST(ns AS REAL)) pos,
         count(*) OVER(PARTITION BY profile,phase,workload) n
  FROM reads
)
SELECT profile,phase,workload,max(n) AS samples,
       round(avg(CASE WHEN pos IN ((n+1)/2,(n+2)/2) THEN ns END)/1e6,3) AS median_ms,
       round(max(CASE WHEN pos=(95*n+99)/100 THEN ns END)/1e6,3) AS p95_ms
FROM ranked GROUP BY profile,phase,workload ORDER BY profile,phase,workload;

SELECT profile,phase,count(*) AS fresh_connections,
       sum(CAST(cases AS INTEGER)) AS cases_per_stream_sum,
       round(sum(CAST(open_ns AS REAL))/1e9,3) AS open_seconds,
       round(avg(CAST(open_ns AS REAL))/1e6,3) AS mean_open_ms
FROM connections GROUP BY profile,phase ORDER BY profile,phase;

SELECT profile,count(*) AS batches,
       round(sum(CAST(open_ns AS REAL))/1e9,3) AS open_seconds,
       round(sum(CAST(insert_ns AS REAL))/1e9,3) AS insert_seconds,
       round(sum(CAST(checkpoint_ns AS REAL))/1e9,3) AS checkpoint_seconds,
       sum(CAST(sqlite_writes AS INTEGER)) AS sqlite_page_writes,
       sum(CAST(sqlite_spills AS INTEGER)) AS sqlite_spills
FROM batches GROUP BY profile;

SELECT profile,phase,
       round(sum(CAST(operation_ns AS REAL))/1e9,3) AS operation_seconds,
       round(sum(CAST(new_object_bytes AS REAL))/1048576,3) AS new_object_mib,
       round(sum(CAST(removed_object_bytes AS REAL))/1048576,3) AS removed_object_mib
FROM storage GROUP BY profile,phase ORDER BY profile,phase;

SELECT profile,phase,seal,
       round(complete_bytes/1048576.0,3) AS complete_mib,
       round(current_bytes/1048576.0,3) AS current_mib,
       round(collectible_bytes/1048576.0,3) AS collectible_mib,
       round(obsolete_bytes/1048576.0,3) AS partially_obsolete_mib,
       objects,dictionary_bytes,
       round(manifest_bytes/1048576.0,3) AS metadata_mib
FROM storage s
WHERE CAST(seal AS INTEGER)=(SELECT max(CAST(seal AS INTEGER)) FROM storage WHERE profile=s.profile)
ORDER BY profile,phase;

SELECT profile,phase,
       CASE WHEN CAST(frame_decoded_bytes AS INTEGER)<=65536 THEN '<=64KiB'
            WHEN CAST(frame_decoded_bytes AS INTEGER)<=262144 THEN '64-256KiB'
            WHEN CAST(frame_decoded_bytes AS INTEGER)<=1048576 THEN '256KiB-1MiB'
            ELSE '>1MiB' END AS frame_size,
       sum(CAST(frames AS INTEGER)) AS frames,
       sum(CAST(dictionary_frames AS INTEGER)) AS dictionary_frames,
       round(sum(CAST(stored_payload_bytes AS REAL))/1048576,3) AS payload_mib
FROM frames f
WHERE CAST(seal AS INTEGER)=(SELECT max(CAST(seal AS INTEGER)) FROM frames WHERE profile=f.profile)
GROUP BY profile,phase,frame_size ORDER BY profile,phase,frame_size;

SELECT profile,phase,
       logical_pages,live_pages,obsolete_page_versions,
       fully_live_frames,partially_obsolete_frames,fully_obsolete_frames,
       round(fully_live_frame_bytes/1048576.0,3) AS live_frame_mib,
       round(partially_obsolete_frame_bytes/1048576.0,3) AS partial_frame_mib,
       round(fully_obsolete_frame_bytes/1048576.0,3) AS dead_frame_mib,
       fully_live_packs,partially_obsolete_packs,fully_obsolete_packs,
       round(collectible_object_bytes/1048576.0,3) AS immediate_gc_mib,
       fully_collectible_blobs,
       round(fully_collectible_blob_bytes/1048576.0,3) AS collectible_blob_mib,
       round(stranded_repack_bytes/1048576.0,3) AS stranded_repack_mib
FROM gc_layout g
WHERE phase IN ('metadata-rollup','metadata-rollup-gc','before-pin-release','pin-released','bounded-gc','full-gc','post-metadata-rollup-gc')
ORDER BY profile,
 CASE phase WHEN 'metadata-rollup' THEN 1 WHEN 'metadata-rollup-gc' THEN 2
            WHEN 'before-pin-release' THEN 3 WHEN 'pin-released' THEN 4
            WHEN 'bounded-gc' THEN 5 WHEN 'full-gc' THEN 6
            ELSE 7 END;

SELECT profile,phase,
       round(current_object_bytes/1048576.0,3) AS current_mib,
       round(retained_object_bytes/1048576.0,3) AS retained_mib,
       round(collectible_object_bytes/1048576.0,3) AS collectible_mib,
       logical_unreachable_packs,round(dead_extent_bytes/1048576.0,3) AS dead_extent_mib,
       objects,deleted_objects,round(deleted_bytes/1048576.0,3) AS deleted_mib
FROM gc_layout
WHERE phase IN ('metadata-rollup','metadata-rollup-gc','before-pin-release','pin-released','bounded-gc','full-gc','post-metadata-rollup-gc')
ORDER BY profile,
 CASE phase WHEN 'metadata-rollup' THEN 1 WHEN 'metadata-rollup-gc' THEN 2
            WHEN 'before-pin-release' THEN 3 WHEN 'pin-released' THEN 4
            WHEN 'bounded-gc' THEN 5 WHEN 'full-gc' THEN 6
            ELSE 7 END;

SELECT "table",kind,access_paths,cases,note
FROM index_coverage ORDER BY "table";

SELECT name,"table","column",index_or_virtual_structure,rows
FROM mutation_coverage ORDER BY name;
