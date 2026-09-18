//! Current full-snapshot and legacy incremental transcript GC replay.
#[cfg(feature = "static")]
#[path = "support/indexed_workload.rs"]
mod indexed_workload;
#[cfg(feature = "static")]
#[path = "support/read_stream.rs"]
mod read_stream;
#[cfg(feature = "static")]
#[allow(dead_code)] // Each workload uses a different subset of the shared adapter.
#[path = "support/replay_sqlite.rs"]
mod sqlite;
#[cfg(feature = "static")]
#[path = "support/transcript_schema.rs"]
mod transcript_schema;
#[cfg(feature = "static")]
#[path = "support/virtual_table.rs"]
mod virtual_table;
#[cfg(feature = "static")]
fn main() -> sqlite::Result<()> {
    replay::run()
}

#[cfg(feature = "static")]
mod replay {
    use super::sqlite::{
        Connection, Fingerprint, Result, Value, bundle_bytes, native_vfs, quote, sidecar, uri,
    };
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};
    use zsqlite::domain::{CacheBytes, DecodedBytes};
    use zsqlite::layout::LayoutPolicy;

    const CORE: &str = "'dim','sess','raw','rec','ft'";
    const FULL: &str = "SELECT r.id,r.uuid,ds.v,dt.v,dr.v,dm.v,r.ts,w.body,r.src,dst.v,r.ord
        FROM rec r JOIN raw w ON w.id=r.raw
        LEFT JOIN dim ds ON ds.id=r.sess LEFT JOIN dim dt ON dt.id=r.type
        LEFT JOIN dim dr ON dr.id=r.role LEFT JOIN dim dm ON dm.id=r.model
        LEFT JOIN dim dst ON dst.id=r.stream";
    const SEARCH: &str = "SELECT r.id,r.uuid,r.ts,r.chat,r.src FROM ft JOIN rec r ON r.id=ft.rowid
        WHERE ft MATCH '\"sqlite\"' ORDER BY rank,r.id LIMIT 50";

    struct Options {
        source: PathBuf,
        output: PathBuf,
        batch: i64,
        seal_batches: usize,
        limit: i64,
        profiles: Vec<String>,
        random_samples: usize,
        churn_epochs: usize,
        mutation_rows: usize,
        preflight: bool,
    }
    impl Options {
        fn parse() -> Result<Self> {
            let mut args = std::env::args().skip(1);
            let mut source = None;
            let mut output = None;
            let mut batch = 512;
            let mut seal_batches = 4;
            let mut limit = i64::MAX;
            let mut profiles = vec!["native".into(), "page".into(), "64k".into(), "1m".into()];
            let mut random_samples = 3_usize;
            let mut churn_epochs = 8_usize;
            let mut mutation_rows = 64_usize;
            let mut preflight = false;
            while let Some(flag) = args.next() {
                if flag == "--bench" {
                    continue;
                }
                if flag == "--preflight" {
                    preflight = true;
                    continue;
                }
                let value = args.next().ok_or("missing option value")?;
                match flag.as_str() {
                    "--source" => source = Some(PathBuf::from(value)),
                    "--output" => output = Some(PathBuf::from(value)),
                    "--batch-rows" => batch = value.parse()?,
                    "--seal-batches" => seal_batches = value.parse()?,
                    "--limit" => limit = value.parse()?,
                    "--profiles" => profiles = value.split(',').map(str::to_owned).collect(),
                    "--random-samples" => random_samples = value.parse()?,
                    "--churn-epochs" => churn_epochs = value.parse()?,
                    "--mutation-rows" => mutation_rows = value.parse()?,
                    _ => return Err(format!("unknown option {flag}").into()),
                }
            }
            if !(1..=16384).contains(&batch)
                || !(1..=128).contains(&seal_batches)
                || !(1..=16).contains(&random_samples)
                || !(4..=32).contains(&churn_epochs)
                || !churn_epochs.is_multiple_of(2)
                || !(1..=256).contains(&mutation_rows)
                || limit < 1
            {
                return Err(
                    "replay bounds: batch 1..16384, seal-batches 1..128, random-samples 1..16, even churn-epochs 4..32, mutation-rows 1..256, positive limit".into(),
                );
            }
            if profiles.first().is_none_or(|name| name != "native")
                || profiles
                    .iter()
                    .any(|name| !["native", "page", "64k", "1m"].contains(&name.as_str()))
                || profiles
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != profiles.len()
            {
                return Err("profiles must start with native, contain no duplicates, and use native,page,64k,1m".into());
            }
            let output = if preflight {
                output.unwrap_or_default()
            } else if let Some(output) = output {
                std::fs::create_dir(&output)?;
                output.canonicalize()?
            } else {
                tempfile::Builder::new()
                    .prefix("zsqlite-replay-")
                    .tempdir()?
                    .keep()
            };
            Ok(Self {
                source: source.ok_or("--source is required")?.canonicalize()?,
                output,
                batch,
                seal_batches,
                limit,
                profiles,
                random_samples,
                churn_epochs,
                mutation_rows,
                preflight,
            })
        }
    }
    fn hash(path: &Path) -> Result<blake3::Hash> {
        let mut file = File::open(path)?;
        let mut buffer = vec![0; 1024 * 1024];
        let mut hash = blake3::Hasher::new();
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                return Ok(hash.finalize());
            }
            hash.update(&buffer[..count]);
        }
    }
    fn reject_pending_source(path: &Path) -> Result<()> {
        for suffix in ["-wal", "-journal"] {
            let candidate = PathBuf::from(format!("{}{suffix}", path.display()));
            match candidate.metadata() {
                Ok(metadata) if metadata.len() != 0 => {
                    return Err(format!(
                        "provide a closed source with no nonempty {suffix} sidecar"
                    )
                    .into());
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
    fn complete_bytes(path: &Path) -> Result<u64> {
        bundle_bytes(path)
    }
    fn make_owner_writable(path: &Path) -> Result<()> {
        let mut permissions = path.metadata()?.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            permissions.set_mode(permissions.mode() | 0o200);
        }
        #[cfg(not(unix))]
        permissions.set_readonly(false);
        std::fs::set_permissions(path, permissions)?;
        Ok(())
    }
    fn objects(path: &Path) -> Result<BTreeMap<PathBuf, u64>> {
        std::fs::read_dir(sidecar(path).join("objects"))?
            .map(|entry| {
                let entry = entry?;
                Ok((entry.path(), entry.metadata()?.len()))
            })
            .collect()
    }
    fn policy(profile: &str) -> Result<zsqlite::StoragePolicy> {
        let layout = LayoutPolicy::default()
            .with_cache(CacheBytes::new(256 * 1024 * 1024))?
            .with_maintenance(DecodedBytes::new(0), 8)?;
        let layout = match profile {
            "64k" => layout.fixed(DecodedBytes::new(65536))?,
            "1m" => layout.fixed(DecodedBytes::new(1024 * 1024))?,
            _ => layout,
        };
        Ok(zsqlite::StoragePolicy::default()
            .with_layout(layout)
            .with_timing(Duration::from_secs(86400), Duration::from_secs(86400))?
            .with_rollover(None)?)
    }
    fn connect(path: &Path, profile: &str, source: &Path, vfs: &str) -> Result<Connection> {
        let db = Connection::open(path, profile != "native", false)?;
        db.execute(&format!(
            "PRAGMA cache_size=-10240; PRAGMA foreign_keys=ON;
            PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0;
            ATTACH DATABASE {} AS source; PRAGMA source.cache_size=-128;",
            quote(&format!("{}&vfs={vfs}", uri(source)))
        ))?;
        Ok(db)
    }
    fn initialize(
        path: &Path,
        profile: &str,
        source: &Path,
        vfs: &str,
        schema: &[String],
    ) -> Result<()> {
        if path.exists() {
            return Err("destination already exists".into());
        }
        let db = connect(path, profile, source, vfs)?;
        db.execute("PRAGMA page_size=4096; PRAGMA journal_mode=WAL; BEGIN;")?;
        for sql in schema {
            db.execute(sql)?;
        }
        // Small lookup/session tables contain final snapshot metadata. Only the
        // rec/raw/FTS ingestion history is simulated, not missing session edits.
        db.execute(
            "INSERT INTO dim SELECT * FROM source.dim ORDER BY id;
            INSERT INTO sess SELECT * FROM source.sess ORDER BY id; COMMIT;",
        )?;
        db.checkpoint()?;
        if profile != "native" {
            zsqlite::configure(path, policy(profile)?)?;
        }
        Ok(())
    }
    struct Case {
        name: String,
        sql: String,
        parameters: Vec<Value>,
        indexed: Option<super::indexed_workload::IndexedCase>,
    }
    fn cases(source: &Connection, end: i64) -> Result<Vec<Case>> {
        let current = source.scalar(&format!(
            "SELECT sess FROM rec WHERE id<={end} AND sess IS NOT NULL ORDER BY id DESC LIMIT 1"
        ))?;
        let older = source.scalar(&format!(
            "SELECT sess FROM rec WHERE id<={} AND sess IS NOT NULL ORDER BY id DESC LIMIT 1",
            (end / 2).max(1)
        ))?;
        Ok(vec![
            Case {
                name: "current-transcript".into(),
                sql: format!("{FULL} WHERE r.sess={current} ORDER BY r.ts,r.id"),
                parameters: Vec::new(),
                indexed: None,
            },
            Case {
                name: "recent-50".into(),
                sql: format!("{FULL} WHERE r.sess={current} ORDER BY r.ts DESC,r.id DESC LIMIT 50"),
                parameters: Vec::new(),
                indexed: None,
            },
            Case {
                name: "older-transcript".into(),
                sql: format!("{FULL} WHERE r.sess={older} ORDER BY r.ts,r.id"),
                parameters: Vec::new(),
                indexed: None,
            },
            Case {
                name: "fts-sqlite-50".into(),
                sql: SEARCH.into(),
                parameters: Vec::new(),
                indexed: None,
            },
        ])
    }
    type Expected = BTreeMap<(String, usize, String), Fingerprint>;
    struct Output {
        reads: File,
        connections: File,
        batches: File,
        storage: File,
        frames: File,
        gc_layout: File,
        checks: File,
        plans: File,
        coverage: File,
        mutations: File,
        expected: Expected,
    }
    impl Output {
        fn new(directory: &Path) -> Result<Self> {
            let mut result = Self {
                reads: File::create_new(directory.join("reads.csv"))?,
                connections: File::create_new(directory.join("connections.csv"))?,
                batches: File::create_new(directory.join("batches.csv"))?,
                storage: File::create_new(directory.join("storage.csv"))?,
                frames: File::create_new(directory.join("frames.csv"))?,
                gc_layout: File::create_new(directory.join("gc-layout.csv"))?,
                checks: File::create_new(directory.join("verification.csv"))?,
                plans: File::create_new(directory.join("query-plans.txt"))?,
                coverage: File::create_new(directory.join("index-coverage.csv"))?,
                mutations: File::create_new(directory.join("mutation-coverage.csv"))?,
                expected: BTreeMap::new(),
            };
            writeln!(
                result.reads,
                "profile,phase,iteration,workload,ns,rows,result_bytes,requested,fetched,inflated,decode_ns,vfs_hits,vfs_misses,sqlite_hits,sqlite_misses,prefetch_used,prefetch_unused,fingerprint"
            )?;
            writeln!(result.connections, "profile,phase,round,open_ns,cases")?;
            writeln!(
                result.batches,
                "profile,batch,end_id,open_ns,insert_ns,checkpoint_ns,sqlite_writes,sqlite_spills"
            )?;
            writeln!(
                result.storage,
                "profile,phase,seal,end_id,operation_ns,complete_bytes,logical_bytes,current_bytes,collectible_bytes,obsolete_bytes,objects,new_object_bytes,removed_object_bytes,dictionary_bytes,dictionary_count,manifest_bytes,manifest_depth"
            )?;
            writeln!(
                result.frames,
                "profile,phase,seal,frame_decoded_bytes,frames,raw_frames,dictionary_frames,stored_payload_bytes"
            )?;
            writeln!(
                result.gc_layout,
                "profile,phase,seal,end_id,logical_pages,total_page_versions,live_pages,obsolete_page_versions,fully_live_frames,partially_obsolete_frames,fully_obsolete_frames,fully_live_frame_bytes,partially_obsolete_frame_bytes,fully_obsolete_frame_bytes,fully_live_packs,partially_obsolete_packs,fully_obsolete_packs,fully_live_pack_bytes,partially_obsolete_pack_bytes,fully_obsolete_pack_bytes,current_object_bytes,retained_object_bytes,collectible_object_bytes,fully_collectible_blobs,fully_collectible_blob_bytes,stranded_repack_bytes,logical_unreachable_packs,dead_extent_bytes,objects,deleted_objects,deleted_bytes"
            )?;
            writeln!(result.checks, "profile,table,rows,bytes,fingerprint")?;
            writeln!(result.coverage, "table,kind,access_paths,cases,note")?;
            writeln!(
                result.mutations,
                "name,table,column,index_or_virtual_structure,rows"
            )?;
            Ok(result)
        }
        fn measure(
            &mut self,
            db: &Connection,
            profile: &str,
            phase: &str,
            iteration: usize,
            case: &Case,
        ) -> Result<()> {
            let before = db.stats(profile != "native")?;
            db.execute("BEGIN")?;
            let start = Instant::now();
            let result = db.fingerprint_params(&case.sql, &case.parameters)?;
            let elapsed = start.elapsed().as_nanos();
            db.execute("COMMIT")?;
            let after = db.stats(profile != "native")?;
            let key = (phase.to_owned(), iteration, case.name.clone());
            if profile == "native" {
                self.expected.insert(key, result);
            } else if self.expected.get(&key) != Some(&result) {
                return Err(format!(
                    "query mismatch: {profile}/{phase}/{iteration}/{}",
                    case.name
                )
                .into());
            }
            writeln!(
                self.reads,
                "{profile},{phase},{iteration},{},{elapsed},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                case.name,
                result.rows,
                result.bytes,
                after.io.requested_bytes - before.io.requested_bytes,
                after.io.fetched_bytes - before.io.fetched_bytes,
                after.io.inflated_bytes - before.io.inflated_bytes,
                after.io.decode_nanoseconds - before.io.decode_nanoseconds,
                after.io.cache_hits - before.io.cache_hits,
                after.io.cache_misses - before.io.cache_misses,
                after.sqlite.hits - before.sqlite.hits,
                after.sqlite.misses - before.sqlite.misses,
                after.cache.extra_pages_requested - before.cache.extra_pages_requested,
                after.cache.extra_pages_evicted_unused - before.cache.extra_pages_evicted_unused,
                result.hash
            )?;
            Ok(())
        }
        #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One coherent CSV snapshot.
        fn snapshot(
            &mut self,
            path: &Path,
            profile: &str,
            phase: &str,
            seal: usize,
            end: i64,
            elapsed: u128,
            before: &BTreeMap<PathBuf, u64>,
            operation_gc: Option<&zsqlite::GcReport>,
        ) -> Result<()> {
            let info = zsqlite::inspect(path)?;
            let after = objects(path)?;
            let written: u64 = after
                .iter()
                .filter(|(path, _)| !before.contains_key(*path))
                .map(|(_, size)| *size)
                .sum();
            let removed: u64 = before
                .iter()
                .filter(|(path, _)| !after.contains_key(*path))
                .map(|(_, size)| *size)
                .sum();
            let manifest = info.manifest.ok_or("missing sealed view")?;
            writeln!(
                self.storage,
                "{profile},{phase},{seal},{end},{elapsed},{},{},{},{},{},{},{written},{removed},{},{},{},{}",
                complete_bytes(path)?,
                info.logical_size,
                info.retention.current_view_bytes,
                info.retention.collectible_bytes,
                info.retention.partially_obsolete_bytes,
                info.retention.objects,
                info.dictionary_bytes,
                info.preferred_dictionaries,
                manifest.head_bytes().get() + manifest.ancestor_bytes().get(),
                manifest.run_depth()
            )?;
            for bin in info.frame_distribution {
                writeln!(
                    self.frames,
                    "{profile},{phase},{seal},{},{},{},{},{}",
                    bin.decoded_bytes.get(),
                    bin.frames,
                    bin.raw_frames,
                    bin.dictionary_frames,
                    bin.stored_payload_bytes.get()
                )?;
            }
            let mut total_page_versions = 0_u64;
            let mut live_pages = 0_u64;
            let mut fully_live_frames = 0_usize;
            let mut partially_obsolete_frames = 0_usize;
            let mut fully_obsolete_frames = 0_usize;
            let mut fully_live_frame_bytes = 0_u64;
            let mut partially_obsolete_frame_bytes = 0_u64;
            let mut fully_obsolete_frame_bytes = 0_u64;
            let mut fully_live_packs = 0_usize;
            let mut partially_obsolete_packs = 0_usize;
            let mut fully_obsolete_packs = 0_usize;
            let mut fully_live_pack_bytes = 0_u64;
            let mut partially_obsolete_pack_bytes = 0_u64;
            let mut fully_obsolete_pack_bytes = 0_u64;
            for pack in &info.pack_occupancy {
                total_page_versions = total_page_versions.saturating_add(pack.total_pages);
                live_pages = live_pages.saturating_add(pack.live_pages);
                fully_live_frames += pack.fully_live_frames;
                partially_obsolete_frames += pack.partially_obsolete_frames;
                fully_obsolete_frames += pack.fully_obsolete_frames;
                fully_live_frame_bytes =
                    fully_live_frame_bytes.saturating_add(pack.fully_live_frame_bytes.get());
                partially_obsolete_frame_bytes = partially_obsolete_frame_bytes
                    .saturating_add(pack.partially_obsolete_frame_bytes.get());
                fully_obsolete_frame_bytes = fully_obsolete_frame_bytes
                    .saturating_add(pack.fully_obsolete_frame_bytes.get());
                if pack.live_pages == 0 {
                    fully_obsolete_packs += 1;
                    fully_obsolete_pack_bytes =
                        fully_obsolete_pack_bytes.saturating_add(pack.stored_bytes.get());
                } else if pack.live_pages == pack.total_pages {
                    fully_live_packs += 1;
                    fully_live_pack_bytes =
                        fully_live_pack_bytes.saturating_add(pack.stored_bytes.get());
                } else {
                    partially_obsolete_packs += 1;
                    partially_obsolete_pack_bytes =
                        partially_obsolete_pack_bytes.saturating_add(pack.stored_bytes.get());
                }
            }
            let stranded_repack_bytes = info
                .pack_occupancy
                .iter()
                .filter(|pack| pack.live_pages > 0)
                .map(zsqlite::storage::PackOccupancy::reclaimable_bytes)
                .sum::<u64>();
            let retention = &info.retention;
            let deleted_objects = operation_gc.map_or(0, |report| report.deleted_objects);
            let deleted_bytes = operation_gc.map_or(0, |report| report.deleted_bytes);
            writeln!(
                self.gc_layout,
                "{profile},{phase},{seal},{end},{},{total_page_versions},{live_pages},{},{fully_live_frames},{partially_obsolete_frames},{fully_obsolete_frames},{fully_live_frame_bytes},{partially_obsolete_frame_bytes},{fully_obsolete_frame_bytes},{fully_live_packs},{partially_obsolete_packs},{fully_obsolete_packs},{fully_live_pack_bytes},{partially_obsolete_pack_bytes},{fully_obsolete_pack_bytes},{},{},{},{},{},{stranded_repack_bytes},{},{},{},{},{}",
                info.page_count,
                total_page_versions.saturating_sub(live_pages),
                retention.current_view_bytes,
                retention.retained_bytes,
                retention.collectible_bytes,
                retention.fully_unreachable_blobs,
                retention.fully_unreachable_blob_bytes,
                retention.logically_unreachable_packs,
                retention.dead_extent_bytes,
                retention.objects,
                deleted_objects,
                deleted_bytes,
            )?;
            self.storage.flush()?;
            self.reads.flush()?;
            self.gc_layout.flush()?;
            Ok(())
        }
    }
    fn verify(
        db: &Connection,
        source: &Connection,
        profile: &str,
        end: i64,
        output: &mut Output,
    ) -> Result<()> {
        for (table, expected_sql, actual_sql) in [
            (
                "dim",
                "SELECT * FROM dim ORDER BY id".to_owned(),
                "SELECT * FROM dim ORDER BY id".to_owned(),
            ),
            (
                "sess",
                "SELECT * FROM sess ORDER BY id".to_owned(),
                "SELECT * FROM sess ORDER BY id".to_owned(),
            ),
            (
                "rec",
                format!("SELECT * FROM rec WHERE id<={end} ORDER BY id"),
                "SELECT * FROM rec ORDER BY id".to_owned(),
            ),
            (
                "raw",
                format!("SELECT * FROM raw WHERE id<={end} ORDER BY id"),
                "SELECT * FROM raw ORDER BY id".to_owned(),
            ),
            (
                "ft-rowids",
                format!("SELECT rowid FROM ft WHERE rowid<={end} ORDER BY rowid"),
                "SELECT rowid FROM ft ORDER BY rowid".to_owned(),
            ),
        ] {
            let expected = source.fingerprint(&expected_sql)?;
            let actual = db.fingerprint(&actual_sql)?;
            if expected != actual {
                return Err(format!("source mismatch: {profile}/{table}").into());
            }
            writeln!(
                output.checks,
                "{profile},{table},{},{},{}",
                actual.rows, actual.bytes, actual.hash
            )?;
        }
        db.execute("CREATE VIRTUAL TABLE temp.replay_vocab USING fts5vocab(main,ft,instance)")?;
        let expected = source.fingerprint(&format!("SELECT term,doc,col,offset FROM replay_vocab WHERE doc<={end} ORDER BY term,doc,col,offset"))?;
        let actual = db.fingerprint(
            "SELECT term,doc,col,offset FROM replay_vocab ORDER BY term,doc,col,offset",
        )?;
        if expected != actual {
            return Err(format!("FTS token/position mismatch: {profile}").into());
        }
        writeln!(
            output.checks,
            "{profile},ft-vocabulary,{},{},{}",
            actual.rows, actual.bytes, actual.hash
        )?;
        if db.rows("PRAGMA integrity_check")? != vec![vec!["ok".to_owned()]]
            || !db.rows("PRAGMA foreign_key_check")?.is_empty()
        {
            return Err(format!("SQLite integrity failure: {profile}").into());
        }
        Ok(())
    }
    fn final_cases(source: &Connection, end: i64) -> Result<Vec<Case>> {
        let sizes = source.rows(&format!("WITH sizes AS (SELECT sess,count(*) n FROM rec WHERE sess IS NOT NULL AND id<={end} GROUP BY sess), ranked AS (SELECT *,row_number() OVER(ORDER BY n,sess) pos,count(*) OVER() total FROM sizes) SELECT sess,n FROM ranked WHERE pos IN (max(1,total/2),max(1,total*9/10),max(1,total*99/100),total) ORDER BY pos"))?;
        let mut cases = Vec::new();
        for (index, row) in sizes.iter().enumerate() {
            let sess: i64 = row[0].parse()?;
            cases.push(Case {
                name: format!("transcript-q{index}-{}", row[1]),
                sql: format!("{FULL} WHERE r.sess={sess} ORDER BY r.ts,r.id"),
                parameters: Vec::new(),
                indexed: None,
            });
        }
        cases.push(Case {
            name: "fts-sqlite-50".into(),
            sql: SEARCH.into(),
            parameters: Vec::new(),
            indexed: None,
        });
        Ok(cases)
    }
    fn indexed_cases(workload: &super::indexed_workload::Workload) -> Vec<Case> {
        workload
            .cases
            .iter()
            .map(|case| Case {
                name: case.name.clone(),
                sql: case.sql.clone(),
                parameters: case.parameters.clone(),
                indexed: Some(case.clone()),
            })
            .collect()
    }
    fn final_reads(
        path: &Path,
        profile: &str,
        phase: &str,
        source: &Path,
        vfs: &str,
        cases: &[Case],
        output: &mut Output,
    ) -> Result<()> {
        let names = cases
            .iter()
            .map(|case| case.name.as_str())
            .collect::<Vec<_>>();
        for round in 0..super::read_stream::DEFAULT_ROUNDS {
            let opening = Instant::now();
            let db = connect(path, profile, source, vfs)?;
            let open_ns = opening.elapsed().as_nanos();
            writeln!(
                output.connections,
                "{profile},{phase},{round},{open_ns},{}",
                cases.len()
            )?;
            for index in super::read_stream::order(&names, phase, round) {
                let case = &cases[index];
                output.measure(&db, profile, &format!("{phase}-stream-first"), round, case)?;
                output.measure(
                    &db,
                    profile,
                    &format!("{phase}-immediate-repeat"),
                    round,
                    case,
                )?;
            }
        }
        Ok(())
    }
    fn churn(
        path: &Path,
        profile: &str,
        source: &Path,
        vfs: &str,
        end: i64,
        epoch: usize,
    ) -> Result<()> {
        let width = (end / 256).clamp(1, 256);
        let predicate = [1_i64, 3, 5, 7]
            .into_iter()
            .map(|part| {
                let center = (end.saturating_mul(part) / 8).clamp(1, end);
                let first = (center - width / 2).max(1);
                let last = (first + width - 1).min(end);
                format!("id BETWEEN {first} AND {last}")
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        let db = connect(path, profile, source, vfs)?;
        let suffix = ":zsqlite-layout-churn:5a5117e5";
        let assignment = if epoch.is_multiple_of(2) {
            format!("uuid=uuid||'{suffix}'")
        } else {
            format!("uuid=substr(uuid,1,length(uuid)-{})", suffix.len())
        };
        // Alternate a deterministic UUID edit and exact restoration. The
        // unique UUID index and table pages genuinely churn, while every even
        // epoch converges to the retained source snapshot.
        db.execute(&format!(
            "BEGIN IMMEDIATE;
             UPDATE rec SET {assignment} WHERE {predicate};
             COMMIT;"
        ))?;
        db.checkpoint()?;
        Ok(())
    }

    fn current_churn(
        path: &Path,
        profile: &str,
        source: &Path,
        vfs: &str,
        plan: &super::transcript_schema::MutationPlan,
        epoch: usize,
    ) -> Result<()> {
        let db = connect(path, profile, source, vfs)?;
        plan.apply(&db, epoch)?;
        db.checkpoint()?;
        Ok(())
    }

    fn verify_current(
        db: &Connection,
        source: &Connection,
        profile: &str,
        queries: &[super::transcript_schema::Verification],
        expected: &mut BTreeMap<String, Fingerprint>,
        output: &mut Output,
    ) -> Result<()> {
        let local_queries = super::transcript_schema::verification_queries(db)?;
        if local_queries.len() != queries.len()
            || local_queries
                .iter()
                .zip(queries)
                .any(|(local, source)| local.table != source.table || local.sql != source.sql)
        {
            return Err(format!("schema verification set differs: {profile}").into());
        }
        for query in &local_queries {
            let actual = db.fingerprint(&query.sql)?;
            let reference = if let Some(reference) = expected.get(&query.table) {
                *reference
            } else {
                let reference = source.fingerprint(&query.sql)?;
                expected.insert(query.table.clone(), reference);
                reference
            };
            if actual != reference {
                return Err(format!(
                    "source mismatch: {profile}/{} after mutation restoration",
                    query.table
                )
                .into());
            }
            writeln!(
                output.checks,
                "{profile},{},{},{},{}",
                query.table, actual.rows, actual.bytes, actual.hash
            )?;
        }
        if db.rows("PRAGMA integrity_check")? != vec![vec!["ok".to_owned()]]
            || !db.rows("PRAGMA foreign_key_check")?.is_empty()
        {
            return Err(format!("SQLite integrity failure: {profile}").into());
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)] // Preserve the physical state sequence in execution order.
    fn run_current(
        options: &Options,
        vfs: &str,
        digest: blake3::Hash,
        source_path: &Path,
        source: &Connection,
    ) -> Result<()> {
        if options.limit != i64::MAX || options.batch != 512 || options.seal_batches != 4 {
            return Err(
                "--limit, --batch-rows and --seal-batches apply only to the legacy transcript schema"
                    .into(),
            );
        }
        let indexed = super::indexed_workload::generate(
            source,
            0x5a51_17e5_d15c_a11e,
            options.random_samples,
            None,
        )?;
        if indexed.cases.is_empty() {
            return Err("current transcript has no indexed random-read cases".into());
        }
        let cases = indexed_cases(&indexed);
        let mutation =
            super::transcript_schema::MutationPlan::build(source, options.mutation_rows)?;
        let verification = super::transcript_schema::verification_queries(source)?;
        let end = i64::try_from(mutation.rows())?;
        let mut output = Output::new(&options.output)?;
        for coverage in &indexed.coverage {
            writeln!(
                output.coverage,
                "{},{},{},{},{}",
                coverage.table, coverage.kind, coverage.access_paths, coverage.cases, coverage.note
            )?;
        }
        for coverage in &mutation.coverage {
            writeln!(
                output.mutations,
                "{},{},{},{},{}",
                coverage.name, coverage.table, coverage.column, coverage.structure, coverage.rows
            )?;
        }
        let mut meta = File::create_new(options.output.join("run.txt"))?;
        writeln!(
            meta,
            "schema=current-transcript-v1\nsource={}\nsource_bytes={}\nsource_blake3={digest}\nprofiles={:?}\nzstd_level=3\nsample_cap_bytes=100663296\ndictionary_cap_bytes=786432\nrandom_samples_per_index={}\nmutation_rows_per_target={}\nmutation_rows_total={}\nchurn_epochs={}\nfinal_read_rounds={}\nfinal_read_execution=stream-first,immediate-repeat\nmanaged_pager_kib=10240\nmanaged_plaintext_cache_kib=262144\nnative_pager_kib=10240\nfull_snapshot_conversion=true\n",
            options.source.display(),
            source_path.metadata()?.len(),
            options.profiles,
            options.random_samples,
            options.mutation_rows,
            mutation.rows(),
            options.churn_epochs,
            super::read_stream::DEFAULT_ROUNDS,
        )?;
        let mut verification_expected = BTreeMap::new();
        for profile in &options.profiles {
            let started = Instant::now();
            let path = options.output.join(format!(
                "{profile}.{}",
                if profile == "native" {
                    "sqlite"
                } else {
                    "zsqlite"
                }
            ));
            let build = Instant::now();
            if profile == "native" {
                std::fs::copy(source_path, &path)?;
                make_owner_writable(&path)?;
            } else {
                zsqlite::convert_to_zsqlite_with_policy(source_path, &path, policy(profile)?)?;
            }
            let journal = connect(&path, profile, source_path, vfs)?;
            if journal
                .rows("PRAGMA journal_mode=WAL")
                .map_err(|error| format!("{profile} enabling WAL: {error}"))?
                != vec![vec!["wal".to_owned()]]
            {
                return Err(format!("could not enable WAL for {profile}").into());
            }
            drop(journal);
            let checkpoint = connect(&path, profile, source_path, vfs)?;
            checkpoint
                .checkpoint()
                .map_err(|error| format!("{profile} initial checkpoint: {error}"))?;
            drop(checkpoint);
            if profile != "native" {
                zsqlite::flush(&path)?;
            }
            writeln!(meta, "{profile}_build_ns={}", build.elapsed().as_nanos())?;
            let mut seal = 0;
            if profile != "native" {
                let before = objects(&path)?;
                output.snapshot(&path, profile, "initial", seal, end, 0, &before, None)?;
            }
            let db = connect(&path, profile, source_path, vfs)?;
            for case in &cases {
                let indexed = case.indexed.as_ref().ok_or("missing indexed case")?;
                let details = super::indexed_workload::validate_plan(&db, indexed)
                    .map_err(|error| format!("{profile}/{} plan: {error}", case.name))?;
                writeln!(
                    output.plans,
                    "{profile}/{}\n{}\n{details:?}",
                    case.name, case.sql,
                )?;
            }
            drop(db);
            final_reads(
                &path,
                profile,
                "history",
                source_path,
                vfs,
                &cases,
                &mut output,
            )
            .map_err(|error| format!("{profile} history reads: {error}"))?;
            for epoch in 0..options.churn_epochs {
                current_churn(&path, profile, source_path, vfs, &mutation, epoch)?;
                let reader = connect(&path, profile, source_path, vfs)?;
                for case in &cases {
                    output.measure(&reader, profile, "churn-read", epoch, case)?;
                }
                drop(reader);
                if profile != "native" {
                    seal += 1;
                    let before = objects(&path)?;
                    let start = Instant::now();
                    zsqlite::flush(&path)?;
                    output.snapshot(
                        &path,
                        profile,
                        "churn-seal",
                        seal,
                        end,
                        start.elapsed().as_nanos(),
                        &before,
                        None,
                    )?;
                }
            }
            let restored = connect(&path, profile, source_path, vfs)?;
            mutation.verify_restored(&restored)?;
            drop(restored);
            final_reads(
                &path,
                profile,
                "before-metadata-rollup",
                source_path,
                vfs,
                &cases,
                &mut output,
            )?;
            if profile != "native" {
                let before = objects(&path)?;
                let start = Instant::now();
                zsqlite::compact(&path)?;
                output.snapshot(
                    &path,
                    profile,
                    "metadata-rollup",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    None,
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                let collected = zsqlite::collect(&path, usize::MAX)?;
                output.snapshot(
                    &path,
                    profile,
                    "metadata-rollup-gc",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    Some(&collected),
                )?;
            }
            final_reads(
                &path,
                profile,
                "after-metadata-rollup",
                source_path,
                vfs,
                &cases,
                &mut output,
            )?;
            let retained = if profile == "native" {
                None
            } else {
                Some(zsqlite::retain(
                    &path,
                    zsqlite::RetentionName::new("before-gc-churn")?,
                )?)
            };
            for epoch in 0..4 {
                current_churn(&path, profile, source_path, vfs, &mutation, epoch)?;
                let reader = connect(&path, profile, source_path, vfs)?;
                for case in &cases {
                    output.measure(&reader, profile, "gc-churn-read", epoch, case)?;
                }
                drop(reader);
                if profile != "native" {
                    seal += 1;
                    let before = objects(&path)?;
                    let start = Instant::now();
                    zsqlite::flush(&path)?;
                    output.snapshot(
                        &path,
                        profile,
                        "gc-churn-seal",
                        seal,
                        end,
                        start.elapsed().as_nanos(),
                        &before,
                        None,
                    )?;
                }
            }
            let restored = connect(&path, profile, source_path, vfs)?;
            mutation.verify_restored(&restored)?;
            drop(restored);
            if profile != "native" {
                let before = objects(&path)?;
                output.snapshot(
                    &path,
                    profile,
                    "before-pin-release",
                    seal,
                    end,
                    0,
                    &before,
                    None,
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                zsqlite::release_retention(
                    &path,
                    retained.ok_or("managed profile lost retention pin")?,
                )?;
                output.snapshot(
                    &path,
                    profile,
                    "pin-released",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    None,
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                let bounded = zsqlite::collect(&path, 8)?;
                output.snapshot(
                    &path,
                    profile,
                    "bounded-gc",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    Some(&bounded),
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                let full = zsqlite::collect(&path, usize::MAX)?;
                output.snapshot(
                    &path,
                    profile,
                    "full-gc",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    Some(&full),
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                zsqlite::compact(&path)?;
                output.snapshot(
                    &path,
                    profile,
                    "metadata-rollup",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    None,
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                let full = zsqlite::collect(&path, usize::MAX)?;
                output.snapshot(
                    &path,
                    profile,
                    "post-metadata-rollup-gc",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    Some(&full),
                )?;
            }
            final_reads(
                &path,
                profile,
                "after-rollup",
                source_path,
                vfs,
                &cases,
                &mut output,
            )?;
            let exported = options.output.join(format!("{profile}-export.sqlite"));
            let verify_path = if profile == "native" {
                &path
            } else {
                zsqlite::verify(&path)?;
                zsqlite::export_to_sqlite(&path, &exported)?;
                &exported
            };
            let verify_db = Connection::open(verify_path, false, true)?;
            verify_current(
                &verify_db,
                source,
                profile,
                &verification,
                &mut verification_expected,
                &mut output,
            )?;
            drop(verify_db);
            if profile != "native" {
                std::fs::remove_file(exported)?;
            }
            writeln!(
                meta,
                "{profile}_final_complete_bytes={}\n{profile}_total_wall_ns={}",
                complete_bytes(&path)?,
                started.elapsed().as_nanos()
            )?;
            writeln!(
                meta,
                "{profile}_process_lifetime_peak_rss_bytes={}",
                super::sqlite::peak_rss_bytes()
                    .map_or_else(|| "unavailable".to_owned(), |bytes| bytes.to_string())
            )?;
            meta.flush()?;
            output.checks.flush()?;
            println!("{profile} verified");
        }
        if hash(&options.source)? != digest {
            return Err("original source changed during benchmark".into());
        }
        reject_pending_source(&options.source)?;
        println!(
            "verified current transcript profiles; results={}",
            options.output.display()
        );
        Ok(())
    }

    #[allow(clippy::too_many_lines)] // Keep the measurement/validation phases in execution order.
    pub fn run() -> Result<()> {
        let options = Options::parse()?;
        reject_pending_source(&options.source)?;
        if options.preflight {
            let source = Connection::open(&options.source, false, true)?;
            let coverage = super::transcript_schema::preflight(&source)?;
            let indexed =
                super::indexed_workload::generate(&source, 0x5a51_17e5_d15c_a11e, 1, None)?;
            if indexed.cases.is_empty() {
                return Err("current transcript has no indexed random-read cases".into());
            }
            for case in &indexed.cases {
                super::indexed_workload::validate_plan(&source, case)?;
                let _fingerprint = source.fingerprint_params(&case.sql, &case.parameters)?;
            }
            println!(
                "current transcript preflight ok: source={} bytes={} indexed_cases={} tables={}",
                options.source.display(),
                options.source.metadata()?.len(),
                indexed.cases.len(),
                indexed.coverage.len(),
            );
            for target in coverage {
                println!(
                    "mutation={} table={} column={} structure={}",
                    target.name, target.table, target.column, target.structure
                );
            }
            return Ok(());
        }
        let vfs = native_vfs()?;
        zsqlite::register_static_vfs().map_err(|code| format!("VFS registration: {code}"))?;
        let digest = hash(&options.source)?;
        let source_path = options.output.join("source.sqlite");
        std::fs::copy(&options.source, &source_path)?;
        if hash(&source_path)? != digest || hash(&options.source)? != digest {
            return Err("source changed while copying".into());
        }
        reject_pending_source(&options.source)?;
        println!(
            "output={} source_bytes={} blake3={digest}",
            options.output.display(),
            source_path.metadata()?.len()
        );
        let source = Connection::open(&source_path, false, true)?;
        if super::transcript_schema::is_current(&source)? {
            return run_current(&options, &vfs, digest, &source_path, &source);
        }
        if super::transcript_schema::looks_current(&source)? {
            return Err(
                "current-like transcript schema is not supported; run --preflight for required V1 objects"
                    .into(),
            );
        }
        source.execute("PRAGMA cache_size=-2048; PRAGMA temp_store=FILE; CREATE VIRTUAL TABLE temp.replay_vocab USING fts5vocab(main,ft,instance)")?;
        let rows = source.scalar("SELECT count(*) FROM rec")?;
        let max_id = source.scalar("SELECT max(id) FROM rec")?;
        if source.scalar("SELECT min(id) FROM rec")? != 1 || rows != max_id {
            return Err("this replay requires contiguous positive rec IDs".into());
        }
        if source.scalar("SELECT count(*) FROM rec WHERE id<>raw")? != 0 {
            return Err("this snapshot requires a different raw-row replay mapping".into());
        }
        let end = max_id.min(options.limit);
        let schema: Vec<String> = source.rows(&format!("SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL AND ((type='table' AND name IN ({CORE})) OR (type='index' AND tbl_name IN ({CORE}))) ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END,name"))?.into_iter().map(|mut row| row.remove(0)).collect();
        let mut output = Output::new(&options.output)?;
        let mut meta = File::create_new(options.output.join("run.txt"))?;
        writeln!(
            meta,
            "source={}\nsource_bytes={}\nsource_blake3={digest}\nend_id={end}\nbatch_rows={}\nseal_batches={}\nprofiles={:?}\nzstd_level=3\nsample_cap_bytes=100663296\ndictionary_cap_bytes=786432\nrandom_samples_per_index={}\nchurn_epochs={}\nfinal_read_rounds={}\nfinal_read_execution=stream-first,immediate-repeat\nmanaged_pager_kib=10240\nmanaged_plaintext_cache_kib=262144\nnative_pager_kib=10240\nauxiliary_schema_not_replayed=true\n",
            options.source.display(),
            source_path.metadata()?.len(),
            options.batch,
            options.seal_batches,
            options.profiles,
            options.random_samples,
            options.churn_epochs,
            super::read_stream::DEFAULT_ROUNDS,
        )?;
        let transcript_cases = final_cases(&source, end)?;
        let replayed_tables = ["dim", "sess", "raw", "rec", "ft"]
            .into_iter()
            .map(str::to_owned)
            .collect::<std::collections::BTreeSet<_>>();
        let mut final_workload: Option<Vec<Case>> = None;
        let batch_ends: Vec<i64> = (0..end)
            .step_by(usize::try_from(options.batch)?)
            .map(|start| (start + options.batch).min(end))
            .collect();
        let replay_cases: Vec<Vec<Case>> = batch_ends
            .iter()
            .map(|end| cases(&source, *end))
            .collect::<Result<_>>()?;
        for profile in &options.profiles {
            let started = Instant::now();
            let path = options.output.join(format!(
                "{profile}.{}",
                if profile == "native" {
                    "sqlite"
                } else {
                    "zsqlite"
                }
            ));
            initialize(&path, profile, &source_path, &vfs, &schema)?;
            let mut connection = None;
            let mut seal = 0;
            for (batch, (&end_id, cases)) in batch_ends.iter().zip(&replay_cases).enumerate() {
                let opening = Instant::now();
                if connection.is_none() {
                    connection = Some(connect(&path, profile, &source_path, &vfs)?);
                }
                let open_ns = opening.elapsed().as_nanos();
                let db = connection.as_ref().unwrap();
                let before = db.stats(profile != "native")?;
                let start_id = i64::try_from(batch)? * options.batch + 1;
                let start = Instant::now();
                db.execute(&format!("BEGIN IMMEDIATE;
                    INSERT INTO raw SELECT * FROM source.raw WHERE id BETWEEN {start_id} AND {end_id} ORDER BY id;
                    INSERT INTO rec SELECT * FROM source.rec WHERE id BETWEEN {start_id} AND {end_id} ORDER BY id;
                    INSERT INTO ft(rowid,t) SELECT id,chat FROM source.rec WHERE id BETWEEN {start_id} AND {end_id} AND chat IS NOT NULL AND chat<>'' ORDER BY id;
                    COMMIT;"))?;
                let insert_ns = start.elapsed().as_nanos();
                let checkpoint = Instant::now();
                db.checkpoint()?;
                let checkpoint_ns = checkpoint.elapsed().as_nanos();
                let after = db.stats(profile != "native")?;
                writeln!(
                    output.batches,
                    "{profile},{batch},{end_id},{open_ns},{insert_ns},{checkpoint_ns},{},{}",
                    after.sqlite.writes - before.sqlite.writes,
                    after.sqlite.spills - before.sqlite.spills
                )?;
                for case in cases {
                    output.measure(db, profile, "replay", batch, case)?;
                }
                if (batch + 1) % options.seal_batches == 0 || end_id == end {
                    drop(connection.take());
                    seal += 1;
                    if profile != "native" {
                        let before = objects(&path)?;
                        let start = Instant::now();
                        zsqlite::flush(&path)?;
                        let elapsed = start.elapsed().as_nanos();
                        output.snapshot(
                            &path, profile, "seal", seal, end_id, elapsed, &before, None,
                        )?;
                    }
                    println!(
                        "{profile} seal={seal} records={end_id}/{end} elapsed_s={:.2} complete_bytes={}",
                        started.elapsed().as_secs_f64(),
                        complete_bytes(&path)?
                    );
                }
            }
            writeln!(
                meta,
                "{profile}_replay_wall_ns={}",
                started.elapsed().as_nanos()
            )?;
            let db = connect(&path, profile, &source_path, &vfs)?;
            if profile == "native" {
                let indexed = super::indexed_workload::generate(
                    &db,
                    0x5a51_17e5_d15c_a11e,
                    options.random_samples,
                    Some(&replayed_tables),
                )?;
                for coverage in &indexed.coverage {
                    writeln!(
                        output.coverage,
                        "{},{},{},{},{}",
                        coverage.table,
                        coverage.kind,
                        coverage.access_paths,
                        coverage.cases,
                        coverage.note
                    )?;
                }
                let mut cases = transcript_cases
                    .iter()
                    .map(|case| Case {
                        name: case.name.clone(),
                        sql: case.sql.clone(),
                        parameters: case.parameters.clone(),
                        indexed: None,
                    })
                    .collect::<Vec<_>>();
                cases.extend(indexed_cases(&indexed));
                final_workload = Some(cases);
            }
            let final_cases = final_workload
                .as_ref()
                .ok_or("native profile did not initialize the indexed workload")?;
            // Plans are validated separately from measured queries. Forced
            // ordinary indexes must still report SEARCH, while FTS5 reports a
            // virtual-table index scan.
            for case in final_cases {
                let details = if let Some(indexed) = &case.indexed {
                    super::indexed_workload::validate_plan(&db, indexed)?
                } else {
                    db.query_params(
                        &format!("EXPLAIN QUERY PLAN {}", case.sql),
                        &case.parameters,
                    )?
                    .into_iter()
                    .filter_map(|row| {
                        row.last()
                            .map(|(_, bytes)| String::from_utf8_lossy(bytes).into_owned())
                    })
                    .collect()
                };
                writeln!(
                    output.plans,
                    "{profile}/{}\n{}\n{details:?}",
                    case.name, case.sql,
                )?;
            }
            drop(db);
            // Each seeded round uses a fresh connection and a deterministic
            // shuffled order across all indexed cases.
            final_reads(
                &path,
                profile,
                "history",
                &source_path,
                &vfs,
                final_cases,
                &mut output,
            )?;
            for epoch in 0..options.churn_epochs {
                churn(&path, profile, &source_path, &vfs, end, epoch)?;
                let reader = connect(&path, profile, &source_path, &vfs)?;
                for case in final_cases {
                    output.measure(&reader, profile, "churn-read", epoch, case)?;
                }
                drop(reader);
                if profile != "native" {
                    seal += 1;
                    let before = objects(&path)?;
                    let start = Instant::now();
                    zsqlite::flush(&path)?;
                    output.snapshot(
                        &path,
                        profile,
                        "churn-seal",
                        seal,
                        end,
                        start.elapsed().as_nanos(),
                        &before,
                        None,
                    )?;
                }
            }
            final_reads(
                &path,
                profile,
                "before-metadata-rollup",
                &source_path,
                &vfs,
                final_cases,
                &mut output,
            )?;
            if profile != "native" {
                let before = objects(&path)?;
                let start = Instant::now();
                zsqlite::compact(&path)?;
                output.snapshot(
                    &path,
                    profile,
                    "metadata-rollup",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    None,
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                let collected = zsqlite::collect(&path, usize::MAX)?;
                output.snapshot(
                    &path,
                    profile,
                    "metadata-rollup-gc",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    Some(&collected),
                )?;
            }
            final_reads(
                &path,
                profile,
                "after-metadata-rollup",
                &source_path,
                &vfs,
                final_cases,
                &mut output,
            )?;
            let retained = if profile == "native" {
                None
            } else {
                Some(zsqlite::retain(
                    &path,
                    zsqlite::RetentionName::new("before-gc-churn")?,
                )?)
            };
            // Mutate the post-rollup layout to measure GC readiness.
            for epoch in 0..4 {
                churn(&path, profile, &source_path, &vfs, end, epoch)?;
                let reader = connect(&path, profile, &source_path, &vfs)?;
                for case in final_cases {
                    output.measure(&reader, profile, "gc-churn-read", epoch, case)?;
                }
                drop(reader);
                if profile != "native" {
                    seal += 1;
                    let before = objects(&path)?;
                    let start = Instant::now();
                    zsqlite::flush(&path)?;
                    output.snapshot(
                        &path,
                        profile,
                        "gc-churn-seal",
                        seal,
                        end,
                        start.elapsed().as_nanos(),
                        &before,
                        None,
                    )?;
                }
            }
            if profile != "native" {
                let before = objects(&path)?;
                output.snapshot(
                    &path,
                    profile,
                    "before-pin-release",
                    seal,
                    end,
                    0,
                    &before,
                    None,
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                zsqlite::release_retention(
                    &path,
                    retained.ok_or("managed profile lost retention pin")?,
                )?;
                output.snapshot(
                    &path,
                    profile,
                    "pin-released",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    None,
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                let bounded = zsqlite::collect(&path, 8)?;
                output.snapshot(
                    &path,
                    profile,
                    "bounded-gc",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    Some(&bounded),
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                let full = zsqlite::collect(&path, usize::MAX)?;
                output.snapshot(
                    &path,
                    profile,
                    "full-gc",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    Some(&full),
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                zsqlite::compact(&path)?;
                output.snapshot(
                    &path,
                    profile,
                    "metadata-rollup",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    None,
                )?;
                let before = objects(&path)?;
                let start = Instant::now();
                let full = zsqlite::collect(&path, usize::MAX)?;
                output.snapshot(
                    &path,
                    profile,
                    "post-metadata-rollup-gc",
                    seal,
                    end,
                    start.elapsed().as_nanos(),
                    &before,
                    Some(&full),
                )?;
            }
            final_reads(
                &path,
                profile,
                "after-rollup",
                &source_path,
                &vfs,
                final_cases,
                &mut output,
            )?;
            // Verify SQL against an exported plain image, so validation scans
            // cannot train the VFS read classifier or contaminate timings.
            let exported = options.output.join(format!("{profile}-export.sqlite"));
            let verify_path = if profile == "native" {
                &path
            } else {
                zsqlite::verify(&path)?;
                zsqlite::export_to_sqlite(&path, &exported)?;
                &exported
            };
            let verify_db = Connection::open(verify_path, false, true)?;
            verify(&verify_db, &source, profile, end, &mut output)?;
            drop(verify_db);
            if profile != "native" {
                // Only this harness-created, verified disposable export is removed.
                std::fs::remove_file(exported)?;
            }
            writeln!(
                meta,
                "{profile}_final_complete_bytes={}\n{profile}_total_wall_ns={}",
                complete_bytes(&path)?,
                started.elapsed().as_nanos()
            )?;
            meta.flush()?;
            output.checks.flush()?;
            writeln!(
                meta,
                "{profile}_process_lifetime_peak_rss_bytes={}",
                super::sqlite::peak_rss_bytes()
                    .map_or_else(|| "unavailable".to_owned(), |bytes| bytes.to_string())
            )?;
            println!("{profile} verified");
        }
        if hash(&options.source)? != digest {
            return Err("original source changed during benchmark".into());
        }
        reject_pending_source(&options.source)?;
        println!(
            "verified all profiles; results={}",
            options.output.display()
        );
        Ok(())
    }
}
