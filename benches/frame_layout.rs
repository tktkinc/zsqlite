//! Reproducible direct-VFS page replay. This measures fetch/decode amplification,
//! not SQL query latency. All profiles get 8 MiB VFS + 2 MiB `SQLite` cache budgets.
#[cfg(feature = "static")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench::run()
}

#[cfg(feature = "static")]
#[allow(dead_code)] // Each workload uses a different subset of the shared adapter.
#[path = "support/replay_sqlite.rs"]
mod sqlite;

#[cfg(feature = "static")]
mod bench {
    use super::sqlite::{Connection, Result, bundle_bytes, sidecar};
    use std::fs::File;
    use std::io::{Read, Write};
    use std::path::{Path, PathBuf};
    use std::time::Instant;
    use zsqlite::domain::{CacheBytes, DecodedBytes};
    use zsqlite::layout::LayoutPolicy;

    fn hash(path: &Path) -> Result<blake3::Hash> {
        let mut file = File::open(path)?;
        let mut hasher = blake3::Hasher::new();
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        Ok(hasher.finalize())
    }
    fn trace(kind: &str, count: usize, pages: u32) -> Vec<u32> {
        let mut state = 0x1234_5678_9abc_def0_u64;
        (0..count)
            .map(|index| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                match kind {
                    "repeated" => 1 + (u32::try_from(index).expect("bounded trace") % pages.min(8)),
                    "grouped" => {
                        1 + ((u32::try_from(index).expect("bounded trace") / 32 * 257
                            + u32::try_from(index).expect("bounded trace") % 32)
                            % pages)
                    }
                    _ => 1 + u32::try_from(state % u64::from(pages)).expect("bounded page"),
                }
            })
            .collect()
    }
    fn fixture(path: &Path) -> Result<()> {
        let db = Connection::open(path, false, false)?;
        db.execute("PRAGMA journal_mode=DELETE; CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT); CREATE VIRTUAL TABLE search USING fts5(body); BEGIN;")?;
        let vocabulary = "quantum climate chemistry encyclopedia stable storage page frame dictionary random access historical archive ";
        for id in 1..=4096 {
            let text = format!("article {id} {}", vocabulary.repeat(24));
            db.execute(&format!("INSERT INTO docs VALUES({id},'{text}'); INSERT INTO search(rowid,body) VALUES({id},'{text}');"))?;
        }
        db.execute("COMMIT; INSERT INTO search(search) VALUES('optimize');")?;
        Ok(())
    }
    fn objects(path: &Path) -> Result<std::collections::BTreeMap<PathBuf, u64>> {
        std::fs::read_dir(sidecar(path).join("objects"))?
            .map(|entry| {
                let entry = entry?;
                Ok((entry.path(), entry.metadata()?.len()))
            })
            .collect()
    }
    fn churn(path: &Path, output: &mut File, profile: &str) -> Result<()> {
        let name = zsqlite::RetentionName::new("long-lived-fork")?;
        let pin = zsqlite::retain(path, name)?;
        let policy = zsqlite::inspect(path)?.policy;
        zsqlite::configure(
            path,
            policy.with_layout(
                policy
                    .layout()
                    .with_maintenance(DecodedBytes::new(64 * 1024 * 1024), 8)?,
            ),
        )?;
        let lifetime = Instant::now();
        for kind in ["repeated", "grouped", "random", "append", "shifting"] {
            let mut elapsed = 0_u128;
            let mut rewritten = 0_u64;
            let mut collected = 0_u64;
            let mut repacked = 0_u64;
            let mut page_writes = 0_u64;
            for epoch in 0..8_u32 {
                let before_objects = objects(path)?;
                let db = Connection::open(path, true, false)?;
                let before_stats = db.stats(true)?;
                let start = Instant::now();
                db.execute("BEGIN IMMEDIATE;")?;
                for index in 0..16_u32 {
                    let id = match kind {
                        "repeated" => 1 + index,
                        "grouped" => 1025 + index,
                        "random" => 1 + (epoch * 977 + index * 127) % 4096,
                        "shifting" => 1 + epoch * 128 + index,
                        _ => 4097 + epoch * 16 + index,
                    };
                    let text = format!(
                        "{kind} epoch {epoch} entry {index} {}",
                        "changed searchable article storage ".repeat(64)
                    );
                    db.execute(&format!("INSERT OR REPLACE INTO docs VALUES({id},'{text}'); DELETE FROM search WHERE rowid={id}; INSERT INTO search(rowid,body) VALUES({id},'{text}');"))?;
                }
                db.execute("COMMIT;")?;
                let after_stats = db.stats(true)?;
                page_writes += after_stats
                    .sqlite
                    .writes
                    .saturating_sub(before_stats.sqlite.writes);
                drop(db);
                zsqlite::flush(path)?;
                repacked += zsqlite::maintain(path)?.decoded_input.get();
                elapsed += start.elapsed().as_nanos();
                let after_objects = objects(path)?;
                rewritten += after_objects
                    .iter()
                    .filter(|(name, _)| !before_objects.contains_key(*name))
                    .map(|(_, bytes)| bytes)
                    .sum::<u64>();
                collected += before_objects
                    .iter()
                    .filter(|(name, _)| !after_objects.contains_key(*name))
                    .map(|(_, bytes)| bytes)
                    .sum::<u64>();
            }
            let gc = zsqlite::collect(path, 0)?;
            writeln!(
                output,
                "{profile},{kind},{elapsed},{},{},{},{},{rewritten},{collected},{repacked},{page_writes},{}",
                gc.current_view_bytes,
                gc.retained_bytes,
                gc.partially_obsolete_bytes,
                gc.objects,
                lifetime.elapsed().as_nanos()
            )?;
        }
        let released = Instant::now();
        zsqlite::release_retention(path, pin)?;
        let before = zsqlite::collect(path, 0)?;
        let after = zsqlite::collect(path, usize::MAX)?;
        writeln!(
            output,
            "{profile},release,{},{},{},{},{},0,{},0,0,{}",
            released.elapsed().as_nanos(),
            after.current_view_bytes,
            after.retained_bytes,
            before.collectible_bytes,
            after.deleted_objects,
            after.deleted_bytes,
            lifetime.elapsed().as_nanos()
        )?;
        zsqlite::verify(path)?;
        Ok(())
    }
    #[allow(clippy::too_many_lines)]
    pub fn run() -> Result<()> {
        let mut source = None;
        let mut reads = 256_usize;
        let mut run_churn = false;
        let mut quick = false;
        let mut build_only = false;
        let mut skip_reads = false;
        let mut args = std::env::args_os().skip(1);
        while let Some(arg) = args.next() {
            match arg.to_str() {
                Some("--source") => source = Some(PathBuf::from(args.next().ok_or("missing source")?)),
                Some("--reads") => reads = args.next().ok_or("missing reads")?.to_str().ok_or("invalid reads")?.parse()?,
                Some("--churn") => run_churn = true,
                Some("--quick") => quick = true,
                Some("--build-only") => build_only = true,
                Some("--skip-reads") => skip_reads = true,
                Some("--bench") => {},
                _ => return Err("usage: --source SQLITE [--reads N] [--quick]; omit source for FTS fixture; --churn requires fixture".into()),
            }
        }
        if reads == 0 || reads > 1_000_000 {
            return Err("reads must be 1..=1000000".into());
        }
        if run_churn && source.is_some() {
            return Err("--churn is supported only for the generated FTS fixture".into());
        }
        if run_churn && build_only {
            return Err("--build-only cannot be combined with --churn".into());
        }
        zsqlite::register_static_vfs().map_err(|code| format!("registration: {code}"))?;
        let directory = tempfile::Builder::new()
            .prefix("zsqlite-layout-")
            .tempdir()?
            .keep();
        let source = if let Some(source) = source {
            let mut wal = source.as_os_str().to_os_string();
            wal.push("-wal");
            let wal = PathBuf::from(wal);
            if wal.metadata().is_ok_and(|metadata| metadata.len() != 0) {
                return Err("source has a nonempty WAL; provide a closed snapshot".into());
            }
            let before = hash(&source)?;
            let owned = directory.join("input.sqlite");
            std::fs::copy(&source, &owned)?;
            if hash(&source)? != before
                || hash(&owned)? != before
                || wal.metadata().is_ok_and(|metadata| metadata.len() != 0)
            {
                return Err("source changed while copying".into());
            }
            owned
        } else {
            let path = directory.join("fts.sqlite");
            fixture(&path)?;
            path
        };
        let source_hash = hash(&source)?;
        println!(
            "source={} bytes={} blake3={} output={}",
            source.display(),
            source.metadata()?.len(),
            source_hash,
            directory.display()
        );
        let mut results = File::create_new(directory.join("reads.csv"))?;
        let mut builds = File::create_new(directory.join("builds.csv"))?;
        writeln!(
            builds,
            "profile,source_bytes,complete_bytes,current_bytes,objects,build_ns"
        )?;
        writeln!(
            results,
            "profile,workload,complete_bytes,current_bytes,objects,build_ns,open_ns,total_ns,p50_ns,p95_ns,p99_ns,requested,fetched,inflated,decode_ns,hits,misses,prefetch_used,prefetch_unused"
        )?;
        let mut writes = File::create_new(directory.join("churn.csv"))?;
        writeln!(
            writes,
            "profile,workload,total_ns,current_bytes,retained_bytes,obsolete_or_collected_bytes,objects,immutable_bytes_rewritten,bytes_collected,repack_decoded_input,sqlite_page_writes,fork_age_ns"
        )?;
        let profiles = if quick {
            vec![("page", 0), ("1m", 1024 * 1024)]
        } else {
            vec![
                ("page", 0),
                ("64k", 65536),
                ("256k", 256 * 1024),
                ("1m", 1024 * 1024),
                ("4m", 4 * 1024 * 1024),
                ("8m", 8 * 1024 * 1024),
            ]
        };
        for level in [3, 9] {
            for (name, frame) in &profiles {
                let profile = format!("{name}-l{level}");
                let path = directory.join(format!("{profile}.zsqlite"));
                let mut policy = LayoutPolicy::default()
                    .with_level(level)?
                    .with_cache(CacheBytes::new(8 * 1024 * 1024))?
                    .with_maintenance(DecodedBytes::new(0), 0)?;
                policy = match frame {
                    0 => policy,
                    bytes => policy.fixed(DecodedBytes::new(*bytes))?,
                };
                let start = Instant::now();
                // Build each layout once; compact intentionally does not
                // recompress an already-flat range after a policy change.
                let info = zsqlite::convert_to_zsqlite_with_policy(
                    &source,
                    &path,
                    zsqlite::StoragePolicy::default().with_layout(policy),
                )?;
                let build_duration = start.elapsed();
                let build = build_duration.as_nanos();
                let exported = directory.join("roundtrip.sqlite");
                zsqlite::export_to_sqlite(&path, &exported)?;
                if hash(&exported)? != source_hash {
                    return Err("export differs from original".into());
                }
                std::fs::remove_file(&exported)?;
                let complete = bundle_bytes(&path)?;
                let gc = zsqlite::collect(&path, 0)?;
                println!(
                    "{profile}: {complete} bytes, {} objects, build {:.3}s",
                    gc.objects,
                    build_duration.as_secs_f64()
                );
                writeln!(
                    builds,
                    "{profile},{},{complete},{},{},{build}",
                    source.metadata()?.len(),
                    gc.current_view_bytes,
                    gc.objects
                )?;
                builds.flush()?;
                if build_only {
                    continue;
                }
                if !skip_reads {
                    for kind in ["random", "grouped", "repeated"] {
                        let opening = Instant::now();
                        let db = Connection::open(&path, true, false)?;
                        let open_ns = opening.elapsed().as_nanos();
                        let (mut latency, stats) =
                            db.replay(&trace(kind, reads, info.page_count), info.page_size)?;
                        drop(db);
                        let complete = bundle_bytes(&path)?;
                        let total: u128 = latency.iter().map(|ns| u128::from(*ns)).sum();
                        latency.sort_unstable();
                        let percentile = |percent| latency[(latency.len() - 1) * percent / 100];
                        writeln!(
                            results,
                            "{profile},{kind},{complete},{},{},{build},{open_ns},{total},{},{},{},{},{},{},{},{},{},{},{}",
                            gc.current_view_bytes,
                            gc.objects,
                            percentile(50),
                            percentile(95),
                            percentile(99),
                            stats.handle.requested_bytes,
                            stats.handle.fetched_bytes,
                            stats.handle.inflated_bytes,
                            stats.handle.decode_nanoseconds,
                            stats.handle.cache_hits,
                            stats.handle.cache_misses,
                            stats.database.extra_pages_requested,
                            stats.database.extra_pages_evicted_unused
                        )?;
                        results.flush()?;
                    }
                }
                zsqlite::configure(&path, zsqlite::StoragePolicy::default().with_layout(policy))?;
                if run_churn {
                    churn(&path, &mut writes, &profile)?;
                    writes.flush()?;
                }
            }
        }
        println!("results: {}", directory.display());
        Ok(())
    }
}
