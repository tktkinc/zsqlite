//! Real indexed SQL workloads against an existing, closed transcript snapshot.
//! Each shuffled round runs in a fresh process; filesystem caches are not flushed.
#[cfg(feature = "static")]
#[path = "support/indexed_workload.rs"]
mod indexed_workload;
#[cfg(feature = "static")]
#[path = "support/read_stream.rs"]
mod read_stream;
#[cfg(feature = "static")]
#[allow(dead_code)]
#[path = "support/replay_sqlite.rs"]
mod sqlite;
#[cfg(feature = "static")]
#[path = "support/virtual_table.rs"]
mod virtual_table;
#[cfg(feature = "static")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    bench::run()
}

#[cfg(feature = "static")]
mod bench {
    use std::fs::File;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::Instant;

    use super::sqlite::{
        Connection, Result, bundle_bytes, fingerprint_rows as fingerprint, read_values,
        write_values,
    };
    const HEADER: &str = "profile,workload,round,execution,query_ns,rows,result_bytes,requested,fetched,inflated,decode_ns,vfs_hits,vfs_misses,sqlite_hits,sqlite_misses,sqlite_used_bytes,prefetch_used,prefetch_unused,fingerprint";
    fn worker(args: &[String]) -> Result<()> {
        if args.len() != 7 {
            return Err("invalid worker invocation".into());
        }
        let profile = &args[1];
        let managed = profile != "native";
        if managed {
            zsqlite::register_static_vfs().map_err(|code| format!("registration: {code}"))?;
        }
        let pager_mib: u64 = args[5].parse()?;
        if !(1..=1024).contains(&pager_mib) {
            return Err("SQLite cache bounds".into());
        }
        let opening = Instant::now();
        let db = Connection::open(Path::new(&args[2]), managed, true)?;
        db.execute(&format!(
            "PRAGMA query_only=ON; PRAGMA cache_size=-{};",
            pager_mib * 1024
        ))?;
        let open_ns = opening.elapsed().as_nanos();
        let directory = Path::new(&args[3]);
        let round: usize = args[4].parse()?;
        let case_count: usize = args[6].parse()?;
        let names = std::fs::read_to_string(directory.join("case-names.txt"))?
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if names.len() != case_count {
            return Err("worker case manifest count differs".into());
        }
        println!("connection,{profile},{round},{open_ns},{case_count}");
        let name_refs = names.iter().map(String::as_str).collect::<Vec<_>>();
        for index in super::read_stream::order(&name_refs, "queries", round) {
            let sql = std::fs::read_to_string(directory.join(format!("query-{index}.sql")))?;
            let parameters = read_values(&directory.join(format!("query-{index}.params")))?;
            for execution in ["stream-first", "immediate-repeat"] {
                let before = db.stats(managed)?;
                let start = Instant::now();
                let rows = db.query_params(&sql, &parameters)?;
                let elapsed = start.elapsed().as_nanos();
                let after = db.stats(managed)?;
                // Full byte-for-byte result validation is outside the timed interval.
                let (hash, bytes) = fingerprint(&rows);
                println!(
                    "query,{profile},{},{round},{execution},{elapsed},{},{bytes},{},{},{},{},{},{},{},{},{},{},{},{hash}",
                    names[index],
                    rows.len(),
                    after.io.requested_bytes - before.io.requested_bytes,
                    after.io.fetched_bytes - before.io.fetched_bytes,
                    after.io.inflated_bytes - before.io.inflated_bytes,
                    after.io.decode_nanoseconds - before.io.decode_nanoseconds,
                    after.io.cache_hits - before.io.cache_hits,
                    after.io.cache_misses - before.io.cache_misses,
                    after.sqlite.hits - before.sqlite.hits,
                    after.sqlite.misses - before.sqlite.misses,
                    after.sqlite.used_bytes,
                    after.cache.extra_pages_requested - before.cache.extra_pages_requested,
                    after.cache.extra_pages_evicted_unused
                        - before.cache.extra_pages_evicted_unused
                );
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub fn run() -> Result<()> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        if args.first().is_some_and(|arg| arg == "--worker") {
            return worker(&args);
        }
        let mut bundles = None;
        let mut repeats = super::read_stream::DEFAULT_ROUNDS;
        let mut cache_mib = 256_u64;
        let mut pager_mib = 10_u64;
        let mut random_samples = 3_usize;
        let mut profiles =
            "page-l3,64k-l3,256k-l3,1m-l3,4m-l3,page-l9,64k-l9,256k-l9,1m-l9,4m-l9".to_owned();
        let mut args = args.iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--bundles" => bundles = Some(PathBuf::from(args.next().ok_or("missing bundle directory")?)),
                "--repeats" => repeats = args.next().ok_or("missing repeat count")?.parse()?,
                "--page-cache-mib" | "--frame-cache-mib" => cache_mib = args.next().ok_or("missing disk cache size")?.parse()?,
                "--sqlite-cache-mib" => pager_mib = args.next().ok_or("missing SQLite cache size")?.parse()?,
                "--random-samples" => random_samples = args.next().ok_or("missing sample count")?.parse()?,
                "--profiles" => profiles.clone_from(args.next().ok_or("missing profiles")?),
                "--bench" => {},
                _ => return Err("usage: --bundles FRAME_LAYOUT_OUTPUT [--profiles CSV] [--repeats N] [--page-cache-mib N] [--sqlite-cache-mib N] [--random-samples N]".into()),
            }
        }
        if !(1..=20).contains(&repeats)
            || cache_mib > 1024
            || !(1..=1024).contains(&pager_mib)
            || !(1..=16).contains(&random_samples)
        {
            return Err("repeat/cache bounds".into());
        }
        let bundles = bundles
            .ok_or("--bundles is required; use closed transcript bundles from frame_layout")?;
        let source = bundles.join("input.sqlite");
        let directory = tempfile::Builder::new()
            .prefix("zsqlite-queries-")
            .tempdir()?
            .keep();
        let native = Connection::open(&source, false, true)?;
        let workload = super::indexed_workload::generate(
            &native,
            0x5a51_17e5_d15c_a11e,
            random_samples,
            None,
        )?;
        if workload.cases.is_empty() {
            return Err("source has no sampleable indexed application records".into());
        }
        let mut plans = File::create_new(directory.join("plans.txt"))?;
        writeln!(
            plans,
            "Every profile: SQLite RAM page cache {pager_mib} MiB. Managed profiles additionally get a VFS disk page cache of {cache_mib} MiB. {repeats} fresh shuffled-stream processes per profile; every case gets a stream-first and immediate-repeat execution. Only the first query in a stream follows connection open. OS cache not flushed."
        )?;
        let mut coverage = File::create_new(directory.join("index-coverage.csv"))?;
        writeln!(coverage, "table,kind,access_paths,cases,note")?;
        for entry in &workload.coverage {
            writeln!(
                coverage,
                "{},{},{},{},{}",
                entry.table, entry.kind, entry.access_paths, entry.cases, entry.note
            )?;
        }
        let mut cases = Vec::new();
        let mut names = File::create_new(directory.join("case-names.txt"))?;
        for (index, case) in workload.cases.into_iter().enumerate() {
            let query_path = directory.join(format!("query-{index}.sql"));
            let parameters_path = directory.join(format!("query-{index}.params"));
            let mut query = File::create_new(&query_path)?;
            writeln!(query, "{}", case.sql)?;
            write_values(&parameters_path, &case.parameters)?;
            let details = super::indexed_workload::validate_plan(&native, &case)?;
            writeln!(plans, "\nnative/{}\n{}\n{details:?}", case.name, case.sql)?;
            let name = case.name.replace([',', '\n', '\r'], "_");
            writeln!(names, "{name}")?;
            cases.push(case);
        }
        drop(names);
        drop(native);
        zsqlite::register_static_vfs().map_err(|code| format!("registration: {code}"))?;
        let mut results = File::create_new(directory.join("queries.csv"))?;
        writeln!(results, "{HEADER}")?;
        let mut connections = File::create_new(directory.join("connections.csv"))?;
        writeln!(connections, "profile,round,open_ns,cases")?;
        let mut storage = File::create_new(directory.join("storage.csv"))?;
        writeln!(
            storage,
            "profile,complete_bytes,current_bytes,objects,dictionary_bytes,page_cache_mib,sqlite_cache_mib"
        )?;
        println!("output={} source={}", directory.display(), source.display());
        let mut expected = std::collections::BTreeMap::new();
        for profile in std::iter::once("native").chain(profiles.split(',')) {
            if profile != "native"
                && (!profile
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                    || profile.is_empty())
            {
                return Err("invalid profile name".into());
            }
            let path = if profile == "native" {
                source.clone()
            } else {
                bundles.join(format!("{profile}.zsqlite"))
            };
            if profile == "native" {
                writeln!(
                    storage,
                    "native,{},{},1,0,0,{pager_mib}",
                    source.metadata()?.len(),
                    source.metadata()?.len()
                )?;
            } else {
                let policy = zsqlite::inspect(&path)?.policy;
                let layout = policy
                    .layout()
                    .with_cache(zsqlite::domain::CacheBytes::new(cache_mib * 1024 * 1024))?
                    .with_maintenance(zsqlite::domain::DecodedBytes::new(0), 0)?;
                zsqlite::configure(&path, policy.with_layout(layout))?;
                let info = zsqlite::inspect(&path)?;
                writeln!(
                    storage,
                    "{profile},{},{},{},{},{cache_mib},{pager_mib}",
                    bundle_bytes(&path)?,
                    info.retention.current_view_bytes,
                    info.retention.objects,
                    info.dictionary_bytes
                )?;
            }
            let plan_db = Connection::open(&path, profile != "native", true)?;
            for case in &cases {
                let details = super::indexed_workload::validate_plan(&plan_db, case)?;
                writeln!(plans, "\n{profile}/{}\n{details:?}", case.name)?;
            }
            drop(plan_db);
            let mut timings = cases
                .iter()
                .map(|case| {
                    (
                        case.name.replace([',', '\n', '\r'], "_"),
                        (Vec::new(), Vec::new()),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            for round in 0..repeats {
                let output = Command::new(std::env::current_exe()?)
                    .arg("--worker")
                    .arg(profile)
                    .arg(&path)
                    .arg(&directory)
                    .arg(round.to_string())
                    .arg(pager_mib.to_string())
                    .arg(cases.len().to_string())
                    .output()?;
                if !output.status.success() {
                    return Err(format!(
                        "{profile}/round-{round}: {}",
                        String::from_utf8_lossy(&output.stderr)
                    )
                    .into());
                }
                for line in String::from_utf8(output.stdout)?.lines() {
                    if let Some(line) = line.strip_prefix("connection,") {
                        if line.split(',').count() != 4 {
                            return Err("invalid worker connection result".into());
                        }
                        writeln!(connections, "{line}")?;
                        continue;
                    }
                    let line = line
                        .strip_prefix("query,")
                        .ok_or("invalid worker result kind")?;
                    let fields = line.split(',').collect::<Vec<_>>();
                    if fields.len() != 19 {
                        return Err("invalid worker query result".into());
                    }
                    let name = fields[1];
                    match expected.get(name) {
                        None => {
                            expected.insert(name.to_owned(), fields[18].to_owned());
                        }
                        Some(hash) if hash == fields[18] => {}
                        Some(_) => {
                            return Err(format!("query result differs: {profile}/{name}").into());
                        }
                    }
                    let timing = timings
                        .get_mut(name)
                        .ok_or("worker returned an unknown workload")?;
                    let ns: u64 = fields[4].parse()?;
                    if fields[3] == "stream-first" {
                        timing.0.push(ns);
                    } else if fields[3] == "immediate-repeat" {
                        timing.1.push(ns);
                    } else {
                        return Err("invalid query execution label".into());
                    }
                    writeln!(results, "{line}")?;
                }
                results.flush()?;
                connections.flush()?;
            }
            for (name, (mut first, mut repeated)) in timings {
                if first.len() != repeats || repeated.len() != repeats {
                    return Err(format!("missing worker measurements: {profile}/{name}").into());
                }
                first.sort_unstable();
                repeated.sort_unstable();
                println!(
                    "{profile}/{name}: stream-first {:.3}ms immediate-repeat {:.3}ms",
                    std::time::Duration::from_nanos(first[first.len() / 2]).as_secs_f64() * 1000.0,
                    std::time::Duration::from_nanos(repeated[repeated.len() / 2]).as_secs_f64()
                        * 1000.0
                );
            }
            storage.flush()?;
        }
        println!("results: {}", directory.display());
        Ok(())
    }
}
