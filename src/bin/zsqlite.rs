use std::path::PathBuf;

enum Command {
    Inspect(PathBuf),
    Verify(PathBuf),
    Flush(PathBuf),
    Compact(PathBuf),
    Convert(PathBuf, PathBuf),
    Adopt(PathBuf),
    Conversion(String, PathBuf),
    Export(PathBuf, PathBuf),
}

fn main() {
    if let Err(error) = run() {
        eprintln!("zsqlite: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    match parse_args()? {
        Command::Inspect(path) => print_inspect(&zsqlite::inspect(path)?),
        Command::Verify(path) => {
            let info = zsqlite::verify(path)?;
            println!("ok: {} pages at TXID {}", info.page_count, info.head_txid);
        }
        Command::Flush(path) => {
            let info = zsqlite::flush(path)?;
            println!("flushed: {} immutable pack(s)", info.pack_count);
        }
        Command::Compact(path) => {
            let info = zsqlite::compact(path)?;
            println!(
                "compacted: {} logical bytes into {} sealed object bytes",
                info.logical_size, info.sealed_object_bytes
            );
        }
        Command::Convert(source, output) => {
            let info = zsqlite::convert_to_zsqlite(source, output)?;
            println!(
                "converted: {} logical bytes into {} sealed object bytes",
                info.logical_size, info.sealed_object_bytes
            );
        }
        Command::Adopt(path) => {
            let info = zsqlite::adopt_to_zsqlite(&path)?;
            println!(
                "ready: {} logical bytes; background conversion can be monitored, paused, or resumed",
                info.logical_size
            );
            run_conversion(&path)?;
        }
        Command::Conversion(action, path) => match action.as_str() {
            "status" => print_conversion(
                &zsqlite::conversion_status(&path)?
                    .ok_or("database has no background conversion")?,
            ),
            "pause" => print_conversion(&zsqlite::pause_conversion(&path)?),
            "resume" => {
                print_conversion(&zsqlite::resume_conversion(&path)?);
                run_conversion(&path)?;
            }
            "run" => run_conversion(&path)?,
            _ => return Err(usage().into()),
        },
        Command::Export(database, output) => {
            let bytes = zsqlite::export_to_sqlite(database, output)?;
            println!("exported: {bytes} bytes");
        }
    }
    Ok(())
}

fn print_conversion(status: &zsqlite::ConversionStatus) {
    let state = if status.complete {
        "complete"
    } else if status.paused {
        "paused"
    } else if status.running {
        "running"
    } else {
        "ready"
    };
    let tenths = u128::from(status.converted_bytes) * 1000 / u128::from(status.total_bytes.max(1));
    println!(
        "conversion: {state}; {}/{} bytes ({}.{}%); worker_active={}",
        status.converted_bytes,
        status.total_bytes,
        tenths / 10,
        tenths % 10,
        status.running
    );
    if let Some(error) = &status.last_error {
        println!("conversion_error: {error}");
    }
}

fn run_conversion(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        match zsqlite::conversion_step(path) {
            Ok(status) => {
                print_conversion(&status);
                if status.complete || status.paused {
                    return Ok(());
                }
            }
            Err(zsqlite::StoreError::Busy) => std::thread::sleep(std::time::Duration::from_secs(1)),
            Err(error) => return Err(error.into()),
        }
    }
}

fn print_inspect(info: &zsqlite::Inspect) {
    println!("database: {}", info.path.display());
    println!("sidecar: {}", info.sidecar_path.display());
    println!("format: V1 active + sealed metadata + catalog + blobs");
    println!("page_size: {}", info.page_size);
    println!("page_count: {}", info.page_count);
    println!("logical_bytes: {}", info.logical_size);
    println!("head_txid: {}", info.head_txid);
    println!("head_history: {}", zsqlite::format::hex(&info.head_history));
    println!("pack_count: {}", info.pack_count);
    println!("active: {}", info.active);
    if let Some(conversion) = &info.conversion {
        print_conversion(conversion);
    }
    println!("file_bytes: {}", info.file_bytes);
    println!("file_allocated_bytes: {}", info.file_allocated_bytes);
    println!("sealed_object_bytes: {}", info.sealed_object_bytes);
    println!(
        "sealed_object_allocated_bytes: {}",
        info.sealed_object_allocated_bytes
    );
    println!("indexed_pages: {}", info.indexed_pages);
    println!("dictionary_bytes: {}", info.dictionary_bytes);
    println!("preferred_dictionaries: {}", info.preferred_dictionaries);
    if let Some(manifest) = &info.manifest {
        println!("manifest_bytes: {}", manifest.head_bytes().get());
        println!(
            "logical_view_hash: {}",
            zsqlite::format::hex(manifest.logical_hash().as_bytes())
        );
        println!(
            "manifest_ancestor_bytes: {}",
            manifest.ancestor_bytes().get()
        );
        println!("manifest_run_depth: {}", manifest.run_depth());
        if let Some(parent) = manifest.parent() {
            println!(
                "logical_parent_hash: {}",
                zsqlite::format::hex(parent.as_bytes())
            );
        }
        println!(
            "resolved_txid_begin: {}",
            manifest.transaction_span().begin().get()
        );
        println!(
            "resolved_txid_end: {}",
            manifest.transaction_span().end().get()
        );
    }
    println!("retention: {:?}", info.retention);
    for bin in &info.frame_distribution {
        println!(
            "frames: decoded={} count={} raw={} dictionary={} payload={}",
            bin.decoded_bytes.get(),
            bin.frames,
            bin.raw_frames,
            bin.dictionary_frames,
            bin.stored_payload_bytes.get()
        );
    }
    for pack in &info.pack_occupancy {
        println!(
            "pack: {:?} live_pages={}/{} stored={} estimated_obsolete={}",
            pack.pack,
            pack.live_pages,
            pack.total_pages,
            pack.stored_bytes.get(),
            pack.reclaimable_bytes()
        );
    }
    println!(
        "rollover_bytes: {}",
        info.policy
            .rollover_bytes()
            .map_or(0, std::num::NonZeroU64::get)
    );
    println!(
        "dictionary_sample_bytes: {}",
        info.policy.dictionary().sample_bytes()
    );
}

fn parse_args() -> Result<Command, String> {
    let mut arguments = std::env::args_os().skip(1);
    let Some(command) = arguments.next() else {
        return Err(usage());
    };
    if command == "--version" || command == "-V" {
        println!("zsqlite {}", env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
    }
    if command == "--help" || command == "-h" {
        println!("{}", usage());
        std::process::exit(0);
    }
    let command = command.to_str().ok_or_else(usage)?;
    let paths = arguments.map(PathBuf::from).collect::<Vec<_>>();
    match (command, paths.as_slice()) {
        ("inspect", [database]) => Ok(Command::Inspect(database.clone())),
        ("verify", [database]) => Ok(Command::Verify(database.clone())),
        ("flush", [database]) => Ok(Command::Flush(database.clone())),
        ("compact", [database]) => Ok(Command::Compact(database.clone())),
        ("convert", [source, output]) => Ok(Command::Convert(source.clone(), output.clone())),
        ("convert", [database]) => Ok(Command::Adopt(database.clone())),
        ("conversion", [action, database])
            if ["status", "pause", "resume", "run"]
                .iter()
                .any(|value| action == *value) =>
        {
            Ok(Command::Conversion(
                action.to_string_lossy().into_owned(),
                database.clone(),
            ))
        }
        ("export", [database, output]) => Ok(Command::Export(database.clone(), output.clone())),
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: zsqlite <inspect|verify|flush|compact> <database.db>\n       zsqlite convert <database.db>\n       zsqlite convert <sqlite-database> <database.db>\n       zsqlite conversion <status|pause|resume|run> <database.db>\n       zsqlite export <database.db> <sqlite-database>".into()
}
