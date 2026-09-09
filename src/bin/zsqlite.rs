use std::path::PathBuf;

enum Command {
    Inspect(PathBuf),
    Verify(PathBuf),
    Flush(PathBuf),
    Compact(PathBuf),
    Convert(PathBuf, PathBuf),
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
            println!("flushed: {} sealed segment(s)", info.sealed_segments);
        }
        Command::Compact(path) => {
            let info = zsqlite::compact(path)?;
            println!(
                "compacted: {} logical bytes into {} segment bytes",
                info.logical_size, info.segment_bytes
            );
        }
        Command::Convert(source, output) => {
            let info = zsqlite::convert_to_zsqlite(source, output)?;
            println!(
                "converted: {} logical bytes into {} segment bytes",
                info.logical_size, info.segment_bytes
            );
        }
        Command::Export(database, output) => {
            let bytes = zsqlite::export_to_sqlite(database, output)?;
            println!("exported: {bytes} bytes");
        }
    }
    Ok(())
}

fn print_inspect(info: &zsqlite::Inspect) {
    println!("database: {}", info.path.display());
    println!("sidecar: {}", info.sidecar_path.display());
    println!("format: V6 active-segment");
    println!("page_size: {}", info.page_size);
    println!("page_count: {}", info.page_count);
    println!("logical_bytes: {}", info.logical_size);
    println!("head_txid: {}", info.head_txid);
    println!("head_history: {}", zsqlite::format::hex(&info.head_history));
    println!("generation: {}", info.generation);
    println!("sealed_segments: {}", info.sealed_segments);
    println!("active: {}", info.active);
    println!("file_bytes: {}", info.file_bytes);
    println!("file_allocated_bytes: {}", info.file_allocated_bytes);
    println!("segment_bytes: {}", info.segment_bytes);
    println!("segment_allocated_bytes: {}", info.segment_allocated_bytes);
    println!("indexed_pages: {}", info.indexed_pages);
    println!("dictionary_bytes: {}", info.dictionary_bytes);
    println!("settle_seconds: {}", info.policy.settle.as_secs());
    println!("max_stale_seconds: {}", info.policy.max_stale.as_secs());
    println!(
        "dictionary_sample_bytes: {}",
        info.policy.dictionary.sample_bytes
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
        ("export", [database, output]) => Ok(Command::Export(database.clone(), output.clone())),
        _ => Err(usage()),
    }
}

fn usage() -> String {
    "usage: zsqlite <inspect|verify|flush|compact> <database.zsqlite>\n       zsqlite convert <sqlite-database> <database.zsqlite>\n       zsqlite export <database.zsqlite> <sqlite-database>".into()
}
