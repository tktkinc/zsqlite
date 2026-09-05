use std::ffi::OsString;
use std::path::PathBuf;

enum Command {
    Inspect { database: PathBuf },
    Verify { database: PathBuf },
    Compact { database: PathBuf },
    Convert { source: PathBuf, output: PathBuf },
    Export { database: PathBuf, output: PathBuf },
}

fn main() {
    if let Err(error) = run() {
        eprintln!("zsqlite: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    match parse_args()? {
        Command::Inspect { database } => {
            let info = zsqlite::inspect(database)?;
            println!("database: {}", info.path.display());
            println!("sidecar: {}", info.sidecar_path.display());
            println!("format: V3 native");
            println!("page_size: {}", info.page_size);
            println!("page_count: {}", info.page_count);
            println!("logical_bytes: {}", info.logical_size);
            println!("base_bytes: {}", info.base_bytes);
            println!("sidecar_bytes: {}", info.sidecar_bytes);
            println!("sidecar_allocated_bytes: {}", info.sidecar_allocated_bytes);
            println!("live_stored_bytes: {}", info.live_stored_bytes);
            println!("generation: {}", info.generation);
            println!("index_generation: {}", info.index_generation);
            println!("indexed_pages: {}", info.indexed_pages);
            println!("live_extents: {}", info.live_extents);
            println!("committed_end: {}", info.committed_end);
            println!("hole_punching: {}", info.hole_punching);
        }
        Command::Verify { database } => {
            let info = zsqlite::verify(database)?;
            println!("ok: {} pages", info.page_count);
        }
        Command::Compact { database } => {
            let info = zsqlite::compact(database)?;
            println!(
                "compacted: {} logical bytes -> {} physical bytes",
                info.logical_size,
                info.base_bytes + info.sidecar_bytes
            );
        }
        Command::Convert { source, output } => {
            let info = zsqlite::convert_to_zsqlite(source, output)?;
            println!(
                "converted: {} logical bytes -> {} sidecar bytes",
                info.logical_size, info.sidecar_bytes
            );
        }
        Command::Export { database, output } => {
            let bytes = zsqlite::export_to_sqlite(database, output)?;
            println!("exported: {bytes} bytes");
        }
    }
    Ok(())
}

fn parse_args() -> Result<Command, String> {
    let mut args = std::env::args_os().skip(1);
    let Some(command) = args.next() else {
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
    let first = args.next().ok_or_else(usage)?;
    let second = args.next();
    if args.next().is_some() {
        return Err(usage());
    }
    command_from(&command, PathBuf::from(first), second.map(PathBuf::from)).ok_or_else(usage)
}

fn command_from(command: &OsString, first: PathBuf, second: Option<PathBuf>) -> Option<Command> {
    match (command.to_str()?, second) {
        ("inspect", None) => Some(Command::Inspect { database: first }),
        ("verify", None) => Some(Command::Verify { database: first }),
        ("compact", None) => Some(Command::Compact { database: first }),
        ("convert", Some(output)) => Some(Command::Convert {
            source: first,
            output,
        }),
        ("export", Some(output)) => Some(Command::Export {
            database: first,
            output,
        }),
        _ => None,
    }
}

fn usage() -> String {
    "usage: zsqlite <inspect|verify|compact> <database>\n       zsqlite convert <sqlite-database> <zsqlite-database>\n       zsqlite export <zsqlite-database> <sqlite-database>".into()
}
