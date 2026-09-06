use std::ffi::OsString;
use std::path::PathBuf;

enum Command {
    Inspect {
        database: PathBuf,
    },
    Verify {
        database: PathBuf,
    },
    Compact {
        database: PathBuf,
        config: zsqlite::CompressionConfig,
    },
    Convert {
        source: PathBuf,
        output: PathBuf,
        config: zsqlite::CompressionConfig,
    },
    Export {
        database: PathBuf,
        output: PathBuf,
    },
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
        Command::Compact { database, config } => {
            let info = zsqlite::compact_with_config(database, config)?;
            println!(
                "compacted: {} logical bytes -> {} physical bytes",
                info.logical_size,
                info.base_bytes + info.sidecar_bytes
            );
        }
        Command::Convert {
            source,
            output,
            config,
        } => {
            let info = zsqlite::convert_to_zsqlite_with_config(source, output, config)?;
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
    let command = command.to_str().ok_or_else(usage)?;
    let remaining = args.collect::<Vec<_>>();
    match command {
        "inspect" | "verify" | "export" => parse_unconfigured(command, &remaining),
        "compact" | "convert" => parse_configured(command, remaining),
        _ => Err(usage()),
    }
}

fn parse_unconfigured(command: &str, arguments: &[OsString]) -> Result<Command, String> {
    match (command, arguments) {
        ("inspect", [database]) => Ok(Command::Inspect {
            database: PathBuf::from(database),
        }),
        ("verify", [database]) => Ok(Command::Verify {
            database: PathBuf::from(database),
        }),
        ("export", [database, output]) => Ok(Command::Export {
            database: PathBuf::from(database),
            output: PathBuf::from(output),
        }),
        _ => Err(usage()),
    }
}

fn parse_configured(command: &str, arguments: Vec<OsString>) -> Result<Command, String> {
    let defaults = zsqlite::CompressionConfig::default();
    let mut extent_size = defaults.extent_bytes();
    let mut seek_size = defaults.seek_chunk_bytes();
    let mut paths = Vec::new();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("--extent-size") => {
                extent_size = parse_byte_size(
                    arguments
                        .next()
                        .ok_or_else(|| "--extent-size requires a value".to_owned())?,
                )?;
            }
            Some("--seek-size") => {
                seek_size = parse_byte_size(
                    arguments
                        .next()
                        .ok_or_else(|| "--seek-size requires a value".to_owned())?,
                )?;
            }
            Some(value) if value.starts_with('-') => {
                return Err(format!("unknown option: {value}\n{}", usage()));
            }
            _ => paths.push(PathBuf::from(argument)),
        }
    }
    let config = zsqlite::CompressionConfig::new(extent_size, seek_size)
        .map_err(|error| error.to_string())?;
    match (command, paths.as_slice()) {
        ("compact", [database]) => Ok(Command::Compact {
            database: database.clone(),
            config,
        }),
        ("convert", [source, output]) => Ok(Command::Convert {
            source: source.clone(),
            output: output.clone(),
            config,
        }),
        _ => Err(usage()),
    }
}

fn parse_byte_size(value: OsString) -> Result<u32, String> {
    let value = value
        .into_string()
        .map_err(|_| "sizes must be valid UTF-8".to_owned())?;
    let lower = value.to_ascii_lowercase();
    let (digits, multiplier) = if let Some(digits) = lower.strip_suffix("mib") {
        (digits, 1024_u32 * 1024)
    } else if let Some(digits) = lower.strip_suffix("kib") {
        (digits, 1024_u32)
    } else {
        (lower.as_str(), 1_u32)
    };
    digits
        .parse::<u32>()
        .ok()
        .and_then(|number| number.checked_mul(multiplier))
        .ok_or_else(|| format!("invalid byte size: {value}"))
}

fn usage() -> String {
    "usage: zsqlite <inspect|verify> <database>\n       zsqlite compact [--extent-size BYTES] [--seek-size BYTES] <database>\n       zsqlite convert [--extent-size BYTES] [--seek-size BYTES] <sqlite-database> <zsqlite-database>\n       zsqlite export <zsqlite-database> <sqlite-database>".into()
}
