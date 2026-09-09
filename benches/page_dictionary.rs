use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use zsqlite::format::{
    FRAME_HEADER_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_INDEX_ENTRY_SIZE, SEGMENT_TRAILER_SIZE,
};

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const INPUT_BUFFER_BYTES: usize = 1024 * 1024;
const SEEK_BYTES: usize = 64 * 1024;
const DICTIONARY_BYTES: usize = 64 * 1024;
const TRAINING_SAMPLES: usize = 8192;
const ZSTD_LEVEL: i32 = 3;
const MIN_SAVINGS: usize = 64;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug)]
struct Metrics {
    frame_count: u64,
    zstd_bytes: u64,
    frame_metadata_bytes: u64,
    raw_bytes: u64,
    sidecar_bytes: u64,
    elapsed: Duration,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("page_dictionary: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let Some(path) = parse_path()? else {
        return Ok(());
    };
    reject_live_auxiliary(&path)?;
    let sqlite_bytes = path.metadata()?.len();
    let (page_size, page_count) = database_layout(&path, sqlite_bytes)?;

    let training_started = Instant::now();
    let samples = training_samples(&path, page_size, page_count, TRAINING_SAMPLES)?;
    let sample_bytes = samples
        .len()
        .checked_mul(page_size)
        .ok_or("sample size overflow")?;
    let dictionary = zstd::dict::from_samples(&samples, DICTIONARY_BYTES)?;
    let training_elapsed = training_started.elapsed();

    let current = analyze(&path, sqlite_bytes, page_size, SEEK_BYTES, &[])?;
    let page_plain = analyze(&path, sqlite_bytes, page_size, page_size, &[])?;
    let page_dictionary = analyze(&path, sqlite_bytes, page_size, page_size, &dictionary)?;

    println!(
        "{} ({}, {} pages at {} KiB)",
        path.display(),
        format_bytes(sqlite_bytes),
        page_count,
        page_size / 1024
    );
    println!(
        "dictionary: {} trained from {} evenly sampled pages ({}) in {:.2?}",
        format_bytes(dictionary.len() as u64),
        samples.len(),
        format_bytes(sample_bytes as u64),
        training_elapsed
    );
    println!(
        "{:>18} {:>10} {:>13} {:>11} {:>11} {:>13} {:>11} {:>10}",
        "layout",
        "frames",
        "zstd bytes",
        "frame meta",
        "raw stored",
        "sidecar est.",
        "sidecar/db",
        "time"
    );
    print_metrics("64 KiB frames", current, sqlite_bytes);
    print_metrics("page frames", page_plain, sqlite_bytes);
    print_metrics("page + dictionary", page_dictionary, sqlite_bytes);
    println!(
        "page + dictionary vs 64 KiB: zstd payload {:+.3}%, estimated sidecar {:+.3}% ({:+.3}% of DB)",
        relative_percent(page_dictionary.zstd_bytes, current.zstd_bytes),
        relative_percent(page_dictionary.sidecar_bytes, current.sidecar_bytes),
        signed_percent(
            page_dictionary.sidecar_bytes,
            current.sidecar_bytes,
            sqlite_bytes
        )
    );
    println!(
        "dictionary benefit for page frames: zstd payload {:+.3}%, estimated sidecar {:+.3}%",
        relative_percent(page_dictionary.zstd_bytes, page_plain.zstd_bytes),
        relative_percent(page_dictionary.sidecar_bytes, page_plain.sidecar_bytes)
    );
    Ok(())
}

fn parse_path() -> Result<Option<PathBuf>> {
    let arguments = std::env::args_os()
        .skip(1)
        .filter(|argument| argument != "--bench")
        .collect::<Vec<_>>();
    if arguments
        .iter()
        .any(|argument| argument == "--help" || argument == "-h")
    {
        println!("{}", usage());
        return Ok(None);
    }
    match arguments.as_slice() {
        [path] => Ok(Some(PathBuf::from(path))),
        _ => Err(usage().into()),
    }
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench page_dictionary -- DATABASE\n\
     \n\
     Compares independent 64 KiB Zstandard frames with one frame per SQLite\n\
     page, with and without one 64 KiB dictionary trained from up to 8192\n\
     evenly sampled database pages. Sidecar estimates use V6 frame, index,\n\
     full-map, dictionary-table, and segment-container overhead."
}

fn database_layout(path: &Path, sqlite_bytes: u64) -> Result<(usize, usize)> {
    let mut input = File::open(path)?;
    let mut header = [0_u8; 100];
    input.read_exact(&mut header)?;
    if header[..16] != SQLITE_MAGIC[..] {
        return Err(format!("{} is not an ordinary SQLite database", path.display()).into());
    }
    let encoded = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded == 1 {
        65_536
    } else {
        usize::from(encoded)
    };
    if !(512..=65_536).contains(&page_size)
        || !page_size.is_power_of_two()
        || sqlite_bytes == 0
        || !sqlite_bytes.is_multiple_of(page_size as u64)
    {
        return Err(format!("{} has an invalid SQLite layout", path.display()).into());
    }
    Ok((page_size, usize::try_from(sqlite_bytes / page_size as u64)?))
}

fn training_samples(
    path: &Path,
    page_size: usize,
    page_count: usize,
    maximum: usize,
) -> Result<Vec<Vec<u8>>> {
    let count = page_count.min(maximum);
    let mut input = File::open(path)?;
    let mut samples = Vec::with_capacity(count);
    for sample in 0..count {
        let page = if count <= 1 {
            0
        } else {
            sample
                .checked_mul(page_count - 1)
                .ok_or("sample offset overflow")?
                / (count - 1)
        };
        input.seek(SeekFrom::Start(u64::try_from(
            page.checked_mul(page_size)
                .ok_or("sample offset overflow")?,
        )?))?;
        let mut bytes = vec![0; page_size];
        input.read_exact(&mut bytes)?;
        samples.push(bytes);
    }
    Ok(samples)
}

fn analyze(
    path: &Path,
    sqlite_bytes: u64,
    page_size: usize,
    frame_bytes: usize,
    dictionary: &[u8],
) -> Result<Metrics> {
    let started = Instant::now();
    let pages_per_buffer = (INPUT_BUFFER_BYTES / page_size).max(1);
    let frame_bytes = frame_bytes.max(page_size);
    let page_count = sqlite_bytes / page_size as u64;
    let mut input = BufReader::with_capacity(INPUT_BUFFER_BYTES, File::open(path)?);
    let mut compressor = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, dictionary)?;
    let mut remaining_pages = page_count;
    let mut frame_count = 0_u64;
    let mut zstd_bytes = 0_u64;
    let mut frame_metadata_bytes = 0_u64;
    let mut raw_bytes = 0_u64;
    let mut stored_bytes = 0_u64;
    let mut raw = vec![0_u8; pages_per_buffer * page_size];

    while remaining_pages != 0 {
        let pages = remaining_pages.min(pages_per_buffer as u64);
        let raw_len = usize::try_from(pages)?
            .checked_mul(page_size)
            .ok_or("buffer size overflow")?;
        input.read_exact(&mut raw[..raw_len])?;
        for frame in raw[..raw_len].chunks(frame_bytes) {
            let compressed = compressor.compress(frame)?;
            frame_count = frame_count.checked_add(1).ok_or("frame count overflow")?;
            frame_metadata_bytes = frame_metadata_bytes
                .checked_add((FRAME_HEADER_SIZE + SEGMENT_INDEX_ENTRY_SIZE) as u64)
                .ok_or("metadata size overflow")?;
            if compressed.len().saturating_add(MIN_SAVINGS) < frame.len() {
                zstd_bytes = zstd_bytes
                    .checked_add(compressed.len() as u64)
                    .ok_or("compressed size overflow")?;
                stored_bytes = stored_bytes
                    .checked_add(compressed.len() as u64)
                    .ok_or("sidecar size overflow")?;
            } else {
                raw_bytes = raw_bytes
                    .checked_add(frame.len() as u64)
                    .ok_or("raw size overflow")?;
                stored_bytes = stored_bytes
                    .checked_add(frame.len() as u64)
                    .ok_or("sidecar size overflow")?;
            }
        }
        remaining_pages = remaining_pages
            .checked_sub(pages)
            .ok_or("remaining page count underflow")?;
    }

    let page_map_upper_bound = page_count
        .checked_mul(20)
        .and_then(|value| value.checked_add(8))
        .ok_or("page map size overflow")?;
    let dictionary_table_bytes = if dictionary.is_empty() {
        8
    } else {
        44_u64
            .checked_add(dictionary.len() as u64)
            .ok_or("dictionary table size overflow")?
    };
    let sidecar_bytes = (SEGMENT_HEADER_SIZE as u64)
        .checked_add(dictionary_table_bytes)
        .and_then(|bytes| bytes.checked_add(stored_bytes))
        .and_then(|bytes| bytes.checked_add(frame_metadata_bytes))
        .and_then(|bytes| bytes.checked_add(page_map_upper_bound))
        .and_then(|bytes| bytes.checked_add(160))
        .and_then(|bytes| bytes.checked_add(SEGMENT_TRAILER_SIZE as u64))
        .ok_or("sidecar size overflow")?;
    Ok(Metrics {
        frame_count,
        zstd_bytes,
        frame_metadata_bytes,
        raw_bytes,
        sidecar_bytes,
        elapsed: started.elapsed(),
    })
}

fn print_metrics(label: &str, metrics: Metrics, sqlite_bytes: u64) {
    println!(
        "{:>18} {:>10} {:>13} {:>11} {:>11} {:>13} {:>10.3}% {:>9.2?}",
        label,
        metrics.frame_count,
        format_bytes(metrics.zstd_bytes),
        format_bytes(metrics.frame_metadata_bytes),
        format_bytes(metrics.raw_bytes),
        format_bytes(metrics.sidecar_bytes),
        percent(metrics.sidecar_bytes, sqlite_bytes),
        metrics.elapsed
    );
}

fn reject_live_auxiliary(path: &Path) -> Result<()> {
    for suffix in ["-wal", "-journal"] {
        let auxiliary = append_suffix(path, suffix);
        if auxiliary
            .metadata()
            .is_ok_and(|metadata| metadata.len() != 0)
        {
            return Err(format!(
                "{} has live data in {}; checkpoint or snapshot it before benchmarking",
                path.display(),
                auxiliary.display()
            )
            .into());
        }
    }
    Ok(())
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value: OsString = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

#[allow(clippy::cast_precision_loss)]
fn format_bytes(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    const KIB: f64 = 1024.0;
    if bytes >= 1024 * 1024 {
        format!("{:.2} MiB", bytes as f64 / MIB)
    } else if bytes >= 1024 {
        format!("{:.2} KiB", bytes as f64 / KIB)
    } else {
        format!("{bytes} B")
    }
}

#[allow(clippy::cast_precision_loss)]
fn percent(value: u64, total: u64) -> f64 {
    value as f64 * 100.0 / total as f64
}

#[allow(clippy::cast_precision_loss)]
fn relative_percent(value: u64, baseline: u64) -> f64 {
    (value as f64 / baseline as f64 - 1.0) * 100.0
}

#[allow(clippy::cast_precision_loss)]
fn signed_percent(value: u64, baseline: u64, total: u64) -> f64 {
    (value as f64 - baseline as f64) * 100.0 / total as f64
}
