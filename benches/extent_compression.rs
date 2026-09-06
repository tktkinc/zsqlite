use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use zsqlite::format::{
    COMMIT_SIZE, EXTENT_HEADER_SIZE, HEADER_SIZE, INDEX_HEADER_SIZE, SECTOR_SIZE,
};

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const LAYOUTS: [(usize, usize); 3] = [
    (1024 * 1024, 1024 * 1024),
    (1024 * 1024, 64 * 1024),
    (64 * 1024, 64 * 1024),
];

// Keep the store-specific constants synchronized with src/store.rs. The
// benchmark models compact_locked followed by checkpoint_index.
const INDEX_ENTRY_SIZE: usize = 24;
const ZSTD_LEVEL: i32 = 3;
const MIN_SAVINGS: usize = 64;
const DIGEST_METADATA_FIXED_SIZE: usize = 52;
const SEEK_TABLE_FIXED_SIZE: usize = 25;
const PER_FRAME_METADATA_SIZE: usize = 40;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, Default)]
struct Metrics {
    target_bytes: usize,
    seek_bytes: usize,
    extent_count: u64,
    frame_count: u64,
    stored_payload_bytes: u64,
    extent_allocation_bytes: u64,
    index_raw_bytes: u64,
    index_stored_bytes: u64,
    estimated_sidecar_bytes: u64,
    elapsed: Duration,
}

#[derive(Debug)]
struct DatabaseResult {
    path: PathBuf,
    sqlite_bytes: u64,
    page_size: usize,
    metrics: [Metrics; 3],
}

fn main() {
    if let Err(error) = run() {
        eprintln!("extent_compression: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let Some(paths) = parse_paths() else {
        return Ok(());
    };
    if paths.is_empty() {
        return Err(usage().into());
    }

    let mut results = Vec::with_capacity(paths.len());
    for path in paths {
        let result = analyze_database(&path)?;
        print_database(&result);
        results.push(result);
    }
    if results.len() > 1 {
        print_total(&results);
    }
    Ok(())
}

fn parse_paths() -> Option<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for argument in std::env::args_os().skip(1) {
        if argument == "--bench" {
            continue;
        }
        if argument == "--help" || argument == "-h" {
            println!("{}", usage());
            return None;
        }
        paths.push(PathBuf::from(argument));
    }
    Some(paths)
}

fn usage() -> &'static str {
    "Usage: cargo bench --bench extent_compression -- DATABASE [DATABASE ...]\n\
     \n\
     Compares 1 MiB/1 MiB, 1 MiB/64 KiB, and 64 KiB/64 KiB extent/seek-frame\n\
     layouts using the production Zstandard level, seek-table and strong-digest\n\
     overhead, packed extent records, and persistent-index encoding. Inputs must\n\
     be closed ordinary SQLite databases. Files with a nonempty -wal or\n\
     -journal companion are rejected."
}

fn analyze_database(path: &Path) -> Result<DatabaseResult> {
    reject_live_auxiliary(path)?;
    let sqlite_bytes = path.metadata()?.len();
    let mut input = File::open(path)?;
    let mut header = [0_u8; 100];
    input.read_exact(&mut header)?;
    if header[..16] != SQLITE_MAGIC[..] {
        return Err(format!("{} is not an ordinary SQLite database", path.display()).into());
    }
    let page_size = sqlite_page_size(&header)
        .ok_or_else(|| format!("{} has an invalid SQLite page size", path.display()))?;
    if sqlite_bytes == 0 || !sqlite_bytes.is_multiple_of(page_size as u64) {
        return Err(format!(
            "{} is not a whole number of {}-byte SQLite pages",
            path.display(),
            page_size
        )
        .into());
    }

    let metrics = [
        analyze_layout(path, sqlite_bytes, page_size, LAYOUTS[0])?,
        analyze_layout(path, sqlite_bytes, page_size, LAYOUTS[1])?,
        analyze_layout(path, sqlite_bytes, page_size, LAYOUTS[2])?,
    ];
    Ok(DatabaseResult {
        path: path.to_path_buf(),
        sqlite_bytes,
        page_size,
        metrics,
    })
}

fn sqlite_page_size(header: &[u8; 100]) -> Option<usize> {
    let encoded = u16::from_be_bytes([header[16], header[17]]);
    let page_size = if encoded == 1 {
        65_536
    } else {
        usize::from(encoded)
    };
    ((512..=65_536).contains(&page_size) && page_size.is_power_of_two()).then_some(page_size)
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

fn analyze_layout(
    path: &Path,
    sqlite_bytes: u64,
    page_size: usize,
    layout: (usize, usize),
) -> Result<Metrics> {
    let started = Instant::now();
    let (target_bytes, seek_bytes) = layout;
    let pages_per_extent = (target_bytes / page_size).max(1);
    let pages_per_frame = (seek_bytes / page_size).max(1);
    let page_count = sqlite_bytes / page_size as u64;
    let extent_count = page_count.div_ceil(pages_per_extent as u64);
    let index_capacity = usize::try_from(extent_count)?
        .checked_mul(INDEX_ENTRY_SIZE)
        .ok_or("index size overflow")?;
    let mut index = Vec::with_capacity(index_capacity);
    let input = File::open(path)?;
    let mut input = BufReader::with_capacity(1024 * 1024, input);
    let mut cursor = HEADER_SIZE as u64;
    let mut remaining_pages = page_count;
    let mut first_page = 1_u64;
    let mut extent_total = 0_u64;
    let mut frame_count = 0_u64;
    let mut stored_payload_bytes = 0_u64;
    let mut extent_allocation_bytes = 0_u64;
    let mut raw = vec![0_u8; pages_per_extent * page_size];

    while remaining_pages != 0 {
        let pages = remaining_pages.min(pages_per_extent as u64);
        let raw_len = usize::try_from(pages)?
            .checked_mul(page_size)
            .ok_or("extent size overflow")?;
        input.read_exact(&mut raw[..raw_len])?;
        let frame_bytes = pages_per_frame
            .checked_mul(page_size)
            .ok_or("seek frame size overflow")?;
        let mut compressed_bytes = 0_usize;
        let mut frames = 0_usize;
        for frame in raw[..raw_len].chunks(frame_bytes) {
            compressed_bytes = compressed_bytes
                .checked_add(zstd::bulk::compress(frame, ZSTD_LEVEL)?.len())
                .ok_or("compressed size overflow")?;
            frames += 1;
        }
        let stored_len = compressed_bytes
            .checked_add(DIGEST_METADATA_FIXED_SIZE)
            .and_then(|size| size.checked_add(SEEK_TABLE_FIXED_SIZE))
            .and_then(|size| size.checked_add(frames.checked_mul(PER_FRAME_METADATA_SIZE)?))
            .ok_or("seek metadata size overflow")?;
        let allocation = (EXTENT_HEADER_SIZE as u64)
            .checked_add(stored_len as u64)
            .ok_or("sidecar size overflow")?;

        index.extend_from_slice(&u32::try_from(first_page)?.to_le_bytes());
        index.extend_from_slice(&u32::try_from(pages)?.to_le_bytes());
        index.extend_from_slice(&cursor.to_le_bytes());
        index.extend_from_slice(&0_u32.to_le_bytes());
        index.extend_from_slice(&0_u32.to_le_bytes());

        cursor = cursor
            .checked_add(allocation)
            .ok_or("sidecar size overflow")?;
        remaining_pages -= pages;
        first_page += pages;
        extent_total += 1;
        frame_count += u64::try_from(frames)?;
        stored_payload_bytes += stored_len as u64;
        extent_allocation_bytes += allocation;
    }
    debug_assert_eq!(extent_total, extent_count);
    debug_assert_eq!(index.len(), index_capacity);

    cursor = cursor
        .checked_add(COMMIT_SIZE as u64)
        .ok_or("sidecar size overflow")?;
    let index_offset = align_up(cursor, SECTOR_SIZE as u64)?;
    let compressed_index = zstd::bulk::compress(&index, ZSTD_LEVEL)?;
    let index_stored_bytes = if compressed_index.len().saturating_add(MIN_SAVINGS) <= index.len() {
        compressed_index.len()
    } else {
        index.len()
    };
    let estimated_sidecar_bytes = index_offset
        .checked_add(INDEX_HEADER_SIZE as u64)
        .and_then(|value| value.checked_add(index_stored_bytes as u64))
        .ok_or("sidecar size overflow")?;

    Ok(Metrics {
        target_bytes,
        seek_bytes,
        extent_count,
        frame_count,
        stored_payload_bytes,
        extent_allocation_bytes,
        index_raw_bytes: index.len() as u64,
        index_stored_bytes: index_stored_bytes as u64,
        estimated_sidecar_bytes,
        elapsed: started.elapsed(),
    })
}

fn align_up(value: u64, alignment: u64) -> Result<u64> {
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or_else(|| "alignment overflow".into())
    }
}

fn print_database(result: &DatabaseResult) {
    println!(
        "\n{} ({}, {} KiB SQLite pages)",
        result.path.display(),
        format_bytes(result.sqlite_bytes),
        result.page_size / 1024
    );
    println!(
        "{:>15} {:>8} {:>9} {:>13} {:>11} {:>13} {:>11} {:>9}",
        "extent/seek",
        "extents",
        "frames",
        "payload",
        "payload/db",
        "sidecar est.",
        "sidecar/db",
        "time"
    );
    for metrics in result.metrics {
        println!(
            "{:>6}/{:<6} KiB {:>8} {:>9} {:>13} {:>10.3}% {:>13} {:>10.3}% {:>8.2?}",
            metrics.target_bytes / 1024,
            metrics.seek_bytes / 1024,
            metrics.extent_count,
            metrics.frame_count,
            format_bytes(metrics.stored_payload_bytes),
            percent(metrics.stored_payload_bytes, result.sqlite_bytes),
            format_bytes(metrics.estimated_sidecar_bytes),
            percent(metrics.estimated_sidecar_bytes, result.sqlite_bytes),
            metrics.elapsed
        );
    }
    print_delta(result.sqlite_bytes, &result.metrics);
}

fn print_delta(sqlite_bytes: u64, metrics: &[Metrics; 3]) {
    let one_frame = metrics[0];
    let seekable = metrics[1];
    let small_extents = metrics[2];
    println!(
        "1 MiB/64 KiB vs 1 MiB/1 MiB: payload {:+.3}% of DB ({:+.3}% relative); sidecar {:+.3}% of DB ({:+.3}% relative)",
        signed_percent(
            seekable.stored_payload_bytes,
            one_frame.stored_payload_bytes,
            sqlite_bytes
        ),
        relative_percent(
            seekable.stored_payload_bytes,
            one_frame.stored_payload_bytes
        ),
        signed_percent(
            seekable.estimated_sidecar_bytes,
            one_frame.estimated_sidecar_bytes,
            sqlite_bytes
        ),
        relative_percent(
            seekable.estimated_sidecar_bytes,
            one_frame.estimated_sidecar_bytes
        )
    );
    println!(
        "1 MiB/64 KiB vs 64 KiB/64 KiB: payload {:+.3}% relative; sidecar {:+.3}% relative",
        relative_percent(
            seekable.stored_payload_bytes,
            small_extents.stored_payload_bytes
        ),
        relative_percent(
            seekable.estimated_sidecar_bytes,
            small_extents.estimated_sidecar_bytes
        )
    );
    println!(
        "extent headers: {} / {} / {}; indexes: {}/{} / {}/{} / {}/{} raw/stored",
        format_bytes(one_frame.extent_allocation_bytes - one_frame.stored_payload_bytes),
        format_bytes(seekable.extent_allocation_bytes - seekable.stored_payload_bytes),
        format_bytes(small_extents.extent_allocation_bytes - small_extents.stored_payload_bytes),
        format_bytes(one_frame.index_raw_bytes),
        format_bytes(one_frame.index_stored_bytes),
        format_bytes(seekable.index_raw_bytes),
        format_bytes(seekable.index_stored_bytes),
        format_bytes(small_extents.index_raw_bytes),
        format_bytes(small_extents.index_stored_bytes)
    );
}

fn print_total(results: &[DatabaseResult]) {
    let sqlite_bytes = results
        .iter()
        .map(|result| result.sqlite_bytes)
        .sum::<u64>();
    let mut metrics = [Metrics::default(); 3];
    for result in results {
        for (total, item) in metrics.iter_mut().zip(result.metrics) {
            total.target_bytes = item.target_bytes;
            total.seek_bytes = item.seek_bytes;
            total.extent_count += item.extent_count;
            total.frame_count += item.frame_count;
            total.stored_payload_bytes += item.stored_payload_bytes;
            total.extent_allocation_bytes += item.extent_allocation_bytes;
            total.index_raw_bytes += item.index_raw_bytes;
            total.index_stored_bytes += item.index_stored_bytes;
            total.estimated_sidecar_bytes += item.estimated_sidecar_bytes;
            total.elapsed += item.elapsed;
        }
    }
    println!(
        "\nTOTAL ({} databases, {})",
        results.len(),
        format_bytes(sqlite_bytes)
    );
    println!(
        "{:>15} {:>8} {:>9} {:>13} {:>11} {:>13} {:>11} {:>9}",
        "extent/seek",
        "extents",
        "frames",
        "payload",
        "payload/db",
        "sidecar est.",
        "sidecar/db",
        "time"
    );
    for item in metrics {
        println!(
            "{:>6}/{:<6} KiB {:>8} {:>9} {:>13} {:>10.3}% {:>13} {:>10.3}% {:>8.2?}",
            item.target_bytes / 1024,
            item.seek_bytes / 1024,
            item.extent_count,
            item.frame_count,
            format_bytes(item.stored_payload_bytes),
            percent(item.stored_payload_bytes, sqlite_bytes),
            format_bytes(item.estimated_sidecar_bytes),
            percent(item.estimated_sidecar_bytes, sqlite_bytes),
            item.elapsed
        );
    }
    print_delta(sqlite_bytes, &metrics);
}

#[allow(clippy::cast_precision_loss)]
fn percent(numerator: u64, denominator: u64) -> f64 {
    numerator as f64 * 100.0 / denominator as f64
}

#[allow(clippy::cast_precision_loss)]
fn relative_percent(value: u64, baseline: u64) -> f64 {
    (value as f64 / baseline as f64 - 1.0) * 100.0
}

#[allow(clippy::cast_precision_loss)]
fn signed_percent(value: u64, baseline: u64, denominator: u64) -> f64 {
    let difference = i128::from(value) - i128::from(baseline);
    difference as f64 * 100.0 / denominator as f64
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
