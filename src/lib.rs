//! Zstandard-compressed page storage and a `SQLite` VFS shim.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

pub mod format;
mod seekable;
mod store;

mod vfs;

pub use store::{CompressionConfig, Inspect, StoreError};

use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Returns metadata for an existing V3 zsqlite database without modifying it.
pub fn inspect(path: impl AsRef<Path>) -> Result<Inspect, StoreError> {
    store::Store::open_existing_read_only(path)?.inspect()
}

/// Verifies all live extents and the `SQLite` header of an existing database.
pub fn verify(path: impl AsRef<Path>) -> Result<Inspect, StoreError> {
    let path = absolute_path(path.as_ref())?;
    store::reject_auxiliary_files(&path)?;
    let mut store = store::Store::open_existing_read_only(&path)?;
    store.acquire_maintenance()?;
    store.verify()?;
    let result = store.inspect();
    store.release_maintenance();
    result
}

/// Compacts an existing database while holding the V3 lifecycle lock.
///
/// This fails with [`StoreError::Busy`] if any `SQLite` process has the database
/// open. Callers must checkpoint WAL and close every connection first.
pub fn compact(path: impl AsRef<Path>) -> Result<Inspect, StoreError> {
    compact_with_config(path, CompressionConfig::default())
}

/// Compacts an existing database using the requested sizes for newly written
/// extents and independently decompressible seek chunks.
pub fn compact_with_config(
    path: impl AsRef<Path>,
    config: CompressionConfig,
) -> Result<Inspect, StoreError> {
    let mut store = store::Store::open_with_config(path, false, config)?;
    store.compact()?;
    store.checkpoint_index()?;
    store.inspect()
}

/// Converts a closed, ordinary `SQLite` database into a distinct V3 zsqlite
/// database. The source is never modified and the destination must not exist.
pub fn convert_to_zsqlite(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<Inspect, StoreError> {
    convert_to_zsqlite_with_config(source, destination, CompressionConfig::default())
}

/// Converts a closed ordinary `SQLite` database with explicitly configured
/// extent and seek-chunk sizes.
pub fn convert_to_zsqlite_with_config(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    config: CompressionConfig,
) -> Result<Inspect, StoreError> {
    let source = absolute_path(source.as_ref())?;
    let destination = absolute_path(destination.as_ref())?;
    store::reject_auxiliary_files(&source)?;
    ensure_bundle_absent(&destination)?;

    let input = File::open(&source)?;
    let length = input.metadata()?.len();
    let mut sqlite_header = [0_u8; 100];
    read_exact_at(&input, 0, &mut sqlite_header)
        .map_err(|_| StoreError::InvalidStandardDatabase)?;
    let page_size = sqlite_page_size(&sqlite_header).ok_or(StoreError::InvalidStandardDatabase)?;
    if length == 0 || !length.is_multiple_of(u64::from(page_size)) {
        return Err(StoreError::InvalidStandardDatabase);
    }

    let staging = unused_staging_path(&destination, "convert")?;
    let mut staging_cleanup = CleanupPaths::new(bundle_paths(&staging).to_vec());
    config.validate_page_size(page_size)?;
    let mut converted = store::Store::open_with_config(&staging, true, config)?;
    let chunk_size = usize::try_from(config.extent_bytes()).map_err(|_| StoreError::Range)?;
    let mut buffer = vec![0_u8; chunk_size];
    let mut offset = 0_u64;
    while offset < length {
        let amount = usize::try_from((length - offset).min(chunk_size as u64))
            .map_err(|_| StoreError::Range)?;
        read_exact_at(&input, offset, &mut buffer[..amount])?;
        converted.write_at(offset, &buffer[..amount])?;
        converted.publish(true)?;
        offset = offset.checked_add(amount as u64).ok_or(StoreError::Range)?;
    }
    converted.checkpoint_index()?;
    converted.verify()?;
    drop(converted);
    let mut final_header = [0_u8; 100];
    read_exact_at(&input, 0, &mut final_header)?;
    if final_header != sqlite_header || input.metadata()?.len() != length {
        return Err(StoreError::Busy);
    }
    store::reject_auxiliary_files(&source)?;

    install_staged_bundle(&staging, &destination)?;
    staging_cleanup.disarm();
    let mut installed = store::Store::open_existing(&destination)?;
    installed.verify()?;
    installed.inspect()
}

/// Exports a closed V3 zsqlite database as a distinct ordinary `SQLite` file.
/// The output can be consumed by stock `SQLite` and tools such as Litestream.
pub fn export_to_sqlite(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<u64, StoreError> {
    let source = absolute_path(source.as_ref())?;
    let destination = absolute_path(destination.as_ref())?;
    store::reject_auxiliary_files(&source)?;
    if destination.exists() {
        return Err(StoreError::DestinationExists(destination));
    }
    let mut source_store = store::Store::open_existing_read_only(&source)?;
    source_store.acquire_maintenance()?;
    source_store.verify()?;
    store::reject_auxiliary_files(&source)?;

    let staging = unused_staging_path(&destination, "export")?;
    let mut cleanup = CleanupPaths::new(vec![staging.clone()]);
    let output = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&staging)?;
    source_store.copy_logical_to(&output)?;
    output.sync_all()?;
    let length = output.metadata()?.len();
    drop(output);
    std::fs::hard_link(&staging, &destination).map_err(|error| {
        if error.kind() == ErrorKind::AlreadyExists {
            StoreError::DestinationExists(destination.clone())
        } else {
            StoreError::Io(error)
        }
    })?;
    let mut destination_cleanup = CleanupPaths::new(vec![destination.clone()]);
    sync_parent_dir(&destination)?;
    std::fs::remove_file(&staging)?;
    cleanup.disarm();
    destination_cleanup.disarm();
    source_store.release_maintenance();
    Ok(length)
}

fn sqlite_page_size(header: &[u8; 100]) -> Option<u32> {
    if header[..16] != *SQLITE_MAGIC
        || !matches!(header[18], 1 | 2)
        || !matches!(header[19], 1 | 2)
        || header[21..24] != [64, 32, 32]
    {
        return None;
    }
    let encoded = u16::from_be_bytes(header[16..18].try_into().ok()?);
    let page_size = if encoded == 1 {
        65_536
    } else {
        u32::from(encoded)
    };
    format::valid_page_size(page_size).then_some(page_size)
}

fn install_staged_bundle(staging: &Path, destination: &Path) -> Result<(), StoreError> {
    ensure_bundle_absent(destination)?;
    let staging_paths = bundle_paths(staging);
    let destination_paths = bundle_paths(destination);
    let mut installed = CleanupPaths::new(Vec::new());
    // Install identity-bearing companions first and make their directory
    // entries durable. The anchor is linked last and makes the bundle visible.
    for index in 1..staging_paths.len() {
        link_no_replace(&staging_paths[index], &destination_paths[index])?;
        installed.paths.push(destination_paths[index].clone());
    }
    sync_parent_dir(destination)?;
    link_no_replace(&staging_paths[0], &destination_paths[0])?;
    installed.paths.push(destination_paths[0].clone());
    sync_parent_dir(destination)?;
    for path in staging_paths {
        std::fs::remove_file(path)?;
    }
    installed.disarm();
    Ok(())
}

fn link_no_replace(source: &Path, destination: &Path) -> Result<(), StoreError> {
    std::fs::hard_link(source, destination).map_err(|error| {
        if error.kind() == ErrorKind::AlreadyExists {
            StoreError::DestinationExists(destination.to_path_buf())
        } else {
            StoreError::Io(error)
        }
    })
}

fn ensure_bundle_absent(path: &Path) -> Result<(), StoreError> {
    for candidate in bundle_paths(path) {
        if candidate.exists() {
            return Err(StoreError::DestinationExists(candidate));
        }
    }
    Ok(())
}

fn bundle_paths(path: &Path) -> [PathBuf; 4] {
    [
        path.to_path_buf(),
        append_suffix(path, "-zsqlite"),
        append_suffix(path, "-zsqlite-lock"),
        append_suffix(path, "-zsqlite-publish"),
    ]
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn unused_staging_path(destination: &Path, purpose: &str) -> Result<PathBuf, StoreError> {
    for _ in 0..1024 {
        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = append_suffix(
            destination,
            &format!(".{purpose}.{}.{sequence}", std::process::id()),
        );
        if bundle_paths(&candidate).iter().all(|path| !path.exists()) {
            return Ok(candidate);
        }
    }
    Err(StoreError::Range)
}

fn absolute_path(path: &Path) -> Result<PathBuf, std::io::Error> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut offset: u64, mut output: &mut [u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !output.is_empty() {
        let amount = match file.read_at(output, offset) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            result => result?,
        };
        if amount == 0 {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }
        offset = offset
            .checked_add(amount as u64)
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        output = &mut output[amount..];
    }
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<(), StoreError> {
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
    Ok(())
}

struct CleanupPaths {
    paths: Vec<PathBuf>,
}

impl CleanupPaths {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }

    fn disarm(&mut self) {
        self.paths.clear();
    }
}

impl Drop for CleanupPaths {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(feature = "static")]
pub use vfs::register_static_vfs;

#[cfg(test)]
mod tests {
    use super::*;

    fn sqlite_page(fill: u8) -> Vec<u8> {
        let mut page = vec![fill; 4096];
        page[..16].copy_from_slice(SQLITE_MAGIC);
        page[16..18].copy_from_slice(&4096_u16.to_be_bytes());
        page
    }

    fn create_store(path: &Path) -> Result<(), StoreError> {
        let mut store = store::Store::open(path, true)?;
        store.write_at(0, &sqlite_page(b't'))?;
        store.publish(true)?;
        store.checkpoint_index()?;
        store.verify()
    }

    #[test]
    fn partial_conversion_install_is_invisible_and_retry_fails_without_overwrite()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let staging = directory.path().join("staging.db");
        let destination = directory.path().join("destination.db");
        create_store(&staging)?;
        let staging_paths = bundle_paths(&staging);
        let destination_paths = bundle_paths(&destination);

        // Model interruption after the three companions were linked and made
        // durable, but before the anchor was linked last.
        for index in 1..staging_paths.len() {
            std::fs::hard_link(&staging_paths[index], &destination_paths[index])?;
        }
        assert!(!destination_paths[0].exists());
        assert!(inspect(&destination).is_err());
        let before: Vec<Vec<u8>> = destination_paths[1..]
            .iter()
            .map(std::fs::read)
            .collect::<Result<_, _>>()?;

        assert!(matches!(
            install_staged_bundle(&staging, &destination),
            Err(StoreError::DestinationExists(_))
        ));
        assert!(!destination_paths[0].exists());
        for (path, expected) in destination_paths[1..].iter().zip(before) {
            assert_eq!(std::fs::read(path)?, expected);
        }
        assert!(staging_paths.iter().all(|path| path.exists()));
        Ok(())
    }

    #[test]
    fn nonempty_sqlite_auxiliary_files_block_offline_maintenance()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("auxiliary.db");
        create_store(&path)?;
        for suffix in ["-journal", "-wal", "-shm"] {
            let auxiliary = append_suffix(&path, suffix);
            std::fs::write(&auxiliary, b"possibly live")?;
            assert!(matches!(verify(&path), Err(StoreError::Busy)), "{suffix}");
            assert_eq!(std::fs::read(&auxiliary)?, b"possibly live");
            std::fs::remove_file(auxiliary)?;
        }
        verify(&path)?;
        Ok(())
    }

    #[test]
    fn empty_sqlite_auxiliary_files_do_not_block_offline_maintenance()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("stale-empty-auxiliary.db");
        create_store(&path)?;
        for suffix in ["-journal", "-wal", "-shm"] {
            let auxiliary = append_suffix(&path, suffix);
            File::create(&auxiliary)?;
            verify(&path)?;
            assert_eq!(std::fs::metadata(&auxiliary)?.len(), 0);
            std::fs::remove_file(auxiliary)?;
        }
        Ok(())
    }
}
