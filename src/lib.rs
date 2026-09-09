//! Zstandard-compressed transactional page segments and a `SQLite` VFS shim.

#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

mod backend;
pub mod format;
mod fs;
mod segment_codec;
mod store;
mod vfs;

pub use store::{DictionaryPolicy, Inspect, StoragePolicy, StoreError};

use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::fs::{absolute_path, read_exact_at, sync_parent_dir};

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Returns metadata for an existing V6 `.zsqlite` database without modifying it.
pub fn inspect(path: impl AsRef<Path>) -> Result<Inspect, StoreError> {
    store::Store::open_existing_read_only(path)?.inspect()
}

/// Verifies every referenced segment, page frame, and the `SQLite` header.
pub fn verify(path: impl AsRef<Path>) -> Result<Inspect, StoreError> {
    let path = absolute_path(path.as_ref())?;
    store::reject_auxiliary_files(&path)?;
    let mut database = store::Store::open_existing_read_only(&path)?;
    database.verify()?;
    database.inspect()
}

/// Seals the current active segment, if any.
pub fn flush(path: impl AsRef<Path>) -> Result<Inspect, StoreError> {
    let mut database = store::Store::open_existing(path)?;
    database.acquire_maintenance()?;
    let result = database.flush_sidecars();
    database.release_maintenance();
    result?;
    database.inspect()
}

/// Replaces all sealed segments with one endpoint-equivalent checkpoint segment.
pub fn compact(path: impl AsRef<Path>) -> Result<Inspect, StoreError> {
    let mut database = store::Store::open_existing(path)?;
    database.compact()?;
    database.inspect()
}

/// Replaces the persisted maintenance and adaptive dictionary policy.
pub fn configure(path: impl AsRef<Path>, policy: StoragePolicy) -> Result<Inspect, StoreError> {
    let mut database = store::Store::open_existing(path)?;
    database.acquire_maintenance()?;
    let result = database.set_storage_policy(policy);
    database.release_maintenance();
    result?;
    database.inspect()
}

/// Converts a closed ordinary `SQLite` database into a distinct V6 zsqlite bundle.
pub fn convert_to_zsqlite(
    source: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<Inspect, StoreError> {
    let source = absolute_path(source.as_ref())?;
    let destination = absolute_path(destination.as_ref())?;
    store::reject_auxiliary_files(&source)?;
    ensure_bundle_absent(&destination)?;

    let input = File::open(&source)?;
    let length = input.metadata()?.len();
    let mut sqlite_header = [0; 100];
    read_exact_at(&input, 0, &mut sqlite_header)
        .map_err(|_| StoreError::InvalidStandardDatabase)?;
    let page_size = sqlite_page_size(&sqlite_header).ok_or(StoreError::InvalidStandardDatabase)?;
    if length == 0 || !length.is_multiple_of(u64::from(page_size)) {
        return Err(StoreError::InvalidStandardDatabase);
    }

    let staging = unused_staging_path(&destination, "convert")?;
    let mut cleanup = CleanupPaths::new(bundle_paths(&staging).to_vec());
    let mut converted = store::Store::open(&staging, true)?;
    if let Some(dictionary) = conversion_dictionary(&input, length, page_size)? {
        converted.install_initial_dictionary(dictionary)?;
    }
    let mut page = vec![0; page_size as usize];
    let mut offset = 0_u64;
    while offset < length {
        read_exact_at(&input, offset, &mut page)?;
        converted.write_at(offset, &page)?;
        offset = offset
            .checked_add(u64::from(page_size))
            .ok_or(StoreError::Range)?;
    }
    converted.publish(true)?;
    converted.flush_sidecars()?;
    converted.verify()?;
    drop(converted);

    let mut final_header = [0; 100];
    read_exact_at(&input, 0, &mut final_header)?;
    if final_header != sqlite_header || input.metadata()?.len() != length {
        return Err(StoreError::Busy);
    }
    store::reject_auxiliary_files(&source)?;
    install_staged_bundle(&staging, &destination)?;
    cleanup.disarm();
    let mut installed = store::Store::open_existing(&destination)?;
    installed.verify()?;
    installed.inspect()
}

fn conversion_dictionary(
    input: &File,
    length: u64,
    page_size: u32,
) -> Result<Option<Vec<u8>>, StoreError> {
    const MAX_SAMPLES: usize = 8192;
    const SAMPLE_BYTES: usize = 32 * 1024 * 1024;
    const DICTIONARY_BYTES: usize = 64 * 1024;
    const MIN_PAGES: usize = 256;
    let page_count =
        usize::try_from(length / u64::from(page_size)).map_err(|_| StoreError::Range)?;
    let sample_count = page_count
        .min(MAX_SAMPLES)
        .min(SAMPLE_BYTES / page_size as usize);
    if sample_count < MIN_PAGES || sample_count.saturating_mul(page_size as usize) < 1024 * 1024 {
        return Ok(None);
    }
    let mut samples = Vec::with_capacity(sample_count);
    for sample in 0..sample_count {
        let page = sample
            .checked_mul(page_count.saturating_sub(1))
            .ok_or(StoreError::Range)?
            / sample_count.saturating_sub(1).max(1);
        let mut bytes = vec![0; page_size as usize];
        read_exact_at(
            input,
            u64::try_from(page)
                .map_err(|_| StoreError::Range)?
                .checked_mul(u64::from(page_size))
                .ok_or(StoreError::Range)?,
            &mut bytes,
        )?;
        samples.push(bytes);
    }
    let split = samples.len() * 4 / 5;
    let training = samples[..split]
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    let dictionary = zstd::dict::from_samples(&training, DICTIONARY_BYTES)
        .map_err(|error| StoreError::Zstd(error.to_string()))?;
    let mut compressor = zstd::bulk::Compressor::with_dictionary(3, &dictionary)
        .map_err(|error| StoreError::Zstd(error.to_string()))?;
    let raw_bytes = samples[split..].iter().map(Vec::len).sum::<usize>();
    let compressed_bytes = samples[split..].iter().try_fold(0_usize, |total, page| {
        let encoded = compressor
            .compress(page)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        total.checked_add(encoded.len()).ok_or(StoreError::Range)
    })?;
    let savings = raw_bytes.saturating_sub(compressed_bytes);
    if savings > dictionary.len() && savings.saturating_mul(10_000) >= raw_bytes.saturating_mul(500)
    {
        Ok(Some(dictionary))
    } else {
        Ok(None)
    }
}

/// Exports a V6 zsqlite database as an ordinary `SQLite` file.
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
    source_store.verify()?;

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
    sync_parent_dir(&destination)?;

    // The destination link is now the committed result. Staging cleanup is
    // post-commit housekeeping and must not turn success into an ambiguous
    // error while leaving the complete destination in place.
    cleanup.disarm();
    if std::fs::remove_file(&staging).is_ok() {
        let _ = sync_parent_dir(&destination);
    }
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
    let file_staging = unused_staging_path(destination, "active")?;
    let mut file_cleanup = CleanupPaths::new(vec![file_staging.clone()]);
    let mut installed = CleanupPaths::new(Vec::new());

    // Build a separately allocated, durable copy before publishing its name.
    // A direct hard link from the staging bundle would leave the installed
    // database aliased if the process died before staging cleanup. Store opens
    // reject aliased mutable active files because two names can otherwise
    // acquire different sidecar locks for the same inode.
    let mut source_file = File::open(&staging_paths[0])?;
    let expected = source_file.metadata()?.len();
    let mut copied_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&file_staging)?;
    let copied = std::io::copy(&mut source_file, &mut copied_file)?;
    if copied != expected {
        return Err(std::io::Error::from(ErrorKind::UnexpectedEof).into());
    }
    copied_file.sync_all()?;
    drop(copied_file);

    // Publish the complete recognizable active file first. Until the sidecar
    // arrives, a racing VFS open fails closed; it cannot create a new database
    // inside a sidecar directory that this installer is about to replace.
    std::fs::hard_link(&file_staging, &destination_paths[0]).map_err(|error| {
        if error.kind() == ErrorKind::AlreadyExists {
            StoreError::DestinationExists(destination.to_path_buf())
        } else {
            StoreError::Io(error)
        }
    })?;
    installed.paths.push(destination_paths[0].clone());
    sync_parent_dir(destination)?;

    // Remove the publication helper before making the sidecar visible. Thus a
    // complete installed bundle never has an aliased active file, even if the
    // process dies before the original conversion staging bundle is cleaned.
    std::fs::remove_file(&file_staging)?;
    file_cleanup.disarm();
    sync_parent_dir(destination)?;

    std::fs::rename(&staging_paths[1], &destination_paths[1])?;
    installed.paths.push(destination_paths[1].clone());

    // Both components are now visible and a racing process may have opened
    // the bundle. Never tear this pair back down through the cleanup guard,
    // even if the directory sync reports an error: that outcome is ambiguous,
    // but deleting beneath the racing opener could lose later writes.
    installed.disarm();
    sync_parent_dir(destination)?;

    // The destination became durable at the preceding directory sync. A
    // failure to remove the now-unreferenced staging file must not turn a
    // successful install into an ambiguous error or tear the destination back
    // down through `installed`'s cleanup guard.
    if std::fs::remove_file(&staging_paths[0]).is_ok() {
        let _ = sync_parent_dir(destination);
    }
    Ok(())
}

fn ensure_bundle_absent(path: &Path) -> Result<(), StoreError> {
    for candidate in bundle_paths(path) {
        if candidate.exists() {
            return Err(StoreError::DestinationExists(candidate));
        }
    }
    Ok(())
}

fn bundle_paths(path: &Path) -> [PathBuf; 2] {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(".d");
    [path.to_path_buf(), PathBuf::from(sidecar)]
}

fn unused_staging_path(destination: &Path, purpose: &str) -> Result<PathBuf, StoreError> {
    for _ in 0..1024 {
        let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let stem = destination
            .file_stem()
            .and_then(|value| value.to_str())
            .unwrap_or("database");
        let candidate = destination.with_file_name(format!(
            ".{stem}.{purpose}.{}.{sequence}.zsqlite",
            std::process::id()
        ));
        if bundle_paths(&candidate).iter().all(|path| !path.exists()) {
            return Ok(candidate);
        }
    }
    Err(StoreError::Range)
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
        // Bundle paths are registered active file first and sidecar second.
        // Remove in reverse so a recognizable file remains as a fail-closed guard
        // until its associated storage is gone.
        for path in self.paths.iter().rev() {
            if path.is_dir() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
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
        page[18] = 1;
        page[19] = 1;
        page[21..24].copy_from_slice(&[64, 32, 32]);
        page
    }

    #[test]
    fn conversion_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source.db");
        let destination = directory.path().join("destination.zsqlite");
        let output = directory.path().join("output.db");
        std::fs::write(&source, sqlite_page(4))?;
        convert_to_zsqlite(&source, &destination)?;
        export_to_sqlite(&destination, &output)?;
        assert_eq!(std::fs::read(source)?, std::fs::read(output)?);
        Ok(())
    }
}
