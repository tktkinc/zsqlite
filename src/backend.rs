//! Immutable segment and root-catalog storage.
//!
//! The filesystem implementation exposes the range-oriented operations a
//! future object-store backend will need. Mutable active segments remain local.

use crate::format::{ActiveId, DatabaseId, Digest, MAX_SECTION_BYTES, RootCatalog, SegmentId, hex};
use crate::store::StoreError;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(crate) trait SegmentBackend: Send + Sync {
    fn put_segment(&self, id: &SegmentId, source: &Path) -> Result<(), StoreError>;
    fn read_segment_range(
        &self,
        id: &SegmentId,
        offset: u64,
        output: &mut [u8],
    ) -> Result<(), StoreError>;
    fn segment_len(&self, id: &SegmentId) -> Result<u64, StoreError>;
    fn list_segments(&self) -> Result<Vec<SegmentId>, StoreError>;
    fn delete_segment(&self, id: &SegmentId) -> Result<(), StoreError>;

    fn put_catalog(&self, catalog: &RootCatalog, bytes: &[u8]) -> Result<Digest, StoreError>;
    fn read_catalog(&self, digest: Digest) -> Result<Vec<u8>, StoreError>;
    fn list_catalogs(&self) -> Result<Vec<Digest>, StoreError>;
    fn delete_catalog(&self, digest: Digest) -> Result<(), StoreError>;
}

#[derive(Debug)]
pub(crate) struct FsSegmentBackend {
    root: PathBuf,
    active: PathBuf,
    segments: PathBuf,
    roots: PathBuf,
    locks: PathBuf,
    database_id: DatabaseId,
}

impl FsSegmentBackend {
    pub(crate) fn open(
        root: PathBuf,
        database_id: DatabaseId,
        create: bool,
    ) -> Result<Self, StoreError> {
        let output = Self {
            active: root.join("active"),
            segments: root.join("segments"),
            roots: root.join("roots"),
            locks: root.join("locks"),
            root,
            database_id,
        };
        if create {
            std::fs::create_dir_all(&output.active)?;
            std::fs::create_dir_all(&output.segments)?;
            std::fs::create_dir_all(&output.roots)?;
            std::fs::create_dir_all(&output.locks)?;
            sync_dir(&output.root)?;
            sync_parent(&output.root)?;
        } else if !output.active.is_dir()
            || !output.segments.is_dir()
            || !output.roots.is_dir()
            || !output.locks.is_dir()
        {
            return Err(StoreError::MissingSidecar);
        }
        Ok(output)
    }

    pub(crate) fn lock_path(&self, kind: &str) -> PathBuf {
        self.locks.join(format!("{kind}.lock"))
    }

    pub(crate) fn active_path(&self, id: ActiveId) -> PathBuf {
        self.active.join(format!("{}.zactive", hex_active(id)))
    }

    pub(crate) fn active_dir(&self) -> &Path {
        &self.active
    }

    pub(crate) fn segment_path(&self, id: &SegmentId) -> PathBuf {
        self.segments.join(id.filename())
    }

    pub(crate) fn catalog_path(&self, catalog: &RootCatalog, digest: Digest) -> PathBuf {
        self.roots.join(format!(
            "{:016x}-{}-{}.zroot",
            catalog.head_txid,
            hex(&catalog.head_history),
            hex(&digest)
        ))
    }

    fn temporary_path(destination: &Path) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name = destination
            .file_name()
            .unwrap_or_default()
            .to_string_lossy();
        destination.with_file_name(format!(".{name}.tmp.{}.{sequence}", std::process::id()))
    }

    fn persist_bytes(destination: &Path, bytes: &[u8]) -> Result<(), StoreError> {
        if destination.exists() {
            if std::fs::read(destination)? == bytes {
                return Ok(());
            }
            return Err(StoreError::Corrupt(0));
        }
        let temporary = Self::temporary_path(destination);
        let mut cleanup = CleanupPath(Some(temporary.clone()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        match std::fs::rename(&temporary, destination) {
            Ok(()) => cleanup.0 = None,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                if std::fs::read(destination)? != bytes {
                    return Err(StoreError::Corrupt(0));
                }
            }
            Err(error) => return Err(error.into()),
        }
        sync_parent(destination)?;
        Ok(())
    }
}

impl SegmentBackend for FsSegmentBackend {
    fn put_segment(&self, id: &SegmentId, source: &Path) -> Result<(), StoreError> {
        let destination = self.segment_path(id);
        if destination.exists() {
            if files_equal(&destination, source)? {
                return Ok(());
            }
            return Err(StoreError::Corrupt(0));
        }
        match std::fs::hard_link(source, &destination) {
            Ok(()) => sync_dir(&self.segments)?,
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                if !files_equal(&destination, source)? {
                    return Err(StoreError::Corrupt(0));
                }
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn read_segment_range(
        &self,
        id: &SegmentId,
        offset: u64,
        output: &mut [u8],
    ) -> Result<(), StoreError> {
        let file = File::open(self.segment_path(id))?;
        read_exact_at(&file, offset, output)?;
        Ok(())
    }

    fn segment_len(&self, id: &SegmentId) -> Result<u64, StoreError> {
        Ok(self.segment_path(id).metadata()?.len())
    }

    fn list_segments(&self) -> Result<Vec<SegmentId>, StoreError> {
        let mut output = Vec::new();
        for entry in std::fs::read_dir(&self.segments)? {
            let name = entry?.file_name();
            if let Some(name) = name.to_str()
                && let Ok(id) = SegmentId::parse_filename(name)
            {
                output.push(id);
            }
        }
        output.sort();
        Ok(output)
    }

    fn delete_segment(&self, id: &SegmentId) -> Result<(), StoreError> {
        match std::fs::remove_file(self.segment_path(id)) {
            Ok(()) => sync_dir(&self.segments)?,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn put_catalog(&self, catalog: &RootCatalog, bytes: &[u8]) -> Result<Digest, StoreError> {
        if catalog.database_id != self.database_id {
            return Err(StoreError::IdentityMismatch);
        }
        let id = crate::format::digest(bytes);
        Self::persist_bytes(&self.catalog_path(catalog, id), bytes)?;
        Ok(id)
    }

    fn read_catalog(&self, digest: Digest) -> Result<Vec<u8>, StoreError> {
        let suffix = format!("-{}.zroot", hex(&digest));
        let path = std::fs::read_dir(&self.roots)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(&suffix))
            })
            .ok_or(StoreError::MissingSidecar)?;
        if path.metadata()?.len() > MAX_SECTION_BYTES {
            return Err(StoreError::Range);
        }
        let bytes = std::fs::read(path)?;
        if crate::format::digest(&bytes) != digest {
            return Err(StoreError::Corrupt(0));
        }
        Ok(bytes)
    }

    fn list_catalogs(&self) -> Result<Vec<Digest>, StoreError> {
        let mut output = Vec::new();
        for entry in std::fs::read_dir(&self.roots)? {
            let name = entry?.file_name();
            let Some(name) = name.to_str().and_then(|value| value.strip_suffix(".zroot")) else {
                continue;
            };
            let Some(encoded) = name.rsplit('-').next() else {
                continue;
            };
            if let Ok(digest) = crate::format::parse_hex_digest(encoded) {
                output.push(digest);
            }
        }
        Ok(output)
    }

    fn delete_catalog(&self, digest: Digest) -> Result<(), StoreError> {
        let suffix = format!("-{}.zroot", hex(&digest));
        for entry in std::fs::read_dir(&self.roots)? {
            let path = entry?.path();
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(&suffix))
            {
                std::fs::remove_file(path)?;
                sync_dir(&self.roots)?;
                break;
            }
        }
        Ok(())
    }
}

fn files_equal(left: &Path, right: &Path) -> Result<bool, StoreError> {
    if left.metadata()?.len() != right.metadata()?.len() {
        return Ok(false);
    }
    let mut left = File::open(left)?;
    let mut right = File::open(right)?;
    let mut left_buffer = vec![0; 64 * 1024];
    let mut right_buffer = vec![0; 64 * 1024];
    loop {
        let left_amount = left.read(&mut left_buffer)?;
        let right_amount = right.read(&mut right_buffer)?;
        if left_amount != right_amount || left_buffer[..left_amount] != right_buffer[..right_amount]
        {
            return Ok(false);
        }
        if left_amount == 0 {
            return Ok(true);
        }
    }
}

pub(crate) fn sidecar_dir(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".d");
    PathBuf::from(value)
}

fn hex_active(value: ActiveId) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(32);
    for byte in value {
        output.push(TABLE[(byte >> 4) as usize] as char);
        output.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    output
}

fn sync_dir(path: &Path) -> Result<(), StoreError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_parent(path: &Path) -> Result<(), StoreError> {
    let parent = path.parent().ok_or(StoreError::Range)?;
    sync_dir(parent)
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

#[cfg(not(unix))]
fn read_exact_at(_file: &File, _offset: u64, _output: &mut [u8]) -> std::io::Result<()> {
    Err(std::io::Error::from(ErrorKind::Unsupported))
}

struct CleanupPath(Option<PathBuf>);

impl Drop for CleanupPath {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filesystem_backend_lists_lexical_segments() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let backend = FsSegmentBackend::open(directory.path().join("db.d"), [1; 32], true)?;
        let source = directory.path().join("active");
        std::fs::write(&source, b"segment")?;
        let a = SegmentId {
            start_txid: 1,
            end_txid: 3,
            end_history: [2; 32],
            physical_digest: [3; 32],
        };
        let b = SegmentId {
            start_txid: 4,
            end_txid: 8,
            end_history: [4; 32],
            physical_digest: [5; 32],
        };
        backend.put_segment(&b, &source)?;
        backend.put_segment(&a, &source)?;
        assert_eq!(backend.list_segments()?, vec![a, b]);
        Ok(())
    }
}
