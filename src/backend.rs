//! Immutable segment storage.
//!
//! The filesystem implementation exposes the range-oriented operations a
//! future object-store backend will need. The mutable active segment is the
//! `.zsqlite` file itself.

use crate::format::SegmentId;
use crate::fs::{sync_dir, sync_parent_dir};
use crate::store::StoreError;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct FsSegmentBackend {
    root: PathBuf,
    segments: PathBuf,
    locks: PathBuf,
}

impl FsSegmentBackend {
    pub(crate) fn open(root: PathBuf, create: bool) -> Result<Self, StoreError> {
        let output = Self {
            segments: root.join("segments"),
            locks: root.join("locks"),
            root,
        };
        if create {
            std::fs::create_dir_all(&output.segments)?;
            std::fs::create_dir_all(&output.locks)?;
            sync_dir(&output.root)?;
            sync_parent_dir(&output.root)?;
        } else if !output.segments.is_dir() || !output.locks.is_dir() {
            return Err(StoreError::MissingSidecar);
        }
        Ok(output)
    }

    pub(crate) fn lock_path(&self, kind: &str) -> PathBuf {
        self.locks.join(format!("{kind}.lock"))
    }

    pub(crate) fn lock_dir(&self) -> &Path {
        &self.locks
    }

    pub(crate) fn segment_path(&self, id: &SegmentId) -> PathBuf {
        self.segments.join(id.filename())
    }

    pub(crate) fn put_segment(&self, id: &SegmentId, source: &Path) -> Result<(), StoreError> {
        let destination = self.segment_path(id);
        if destination.exists() {
            if files_equal(&destination, source)? {
                File::open(&destination)?.sync_all()?;
                sync_dir(&self.segments)?;
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
                File::open(&destination)?.sync_all()?;
                sync_dir(&self.segments)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub(crate) fn list_segments(&self) -> Result<Vec<SegmentId>, StoreError> {
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

    pub(crate) fn delete_segment(&self, id: &SegmentId) -> Result<(), StoreError> {
        match std::fs::remove_file(self.segment_path(id)) {
            Ok(()) => sync_dir(&self.segments)?,
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filesystem_backend_lists_lexical_segments() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let backend = FsSegmentBackend::open(directory.path().join("db.d"), true)?;
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
