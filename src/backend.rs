//! Bundle directories and stable SQLite/publication lock carriers.
use crate::fs::{sync_dir, sync_parent_dir};
use crate::store::StoreError;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(crate) struct LocalCoordination {
    root: PathBuf,
    locks: PathBuf,
    storage: crate::storage::Storage,
}
impl LocalCoordination {
    pub(crate) fn open(root: PathBuf, create: bool) -> Result<Self, StoreError> {
        let storage = crate::storage::Storage::for_sidecar(&root, create)?;
        let output = Self {
            locks: storage.coordination_directory().join("locks"),
            root,
            storage,
        };
        if create {
            crate::fs::create_dir_all(output.root.join("objects"))?;
            crate::fs::create_dir_all(&output.locks)?;
            sync_dir(&output.root)?;
            sync_parent_dir(&output.root)?;
        } else if !output.root.is_dir() || !output.locks.is_dir() {
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
    /// The catalog for this sidecar, resolved once rather than per operation.
    pub(crate) fn catalog(&self) -> crate::storage::Catalog {
        crate::storage::Catalog::for_sidecar(self.storage.clone(), &self.root)
    }
}
pub(crate) fn sidecar_dir(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".d");
    PathBuf::from(value)
}
