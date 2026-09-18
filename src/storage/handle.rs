//! Explicit backend binding and reconstruction of a local active pagefile.
use super::adapter::{FilesystemBackend, StorageBackend};
use super::objects::Catalog;
use crate::domain::{AttachmentId, PackId, StoredBytes};
use crate::fs::{ExclusiveLock, absolute_path, sync_dir, sync_parent_dir, write_all_at};
use crate::{Inspect, StoreError};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}
struct StorageInner {
    backend: Arc<dyn StorageBackend>,
    coordination: PathBuf,
    namespace: PathBuf,
    head: String,
}
impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("head", &self.inner.head)
            .field("backend", &self.backend().identity())
            .field("coordination", &self.inner.coordination)
            .finish()
    }
}
static BINDINGS: OnceLock<Mutex<BTreeMap<PathBuf, Storage>>> = OnceLock::new();
fn bindings() -> &'static Mutex<BTreeMap<PathBuf, Storage>> {
    BINDINGS.get_or_init(|| Mutex::new(BTreeMap::new()))
}
pub(super) fn canonical(path: &Path) -> Result<PathBuf, StoreError> {
    let path = absolute_path(path)?;
    if let Ok(canonical) = path.canonicalize() {
        return Ok(canonical);
    }
    Ok(match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) => parent
            .canonicalize()
            .map_or(path.clone(), |parent| parent.join(name)),
        _ => path,
    })
}
pub(super) fn validate_head(name: &str) -> Result<(), StoreError> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(StoreError::InvalidConfiguration(
            "head names must be 1..=128 ASCII letters, digits, '.', '_' or '-'",
        ));
    }
    Ok(())
}
impl Storage {
    /// One backend namespace and its common, host-local coordination directory.
    /// All local processes using this namespace must use this same directory.
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        coordination: impl AsRef<Path>,
    ) -> Result<Self, StoreError> {
        let coordination = absolute_path(coordination.as_ref())?;
        for directory in [
            "locks",
            "readers",
            "object-readers",
            "physical-readers",
            "staging",
        ] {
            std::fs::create_dir_all(coordination.join(directory))?;
        }
        sync_dir(&coordination)?;
        let coordination = coordination.canonicalize()?;
        Ok(Self {
            inner: Arc::new(StorageInner {
                backend,
                namespace: coordination.clone(),
                coordination,
                head: "main".to_owned(),
            }),
        })
    }
    #[must_use]
    pub fn backend(&self) -> &dyn StorageBackend {
        self.inner.backend.as_ref()
    }
    #[must_use]
    pub fn coordination_directory(&self) -> &Path {
        &self.inner.coordination
    }
    #[must_use]
    pub fn head_name(&self) -> &str {
        &self.inner.head
    }
    pub(crate) fn namespace_directory(&self) -> &Path {
        &self.inner.namespace
    }
    /// Select a named head in this shared object namespace. This does not create
    /// a fork; use `fork` to publish a new head from the finalized source seal.
    pub fn head(&self, name: &str) -> Result<Self, StoreError> {
        validate_head(name)?;
        let coordination = if name == "main" {
            self.inner.namespace.clone()
        } else {
            self.inner.namespace.join("heads").join(super::objects::hex(
                *blake3::hash(name.as_bytes()).as_bytes(),
            ))
        };
        crate::fs::create_dir_all_synced(&coordination.join("locks"))?;
        Ok(Self {
            inner: Arc::new(StorageInner {
                backend: Arc::clone(&self.inner.backend),
                namespace: self.inner.namespace.clone(),
                coordination,
                head: name.to_owned(),
            }),
        })
    }
    /// Create an independent writable head from this head's latest finalized
    /// seal. Its objects remain shared; opening a local path bootstraps lazily.
    pub fn fork(&self, name: &str) -> Result<Self, StoreError> {
        let fork = self.head(name)?;
        let catalog = Catalog::configured(self.clone(), PathBuf::new());
        let guard = catalog.lock()?;
        if guard.state().heads.entries.contains_key(name.as_bytes()) {
            return Err(StoreError::InvalidConfiguration("head already exists"));
        }
        let header = guard.state().sealed.ok_or(StoreError::NoSealedHead)?;
        let view = guard.pin(crate::domain::ManifestId::from_bytes(
            header.parent_physical_digest,
        ))?;
        guard.validate_descriptor(&header, &view)?;
        let head = super::catalog::Head {
            attachment: None,
            sealed: Some(header),
            pending: None,
        };
        guard
            .state_mut()
            .heads
            .insert(name.as_bytes().to_vec(), head.encode());
        guard.state_mut().publish_confirmed(self.backend())?;
        Ok(fork)
    }
    /// Remove this fork's durable head after its local readers/writers close.
    /// Its old pagefiles are fenced; unreferenced objects become collectible.
    /// Keep one head until deleting the database through the normal delete API.
    pub fn remove_head(&self) -> Result<(), StoreError> {
        let _lifecycle = ExclusiveLock::acquire(
            &self.coordination_directory().join("locks/lifecycle.lock"),
            true,
        )?;
        let catalog = Catalog::configured(self.clone(), PathBuf::new());
        let guard = catalog.lock()?;
        if guard.state().heads.entries.len() <= 1 {
            return Err(StoreError::Busy);
        }
        if !guard
            .state()
            .heads
            .entries
            .contains_key(self.head_name().as_bytes())
        {
            return Err(StoreError::NoSealedHead);
        }
        let mut state = guard.state_mut();
        state.attachment = None;
        state.sealed = None;
        state.pending = None;
        state.publish_confirmed(self.backend())
    }
    pub fn heads(&self) -> Result<Vec<String>, StoreError> {
        let catalog = Catalog::configured(self.clone(), PathBuf::new());
        let guard = catalog.lock()?;
        guard
            .state()
            .heads
            .entries
            .keys()
            .map(|key| String::from_utf8(key.clone()).map_err(|_| StoreError::Corrupt(0)))
            .collect()
    }
    pub(crate) fn bind(&self, path: &Path) -> Result<PathBuf, StoreError> {
        let path = canonical(&crate::facade::storage_path(path))?;
        crate::facade::validate_notice(&path)?;
        let sidecar = crate::backend::sidecar_dir(&path);
        let mut bindings = bindings().lock().map_err(|_| StoreError::Busy)?;
        if let Some(existing) = bindings.get(&sidecar)
            && (existing.backend().identity() != self.backend().identity()
                || existing.coordination_directory() != self.coordination_directory())
        {
            return Err(StoreError::IdentityMismatch);
        }
        bindings.insert(sidecar, self.clone());
        Ok(path)
    }
    pub(crate) fn for_sidecar(root: &Path, create: bool) -> Result<Self, StoreError> {
        let root = canonical(root)?;
        if let Some(storage) = bindings()
            .lock()
            .map_err(|_| StoreError::Busy)?
            .get(&root)
            .cloned()
        {
            return Ok(storage);
        }
        let backend = FilesystemBackend::open_mode(&root, create)?;
        Self::new(Arc::new(backend), root)
    }
    pub(crate) fn bound(root: &Path) -> Result<Option<Self>, StoreError> {
        Ok(bindings()
            .lock()
            .map_err(|_| StoreError::Busy)?
            .get(&canonical(root)?)
            .cloned())
    }
    /// Configure a named VFS once. Opening a missing local database through it
    /// lazily bootstraps the published sealed head, or creates an empty namespace
    /// when `SQLite` requests CREATE and no sealed head exists.
    #[cfg(feature = "static")]
    pub fn register_vfs(&self, name: &str) -> Result<(), StoreError> {
        crate::vfs::register_storage_static_vfs(name, self.clone())
            .map_err(|_| StoreError::InvalidConfiguration("cannot register storage VFS"))
    }
    pub(crate) fn is_bound(root: &Path) -> Result<bool, StoreError> {
        Ok(bindings()
            .lock()
            .map_err(|_| StoreError::Busy)?
            .contains_key(&canonical(root)?))
    }
    /// Create a new local database attached to an empty storage namespace.
    pub fn create(&self, path: impl AsRef<Path>) -> Result<Database, StoreError> {
        let path = self.bind(path.as_ref())?;
        if path.exists() {
            return Err(StoreError::DestinationExists(path));
        }
        let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), true)?;
        if catalog.lock()?.namespace_database().is_some() {
            return Err(StoreError::Busy);
        }
        let store = crate::store::Store::open(&path, true)?;
        crate::facade::ensure_notice(&path)?;
        Ok(Database {
            storage: self.clone(),
            path,
            store: Mutex::new(store),
        })
    }
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Database, StoreError> {
        let path = self.bind(path.as_ref())?;
        let store = crate::store::Store::open_existing(&path)?;
        Ok(Database {
            storage: self.clone(),
            path,
            store: Mutex::new(store),
        })
    }
    /// Restore the finalized sealed head. Unsealed pagefile/WAL changes cannot
    /// be recovered from this interface. Payloads remain lazy and the extracted
    /// page cache starts empty. Existing destinations are never overwritten.
    #[allow(clippy::too_many_lines)] // Keep the publication/install ordering visible together.
    pub fn bootstrap(&self, destination: impl AsRef<Path>) -> Result<Database, StoreError> {
        let logical = absolute_path(destination.as_ref())?;
        let path = self.bind(&logical)?;
        if path.exists() || (logical != path && logical.exists()) {
            return Err(StoreError::DestinationExists(logical));
        }
        crate::store::reject_auxiliary_files(&path)?;
        let lifecycle = ExclusiveLock::acquire(
            &self.coordination_directory().join("locks/lifecycle.lock"),
            true,
        )?;
        let publication = ExclusiveLock::acquire(
            &self.coordination_directory().join("locks/publication.lock"),
            true,
        )?;
        let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), true)?;
        let guard = catalog.lock()?;
        let mut header = guard.state().sealed.ok_or(StoreError::NoSealedHead)?;
        let view = super::view::PinnedView::open_for_bootstrap(
            &guard,
            crate::domain::ManifestId::from_bytes(header.parent_physical_digest),
            &lifecycle,
        )?;
        guard.validate_descriptor(&header, &view)?;
        if view.logical_size().pages() != 0 {
            let page = view.resolve(crate::domain::PageNumber::new(1)?)?.read()?;
            let encoded_size = page
                .get(16..18)
                .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]));
            let page_size =
                encoded_size.map(|size| if size == 1 { 65_536 } else { u32::from(size) });
            if page.len() < 100
                || &page[..16] != b"SQLite format 3\0"
                || page_size != Some(header.page_size)
            {
                return Err(StoreError::InvalidStandardDatabase);
            }
        }
        let mut nonce = [0; 32];
        getrandom::fill(&mut nonce).map_err(std::io::Error::other)?;
        header.attachment_id = nonce;
        let parent = path.parent().ok_or(StoreError::Range)?;
        let mut staging = tempfile::NamedTempFile::new_in(parent)?;
        write_all_at(staging.as_file(), 0, &header.encode())?;
        let state = crate::format::ActiveState {
            database_id: header.database_id,
            sequence: 1,
            txid: header.start_txid.checked_sub(1).ok_or(StoreError::Range)?,
            logical_size: header.base_logical_size,
            page_size: header.page_size,
            history: header.base_history,
            commit_unix: 0,
            record_count: 0,
            truncate_pages: None,
        };
        write_all_at(
            staging.as_file(),
            crate::format::ACTIVE_HEADER_SIZE as u64,
            &state.encode(),
        )?;
        staging
            .as_file()
            .set_len(crate::format::ACTIVE_METADATA_SIZE as u64)?;
        staging.as_file().sync_all()?;
        #[cfg(test)]
        super::faults::check(super::faults::Point::BootstrapDataSynced)?;
        // Claiming an attachment does not advance the sealed endpoint or select
        // an unfinished seal. The token fences any superseded local pagefile.
        guard.state_mut().attachment = Some(AttachmentId::from_bytes(nonce));
        guard.state_mut().publish_confirmed(self.backend())?;
        guard.record_local_attachment(AttachmentId::from_bytes(nonce))?;
        #[cfg(test)]
        super::faults::check(super::faults::Point::BootstrapClaimed)?;
        std::fs::create_dir_all(crate::backend::sidecar_dir(&path))?;
        sync_parent_dir(&crate::backend::sidecar_dir(&path))?;
        // Use a no-replace rename: a crash cannot leave a hardlink alias to the
        // writable pagefile, and a competing destination is never overwritten.
        staging.flush().map_err(StoreError::Io)?;
        match crate::fs::install_pagefile(staging, &path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(StoreError::DestinationExists(path));
            }
            Err(error) => return Err(StoreError::Io(error)),
        }
        #[cfg(test)]
        super::faults::check(super::faults::Point::BootstrapInstalled)
            .map_err(StoreError::PublicationUncertain)?;
        sync_parent_dir(&path)
            .map_err(|error| StoreError::PublicationUncertain(std::io::Error::other(error)))?;
        // Old pending candidates remain protected until recovery can prove their
        // old pagefile is fenced. This successful attachment is that proof.
        if guard.state().pending.is_some() {
            guard.state_mut().pending = None;
            guard.state_mut().publish_confirmed(self.backend())?;
        }
        let lifecycle = lifecycle.into_shared_file()?;
        let store = crate::store::Store::from_bootstrap(
            path.clone(),
            &header,
            view,
            lifecycle,
            &publication,
        )?;
        drop(guard);
        drop(publication);
        crate::facade::ensure_notice(&path)?;
        Ok(Database {
            storage: self.clone(),
            path,
            store: Mutex::new(store),
        })
    }
}
use std::io::Write as _;

/// A bound local database with an active pagefile and the normal extracted-page
/// cache. Dropping this handle closes its local Store; it does not delete data.
pub struct Database {
    storage: Storage,
    path: PathBuf,
    store: Mutex<crate::store::Store>,
}
impl Database {
    pub(crate) fn into_store(self) -> Result<crate::store::Store, StoreError> {
        self.store.into_inner().map_err(|_| StoreError::Busy)
    }
    fn store(&self) -> Result<std::sync::MutexGuard<'_, crate::store::Store>, StoreError> {
        self.store.lock().map_err(|_| StoreError::Busy)
    }
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
    #[must_use]
    pub fn storage(&self) -> &Storage {
        &self.storage
    }
    pub fn inspect(&self) -> Result<Inspect, StoreError> {
        let mut store = self.store()?;
        store.acquire_maintenance()?;
        let result = store.inspect();
        store.release_maintenance();
        result
    }
    pub fn read_at(&self, offset: u64, bytes: &mut [u8]) -> Result<usize, StoreError> {
        let mut store = self.store()?;
        store.acquire_maintenance()?;
        let result = store.read_at(offset, bytes);
        store.release_maintenance();
        result
    }
    pub fn flush(&self) -> Result<Inspect, StoreError> {
        let mut store = self.store()?;
        store.acquire_maintenance()?;
        let result = store.flush_sidecars();
        store.release_maintenance();
        result?;
        store.inspect()
    }
    pub fn compact(&self) -> Result<Inspect, StoreError> {
        let mut store = self.store()?;
        store.compact()?;
        store.inspect()
    }
    pub fn verify(&self) -> Result<Inspect, StoreError> {
        let mut store = self.store()?;
        store.acquire_maintenance()?;
        let result = store.verify().and_then(|()| store.inspect());
        store.release_maintenance();
        result
    }
    pub fn retain(&self, name: super::RetentionName) -> Result<super::DurablePin, StoreError> {
        self.store()?.retain_view(name, false)
    }
    pub fn open_retained(
        &self,
        name: &super::RetentionName,
    ) -> Result<super::PinnedView, StoreError> {
        self.store()?.retained_view(name)
    }
    pub fn release_retention(&self, pin: super::DurablePin) -> Result<(), StoreError> {
        self.store()?.release_view(pin)
    }
    pub fn collect(&self, budget: usize) -> Result<super::GcReport, StoreError> {
        self.store()?.gc_report(budget)
    }
    /// Repack a bounded batch with one metadata checkpoint, then collect
    /// unreachable objects within the configured deletion budget.
    pub fn maintain(&self) -> Result<super::MaintenanceReport, StoreError> {
        self.store()?.repack_once()
    }
    pub fn relocate(
        &self,
        packs: &[PackId],
        byte_budget: StoredBytes,
    ) -> Result<super::RelocationReport, StoreError> {
        let catalog = Catalog::open(&crate::backend::sidecar_dir(&self.path), false)?;
        catalog.lock()?.relocate(packs, byte_budget)
    }
}
