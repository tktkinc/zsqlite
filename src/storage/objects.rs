use super::adapter::{BackendError, ObjectKey as PhysicalKey};
use crate::StoreError;
use crate::domain::{DictionaryId, ManifestId, PackId, StagingId, StoredBytes, StoredRange};
use crate::fs::{ExclusiveLock, read_exact_at, write_all_at};
use std::cell::{Ref, RefCell, RefMut};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod private {
    pub trait Sealed {}
}
pub(crate) trait ObjectKind: private::Sealed {
    type Id: Copy + Eq;
    const EXTENSION: &'static str;
    fn id(bytes: [u8; 32]) -> Self::Id;
    fn bytes(id: Self::Id) -> [u8; 32];
}
macro_rules! kinds {
    ($($kind:ident, $id:ident, $extension:literal);+ $(;)?) => {$ (
        #[derive(Debug)] pub(crate) enum $kind {}
        impl private::Sealed for $kind {}
        impl ObjectKind for $kind {
            type Id = $id;
            const EXTENSION: &'static str = $extension;
            fn id(bytes: [u8;32]) -> Self::Id { $id::from_bytes(bytes) }
            fn bytes(id: Self::Id) -> [u8;32] { *id.as_bytes() }
        }
    )+};
}
kinds!(Pack, PackId, "pack"; Dictionary, DictionaryId, "dict"; Manifest, ManifestId, "view");

#[derive(Debug)]
pub(crate) struct Catalog {
    root: Arc<PathBuf>,
    active_path: Arc<PathBuf>,
    storage: super::Storage,
}
impl Catalog {
    pub(crate) fn open(root: &Path, create: bool) -> Result<Self, StoreError> {
        let storage = super::Storage::for_sidecar(root, create)?;
        Ok(Self::configured(storage, root.with_extension("")))
    }
    pub(super) fn configured(storage: super::Storage, active_path: PathBuf) -> Self {
        Self {
            root: Arc::new(storage.namespace_directory().to_path_buf()),
            active_path: Arc::new(active_path),
            storage,
        }
    }
    pub(crate) fn lock(&self) -> Result<CatalogGuard, StoreError> {
        self.lock_mode(false)
    }
    pub(crate) fn try_lock(&self) -> Result<CatalogGuard, StoreError> {
        self.lock_mode(true)
    }
    fn lock_mode(&self, nonblocking: bool) -> Result<CatalogGuard, StoreError> {
        let lock = ExclusiveLock::acquire(&self.root.join("locks/catalog.lock"), nonblocking)?;
        let state =
            super::catalog::State::load_for(self.storage.backend(), self.storage.head_name())?;
        Ok(CatalogGuard {
            root: Arc::clone(&self.root),
            active_path: Arc::clone(&self.active_path),
            storage: self.storage.clone(),
            state: RefCell::new(state),
            _lock: lock,
        })
    }
}

/// Exclusive catalogue authority, acquired after the `SQLite` publication lock.
/// Does not implement Clone. RAII releases the OS lock on every error path.
#[derive(Debug)]
pub(crate) struct CatalogGuard {
    root: Arc<PathBuf>,
    active_path: Arc<PathBuf>,
    storage: super::Storage,
    state: RefCell<super::catalog::State>,
    _lock: ExclusiveLock,
}

struct Staging {
    path: PathBuf,
    file: File,
}
impl Drop for Staging {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[must_use]
pub(crate) struct Building<'catalog, K: ObjectKind> {
    guard: &'catalog CatalogGuard,
    staging: Staging,
    length: u64,
    hasher: blake3::Hasher,
    _kind: PhantomData<K>,
}

#[must_use]
pub(crate) struct Finalized<'catalog, K: ObjectKind> {
    guard: &'catalog CatalogGuard,
    staging: Staging,
    id: K::Id,
    length: StoredBytes,
}

/// A synced object in this catalogue. This is not a reachability proof.
#[must_use]
pub(crate) struct Durable<'catalog, K: ObjectKind> {
    guard: &'catalog CatalogGuard,
    id: K::Id,
    length: StoredBytes,
}
impl<K: ObjectKind> Durable<'_, K> {
    #[must_use]
    pub(crate) fn id(&self) -> K::Id {
        self.id
    }
    #[must_use]
    pub(crate) const fn length(&self) -> StoredBytes {
        self.length
    }
    pub(crate) fn belongs_to(&self, guard: &CatalogGuard) -> bool {
        std::ptr::eq(self.guard, guard)
    }
}

impl<'g> Durable<'g, Pack> {
    pub(super) fn pack(guard: &'g CatalogGuard, id: PackId, length: StoredBytes) -> Self {
        Self { guard, id, length }
    }
}

/// Header/footer identity verified against its catalog name and requested ID.
/// This does not verify page payloads or establish reachability.
pub(super) struct AuthenticatedSegment {
    container: super::segment::Container,
    length: StoredBytes,
}
impl AuthenticatedSegment {
    pub(super) fn container(&self) -> &super::segment::Container {
        &self.container
    }
}

impl CatalogGuard {
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }
    pub(super) fn storage(&self) -> &super::Storage {
        &self.storage
    }
    pub(super) fn state(&self) -> Ref<'_, super::catalog::State> {
        self.state.borrow()
    }
    pub(super) fn state_mut(&self) -> RefMut<'_, super::catalog::State> {
        self.state.borrow_mut()
    }
    pub(super) fn active_path(&self) -> &Path {
        &self.active_path
    }
    pub(super) fn publish_catalog(&self) -> Result<(), StoreError> {
        self.state_mut().publish(self.storage.backend())
    }
    pub(super) fn physical_lease_path(&self, key: PhysicalKey) -> PathBuf {
        self.root.join("physical-readers").join(key.name())
    }
    pub(crate) fn namespace_database(&self) -> Option<crate::domain::DatabaseId> {
        self.state().database
    }
    pub(crate) fn initialize_namespace(
        &self,
        database: crate::domain::DatabaseId,
    ) -> Result<crate::domain::AttachmentId, StoreError> {
        let mut state = self.state_mut();
        if state.sealed.is_some() || state.database.is_some_and(|existing| existing != database) {
            return Err(StoreError::IdentityMismatch);
        }
        let mut nonce = [0; 32];
        getrandom::fill(&mut nonce).map_err(std::io::Error::other)?;
        let attachment = state
            .attachment
            .unwrap_or(crate::domain::AttachmentId::from_bytes(nonce));
        if state.database.is_some() {
            self.validate_local_attachment(attachment)?;
        }
        self.record_local_attachment(attachment)?;
        state.database = Some(database);
        state.attachment = Some(attachment);
        state.publish(self.storage.backend())?;
        Ok(attachment)
    }
    fn local_attachment_record(
        &self,
        attachment: crate::domain::AttachmentId,
    ) -> Result<Vec<u8>, StoreError> {
        let mut record = self.storage.backend().identity().as_bytes().to_vec();
        record.extend(attachment.as_bytes());
        record.extend(
            super::handle::canonical(&self.active_path)?
                .as_os_str()
                .as_encoded_bytes(),
        );
        Ok(record)
    }
    pub(super) fn record_local_attachment(
        &self,
        attachment: crate::domain::AttachmentId,
    ) -> Result<(), StoreError> {
        let mut file = tempfile::NamedTempFile::new_in(self.storage.coordination_directory())?;
        std::io::Write::write_all(&mut file, &self.local_attachment_record(attachment)?)?;
        file.as_file().sync_all()?;
        file.persist(
            self.storage
                .coordination_directory()
                .join("active-location"),
        )
        .map_err(|error| StoreError::Io(error.error))?;
        crate::fs::sync_dir(self.storage.coordination_directory())
    }
    pub(crate) fn relocate_local_attachment(&self, destination: &Path) -> Result<(), StoreError> {
        let attachment = self.state().attachment.ok_or(StoreError::StaleAttachment)?;
        let mut record = self.storage.backend().identity().as_bytes().to_vec();
        record.extend(attachment.as_bytes());
        record.extend(
            super::handle::canonical(destination)?
                .as_os_str()
                .as_encoded_bytes(),
        );
        let mut file = tempfile::NamedTempFile::new_in(self.storage.coordination_directory())?;
        std::io::Write::write_all(&mut file, &record)?;
        file.as_file().sync_all()?;
        file.persist(
            self.storage
                .coordination_directory()
                .join("active-location"),
        )
        .map_err(|error| StoreError::Io(error.error))?;
        crate::fs::sync_dir(self.storage.coordination_directory())
    }
    fn validate_local_attachment(
        &self,
        attachment: crate::domain::AttachmentId,
    ) -> Result<(), StoreError> {
        match std::fs::read(
            self.storage
                .coordination_directory()
                .join("active-location"),
        ) {
            Ok(record) if record != self.local_attachment_record(attachment)? => {
                Err(StoreError::StaleAttachment)
            }
            Ok(_) => Ok(()),
            // Local coordination can be reconstructed from the authoritative
            // attachment token. Bootstrap does not need the old directory.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
    pub(crate) fn validate_attachment(
        &self,
        header: &crate::format::ActiveHeader,
    ) -> Result<(), StoreError> {
        let state = self.state();
        if state
            .database
            .as_ref()
            .map(crate::domain::DatabaseId::as_bytes)
            != Some(&header.database_id)
        {
            return Err(StoreError::IdentityMismatch);
        }
        if state
            .attachment
            .as_ref()
            .map(crate::domain::AttachmentId::as_bytes)
            != Some(&header.attachment_id)
        {
            return Err(StoreError::StaleAttachment);
        }
        self.validate_local_attachment(crate::domain::AttachmentId::from_bytes(
            header.attachment_id,
        ))?;
        Ok(())
    }
    pub(crate) fn validate_descriptor(
        &self,
        header: &crate::format::ActiveHeader,
        view: &super::PinnedView,
    ) -> Result<(), StoreError> {
        if self
            .state()
            .database
            .as_ref()
            .map(crate::domain::DatabaseId::as_bytes)
            != Some(&header.database_id)
        {
            return Err(StoreError::IdentityMismatch);
        }
        crate::store::validate_policy(header.policy)?;
        let endpoint = view.endpoint();
        if endpoint.0.as_bytes() != &header.database_id
            || endpoint.1.get().checked_add(1) != Some(header.start_txid)
            || endpoint.2.as_bytes() != &header.base_history
            || view.logical_size().get() != header.base_logical_size
            || view.logical_size().page_size().get() != header.page_size
            || view.id().as_bytes() != &header.parent_physical_digest
        {
            return Err(StoreError::IdentityMismatch);
        }
        Ok(())
    }
    pub(crate) fn prepare_seal(
        &self,
        header: &crate::format::ActiveHeader,
    ) -> Result<(), StoreError> {
        self.validate_attachment(header)?;
        self.state_mut().pending = Some(*header);
        self.publish_catalog()
    }
    pub(crate) fn finish_seal(
        &self,
        header: &crate::format::ActiveHeader,
    ) -> Result<(), StoreError> {
        self.validate_attachment(header)?;
        let mut state = self.state_mut();
        if let Some(pending) = state.pending {
            if pending.encode() != header.encode() {
                return Err(StoreError::Busy);
            }
            state.sealed = Some(pending);
            state.pending = None;
            state.publish(self.storage.backend())?;
        }
        Ok(())
    }
    pub(crate) fn recover_seal(
        &self,
        header: &crate::format::ActiveHeader,
    ) -> Result<(), StoreError> {
        self.validate_attachment(header)?;
        let pending = self.state().pending;
        if let Some(pending) = pending
            && pending.encode() == header.encode()
        {
            self.finish_seal(header)?;
        } else if pending.is_some()
            && self
                .state()
                .sealed
                .is_none_or(|sealed| sealed.parent_physical_digest == header.parent_physical_digest)
        {
            // The previous attachment survived. Fencing this candidate with a
            // fresh catalog revision makes abandoning it safe even after an
            // uncertain CAS acknowledgment.
            self.state_mut().pending = None;
            self.publish_catalog()?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn path<K: ObjectKind>(&self, id: K::Id) -> PathBuf {
        let key = if K::EXTENSION == "pack" {
            self.placement(PackId::from_bytes(K::bytes(id)))
                .map(|placement| PhysicalKey::Blob(placement.preferred().extent.blob()))
        } else {
            Ok(physical_key::<K>(id))
        };
        self.root
            .join("objects")
            .join(key.map_or_else(|_| object_name::<K>(id), PhysicalKey::name))
    }
    pub(crate) fn build<K: ObjectKind>(&self) -> Result<Building<'_, K>, StoreError> {
        let mut nonce = [0_u8; 32];
        getrandom::fill(&mut nonce).map_err(std::io::Error::other)?;
        let path = self
            .root
            .join("staging")
            .join(format!("{}.pending", hex(nonce)));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Building {
            guard: self,
            staging: Staging { path, file },
            length: 0,
            hasher: blake3::Hasher::new(),
            _kind: PhantomData,
        })
    }
    /// Revalidate an already installed dependency before constructing a receipt.
    pub(crate) fn validate<K: ObjectKind>(
        &self,
        id: K::Id,
        limit: u64,
    ) -> Result<Durable<'_, K>, StoreError> {
        if super::segment::is_manifest::<K>() {
            let authenticated = self.read_manifest(ManifestId::from_bytes(K::bytes(id)), limit)?;
            return Ok(Durable {
                guard: self,
                id,
                length: authenticated.length,
            });
        }
        let bytes = if K::EXTENSION == "pack" {
            let pack = PackId::from_bytes(K::bytes(id));
            let length = super::placement::PlacementPin::new(self, [pack])?
                .copy_pack(pack, &mut std::io::sink())?;
            return Ok(Durable {
                guard: self,
                id,
                length,
            });
        } else {
            let bytes =
                super::catalog::read_all(self.storage.backend(), physical_key::<K>(id), limit)?;
            if *blake3::hash(&bytes).as_bytes() != K::bytes(id) {
                return Err(StoreError::Corrupt(0));
            }
            bytes
        };
        let length = bytes.len() as u64;
        Ok(Durable {
            guard: self,
            id,
            length: StoredBytes::new(length),
        })
    }
    /// Authenticate the header/footer once and preserve that proof for parsing.
    pub(super) fn read_manifest(
        &self,
        id: ManifestId,
        limit: u64,
    ) -> Result<AuthenticatedSegment, StoreError> {
        let bytes =
            super::catalog::read_all(self.storage.backend(), PhysicalKey::Manifest(id), limit)?;
        let length = bytes.len() as u64;
        let container = super::segment::read_bytes(&bytes)?;
        if container.id != id {
            return Err(StoreError::Corrupt(0));
        }
        Ok(AuthenticatedSegment {
            container,
            length: StoredBytes::new(length),
        })
    }
    pub(crate) fn read<K: ObjectKind>(
        &self,
        id: K::Id,
        range: StoredRange,
    ) -> Result<Vec<u8>, StoreError> {
        if K::EXTENSION == "pack" {
            let pack = PackId::from_bytes(K::bytes(id));
            return super::placement::PlacementPin::new(self, [pack])?.read(pack, range);
        }
        super::catalog::read_range(self.storage.backend(), physical_key::<K>(id), range)
    }
    pub(super) fn inventory(&self) -> Result<BTreeSet<PhysicalKey>, StoreError> {
        let mut keys = BTreeSet::new();
        let mut after = None;
        loop {
            let batch = self.storage.backend().inventory(after, 4096)?;
            if batch.is_empty() {
                break;
            }
            if batch.len() > 4096 {
                return Err(StoreError::Range);
            }
            for key in batch {
                if after.is_some_and(|after| key <= after) || !keys.insert(key) {
                    return Err(StoreError::Corrupt(0));
                }
                after = Some(key);
            }
        }
        Ok(keys)
    }
    #[cfg(test)]
    pub(crate) fn all_objects(&self) -> Result<BTreeSet<ObjectKey>, StoreError> {
        let mut keys = BTreeSet::new();
        for key in self.state().endpoints.entries.keys() {
            keys.insert(ObjectKey::Manifest(ManifestId::from_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Corrupt(0))?,
            )));
        }
        for key in self.state().placements.entries.keys() {
            keys.insert(ObjectKey::Pack(PackId::from_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Corrupt(0))?,
            )));
        }
        for key in self.inventory()? {
            if let PhysicalKey::Dictionary(id) = key {
                keys.insert(ObjectKey::Dictionary(id));
            }
        }
        Ok(keys)
    }
    pub(super) fn logical_length(&self, key: ObjectKey) -> Result<u64, StoreError> {
        match key {
            ObjectKey::Pack(id) => Ok(self.placement(id)?.length.get()),
            ObjectKey::Manifest(id) => Ok(self
                .storage
                .backend()
                .stat(PhysicalKey::Manifest(id))?
                .ok_or(BackendError::Missing(PhysicalKey::Manifest(id)))?
                .get()),
            ObjectKey::Dictionary(id) => Ok(self
                .storage
                .backend()
                .stat(PhysicalKey::Dictionary(id))?
                .ok_or(BackendError::Missing(PhysicalKey::Dictionary(id)))?
                .get()),
            ObjectKey::Staging(_) => Err(StoreError::Corrupt(0)),
        }
    }
}

impl Building<'_, Manifest> {
    pub(super) fn segment_header(
        &mut self,
        header: super::segment::Header,
    ) -> Result<(), StoreError> {
        if self.length < super::segment::HEADER_SIZE as u64 {
            return Err(StoreError::Range);
        }
        write_all_at(&self.staging.file, 0, &header.encode())?;
        Ok(())
    }
}
impl<'catalog, K: ObjectKind> Building<'catalog, K> {
    pub(crate) fn append(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
        let end = self
            .length
            .checked_add(bytes.len() as u64)
            .ok_or(StoreError::Range)?;
        if K::EXTENSION != "pack" && end > 512 * 1024 * 1024 {
            return Err(StoreError::Range);
        }
        write_all_at(&self.staging.file, self.length, bytes)?;
        if K::EXTENSION != "pack" {
            self.hasher.update(bytes);
        }
        self.length = end;
        Ok(())
    }
    pub(crate) fn finalize(self) -> Result<Finalized<'catalog, K>, StoreError> {
        self.staging.file.sync_all()?;
        #[cfg(test)]
        super::faults::check(super::faults::Point::ObjectDataSynced)?;
        let mut id = K::id(*self.hasher.finalize().as_bytes());
        if K::EXTENSION == "pack" {
            let pack = super::pack::copy(
                |offset, bytes| Ok(read_exact_at(&self.staging.file, offset, bytes)?),
                self.length,
                &mut std::io::sink(),
                false,
            )?;
            id = K::id(*pack.as_bytes());
        }
        if super::segment::is_manifest::<K>() {
            id = K::id(*super::segment::read(&self.staging.file)?.id.as_bytes());
        }
        Ok(Finalized {
            guard: self.guard,
            staging: self.staging,
            id,
            length: StoredBytes::new(self.length),
        })
    }
}
impl<'catalog, K: ObjectKind> Finalized<'catalog, K> {
    pub(crate) fn install(self) -> Result<Durable<'catalog, K>, StoreError> {
        if K::EXTENSION == "pack" {
            let mut writer = super::placement::BlobWriter::new(self.guard)?;
            let mut file = self.staging.file.try_clone()?;
            std::io::Seek::rewind(&mut file)?;
            std::io::copy(&mut file, &mut writer)?;
            let pack = PackId::from_bytes(K::bytes(self.id));
            writer.complete_pack(pack, 32)?;
            let (_, extents) = writer.finish()?;
            let _receipt = self.guard.install_pack_extent(pack, extents[&pack])?;
        } else {
            use std::io::{Seek, SeekFrom};
            let mut file = self.staging.file.try_clone()?;
            file.seek(SeekFrom::Start(0))?;
            self.guard
                .storage
                .backend()
                .put(physical_key::<K>(self.id), self.length, &mut file)?;
            if super::segment::is_manifest::<K>() {
                let container = super::segment::read(&self.staging.file)?;
                self.guard.state_mut().endpoints.insert(
                    K::bytes(self.id).to_vec(),
                    super::catalog::endpoint_value(container.header),
                );
                self.guard.publish_catalog()?;
            }
        }
        Ok(Durable {
            guard: self.guard,
            id: self.id,
            length: self.length,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ObjectKey {
    Manifest(ManifestId),
    Pack(PackId),
    Dictionary(DictionaryId),
    Staging(StagingId),
}
impl ObjectKey {
    pub(super) fn parse(name: &str) -> Result<Self, StoreError> {
        if name.ends_with(".segment") {
            return super::segment::parse_filename(name).map(Self::Manifest);
        }
        let (id, kind) = name.split_once('.').ok_or(StoreError::Corrupt(0))?;
        let digest = parse_hex(id)?;
        match kind {
            "pack" => Ok(Self::Pack(PackId::from_bytes(digest))),
            "dict" => Ok(Self::Dictionary(DictionaryId::from_bytes(digest))),
            "pending" => Ok(Self::Staging(StagingId::from_bytes(digest))),
            _ => Err(StoreError::Corrupt(0)),
        }
    }
    /// Reader leases use physical IDs, not discoverable segment filenames.
    /// A ".view" lease is current metadata; a ".view" payload object is rejected.
    pub(super) fn parse_lease(name: &str) -> Result<Self, StoreError> {
        if let Some(id) = name.strip_suffix(".view") {
            return Ok(Self::Manifest(ManifestId::from_bytes(parse_hex(id)?)));
        }
        Self::parse(name)
    }
    pub(super) fn lease_path(self, guard: &CatalogGuard) -> Result<PathBuf, StoreError> {
        let name = match self {
            Self::Manifest(id) => object_name::<Manifest>(id),
            Self::Pack(id) => object_name::<Pack>(id),
            Self::Dictionary(id) => object_name::<Dictionary>(id),
            Self::Staging(_) => return Err(StoreError::Corrupt(0)),
        };
        Ok(guard.root.join("object-readers").join(name))
    }
}

/// Created only after every current, retained, and leased root has been traced.
/// The borrow keeps catalogue exclusion alive until all permits are gone.
pub(crate) struct GcSession<'catalog> {
    guard: &'catalog CatalogGuard,
    live: BTreeSet<PhysicalKey>,
    issued: BTreeSet<PhysicalKey>,
}
/// Single-use authority for exactly one unreachable object in this session.
#[must_use]
pub(crate) struct DeletionPermit<'session> {
    session: &'session GcSession<'session>,
    key: PhysicalKey,
}
impl<'catalog> GcSession<'catalog> {
    pub(super) fn traced(roots: super::retention::ValidatedRoots<'catalog>) -> Self {
        let (guard, live) = roots.into_parts();
        Self {
            guard,
            live,
            issued: BTreeSet::new(),
        }
    }
    pub(super) fn permit(&mut self, key: PhysicalKey) -> Option<DeletionPermit<'_>> {
        if self.live.contains(&key) || !self.issued.insert(key) {
            return None;
        }
        Some(DeletionPermit { session: self, key })
    }
}
impl DeletionPermit<'_> {
    pub(crate) fn delete(self) -> Result<(), StoreError> {
        self.session
            .guard
            .storage
            .backend()
            .delete(super::adapter::DeletePermit::new(
                self.session.guard,
                self.key,
            ))?;
        Ok(())
    }
}

pub(crate) fn object_name<K: ObjectKind>(id: K::Id) -> String {
    format!("{}.{}", hex(K::bytes(id)), K::EXTENSION)
}
pub(crate) fn hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut output = String::with_capacity(64);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to string");
    }
    output
}
pub(crate) fn parse_hex(value: &str) -> Result<[u8; 32], StoreError> {
    if value.len() != 64 || !value.is_ascii() {
        return Err(StoreError::Corrupt(0));
    }
    let mut bytes = [0_u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| StoreError::Corrupt(0))?;
    }
    Ok(bytes)
}

fn physical_key<K: ObjectKind>(id: K::Id) -> PhysicalKey {
    match K::EXTENSION {
        "dict" => PhysicalKey::Dictionary(DictionaryId::from_bytes(K::bytes(id))),
        "view" => PhysicalKey::Manifest(ManifestId::from_bytes(K::bytes(id))),
        _ => unreachable!("logical packs resolve through placements"),
    }
}

impl Durable<'_, Pack> {
    pub(super) fn set_pack_cohort(&self, cohort: [u8; 32]) -> Result<(), StoreError> {
        self.guard.set_cohort(self.id, cohort)
    }
}
