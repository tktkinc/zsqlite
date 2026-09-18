//! Transport-only storage contract. One backend instance identifies one namespace.
//!
//! Implementations must make successful puts and root publication durable and
//! immediately readable by other clients. Listing is ordered, exclusive of the
//! cursor, and may omit concurrent additions; it never establishes reachability.
//! A stat result establishes length/existence, never authenticated contents.
use crate::domain::{
    BackendId, BlobId, DictionaryId, IndexId, ManifestId, StoredBytes, StoredRange,
};
use crate::fs::{ExclusiveLock, read_exact_at, sync_dir};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

pub const MAX_BATCH_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_ROOT_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectKey {
    Blob(BlobId),
    Manifest(ManifestId),
    Dictionary(DictionaryId),
    Index(IndexId),
}
impl ObjectKey {
    pub(crate) fn name(self) -> String {
        let (id, extension) = match self {
            Self::Blob(id) => (*id.as_bytes(), "blob"),
            Self::Manifest(id) => (*id.as_bytes(), "segment"),
            Self::Dictionary(id) => (*id.as_bytes(), "dict"),
            Self::Index(id) => (*id.as_bytes(), "index"),
        };
        format!("{}.{extension}", super::objects::hex(id))
    }
    pub(crate) fn parse(name: &str) -> Result<Self, BackendError> {
        let (id, extension) = name.split_once('.').ok_or(BackendError::InvalidData)?;
        let id = super::objects::parse_hex(id).map_err(|_| BackendError::InvalidData)?;
        match extension {
            "blob" => Ok(Self::Blob(BlobId::from_bytes(id))),
            "segment" => Ok(Self::Manifest(ManifestId::from_bytes(id))),
            "dict" => Ok(Self::Dictionary(DictionaryId::from_bytes(id))),
            "index" => Ok(Self::Index(IndexId::from_bytes(id))),
            _ => Err(BackendError::InvalidData),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("backend transport: {message}")]
    Transport { message: String, retryable: bool },
    #[error("missing immutable object: {0:?}")]
    Missing(ObjectKey),
    #[error("immutable object conflicts with existing contents: {0:?}")]
    IdentityMismatch(ObjectKey),
    #[error("invalid backend data")]
    InvalidData,
    #[error("invalid or excessive backend range")]
    Range,
    #[error("stale catalog publication")]
    Stale,
    #[error("backend publication may have become visible: {0}")]
    Uncertain(String),
    #[error("deletion permit belongs to a different storage namespace")]
    WrongNamespace,
}

/// Adapter-owned comparison token, not a content identity. Tokens must never be
/// reused, even when a root returns to earlier bytes (ABA).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Revision(Vec<u8>);
impl Revision {
    pub fn new(bytes: Vec<u8>) -> Result<Self, BackendError> {
        if bytes.is_empty() || bytes.len() > 256 {
            return Err(BackendError::Range);
        }
        Ok(Self(bytes))
    }
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
}
#[derive(Clone, Debug)]
pub struct RootRecord {
    revision: Revision,
    bytes: Vec<u8>,
}
impl RootRecord {
    pub fn new(revision: Revision, bytes: Vec<u8>) -> Result<Self, BackendError> {
        if bytes.len() > MAX_ROOT_BYTES {
            return Err(BackendError::Range);
        }
        Ok(Self { revision, bytes })
    }
    #[must_use]
    pub fn revision(&self) -> &Revision {
        &self.revision
    }
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
#[derive(Debug)]
pub enum Publication {
    Applied(Revision),
    Stale,
    /// The caller must reread the root before retrying or collecting objects.
    Uncertain(String),
}

#[derive(Clone, Copy, Debug)]
pub struct ObjectRange {
    key: ObjectKey,
    range: StoredRange,
}
impl ObjectRange {
    pub fn new(key: ObjectKey, range: StoredRange) -> Result<Self, BackendError> {
        if range.length().get() == 0 {
            return Err(BackendError::Range);
        }
        Ok(Self { key, range })
    }
    #[must_use]
    pub const fn key(self) -> ObjectKey {
        self.key
    }
    #[must_use]
    pub const fn range(self) -> StoredRange {
        self.range
    }
}

/// Single-use physical deletion authority minted by the core while catalog
/// exclusion and a validated root traversal remain alive.
/// ```compile_fail
/// use zsqlite::storage::adapter::DeletePermit;
/// let permit = DeletePermit { };
/// ```
/// ```compile_fail
/// use zsqlite::storage::adapter::{DeletePermit, StorageBackend};
/// fn twice(backend: &dyn StorageBackend, permit: DeletePermit<'_>) {
///     backend.delete(permit).unwrap();
///     backend.delete(permit).unwrap();
/// }
/// ```
#[must_use]
pub struct DeletePermit<'a> {
    namespace: BackendId,
    key: ObjectKey,
    _exclusion: &'a super::objects::CatalogGuard,
}
impl<'a> DeletePermit<'a> {
    pub(crate) fn new(guard: &'a super::objects::CatalogGuard, key: ObjectKey) -> Self {
        Self {
            namespace: guard.storage().backend().identity(),
            key,
            _exclusion: guard,
        }
    }
    #[must_use]
    pub const fn key(&self) -> ObjectKey {
        self.key
    }
    pub fn authorize(&self, backend: BackendId) -> Result<ObjectKey, BackendError> {
        if backend != self.namespace {
            return Err(BackendError::WrongNamespace);
        }
        Ok(self.key)
    }
}

/// An adapter-owned unpublished object. Dropping it aborts installation.
/// `finish` consumes the writer and durably installs its bytes under the final
/// key; it must reject an existing key with different bytes or a wrong length.
/// ```compile_fail
/// use zsqlite::storage::adapter::{ObjectWriter, ObjectKey};
/// use zsqlite::domain::StoredBytes;
/// fn twice(writer: Box<dyn ObjectWriter>, key: ObjectKey) {
///     writer.finish(key, StoredBytes::new(8)).unwrap();
///     writer.finish(key, StoredBytes::new(8)).unwrap();
/// }
/// ```
pub trait ObjectWriter: Write + Send {
    fn finish(self: Box<Self>, key: ObjectKey, length: StoredBytes) -> Result<(), BackendError>;
}

/// Implement this trait outside zsqlite to provide a storage transport. The
/// backend does not parse database manifests, select packs, or decide liveness.
/// Calls are synchronous to match `SQLite`; an adapter may own a runtime internally.
pub trait StorageBackend: Send + Sync {
    fn identity(&self) -> BackendId;
    /// Begin streaming without knowing the final content-derived key or length.
    fn begin_write(&self) -> Result<Box<dyn ObjectWriter + '_>, BackendError>;
    /// Convenience for an already named object. Consume exactly `length` bytes
    /// followed by EOF; a failed stream never installs a partial object.
    fn put(
        &self,
        key: ObjectKey,
        length: StoredBytes,
        source: &mut dyn Read,
    ) -> Result<(), BackendError> {
        if length.get() == 0 {
            return Err(BackendError::Range);
        }
        let mut writer = self.begin_write()?;
        let copied = std::io::copy(&mut source.take(length.get()), &mut writer)?;
        let mut extra = [0];
        if copied != length.get() || source.read(&mut extra)? != 0 {
            return Err(BackendError::Range);
        }
        writer.finish(key, length)
    }
    /// Results correspond one-for-one to requests, in input order, with exact lengths.
    fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError>;
    fn stat(&self, key: ObjectKey) -> Result<Option<StoredBytes>, BackendError>;
    fn read_root(&self) -> Result<Option<RootRecord>, BackendError>;
    fn compare_exchange_root(
        &self,
        expected: Option<&Revision>,
        bytes: &[u8],
    ) -> Result<Publication, BackendError>;
    /// Return strictly increasing keys after `after`, at most `limit` (1..=4096).
    fn inventory(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, BackendError>;
    fn delete(&self, permit: DeletePermit<'_>) -> Result<(), BackendError>;
}

pub(crate) fn check_batch(requests: &[ObjectRange]) -> Result<(), BackendError> {
    if requests.is_empty() || requests.len() > 4096 {
        return Err(BackendError::Range);
    }
    let mut total = 0_u64;
    for request in requests {
        total = total
            .checked_add(request.range.length().get())
            .ok_or(BackendError::Range)?;
    }
    if total > MAX_BATCH_BYTES {
        return Err(BackendError::Range);
    }
    Ok(())
}
fn nonce() -> Result<[u8; 32], BackendError> {
    let mut bytes = [0; 32];
    getrandom::fill(&mut bytes).map_err(std::io::Error::other)?;
    Ok(bytes)
}
fn storage_io(error: crate::StoreError) -> BackendError {
    match error {
        crate::StoreError::Io(error) => BackendError::Io(error),
        other => BackendError::Transport {
            message: other.to_string(),
            retryable: false,
        },
    }
}

#[derive(Debug)]
pub struct FilesystemBackend {
    root: PathBuf,
    identity: BackendId,
}
impl FilesystemBackend {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, BackendError> {
        Self::open_mode(root.as_ref(), true)
    }
    pub(crate) fn open_mode(root: &Path, create: bool) -> Result<Self, BackendError> {
        if create {
            crate::fs::create_dir_all_synced(&root.join("objects")).map_err(storage_io)?;
            crate::fs::create_dir_all_synced(&root.join("locks")).map_err(storage_io)?;
        }
        let root = root.canonicalize()?;
        let identity_path = root.join("backend.identity");
        if !identity_path.exists() && create {
            let mut temporary = tempfile::NamedTempFile::new_in(&root)?;
            temporary.write_all(&nonce()?)?;
            temporary.as_file().sync_all()?;
            match std::fs::hard_link(temporary.path(), &identity_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            sync_dir(&root).map_err(storage_io)?;
        }
        let identity: [u8; 32] = std::fs::read(identity_path)?
            .try_into()
            .map_err(|_| BackendError::InvalidData)?;
        Ok(Self {
            root,
            identity: BackendId::from_bytes(identity),
        })
    }
    /// Diagnostics for this filesystem implementation only; core storage uses keys.
    #[must_use]
    pub fn object_path(&self, key: ObjectKey) -> PathBuf {
        self.root.join("objects").join(key.name())
    }
    fn read_root_file(&self) -> Result<Option<RootRecord>, BackendError> {
        let bytes = match std::fs::File::open(self.root.join("catalog-head")) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take((MAX_ROOT_BYTES + 128) as u64)
                    .read_to_end(&mut bytes)?;
                bytes
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if bytes.len() < 72
            || &bytes[..8] != b"ZHEAD001"
            || blake3::hash(&bytes[..bytes.len() - 32]).as_bytes() != &bytes[bytes.len() - 32..]
        {
            return Err(BackendError::InvalidData);
        }
        Ok(Some(RootRecord::new(
            Revision::new(bytes[8..40].to_vec())?,
            bytes[40..bytes.len() - 32].to_vec(),
        )?))
    }
}
struct FilesystemWriter<'a> {
    backend: &'a FilesystemBackend,
    staging: tempfile::NamedTempFile,
}
impl Write for FilesystemWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.staging.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.staging.flush()
    }
}
impl ObjectWriter for FilesystemWriter<'_> {
    fn finish(self: Box<Self>, key: ObjectKey, length: StoredBytes) -> Result<(), BackendError> {
        let staging = &self.staging;
        if length.get() == 0 || staging.as_file().metadata()?.len() != length.get() {
            return Err(BackendError::Range);
        }
        staging.as_file().sync_all()?;
        #[cfg(test)]
        super::faults::check(super::faults::Point::ObjectDataSynced)?;
        match std::fs::hard_link(staging.path(), self.backend.object_path(key)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let file = std::fs::File::open(self.backend.object_path(key))?;
                if file.metadata()?.len() != length.get() {
                    return Err(BackendError::IdentityMismatch(key));
                }
                let mut left = vec![0; 65536];
                let mut right = vec![0; 65536];
                let mut offset = 0;
                while offset < length.get() {
                    let count = usize::try_from((length.get() - offset).min(left.len() as u64))
                        .map_err(|_| BackendError::Range)?;
                    read_exact_at(&file, offset, &mut left[..count])?;
                    read_exact_at(staging.as_file(), offset, &mut right[..count])?;
                    if left[..count] != right[..count] {
                        return Err(BackendError::IdentityMismatch(key));
                    }
                    offset += count as u64;
                }
            }
            Err(error) => return Err(error.into()),
        }
        #[cfg(test)]
        super::faults::check(super::faults::Point::ObjectLinked)?;
        sync_dir(&self.backend.root.join("objects")).map_err(storage_io)?;
        #[cfg(test)]
        super::faults::check(super::faults::Point::ObjectDirectorySynced)?;
        Ok(())
    }
}
impl StorageBackend for FilesystemBackend {
    fn identity(&self) -> BackendId {
        self.identity
    }
    fn begin_write(&self) -> Result<Box<dyn ObjectWriter + '_>, BackendError> {
        Ok(Box::new(FilesystemWriter {
            backend: self,
            staging: tempfile::NamedTempFile::new_in(self.root.join("objects"))?,
        }))
    }
    fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError> {
        check_batch(requests)?;
        requests
            .iter()
            .map(|request| {
                let file = std::fs::File::open(self.object_path(request.key)).map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        BackendError::Missing(request.key)
                    } else {
                        error.into()
                    }
                })?;
                request
                    .range
                    .within(StoredBytes::new(file.metadata()?.len()))
                    .map_err(|_| BackendError::Range)?;
                let mut bytes = vec![
                    0;
                    request
                        .range
                        .length()
                        .as_usize()
                        .map_err(|_| BackendError::Range)?
                ];
                read_exact_at(&file, request.range.offset().get(), &mut bytes)?;
                Ok(bytes)
            })
            .collect()
    }
    fn stat(&self, key: ObjectKey) -> Result<Option<StoredBytes>, BackendError> {
        match self.object_path(key).metadata() {
            Ok(metadata) => Ok(Some(StoredBytes::new(metadata.len()))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    fn read_root(&self) -> Result<Option<RootRecord>, BackendError> {
        self.read_root_file()
    }
    fn compare_exchange_root(
        &self,
        expected: Option<&Revision>,
        bytes: &[u8],
    ) -> Result<Publication, BackendError> {
        if bytes.len() > MAX_ROOT_BYTES {
            return Err(BackendError::Range);
        }
        let _lock = ExclusiveLock::acquire(&self.root.join("locks/backend-root.lock"), false)
            .map_err(storage_io)?;
        let current = self.read_root_file()?;
        if current.as_ref().map(RootRecord::revision) != expected {
            return Ok(Publication::Stale);
        }
        let revision = Revision::new(nonce()?.to_vec())?;
        let mut encoded = b"ZHEAD001".to_vec();
        encoded.extend(revision.bytes());
        encoded.extend(bytes);
        encoded.extend(blake3::hash(&encoded).as_bytes());
        let mut staging = tempfile::NamedTempFile::new_in(&self.root)?;
        staging.write_all(&encoded)?;
        staging.as_file().sync_all()?;
        std::fs::rename(staging.path(), self.root.join("catalog-head"))?;
        #[cfg(test)]
        if let Err(error) = super::faults::check(super::faults::Point::RootRenamed) {
            return Ok(Publication::Uncertain(error.to_string()));
        }
        if let Err(error) = sync_dir(&self.root) {
            return Ok(Publication::Uncertain(error.to_string()));
        }
        #[cfg(test)]
        if let Err(error) = super::faults::check(super::faults::Point::RootDirectorySynced) {
            return Ok(Publication::Uncertain(error.to_string()));
        }
        Ok(Publication::Applied(revision))
    }
    fn inventory(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, BackendError> {
        if limit == 0 || limit > 4096 {
            return Err(BackendError::Range);
        }
        let mut keys = Vec::new();
        let entries = match std::fs::read_dir(self.root.join("objects")) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().ok_or(BackendError::InvalidData)?;
            if name.starts_with('.') {
                continue;
            }
            let key = ObjectKey::parse(name)?;
            if after.is_none_or(|after| key > after) {
                keys.push(key);
            }
        }
        keys.sort_unstable();
        keys.truncate(limit);
        Ok(keys)
    }
    fn delete(&self, permit: DeletePermit<'_>) -> Result<(), BackendError> {
        let key = permit.authorize(self.identity)?;
        match std::fs::remove_file(self.object_path(key)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        #[cfg(test)]
        super::faults::check(super::faults::Point::ObjectRemoved)?;
        sync_dir(&self.root.join("objects")).map_err(storage_io)?;
        #[cfg(test)]
        super::faults::check(super::faults::Point::ObjectDeletionSynced)?;
        Ok(())
    }
}

#[derive(Default)]
struct MemoryState {
    objects: BTreeMap<ObjectKey, Vec<u8>>,
    root: Option<RootRecord>,
}
pub struct MemoryBackend {
    identity: BackendId,
    state: Mutex<MemoryState>,
}
impl MemoryBackend {
    pub fn new() -> Result<Self, BackendError> {
        Ok(Self {
            identity: BackendId::from_bytes(nonce()?),
            state: Mutex::new(MemoryState::default()),
        })
    }
    fn state(&self) -> Result<std::sync::MutexGuard<'_, MemoryState>, BackendError> {
        self.state.lock().map_err(|_| BackendError::InvalidData)
    }
}
struct MemoryWriter<'a> {
    backend: &'a MemoryBackend,
    bytes: Vec<u8>,
}
impl Write for MemoryWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes
            .try_reserve(bytes.len())
            .map_err(std::io::Error::other)?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl ObjectWriter for MemoryWriter<'_> {
    fn finish(self: Box<Self>, key: ObjectKey, length: StoredBytes) -> Result<(), BackendError> {
        if length.get() == 0 || self.bytes.len() as u64 != length.get() {
            return Err(BackendError::Range);
        }
        #[cfg(test)]
        super::faults::check(super::faults::Point::ObjectDataSynced)?;
        let mut state = self.backend.state()?;
        if let Some(existing) = state.objects.get(&key) {
            if existing != &self.bytes {
                return Err(BackendError::IdentityMismatch(key));
            }
        } else {
            state.objects.insert(key, self.bytes);
        }
        Ok(())
    }
}
impl StorageBackend for MemoryBackend {
    fn identity(&self) -> BackendId {
        self.identity
    }
    fn begin_write(&self) -> Result<Box<dyn ObjectWriter + '_>, BackendError> {
        Ok(Box::new(MemoryWriter {
            backend: self,
            bytes: Vec::new(),
        }))
    }
    fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError> {
        check_batch(requests)?;
        let state = self.state()?;
        requests
            .iter()
            .map(|request| {
                let bytes = state
                    .objects
                    .get(&request.key)
                    .ok_or(BackendError::Missing(request.key))?;
                let begin = request
                    .range
                    .offset()
                    .as_usize()
                    .map_err(|_| BackendError::Range)?;
                let end = request
                    .range
                    .end()
                    .as_usize()
                    .map_err(|_| BackendError::Range)?;
                Ok(bytes.get(begin..end).ok_or(BackendError::Range)?.to_vec())
            })
            .collect()
    }
    fn stat(&self, key: ObjectKey) -> Result<Option<StoredBytes>, BackendError> {
        Ok(self
            .state()?
            .objects
            .get(&key)
            .map(|bytes| StoredBytes::new(bytes.len() as u64)))
    }
    fn read_root(&self) -> Result<Option<RootRecord>, BackendError> {
        Ok(self.state()?.root.clone())
    }
    fn compare_exchange_root(
        &self,
        expected: Option<&Revision>,
        bytes: &[u8],
    ) -> Result<Publication, BackendError> {
        let mut state = self.state()?;
        if state.root.as_ref().map(RootRecord::revision) != expected {
            return Ok(Publication::Stale);
        }
        let revision = Revision::new(nonce()?.to_vec())?;
        state.root = Some(RootRecord::new(revision.clone(), bytes.to_vec())?);
        Ok(Publication::Applied(revision))
    }
    fn inventory(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, BackendError> {
        if limit == 0 || limit > 4096 {
            return Err(BackendError::Range);
        }
        Ok(self
            .state()?
            .objects
            .keys()
            .copied()
            .filter(|key| after.is_none_or(|after| *key > after))
            .take(limit)
            .collect())
    }
    fn delete(&self, permit: DeletePermit<'_>) -> Result<(), BackendError> {
        self.state()?
            .objects
            .remove(&permit.authorize(self.identity)?);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    Put,
    Read,
    Stat,
    RootRead,
    Publish,
    Inventory,
    Delete,
}
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    Before,
    AfterPublish,
    CorruptRead,
    ShortRead,
}
#[derive(Clone, Debug, Default)]
pub struct BackendStatistics {
    /// Begun streams, including streams abandoned without installing an object.
    pub write_starts: u64,
    pub puts: u64,
    pub reads: u64,
    pub batches: u64,
    pub read_bytes: u64,
    pub blob_reads: u64,
    pub blob_read_bytes: u64,
    pub stat_calls: u64,
    pub blob_stat_calls: u64,
    pub root_reads: u64,
    pub inventory_calls: u64,
    pub written_bytes: u64,
    pub publications: u64,
    pub deletes: u64,
    pub put_keys: Vec<ObjectKey>,
}
/// Deterministic one-shot transport fault injection and operation accounting.
/// The wrapped backend owns durable state independently of this wrapper.
pub struct FaultBackend {
    inner: Arc<dyn StorageBackend>,
    fault: Mutex<Option<(Operation, usize, Fault)>>,
    statistics: Mutex<BackendStatistics>,
}
impl FaultBackend {
    #[must_use]
    pub fn new(inner: Arc<dyn StorageBackend>) -> Self {
        Self {
            inner,
            fault: Mutex::new(None),
            statistics: Mutex::new(BackendStatistics::default()),
        }
    }
    pub fn inject(&self, operation: Operation, call: usize, fault: Fault) {
        *self.fault.lock().expect("fault lock") = Some((operation, call.max(1), fault));
    }
    #[must_use]
    pub fn statistics(&self) -> BackendStatistics {
        self.statistics.lock().expect("statistics lock").clone()
    }
    pub fn reset_statistics(&self) {
        *self.statistics.lock().expect("statistics lock") = BackendStatistics::default();
    }
    fn check(&self, operation: Operation) -> Result<Option<Fault>, BackendError> {
        let mut slot = self.fault.lock().map_err(|_| BackendError::InvalidData)?;
        let Some((selected, remaining, fault)) = slot.as_mut() else {
            return Ok(None);
        };
        if *selected != operation {
            return Ok(None);
        }
        *remaining -= 1;
        if *remaining != 0 {
            return Ok(None);
        }
        let fault = *fault;
        *slot = None;
        if matches!(fault, Fault::Before) {
            return Err(BackendError::Transport {
                message: format!("injected {operation:?}"),
                retryable: true,
            });
        }
        Ok(Some(fault))
    }
}
struct FaultWriter<'a> {
    backend: &'a FaultBackend,
    inner: Box<dyn ObjectWriter + 'a>,
}
impl Write for FaultWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
impl ObjectWriter for FaultWriter<'_> {
    fn finish(self: Box<Self>, key: ObjectKey, length: StoredBytes) -> Result<(), BackendError> {
        self.inner.finish(key, length)?;
        let mut statistics = self
            .backend
            .statistics
            .lock()
            .map_err(|_| BackendError::InvalidData)?;
        statistics.puts += 1;
        statistics.written_bytes += length.get();
        statistics.put_keys.push(key);
        Ok(())
    }
}
impl StorageBackend for FaultBackend {
    fn identity(&self) -> BackendId {
        self.inner.identity()
    }
    fn begin_write(&self) -> Result<Box<dyn ObjectWriter + '_>, BackendError> {
        self.check(Operation::Put)?;
        let inner = self.inner.begin_write()?;
        self.statistics
            .lock()
            .map_err(|_| BackendError::InvalidData)?
            .write_starts += 1;
        Ok(Box::new(FaultWriter {
            backend: self,
            inner,
        }))
    }
    fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError> {
        let fault = self.check(Operation::Read)?;
        let mut result = self.inner.read_ranges(requests)?;
        let mut statistics = self
            .statistics
            .lock()
            .map_err(|_| BackendError::InvalidData)?;
        statistics.reads += requests.len() as u64;
        statistics.batches += 1;
        statistics.read_bytes += result.iter().map(|bytes| bytes.len() as u64).sum::<u64>();
        for (request, bytes) in requests.iter().zip(&result) {
            if matches!(request.key(), ObjectKey::Blob(_)) {
                statistics.blob_reads += 1;
                statistics.blob_read_bytes += bytes.len() as u64;
            }
        }
        match fault {
            Some(Fault::CorruptRead) => {
                if let Some(byte) = result.first_mut().and_then(|bytes| bytes.first_mut()) {
                    *byte ^= 1;
                }
            }
            Some(Fault::ShortRead) => {
                if let Some(bytes) = result.first_mut() {
                    bytes.pop();
                }
            }
            _ => {}
        }
        Ok(result)
    }
    fn stat(&self, key: ObjectKey) -> Result<Option<StoredBytes>, BackendError> {
        self.check(Operation::Stat)?;
        let mut statistics = self
            .statistics
            .lock()
            .map_err(|_| BackendError::InvalidData)?;
        statistics.stat_calls += 1;
        statistics.blob_stat_calls += u64::from(matches!(key, ObjectKey::Blob(_)));
        drop(statistics);
        self.inner.stat(key)
    }
    fn read_root(&self) -> Result<Option<RootRecord>, BackendError> {
        self.check(Operation::RootRead)?;
        self.statistics
            .lock()
            .map_err(|_| BackendError::InvalidData)?
            .root_reads += 1;
        self.inner.read_root()
    }
    fn compare_exchange_root(
        &self,
        expected: Option<&Revision>,
        bytes: &[u8],
    ) -> Result<Publication, BackendError> {
        let fault = self.check(Operation::Publish)?;
        let result = self.inner.compare_exchange_root(expected, bytes)?;
        self.statistics
            .lock()
            .map_err(|_| BackendError::InvalidData)?
            .publications += 1;
        if matches!(fault, Some(Fault::AfterPublish)) && matches!(result, Publication::Applied(_)) {
            return Ok(Publication::Uncertain("injected after publication".into()));
        }
        Ok(result)
    }
    fn inventory(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, BackendError> {
        self.check(Operation::Inventory)?;
        self.statistics
            .lock()
            .map_err(|_| BackendError::InvalidData)?
            .inventory_calls += 1;
        self.inner.inventory(after, limit)
    }
    fn delete(&self, permit: DeletePermit<'_>) -> Result<(), BackendError> {
        self.check(Operation::Delete)?;
        self.inner.delete(permit)?;
        self.statistics
            .lock()
            .map_err(|_| BackendError::InvalidData)?
            .deletes += 1;
        Ok(())
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;

    #[test]
    fn large_staged_file_is_installed_without_copying_or_limiting_offsets()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::domain::{BlobBytes, BlobOffset, FileOffset};
        let directory = tempfile::tempdir()?;
        let backend = FilesystemBackend::open(directory.path())?;
        let staging = tempfile::NamedTempFile::new_in(backend.root.join("objects"))?;
        let offset = 1024 * 1024 * 1024_u64;
        // A sparse staging file exercises >512 MiB offsets without allocating
        // or writing a gigabyte. Finalization must install this exact inode.
        staging.as_file().set_len(offset + 4)?;
        crate::fs::write_all_at(staging.as_file(), offset, b"tail")?;
        #[cfg(unix)]
        let inode = {
            use std::os::unix::fs::MetadataExt;
            staging.as_file().metadata()?.ino()
        };
        let key = ObjectKey::Blob(BlobId::from_bytes([9; 32]));
        let writer = FilesystemWriter {
            backend: &backend,
            staging,
        };
        Box::new(writer).finish(key, StoredBytes::new(offset + 4))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(backend.object_path(key).metadata()?.ino(), inode);
        }
        let range = ObjectRange::new(
            key,
            StoredRange::new(FileOffset::new(offset), StoredBytes::new(4))?,
        )?;
        assert_eq!(backend.read_ranges(&[range])?, [b"tail".to_vec()]);
        super::super::BlobExtent::new(
            BlobId::from_bytes([9; 32]),
            BlobOffset::new(offset),
            BlobBytes::new(4),
            BlobBytes::new(offset + 4),
            StoredBytes::new(4),
        )?;
        Ok(())
    }
}
