//! Transactional page storage built from mutable active and immutable sealed segments.

#![forbid(unsafe_code)]

use crate::DictionaryPolicy;
use crate::backend::{LocalCoordination, sidecar_dir};
use crate::format::{
    ACTIVE_HEADER_SIZE, ACTIVE_MAGIC, ACTIVE_METADATA_SIZE, ACTIVE_STATE_SIZE, ActiveHeader,
    ActiveState, Codec, DatabaseId, DictionaryPolicyRecord, Digest, FRAME_HEADER_SIZE, FrameHeader,
    StoragePolicyRecord, genesis_history, valid_page_size,
};
use crate::fs::{
    absolute_path, allocated_bytes, lock_exclusive, lock_shared, read_exact_at, sync_file,
    sync_parent_dir, unlock_file, write_all_at,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const ACTIVE_STATE_A_OFFSET: u64 = ACTIVE_HEADER_SIZE as u64;
const ACTIVE_STATE_B_OFFSET: u64 = ACTIVE_STATE_A_OFFSET + ACTIVE_STATE_SIZE as u64;
const ACTIVE_METADATA_END: u64 = ACTIVE_METADATA_SIZE as u64;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error(transparent)]
    Backend(#[from] crate::storage::adapter::BackendError),
    #[error("storage has no finalized sealed database head")]
    NoSealedHead,
    #[error("the local pagefile belongs to a superseded storage attachment")]
    StaleAttachment,
    #[error(transparent)]
    Value(#[from] crate::domain::ValueError),
    #[error("publication may have become visible; reload before retrying: {0}")]
    PublicationUncertain(std::io::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid zsqlite metadata: {0}")]
    Format(#[from] crate::format::FormatError),
    #[error("database file exists but its sidecar directory is missing")]
    MissingSidecar,
    #[error("database file and sealed segments have different identities")]
    IdentityMismatch,
    #[error("database is corrupt at byte {0}; no automatic repair was attempted")]
    Corrupt(u64),
    #[error("database is busy in another process")]
    Busy,
    #[error("cannot determine SQLite page size from the first page")]
    UnknownPageSize,
    #[error("invalid SQLite page size {0}")]
    InvalidPageSize(u32),
    #[error("page {page_no} has stored length {actual}, expected {expected}")]
    InvalidPageLength {
        page_no: u32,
        actual: usize,
        expected: usize,
    },
    #[error("page {0} has a BLAKE3 mismatch")]
    PageChecksum(u32),
    #[error("numeric or allocation limit exceeded")]
    Range,
    #[error("zstd error: {0}")]
    Zstd(String),
    #[error("database is not a current-format zsqlite database")]
    NotZsqlite,
    #[error("database was opened read-only")]
    ReadOnly,
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("retention name already exists: {0}")]
    RetentionExists(String),
    #[error("input is not a complete, page-aligned SQLite database")]
    InvalidStandardDatabase,
    #[error("unsupported filesystem or platform operation")]
    Unsupported,
    #[error("invalid storage configuration: {0}")]
    InvalidConfiguration(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoragePolicy {
    settle: Duration,
    max_stale: Duration,
    /// Seal the active file at a committed boundary once its raw records
    /// reach this many bytes. Zero disables size-triggered sealing.
    rollover_bytes: u64,
    dictionary: DictionaryPolicy,
    layout: crate::layout::LayoutPolicy,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            settle: Duration::from_secs(5 * 60),
            max_stale: Duration::from_secs(60 * 60),
            rollover_bytes: 0,
            dictionary: DictionaryPolicy::default(),
            layout: crate::layout::LayoutPolicy::default(),
        }
    }
}

impl StoragePolicy {
    pub fn with_timing(
        mut self,
        settle: Duration,
        max_stale: Duration,
    ) -> Result<Self, StoreError> {
        if settle.subsec_nanos() != 0 || max_stale.subsec_nanos() != 0 {
            return Err(StoreError::InvalidConfiguration(
                "maintenance durations must be whole seconds",
            ));
        }
        self.settle = settle;
        self.max_stale = max_stale;
        self.encode()?;
        Ok(self)
    }
    pub fn with_rollover(
        mut self,
        bytes: Option<std::num::NonZeroU64>,
    ) -> Result<Self, StoreError> {
        self.rollover_bytes = bytes.map_or(0, std::num::NonZeroU64::get);
        self.encode()?;
        Ok(self)
    }
    #[must_use]
    pub fn with_dictionary(mut self, dictionary: DictionaryPolicy) -> Self {
        self.dictionary = dictionary;
        self
    }
    #[must_use]
    pub const fn settle(self) -> Duration {
        self.settle
    }
    #[must_use]
    pub const fn max_stale(self) -> Duration {
        self.max_stale
    }
    #[must_use]
    pub const fn rollover_bytes(self) -> Option<std::num::NonZeroU64> {
        std::num::NonZeroU64::new(self.rollover_bytes)
    }
    #[must_use]
    pub const fn dictionary(self) -> DictionaryPolicy {
        self.dictionary
    }
    #[must_use]
    pub fn with_layout(mut self, layout: crate::layout::LayoutPolicy) -> Self {
        self.layout = layout;
        self
    }
    #[must_use]
    pub fn layout(self) -> crate::layout::LayoutPolicy {
        self.layout
    }
    fn encode(self) -> Result<StoragePolicyRecord, StoreError> {
        let seconds = |value: Duration| {
            u32::try_from(value.as_secs())
                .map_err(|_| StoreError::InvalidConfiguration("duration is too large"))
        };
        let record = StoragePolicyRecord {
            settle_seconds: seconds(self.settle)?,
            max_stale_seconds: seconds(self.max_stale)?,
            rollover_bytes: self.rollover_bytes,
            dictionary: DictionaryPolicyRecord {
                dictionary_bytes: self.dictionary.dictionary_bytes(),
                sample_bytes: self.dictionary.sample_bytes(),
            },
        };
        validate_policy(record)?;
        Ok(record)
    }

    fn decode(value: StoragePolicyRecord, layout: crate::layout::LayoutPolicy) -> Self {
        Self {
            settle: Duration::from_secs(u64::from(value.settle_seconds)),
            layout,
            max_stale: Duration::from_secs(u64::from(value.max_stale_seconds)),
            rollover_bytes: value.rollover_bytes,
            dictionary: DictionaryPolicy::new(
                value.dictionary.dictionary_bytes,
                value.dictionary.sample_bytes,
            )
            .expect("validated storage policy"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Inspect {
    pub path: PathBuf,
    pub sidecar_path: PathBuf,
    pub page_size: u32,
    pub page_count: u32,
    pub logical_size: u64,
    pub head_txid: u64,
    /// Sealed lineage digest; mutable publications only advance `head_txid`.
    pub head_history: Digest,
    pub pack_count: usize,
    pub active: bool,
    pub file_bytes: u64,
    pub file_allocated_bytes: u64,
    pub sealed_object_bytes: u64,
    pub sealed_object_allocated_bytes: u64,
    pub indexed_pages: usize,
    pub dictionary_bytes: usize,
    pub preferred_dictionaries: usize,
    pub frame_distribution: Vec<crate::storage::FrameDistribution>,
    pub manifest: Option<crate::storage::ManifestStatistics>,
    pub pack_occupancy: Vec<crate::storage::PackOccupancy>,
    pub retention: crate::storage::GcReport,
    pub policy: StoragePolicy,
}

enum LocalRead {
    Zero(crate::domain::PageSize),
    Raw(Vec<u8>),
}
impl LocalRead {
    fn into_bytes(self) -> Vec<u8> {
        match self {
            Self::Zero(size) => vec![0; size.as_usize()],
            Self::Raw(bytes) => bytes,
        }
    }
}

#[derive(Debug)]
struct BootstrapFile {
    file: File,
    path: PathBuf,
}

impl Drop for BootstrapFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

struct CleanupFile(PathBuf);

impl Drop for CleanupFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Debug)]
struct ActiveFile {
    file: File,
    header: ActiveHeader,
    state_sequence: u64,
    active_records: BTreeMap<u32, u64>,
    truncate_pages: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeadState {
    page_size: u32,
    logical_size: u64,
    txid: u64,
    history: Digest,
    active_commit_offset: u64,
    active_commit_end: u64,
    oldest_dirty_unix: u64,
    last_dirty_unix: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationPhase {
    Unlocked,
    Transaction,
    Checkpoint,
    Maintenance,
}

#[derive(Debug)]
enum PublicationOwner {
    None,
    Transaction(crate::fs::ExclusiveLock),
    Checkpoint(crate::fs::ExclusiveLock),
    Maintenance(crate::fs::ExclusiveLock),
}

impl PublicationOwner {
    fn is_locked(&self) -> bool {
        !matches!(self, Self::None)
    }
    fn phase(&self) -> PublicationPhase {
        match self {
            Self::None => PublicationPhase::Unlocked,
            Self::Transaction(_) => PublicationPhase::Transaction,
            Self::Checkpoint(_) => PublicationPhase::Checkpoint,
            Self::Maintenance(_) => PublicationPhase::Maintenance,
        }
    }
    fn guard(&self) -> Result<&crate::fs::ExclusiveLock, StoreError> {
        match self {
            Self::None => Err(StoreError::Busy),
            Self::Transaction(guard) | Self::Checkpoint(guard) | Self::Maintenance(guard) => {
                Ok(guard)
            }
        }
    }
    fn transition(&mut self, phase: PublicationPhase) {
        let previous = std::mem::replace(self, Self::None);
        let guard = match previous {
            Self::None => return,
            Self::Transaction(guard) | Self::Checkpoint(guard) | Self::Maintenance(guard) => guard,
        };
        *self = match phase {
            PublicationPhase::Unlocked => Self::None,
            PublicationPhase::Transaction => Self::Transaction(guard),
            PublicationPhase::Maintenance => Self::Maintenance(guard),
            PublicationPhase::Checkpoint => Self::Checkpoint(guard),
        };
    }
}

/// Consumed only after releasing durable construction borrows. Failed active
/// installation must release catalog exclusion before reloading visible state.
#[must_use]
#[allow(clippy::large_enum_variant)] // One bounded, short-lived publication; no heap indirection needed.
enum ViewPublication {
    Installed {
        active: ActiveFile,
        view: crate::storage::PinnedView,
    },
    ReloadRequired(StoreError),
}

#[derive(Clone, Copy)]
enum ActiveBase<'view, 'catalog> {
    Genesis,
    Existing(&'view crate::storage::PinnedView),
    Installed(&'view crate::storage::DurableView<'catalog>),
}

impl ActiveBase<'_, '_> {
    fn validate(&self, database: DatabaseId, head: HeadState) -> Result<Digest, StoreError> {
        let (id, endpoint) = match self {
            Self::Genesis => {
                if head.txid != 0 || head.logical_size != 0 || head.history != genesis_history() {
                    return Err(StoreError::IdentityMismatch);
                }
                return Ok([0; 32]);
            }
            Self::Existing(view) => {
                let (database, txid, history) = view.endpoint();
                (view.id(), (database, view.logical_size(), txid, history))
            }
            Self::Installed(view) => (view.id(), view.endpoint()),
        };
        if endpoint.0.as_bytes() != &database
            || endpoint.1.get() != head.logical_size
            || endpoint.2.get() != head.txid
            || (!matches!(self, Self::Installed(_)) && endpoint.3.as_bytes() != &head.history)
        {
            return Err(StoreError::IdentityMismatch);
        }
        Ok(*id.as_bytes())
    }
}

/// Internal page store. `SQLite`'s ordinary lock file protocol surrounds access.
pub(crate) struct Store {
    path: PathBuf,
    sidecar_path: PathBuf,
    coordination: LocalCoordination,
    publication: File,
    _lifecycle: File,
    writable: bool,
    publication_owner: PublicationOwner,
    head: HeadState,
    view: Option<crate::storage::PinnedView>,
    layout: crate::layout::LayoutPolicy,
    page_cache: crate::storage::PageCache,
    read_io: crate::statistics::HandleIoStats,
    active: ActiveFile,
    pending_raw_pages: BTreeSet<u32>,
    pending_raw_originals: BTreeMap<u32, Vec<u8>>,
    pending_size: u64,
    pending_truncate_pages: Option<u32>,
    pending_dirty: bool,
    bootstrap: Option<BootstrapFile>,
}

impl Store {
    pub(crate) fn open_existing(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_mode(path.as_ref(), false, true)
    }

    pub(crate) fn open_existing_read_only(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_mode(path.as_ref(), false, false)
    }

    pub(crate) fn open(path: impl AsRef<Path>, create: bool) -> Result<Self, StoreError> {
        Self::open_mode(path.as_ref(), create, true)
    }

    fn open_mode(path: &Path, create: bool, writable: bool) -> Result<Self, StoreError> {
        let path = absolute_path(path)?;
        let existed = path.exists();
        if !existed && let Some(storage) = crate::storage::Storage::bound(&sidecar_dir(&path))? {
            match storage.bootstrap(&path) {
                Ok(database) => {
                    let mut store = database.into_store()?;
                    store.writable = writable;
                    return Ok(store);
                }
                Err(StoreError::NoSealedHead) if create && writable => {}
                Err(error) => return Err(error),
            }
        }

        if existed
            && path.metadata()?.len() != 0
            && !sidecar_dir(&path).exists()
            && !crate::storage::Storage::is_bound(&sidecar_dir(&path))?
        {
            let mut magic = [0; 8];
            let file = File::open(&path)?;
            if read_exact_at(&file, 0, &mut magic).is_ok() && magic == *ACTIVE_MAGIC {
                return Err(StoreError::MissingSidecar);
            }
            return Err(StoreError::NotZsqlite);
        }
        if !existed && (!create || !writable) {
            return Err(StoreError::NotZsqlite);
        }
        let mut options = OpenOptions::new();
        options
            .read(true)
            .write(writable)
            .create(create && writable);
        let file = options.open(&path)?;
        if !existed || file.metadata()?.len() == 0 {
            if !create || !writable {
                return Err(StoreError::NotZsqlite);
            }
            return Self::initialize(path, file);
        }
        let header = read_active_header(&file)?;
        reject_aliased_active(&file)?;
        let coordination = LocalCoordination::open(sidecar_dir(&path), false)?;
        let publication = open_lock(&coordination.lock_path("publication"), writable, false)?;
        let lifecycle = open_lock(&coordination.lock_path("lifecycle"), writable, false)?;
        lock_shared(&lifecycle, false)?;
        let mut store = Self::blank(
            path,
            file,
            header,
            coordination,
            publication,
            lifecycle,
            writable,
        );
        store.reload()?;
        Ok(store)
    }

    fn initialize(path: PathBuf, file: File) -> Result<Self, StoreError> {
        let mut database_id = random_bytes()?;
        let coordination = LocalCoordination::open(sidecar_dir(&path), true)?;
        let catalog = crate::storage::Catalog::open(&sidecar_dir(&path), true)?;
        let publication = open_lock(&coordination.lock_path("publication"), true, true)?;
        let lifecycle = open_lock(&coordination.lock_path("lifecycle"), true, true)?;
        // SQLite's parent VFS uses a distinct, stable inode for its native
        // locking protocol. The database file is also opened by Store maintenance
        // APIs, and POSIX fcntl locks are process-associated: closing any fd
        // for the locked inode would otherwise release SQLite's locks.
        drop(open_lock(&coordination.lock_path("sqlite"), true, true)?);
        File::open(coordination.lock_dir())?.sync_all()?;
        lock_exclusive(&publication, false)?;
        if file.metadata()?.len() != 0 {
            unlock_file(&publication)?;
            drop(publication);
            drop(coordination);
            drop(file);
            return Self::open_mode(&path, false, true);
        }
        let guard = catalog.lock()?;
        if let Some(existing) = guard.namespace_database() {
            database_id = *existing.as_bytes();
        }
        let attachment =
            guard.initialize_namespace(crate::domain::DatabaseId::from_bytes(database_id))?;
        let policy = StoragePolicy::default().encode()?;
        let header = ActiveHeader {
            attachment_id: *attachment.as_bytes(),
            database_id,
            page_size: 0,
            start_txid: 1,
            base_history: genesis_history(),
            parent_physical_digest: [0; 32],
            base_logical_size: 0,
            policy,
            layout: crate::layout::LayoutPolicy::default(),
        };
        write_all_at(&file, 0, &header.encode())?;
        write_all_at(
            &file,
            ACTIVE_STATE_A_OFFSET,
            &ActiveState {
                database_id,
                sequence: 1,
                txid: 0,
                logical_size: 0,
                page_size: 0,
                history: genesis_history(),
                commit_unix: 0,
                record_count: 0,
                truncate_pages: None,
            }
            .encode(),
        )?;
        file.set_len(ACTIVE_METADATA_END)?;
        file.sync_all()?;
        sync_parent_dir(&path)?;
        // Establish the lifetime lease before releasing publication. Without
        // this handoff, a racing xDelete could remove the freshly initialized
        // bundle in the gap and leave the successful opener attached to
        // unlinked files.
        lock_shared(&lifecycle, false)?;
        unlock_file(&publication)?;
        drop(guard);
        let mut store = Self::blank(
            path,
            file,
            header,
            coordination,
            publication,
            lifecycle,
            true,
        );
        store.reload()?;
        Ok(store)
    }

    #[allow(clippy::large_types_passed_by_value)]
    fn blank(
        path: PathBuf,
        file: File,
        header: ActiveHeader,
        coordination: LocalCoordination,
        publication: File,
        lifecycle: File,
        writable: bool,
    ) -> Self {
        Self {
            sidecar_path: sidecar_dir(&path),
            path,
            coordination,
            publication,
            _lifecycle: lifecycle,
            writable,
            publication_owner: PublicationOwner::None,
            head: HeadState {
                page_size: 0,
                logical_size: 0,
                txid: 0,
                history: genesis_history(),
                active_commit_offset: 0,
                active_commit_end: 0,
                oldest_dirty_unix: 0,
                last_dirty_unix: 0,
            },
            view: None,
            layout: crate::layout::LayoutPolicy::default(),
            page_cache: crate::storage::PageCache::new(
                crate::layout::LayoutPolicy::default().cache(),
            )
            .expect("constant cache budget"),
            read_io: crate::statistics::HandleIoStats::default(),
            active: ActiveFile {
                file,
                header,
                state_sequence: 1,
                active_records: BTreeMap::new(),
                truncate_pages: None,
            },
            pending_raw_pages: BTreeSet::new(),
            pending_raw_originals: BTreeMap::new(),
            pending_size: 0,
            pending_truncate_pages: None,
            pending_dirty: false,
            bootstrap: None,
        }
    }

    pub(crate) fn from_bootstrap(
        path: PathBuf,
        header: &ActiveHeader,
        view: crate::storage::PinnedView,
        lifecycle: File,
        _publication: &crate::fs::ExclusiveLock,
    ) -> Result<Self, StoreError> {
        let coordination = LocalCoordination::open(sidecar_dir(&path), false)?;
        let publication = open_lock(&coordination.lock_path("publication"), true, false)?;
        drop(open_lock(&coordination.lock_path("sqlite"), true, true)?);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let current = file.try_clone()?;
        let mut store = Self::blank(
            path,
            file,
            *header,
            coordination,
            publication,
            lifecycle,
            true,
        );
        store.load_local(current, *header, Some(view))?;
        Ok(store)
    }

    pub(crate) fn delete_bundle(path: impl AsRef<Path>) -> Result<(), StoreError> {
        let path = absolute_path(path.as_ref())?;
        let file = File::open(&path)?;
        let header = read_active_header(&file)?;
        let sidecar = sidecar_dir(&path);
        if !sidecar.exists() && !crate::storage::Storage::is_bound(&sidecar)? {
            // A file-first conversion or the final local cleanup of a delete
            // can leave only its recognizable pagefile. No backend objects
            // exist here and no physical deletion permit is needed.
            std::fs::remove_file(&path)?;
            return sync_parent_dir(&path);
        }
        let catalog = crate::storage::Catalog::open(&sidecar, false)?;
        let storage = crate::storage::Storage::for_sidecar(&sidecar, false)?;
        let lifecycle = crate::fs::ExclusiveLock::acquire(
            &storage
                .coordination_directory()
                .join("locks/lifecycle.lock"),
            true,
        )?;
        let _publication = crate::fs::ExclusiveLock::acquire(
            &storage
                .coordination_directory()
                .join("locks/publication.lock"),
            true,
        )?;
        let guard = catalog.lock()?;
        guard.destroy(&header, &lifecycle)?;
        std::fs::remove_file(&path)?;
        sync_parent_dir(&path)?;
        // Only the default backend shares its namespace with the sidecar.
        // Its immutable objects were deleted with permits above; the remaining
        // empty root and coordination files can now be removed as a bundle.
        if !crate::storage::Storage::is_bound(&sidecar)?
            || (sidecar != storage.coordination_directory() && sidecar.exists())
        {
            std::fs::remove_dir_all(&sidecar)?;
        }
        sync_parent_dir(&path)
    }

    pub(crate) fn upgrade_writable(&mut self) -> Result<bool, StoreError> {
        if self.writable {
            return Ok(false);
        }

        // Stage every fallible open first so an error leaves this instance
        // consistently read-only.
        let active = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let publication = open_lock(&self.coordination.lock_path("publication"), true, false)?;

        self.publication = publication;
        self.active.file = active;
        self.writable = true;
        Ok(true)
    }

    pub(crate) fn logical_size(&self) -> u64 {
        self.pending_size
    }

    pub(crate) fn sqlite_lock_path(&self) -> PathBuf {
        self.coordination.lock_path("sqlite")
    }

    pub(crate) fn database_has_moved(&self) -> Result<bool, StoreError> {
        let open = self.active.file.metadata()?;
        let current = match self.path.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error.into()),
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if open.dev() == current.dev() && open.ino() == current.ino() {
                return Ok(false);
            }
            // Rollover deliberately replaces the active pathname. It is not a
            // SQLite "moved database" event when the new inode continues the
            // same authenticated lineage.
            let current_file = File::open(&self.path)?;
            Ok(match read_active_header(&current_file) {
                Ok(header) => header.database_id != self.active.header.database_id,
                Err(_) => true,
            })
        }
        #[cfg(not(unix))]
        {
            // Store's writable/locking implementation is currently Unix-only.
            // Keep the conservative pathname-exists result for other targets.
            let _ = (open, current);
            Ok(false)
        }
    }

    fn current_inode_changed(&self) -> Result<bool, StoreError> {
        let open = self.active.file.metadata()?;
        let current = self.path.metadata()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Ok(open.dev() != current.dev() || open.ino() != current.ino())
        }
        #[cfg(not(unix))]
        {
            let _ = (open, current);
            Ok(false)
        }
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.pending_dirty
            || !self.pending_raw_pages.is_empty()
            || !self.pending_raw_originals.is_empty()
            || self.bootstrap.is_some()
    }

    pub(crate) fn refresh(&mut self) -> Result<(), StoreError> {
        if self.has_pending() {
            return Ok(());
        }
        let mutable_advanced =
            read_active_state(&self.active.file, self.active.header.database_id)?.sequence
                > self.active.state_sequence;
        if mutable_advanced
            || self.current_inode_changed()?
            || self.active.file.metadata()?.len() > self.head.active_commit_end
        {
            self.reload()?;
        }
        Ok(())
    }

    fn begin_write(&mut self) -> Result<(), StoreError> {
        if !self.writable {
            return Err(StoreError::ReadOnly);
        }
        if !self.publication_owner.is_locked() {
            // SQLite's own busy handler cannot interrupt a blocking flock
            // hidden inside xWrite. Return BUSY instead of allowing a paused
            // maintenance process to wedge the entire connection forever.
            self.publication_owner = PublicationOwner::Transaction(
                crate::fs::ExclusiveLock::on_file(&self.publication, true)?,
            );
            if let Err(error) = self.reload().and_then(|()| {
                if self.active.file.metadata()?.len() > self.head.active_commit_end {
                    self.active.file.set_len(self.head.active_commit_end)?;
                }
                Ok(())
            }) {
                self.release_publication();
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn discard_pending(&mut self) {
        self.discard_mutable_pending();
    }

    pub(crate) fn read_at(&mut self, offset: u64, output: &mut [u8]) -> Result<usize, StoreError> {
        output.fill(0);
        if output.is_empty() || offset >= self.pending_size {
            return Ok(0);
        }
        let actual = usize::try_from((self.pending_size - offset).min(output.len() as u64))
            .map_err(|_| StoreError::Range)?;
        if self.head.page_size == 0 {
            if let Some(bootstrap) = &self.bootstrap {
                let available = bootstrap.file.metadata()?.len().saturating_sub(offset);
                let copied = actual.min(usize::try_from(available).unwrap_or(usize::MAX));
                if copied != 0 {
                    read_exact_at(&bootstrap.file, offset, &mut output[..copied])?;
                }
            }
            return Ok(actual);
        }
        if actual > self.head.page_size as usize {
            return self.read_batched(offset, &mut output[..actual]);
        }
        let page_size = self.head.page_size as usize;
        let page_size_u64 = u64::from(self.head.page_size);
        let mut copied = 0_usize;
        while copied < actual {
            let logical = offset
                .checked_add(u64::try_from(copied).map_err(|_| StoreError::Range)?)
                .ok_or(StoreError::Range)?;
            let page_no = u32::try_from(
                logical
                    .checked_div(page_size_u64)
                    .and_then(|page| page.checked_add(1))
                    .ok_or(StoreError::Range)?,
            )
            .map_err(|_| StoreError::Range)?;
            let within = usize::try_from(logical % page_size_u64).map_err(|_| StoreError::Range)?;
            let amount = (actual - copied).min(page_size - within);
            let page = self.read_page(page_no)?;
            output[copied..copied + amount].copy_from_slice(&page[within..within + amount]);
            copied += amount;
        }
        Ok(actual)
    }

    fn read_batched(&mut self, offset: u64, output: &mut [u8]) -> Result<usize, StoreError> {
        let size = u64::from(self.head.page_size);
        let mut copied = 0;
        while copied < output.len() {
            let logical = offset.checked_add(copied as u64).ok_or(StoreError::Range)?;
            let first = logical / size + 1;
            let end = (offset
                .checked_add(output.len() as u64)
                .ok_or(StoreError::Range)?
                - 1)
                / size
                + 1;
            let last = end.min(first + 255);
            let mut pages = BTreeMap::new();
            let mut sealed = Vec::new();
            for number in first..=last {
                let number = crate::domain::PageNumber::new(
                    u32::try_from(number).map_err(|_| StoreError::Range)?,
                )?;
                if let Some(page) = self.read_local_page(number.get())? {
                    if let LocalRead::Raw(bytes) = &page {
                        self.read_io.fetched_bytes = self
                            .read_io
                            .fetched_bytes
                            .saturating_add(bytes.len() as u64 + FRAME_HEADER_SIZE as u64);
                    }
                    pages.insert(number, page.into_bytes());
                } else {
                    sealed.push(number);
                }
            }
            if !sealed.is_empty() {
                let view = self.view.as_ref().ok_or(StoreError::Corrupt(0))?;
                let active = &self.active;
                let truncate = self
                    .pending_truncate_pages
                    .into_iter()
                    .chain(active.truncate_pages)
                    .min();
                let fetched =
                    self.page_cache
                        .read_many(view, &sealed, &mut self.read_io, |page| {
                            active.active_records.contains_key(&page.get())
                                || truncate.is_some_and(|last| page.get() > last)
                        })?;
                pages.extend(sealed.into_iter().zip(fetched));
            }
            for (_, page) in pages {
                let within = usize::try_from((offset + copied as u64) % size)
                    .map_err(|_| StoreError::Range)?;
                let count = (output.len() - copied).min(page.len() - within);
                output[copied..copied + count].copy_from_slice(&page[within..within + count]);
                copied += count;
            }
        }
        Ok(copied)
    }

    pub(crate) fn write_at(&mut self, offset: u64, input: &[u8]) -> Result<usize, StoreError> {
        if input.is_empty() {
            return Ok(0);
        }
        self.begin_write()?;
        let end = offset
            .checked_add(input.len() as u64)
            .ok_or(StoreError::Range)?;
        if self.head.page_size == 0 {
            self.write_bootstrap(offset, input, end)?;
            return Ok(input.len());
        }
        self.write_mutable_pages(offset, input)?;
        self.pending_size = self.pending_size.max(end);
        self.pending_dirty = true;
        Ok(input.len())
    }

    fn write_bootstrap(&mut self, offset: u64, input: &[u8], end: u64) -> Result<(), StoreError> {
        if self.bootstrap.is_none() {
            let id: [u8; 16] = random_bytes()?;
            let path = active_staging_path(&self.path, id).with_extension("zbootstrap");
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)?;
            self.bootstrap = Some(BootstrapFile { file, path });
        }
        let bootstrap = self.bootstrap.as_ref().ok_or(StoreError::Range)?;
        write_all_at(&bootstrap.file, offset, input)?;
        if bootstrap.file.metadata()?.len() < end {
            bootstrap.file.set_len(end)?;
        }
        self.pending_size = self.pending_size.max(end);
        self.pending_dirty = true;
        let mut header = [0; 100];
        if bootstrap.file.metadata()?.len() >= 100 {
            read_exact_at(&bootstrap.file, 0, &mut header)?;
        }
        if header[..16] == *SQLITE_MAGIC {
            let page_size = parse_page_size(&header).ok_or(StoreError::UnknownPageSize)?;
            self.head.page_size = page_size;
            self.ensure_active()?;
            let bootstrap = self.bootstrap.take().ok_or(StoreError::Range)?;
            let length = bootstrap.file.metadata()?.len();
            let mut page = vec![0; page_size as usize];
            let mut page_offset = 0_u64;
            while page_offset < length {
                page.fill(0);
                let amount = usize::try_from((length - page_offset).min(u64::from(page_size)))
                    .map_err(|_| StoreError::Range)?;
                read_exact_at(&bootstrap.file, page_offset, &mut page[..amount])?;
                let page_no = u32::try_from(page_offset / u64::from(page_size) + 1)
                    .map_err(|_| StoreError::Range)?;
                self.write_mutable_page(page_no, &page)?;
                page_offset = page_offset
                    .checked_add(u64::from(page_size))
                    .ok_or(StoreError::Range)?;
            }
        }
        Ok(())
    }

    fn active_record_offset(&self, page_no: u32) -> Result<u64, StoreError> {
        self.active
            .active_records
            .get(&page_no)
            .copied()
            .ok_or(StoreError::Range)
    }

    fn active_frame_header(&self, page_no: u32) -> FrameHeader {
        FrameHeader {
            page_no,
            codec: Codec::Raw,
            dictionary_index: u16::MAX,
            stored_len: self.head.page_size,
            raw_len: self.head.page_size,
        }
    }

    fn read_mutable_page(&self, page_no: u32) -> Result<Vec<u8>, StoreError> {
        let offset = self.active_record_offset(page_no)?;
        let mut encoded = [0; FRAME_HEADER_SIZE];
        read_exact_at(&self.active.file, offset, &mut encoded)?;
        let header = FrameHeader::decode(&encoded)?;
        if header.page_no != page_no
            || header.codec != Codec::Raw
            || header.dictionary_index != u16::MAX
            || header.stored_len != self.head.page_size
            || header.raw_len != self.head.page_size
        {
            return Err(StoreError::Corrupt(offset));
        }
        let mut page = vec![0; self.head.page_size as usize];
        read_exact_at(
            &self.active.file,
            offset + FRAME_HEADER_SIZE as u64,
            &mut page,
        )?;
        Ok(page)
    }

    fn overwrite_active_record(&self, page_no: u32, page: &[u8]) -> Result<(), StoreError> {
        let offset = self.active_record_offset(page_no)?;
        write_all_at(&self.active.file, offset + FRAME_HEADER_SIZE as u64, page)?;
        Ok(())
    }

    fn append_active_record(&mut self, page_no: u32, page: &[u8]) -> Result<(), StoreError> {
        let offset = self.active.file.metadata()?.len();
        let header = self.active_frame_header(page_no);
        let end = offset
            .checked_add(header.record_len())
            .ok_or(StoreError::Range)?;
        self.active.file.set_len(end)?;
        write_all_at(&self.active.file, offset + FRAME_HEADER_SIZE as u64, page)?;
        write_all_at(&self.active.file, offset, &header.encode())?;
        self.active.active_records.insert(page_no, offset);
        Ok(())
    }

    fn write_mutable_page(&mut self, page_no: u32, page: &[u8]) -> Result<(), StoreError> {
        if page.len() != self.head.page_size as usize {
            return Err(StoreError::InvalidPageLength {
                page_no,
                actual: page.len(),
                expected: self.head.page_size as usize,
            });
        }
        if page_no == 1 && parse_page_size(page) != Some(self.head.page_size) {
            return Err(StoreError::InvalidPageSize(
                parse_page_size(page).unwrap_or(0),
            ));
        }
        if !self.pending_raw_originals.contains_key(&page_no) {
            let original = self.read_page(page_no)?;
            self.pending_raw_originals.insert(page_no, original);
        }
        self.page_cache
            .invalidate(crate::domain::PageNumber::new(page_no)?);
        if self.active.active_records.contains_key(&page_no) {
            self.overwrite_active_record(page_no, page)?;
        } else {
            self.append_active_record(page_no, page)?;
        }
        self.pending_raw_pages.insert(page_no);
        Ok(())
    }

    fn write_mutable_pages(&mut self, offset: u64, input: &[u8]) -> Result<(), StoreError> {
        let page_size = self.head.page_size as usize;
        let page_size_u64 = u64::from(self.head.page_size);
        let mut consumed = 0_usize;
        while consumed < input.len() {
            let logical = offset
                .checked_add(u64::try_from(consumed).map_err(|_| StoreError::Range)?)
                .ok_or(StoreError::Range)?;
            let page_no =
                u32::try_from(logical / page_size_u64 + 1).map_err(|_| StoreError::Range)?;
            let within = usize::try_from(logical % page_size_u64).map_err(|_| StoreError::Range)?;
            let amount = (input.len() - consumed).min(page_size - within);
            let mut page = if within == 0 && amount == page_size {
                input[consumed..consumed + amount].to_vec()
            } else if self
                .pending_truncate_pages
                .is_some_and(|preserved_pages| page_no > preserved_pages)
            {
                vec![0; page_size]
            } else {
                self.read_page(page_no)?
            };
            page[within..within + amount].copy_from_slice(&input[consumed..consumed + amount]);
            self.write_mutable_page(page_no, &page)?;
            consumed += amount;
        }
        Ok(())
    }

    fn discard_mutable_pending(&mut self) {
        let committed_end = self.head.active_commit_end;
        for (page_no, page) in std::mem::take(&mut self.pending_raw_originals) {
            if self
                .active
                .active_records
                .get(&page_no)
                .is_some_and(|offset| *offset < committed_end)
            {
                let _ = self.overwrite_active_record(page_no, &page);
            }
        }
        let _ = self.active.file.set_len(committed_end);
        self.active
            .active_records
            .retain(|_, offset| *offset < committed_end);
        self.pending_raw_pages.clear();
        self.pending_size = self.head.logical_size;
        self.pending_truncate_pages = None;
        self.pending_dirty = false;
        self.bootstrap = None;
        if self.publication_owner.phase() == PublicationPhase::Checkpoint {
            self.publication_owner
                .transition(PublicationPhase::Transaction);
        }
        self.release_publication();
    }

    pub(crate) fn truncate(&mut self, size: u64) -> Result<(), StoreError> {
        self.begin_write()?;
        if self.head.page_size == 0 {
            if let Some(bootstrap) = &self.bootstrap {
                bootstrap.file.set_len(size)?;
            }
        } else if !size.is_multiple_of(u64::from(self.head.page_size)) {
            return Err(StoreError::InvalidPageSize(self.head.page_size));
        }
        let max_page = if self.head.page_size == 0 {
            0
        } else {
            page_count(size, self.head.page_size)?
        };
        if self.head.page_size != 0 && size < self.pending_size {
            self.page_cache
                .invalidate_where(|page| page.get() > max_page);
            let pages = self
                .active
                .active_records
                .range(max_page.saturating_add(1)..)
                .map(|(page_no, _)| *page_no)
                .collect::<Vec<_>>();
            let zero = vec![0; self.head.page_size as usize];
            for page_no in pages {
                self.write_mutable_page(page_no, &zero)?;
            }
            self.pending_truncate_pages = Some(
                self.pending_truncate_pages
                    .map_or(max_page, |previous| previous.min(max_page)),
            );
        }
        self.pending_size = size;
        self.pending_dirty = true;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn publish(&mut self, durable: bool) -> Result<(), StoreError> {
        self.publish_inner(durable, false, false)
    }

    /// Publishes a commit using the durability strength requested by `SQLite`'s
    /// xSync flags. On macOS, FULL includes `F_FULLFSYNC` for both the active
    /// page data and the state sector that makes it visible.
    pub(crate) fn publish_synced(&mut self, full_sync: bool) -> Result<(), StoreError> {
        self.publish_inner(true, false, full_sync)
    }

    /// Publishes a WAL checkpoint write while retaining the publication
    /// lease until `SQLite` releases its checkpoint lock. This keeps a sequence
    /// of page-at-a-time checkpoint publications linear instead of reloading
    /// and revalidating the growing active log before every page.
    pub(crate) fn publish_checkpoint(&mut self, durable: bool) -> Result<(), StoreError> {
        self.publish_inner(durable, true, false)
    }

    pub(crate) fn publish_checkpoint_synced(&mut self, full_sync: bool) -> Result<(), StoreError> {
        self.publish_inner(true, true, full_sync)
    }

    /// Reserves publication before `SQLite` starts copying WAL frames into the
    /// main image. Contention must be reported from the checkpoint-lock
    /// callback, where `SQLITE_BUSY` has its documented meaning, rather than
    /// from `xWrite`, where `SQLite` can mistake it for a benign partial
    /// checkpoint and later remove an incompletely backfilled WAL.
    pub(crate) fn begin_checkpoint_publication(&mut self) -> Result<(), StoreError> {
        if self.publication_owner.phase() == PublicationPhase::Checkpoint {
            return Ok(());
        }
        if self.publication_owner.is_locked() || self.has_pending() {
            return Err(StoreError::Busy);
        }
        self.begin_write()?;
        self.publication_owner
            .transition(PublicationPhase::Checkpoint);
        Ok(())
    }

    pub(crate) fn finish_checkpoint_publication(&mut self) {
        if self.publication_owner.phase() == PublicationPhase::Checkpoint {
            self.publication_owner
                .transition(PublicationPhase::Transaction);
        }
        self.release_publication();
    }

    #[allow(clippy::too_many_lines)]
    fn publish_inner(
        &mut self,
        durable: bool,
        retain_publication: bool,
        full_sync: bool,
    ) -> Result<(), StoreError> {
        if !self.has_pending() {
            if durable {
                self.begin_write()?;
                sync_file(&self.active.file, full_sync)?;
            }
            if !retain_publication {
                self.release_publication();
            }
            return Ok(());
        }
        if self.head.page_size == 0 {
            if self.pending_size == 0 {
                self.pending_dirty = false;
                self.bootstrap = None;
                if !retain_publication {
                    self.release_publication();
                }
                return Ok(());
            }
            return Err(StoreError::UnknownPageSize);
        }
        let txid = self.head.txid.checked_add(1).ok_or(StoreError::Range)?;
        // Mutable publications advance the transaction counter, not sealed lineage.
        let resulting_history = self.head.history;
        let raw_end = self.active.file.metadata()?.len();
        if durable {
            sync_file(&self.active.file, full_sync)?;
        }
        let now = unix_time();
        let sequence = self
            .active
            .state_sequence
            .checked_add(1)
            .ok_or(StoreError::Range)?;
        let state = ActiveState {
            database_id: self.active.header.database_id,
            sequence,
            txid,
            logical_size: self.pending_size,
            page_size: self.head.page_size,
            history: resulting_history,
            commit_unix: now,
            record_count: u64::try_from(self.active.active_records.len())
                .map_err(|_| StoreError::Range)?,
            truncate_pages: match (self.active.truncate_pages, self.pending_truncate_pages) {
                (Some(previous), Some(current)) => Some(previous.min(current)),
                (Some(previous), None) => Some(previous),
                (None, current) => current,
            },
        };
        write_all_at(
            &self.active.file,
            active_state_offset(sequence),
            &state.encode(),
        )?;
        // The new state may already be visible. Even a failed final sync must
        // not let pending-write cleanup restore bytes under this newer endpoint.
        let publication_sync = (|| {
            #[cfg(test)]
            crate::storage::faults::check(crate::storage::faults::Point::ActiveStateWritten)?;
            if durable {
                sync_file(&self.active.file, full_sync)?;
            }
            #[cfg(test)]
            crate::storage::faults::check(crate::storage::faults::Point::ActiveStateSynced)?;
            Ok(())
        })();

        self.active.state_sequence = sequence;
        self.active.truncate_pages = state.truncate_pages;
        self.head.logical_size = self.pending_size;
        self.head.txid = txid;
        self.head.history = resulting_history;
        self.head.active_commit_offset = active_state_offset(sequence);
        self.head.active_commit_end = raw_end;
        if self.head.oldest_dirty_unix == 0 {
            self.head.oldest_dirty_unix = now;
        }
        self.head.last_dirty_unix = now;
        if retain_publication {
            self.publication_owner
                .transition(PublicationPhase::Checkpoint);
        }
        self.pending_raw_pages.clear();
        self.pending_raw_originals.clear();
        self.pending_truncate_pages = None;
        self.pending_dirty = false;
        self.bootstrap = None;
        if retain_publication {
            self.publication_owner
                .transition(PublicationPhase::Checkpoint);
        } else {
            self.release_publication();
            publication_sync.map_err(StoreError::PublicationUncertain)?;
            self.rollover_at_size_target()?;
            return Ok(());
        }
        publication_sync.map_err(StoreError::PublicationUncertain)
    }

    fn ensure_active(&mut self) -> Result<(), StoreError> {
        if self.head.page_size == 0 {
            return Err(StoreError::UnknownPageSize);
        }
        let header = self.active.header;
        if header.page_size == self.head.page_size {
            return Ok(());
        }
        if header.page_size != 0 || self.head.txid != 0 || self.head.active_commit_offset != 0 {
            return Err(StoreError::IdentityMismatch);
        }
        self.install_active(self.active.header.policy)
    }

    fn install_active(&mut self, policy: StoragePolicyRecord) -> Result<(), StoreError> {
        let base = self
            .view
            .as_ref()
            .map_or(ActiveBase::Genesis, ActiveBase::Existing);
        let active = self.create_active(base, policy, self.publication_owner.guard()?, None)?;
        self.adopt_active(active);
        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)] // Consume the receipt before transferring catalog authority.
    fn prepare_view_publication(
        &self,
        guard: &crate::storage::CatalogGuard,
        durable: crate::storage::DurableView<'_>,
    ) -> Result<ViewPublication, StoreError> {
        let view = guard.pin(durable.id())?;
        Ok(
            match self.create_active(
                ActiveBase::Installed(&durable),
                self.active.header.policy,
                self.publication_owner.guard()?,
                Some(guard),
            ) {
                Ok(active) => ViewPublication::Installed { active, view },
                Err(error) => ViewPublication::ReloadRequired(error),
            },
        )
    }

    fn finish_view_publication(
        &mut self,
        guard: crate::storage::CatalogGuard,
        publication: ViewPublication,
    ) -> Result<crate::storage::CatalogGuard, StoreError> {
        match publication {
            ViewPublication::Installed { active, view } => {
                self.view = Some(view);
                self.adopt_active(active);
                guard.finish_seal(&self.active.header)?;
                Ok(guard)
            }
            ViewPublication::ReloadRequired(error) => {
                // The active rename may be visible. Release catalog exclusion
                // before reload; never clean up possibly referenced objects.
                drop(guard);
                let _ = self.reload();
                Err(error)
            }
        }
    }

    fn create_active(
        &self,
        base: ActiveBase<'_, '_>,
        policy: StoragePolicyRecord,
        _publication: &crate::fs::ExclusiveLock,
        catalog: Option<&crate::storage::CatalogGuard>,
    ) -> Result<ActiveFile, StoreError> {
        let parent_physical_digest = base.validate(self.active.header.database_id, self.head)?;
        let sealed_history = match base {
            ActiveBase::Installed(view) => *view.endpoint().3.as_bytes(),
            _ => self.head.history,
        };
        let staging_id = random_bytes()?;
        let header = ActiveHeader {
            attachment_id: self.active.header.attachment_id,
            database_id: self.active.header.database_id,
            page_size: self.head.page_size,
            start_txid: self.head.txid.checked_add(1).ok_or(StoreError::Range)?,
            base_history: sealed_history,
            parent_physical_digest,
            base_logical_size: self.head.logical_size,
            policy,
            layout: self.layout,
        };
        if let Some(catalog) = catalog {
            catalog.prepare_seal(&header)?;
        }
        let path = active_staging_path(&self.path, staging_id);
        let _cleanup = CleanupFile(path.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        write_all_at(&file, 0, &header.encode())?;
        write_all_at(
            &file,
            ACTIVE_STATE_A_OFFSET,
            &ActiveState {
                database_id: header.database_id,
                sequence: 1,
                txid: self.head.txid,
                logical_size: self.head.logical_size,
                page_size: self.head.page_size,
                history: sealed_history,
                commit_unix: 0,
                record_count: 0,
                truncate_pages: None,
            }
            .encode(),
        )?;
        file.set_len(ACTIVE_METADATA_END)?;
        file.sync_all()?;
        #[cfg(test)]
        crate::storage::faults::check(crate::storage::faults::Point::ActiveDataSynced)?;
        std::fs::rename(&path, &self.path)?;
        #[cfg(test)]
        crate::storage::faults::check(crate::storage::faults::Point::ActiveRenamed)
            .map_err(StoreError::PublicationUncertain)?;
        sync_parent_dir(&self.path).map_err(|error| match error {
            StoreError::Io(error) => StoreError::PublicationUncertain(error),
            other => other,
        })?;
        #[cfg(test)]
        crate::storage::faults::check(crate::storage::faults::Point::ActiveDirectorySynced)
            .map_err(StoreError::PublicationUncertain)?;
        let current = OpenOptions::new()
            .read(true)
            .write(self.writable)
            .open(&self.path)
            .map_err(StoreError::PublicationUncertain)?;
        Ok(ActiveFile {
            file: current,
            header,
            state_sequence: 1,
            active_records: BTreeMap::new(),
            truncate_pages: None,
        })
    }

    fn adopt_active(&mut self, active: ActiveFile) {
        let header = active.header;
        self.active = active;
        self.head.history = header.base_history;
        self.head.active_commit_offset = 0;
        self.head.active_commit_end = ACTIVE_METADATA_END;
        self.head.oldest_dirty_unix = 0;
        self.head.last_dirty_unix = 0;
        self.synchronize_read_cache();
    }

    fn synchronize_read_cache(&mut self) {
        let active = &self.active;
        let pages = self
            .head
            .logical_size
            .checked_div(u64::from(self.head.page_size))
            .unwrap_or(0);
        self.page_cache.synchronize(self.view.as_ref(), |page| {
            u64::from(page.get()) > pages
                || active.active_records.contains_key(&page.get())
                || active.truncate_pages.is_some_and(|last| page.get() > last)
        });
    }

    fn read_local_page(&self, page_no: u32) -> Result<Option<LocalRead>, StoreError> {
        let size = crate::domain::PageSize::new(self.head.page_size)?;
        if self
            .pending_truncate_pages
            .is_some_and(|preserved_pages| page_no > preserved_pages)
            && !self.pending_raw_pages.contains(&page_no)
        {
            return Ok(Some(LocalRead::Zero(size)));
        }
        if self.active.active_records.contains_key(&page_no) {
            return self
                .read_mutable_page(page_no)
                .map(|bytes| Some(LocalRead::Raw(bytes)));
        }
        if self
            .active
            .truncate_pages
            .is_some_and(|preserved_pages| page_no > preserved_pages)
        {
            return Ok(Some(LocalRead::Zero(size)));
        }
        let Some(view) = self.view.as_ref() else {
            return Ok(Some(LocalRead::Zero(size)));
        };
        if page_no > view.logical_size().pages() {
            return Ok(Some(LocalRead::Zero(size)));
        }
        Ok(None)
    }

    fn read_page(&mut self, page_no: u32) -> Result<Vec<u8>, StoreError> {
        if let Some(page) = self.read_local_page(page_no)? {
            if let LocalRead::Raw(bytes) = &page {
                self.read_io.fetched_bytes = self
                    .read_io
                    .fetched_bytes
                    .saturating_add(bytes.len() as u64 + FRAME_HEADER_SIZE as u64);
            }
            return Ok(page.into_bytes());
        }
        let view = self.view.as_ref().ok_or(StoreError::Corrupt(0))?;
        let active = &self.active;
        let truncate = self
            .pending_truncate_pages
            .into_iter()
            .chain(active.truncate_pages)
            .min();
        self.page_cache.read(
            view,
            crate::domain::PageNumber::new(page_no)?,
            &mut self.read_io,
            |page| {
                active.active_records.contains_key(&page.get())
                    || truncate.is_some_and(|last| page.get() > last)
            },
        )
    }
    fn read_maintenance_page(
        &self,
        page_no: u32,
        cache: &mut crate::storage::PageCache,
    ) -> Result<Vec<u8>, StoreError> {
        if let Some(page) = self.read_local_page(page_no)? {
            return Ok(page.into_bytes());
        }
        let view = self.view.as_ref().ok_or(StoreError::Corrupt(0))?;
        let active = &self.active;
        let truncate = self
            .pending_truncate_pages
            .into_iter()
            .chain(active.truncate_pages)
            .min();
        cache.read(
            view,
            crate::domain::PageNumber::new(page_no)?,
            &mut crate::statistics::HandleIoStats::default(),
            |page| {
                active.active_records.contains_key(&page.get())
                    || truncate.is_some_and(|last| page.get() > last)
            },
        )
    }
    #[allow(clippy::too_many_lines)]
    fn reload(&mut self) -> Result<(), StoreError> {
        let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
        let catalog_guard = catalog.lock()?;
        let mut options = OpenOptions::new();
        options.read(true).write(self.writable);
        let current = options.open(&self.path)?;
        let header = read_active_header(&current)?;
        if header.database_id != self.active.header.database_id {
            return Err(StoreError::IdentityMismatch);
        }
        catalog_guard.validate_attachment(&header)?;
        if self.writable {
            catalog_guard.recover_seal(&header)?;
        }
        let view = if header.parent_physical_digest == [0; 32] {
            None
        } else {
            Some(catalog_guard.pin(crate::domain::ManifestId::from_bytes(
                header.parent_physical_digest,
            ))?)
        };
        self.load_local(current, header, view)
    }

    #[allow(clippy::large_types_passed_by_value, clippy::too_many_lines)]
    fn load_local(
        &mut self,
        current: File,
        header: ActiveHeader,
        view: Option<crate::storage::PinnedView>,
    ) -> Result<(), StoreError> {
        if view
            .as_ref()
            .is_some_and(|view| view.endpoint().0.as_bytes() != &header.database_id)
        {
            return Err(StoreError::IdentityMismatch);
        }
        let sealed_logical_size = view.as_ref().map_or(0, |view| view.logical_size().get());
        let sealed_head_txid = view.as_ref().map_or(0, |view| view.endpoint().1.get());
        let sealed_head_history = view
            .as_ref()
            .map_or_else(genesis_history, |view| *view.endpoint().2.as_bytes());
        if header.start_txid != sealed_head_txid.checked_add(1).ok_or(StoreError::Range)?
            || header.base_history != sealed_head_history
            || header.base_logical_size != sealed_logical_size
            || view
                .as_ref()
                .is_some_and(|view| view.logical_size().page_size().get() != header.page_size)
        {
            return Err(StoreError::IdentityMismatch);
        }
        let state = read_active_state(&current, header.database_id)?;
        if state.page_size != header.page_size
            || state.history != sealed_head_history
            || state.txid < sealed_head_txid
            || (state.txid == sealed_head_txid && state.logical_size != sealed_logical_size)
            || (state.txid > sealed_head_txid && state.txid < header.start_txid)
        {
            return Err(StoreError::IdentityMismatch);
        }
        let (active_records, expected_active_end) = load_active_records(
            &current,
            ACTIVE_METADATA_END,
            state.page_size,
            state.record_count,
        )?;
        let file_len = current.metadata()?.len();
        if file_len < expected_active_end {
            return Err(StoreError::Corrupt(file_len));
        }
        self.active = ActiveFile {
            file: current,
            header,
            state_sequence: state.sequence,
            active_records,
            truncate_pages: state.truncate_pages,
        };
        self.head = HeadState {
            page_size: state.page_size,
            logical_size: state.logical_size,
            txid: state.txid,
            history: state.history,
            active_commit_offset: if state.txid < header.start_txid {
                0
            } else {
                active_state_offset(state.sequence)
            },
            active_commit_end: expected_active_end,
            oldest_dirty_unix: state.commit_unix,
            last_dirty_unix: state.commit_unix,
        };
        self.view = view;
        self.layout = header.layout;
        self.page_cache.set_budget(self.layout.cache())?;
        self.pending_size = state.logical_size;
        self.pending_truncate_pages = None;
        self.pending_dirty = false;
        self.pending_raw_pages.clear();
        self.pending_raw_originals.clear();
        self.bootstrap = None;
        self.synchronize_read_cache();
        Ok(())
    }

    pub(crate) fn flush_sidecars(&mut self) -> Result<(), StoreError> {
        self.begin_write()?;
        if self.has_pending() {
            self.publish(true)?;
            self.begin_write()?;
        }
        let header = self.active.header;
        if self.head.txid < header.start_txid {
            self.release_publication();
            return Ok(());
        }
        self.seal_active()?;
        self.release_publication();
        Ok(())
    }

    fn dictionary_policy(&self) -> DictionaryPolicy {
        DictionaryPolicy::new(
            self.active.header.policy.dictionary.dictionary_bytes,
            self.active.header.policy.dictionary.sample_bytes,
        )
        .expect("validated active policy")
    }

    fn seal_active(&mut self) -> Result<(), StoreError> {
        use crate::domain::{
            DatabaseId, HistoryHash, LineageId, LogicalBytes, PageNumber, PageSize, TransactionId,
        };
        if self.head.txid < self.active.header.start_txid {
            return Ok(());
        }
        let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
        let guard = catalog.lock()?;
        let size = LogicalBytes::new(self.head.logical_size, PageSize::new(self.head.page_size)?)?;
        let txid = TransactionId::new(self.head.txid)?;
        let pages = self
            .active
            .active_records
            .keys()
            .copied()
            .filter(|page| *page <= size.pages())
            .map(|page| Ok((PageNumber::new(page)?, txid)))
            .collect::<Result<Vec<_>, StoreError>>()?;
        let endpoint = crate::storage::SealEndpoint {
            dictionary: self.dictionary_policy(),
            database: DatabaseId::from_bytes(self.active.header.database_id),
            lineage: LineageId::from_bytes(self.active.header.database_id),
            size,
            txid,
            history: HistoryHash::from_bytes(self.head.history),
            truncate: self.active.truncate_pages,
        };
        let durable = crate::storage::seal(
            &guard,
            self.view.as_ref(),
            endpoint,
            &pages,
            |page| self.read_mutable_page(page.get()),
            self.layout,
            crate::storage::ManifestMode::Incremental,
        )?;
        let publication = self.prepare_view_publication(&guard, durable)?;
        let mut guard = self.finish_view_publication(guard, publication)?;
        let _report = guard.collect(self.layout.deletion_budget())?;
        Ok(())
    }

    pub(crate) fn compact(&mut self) -> Result<(), StoreError> {
        self.acquire_maintenance()?;
        let result = self.compact_inner();
        self.release_maintenance();
        result
    }

    fn compact_inner(&mut self) -> Result<(), StoreError> {
        if self.has_pending() || self.head.txid >= self.active.header.start_txid {
            return Err(StoreError::Busy);
        }
        let Some(view) = self.view.as_ref() else {
            return Ok(());
        };
        let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
        let guard = catalog.lock()?;
        // Compaction is an LSM metadata merge. Payload packs are immutable and
        // frame payload bytes are neither read, decoded, nor rewritten.
        if view.is_checkpoint() {
            return Ok(());
        }
        let durable = crate::storage::checkpoint_manifest(&guard, view)?;
        let publication = self.prepare_view_publication(&guard, durable)?;
        self.finish_view_publication(guard, publication)?;
        Ok(())
    }

    pub(crate) fn verify(&mut self) -> Result<(), StoreError> {
        let mut cache = crate::storage::PageCache::maintenance()?;
        let expected_pages = page_count(self.head.logical_size, self.head.page_size)? as usize;
        let _ = self.page_txid_map()?;
        for page_no in 1..=u32::try_from(expected_pages).map_err(|_| StoreError::Range)? {
            let page = self.read_maintenance_page(page_no, &mut cache)?;
            if page_no == 1
                && (page.len() < 100
                    || page[..16] != *SQLITE_MAGIC
                    || parse_page_size(&page) != Some(self.head.page_size))
            {
                return Err(StoreError::Corrupt(0));
            }
        }
        if let Some(view) = &self.view {
            let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
            view.verify(&catalog.lock()?)?;
        }
        Ok(())
    }

    pub(crate) fn copy_logical_to(&mut self, destination: &File) -> Result<(), StoreError> {
        let mut cache = crate::storage::PageCache::maintenance()?;
        destination.set_len(self.head.logical_size)?;
        for page_no in 1..=page_count(self.head.logical_size, self.head.page_size)? {
            let page = self.read_maintenance_page(page_no, &mut cache)?;
            write_all_at(
                destination,
                u64::from(page_no - 1) * u64::from(self.head.page_size),
                &page,
            )?;
        }
        Ok(())
    }

    pub(crate) fn inspect(&self) -> Result<Inspect, StoreError> {
        let (sealed_object_bytes, sealed_object_allocated_bytes) = self
            .view
            .as_ref()
            .map_or(Ok((0, 0)), crate::storage::PinnedView::object_bytes)?;
        let file_metadata = self.active.file.metadata()?;
        let logical_page_count = page_count(self.head.logical_size, self.head.page_size)?;
        let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
        let mut guard = catalog.lock()?;
        Ok(Inspect {
            manifest: self
                .view
                .as_ref()
                .map(crate::storage::PinnedView::manifest_statistics)
                .transpose()?,
            preferred_dictionaries: self
                .view
                .as_ref()
                .map_or(0, crate::storage::PinnedView::preferred_dictionary_count),
            frame_distribution: self
                .view
                .as_ref()
                .map_or_else(Vec::new, crate::storage::PinnedView::frame_distribution),
            pack_occupancy: self
                .view
                .as_ref()
                .map_or_else(|| Ok(Vec::new()), |view| view.occupancy(&guard))?,
            retention: guard.collect(0)?,
            path: self.path.clone(),
            sidecar_path: self.sidecar_path.clone(),
            page_size: self.head.page_size,
            page_count: logical_page_count,
            logical_size: self.head.logical_size,
            head_txid: self.head.txid,
            head_history: self.head.history,
            pack_count: self
                .view
                .as_ref()
                .map_or(0, crate::storage::PinnedView::pack_count),
            active: self.head.txid >= self.active.header.start_txid,
            file_bytes: file_metadata.len(),
            file_allocated_bytes: allocated_bytes(&file_metadata),
            sealed_object_bytes,
            sealed_object_allocated_bytes,
            indexed_pages: self
                .page_txid_map()?
                .iter()
                .filter(|txid| **txid != 0)
                .count(),
            dictionary_bytes: self
                .view
                .as_ref()
                .map_or(0, crate::storage::PinnedView::dictionary_bytes),
            policy: StoragePolicy::decode(self.active.header.policy, self.layout),
        })
    }

    pub(crate) fn set_storage_policy(&mut self, policy: StoragePolicy) -> Result<(), StoreError> {
        self.begin_write()?;
        if self.head.txid >= self.active.header.start_txid {
            self.seal_active()?;
        }
        self.layout = policy.layout;
        self.page_cache.set_budget(self.layout.cache())?;
        self.install_active(policy.encode()?)?;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn read_with_statistics(
        &mut self,
        offset: u64,
        output: &mut [u8],
        statistics: &mut crate::statistics::HandleIoStats,
    ) -> Result<usize, StoreError> {
        statistics.requested_bytes = statistics
            .requested_bytes
            .saturating_add(output.len() as u64);
        let before = self.read_io;
        let result = self.read_at(offset, output);
        statistics.fetched_bytes = statistics.fetched_bytes.saturating_add(
            self.read_io
                .fetched_bytes
                .saturating_sub(before.fetched_bytes),
        );
        statistics.inflated_bytes = statistics.inflated_bytes.saturating_add(
            self.read_io
                .inflated_bytes
                .saturating_sub(before.inflated_bytes),
        );
        statistics.decode_nanoseconds = statistics.decode_nanoseconds.saturating_add(
            self.read_io
                .decode_nanoseconds
                .saturating_sub(before.decode_nanoseconds),
        );
        statistics.cache_hits = statistics
            .cache_hits
            .saturating_add(self.read_io.cache_hits.saturating_sub(before.cache_hits));
        statistics.cache_misses = statistics.cache_misses.saturating_add(
            self.read_io
                .cache_misses
                .saturating_sub(before.cache_misses),
        );
        result
    }

    pub(crate) fn cache_stats(&self) -> crate::statistics::DatabaseCacheStats {
        self.page_cache.stats()
    }

    pub(crate) fn retain_view(
        &mut self,
        name: crate::storage::RetentionName,
        replace: bool,
    ) -> Result<crate::storage::DurablePin, StoreError> {
        self.acquire_maintenance()?;
        let result = (|| {
            self.flush_sidecars()?;
            let view = self.view.as_ref().ok_or(StoreError::UnknownPageSize)?;
            let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
            catalog.lock()?.retain(name, view, replace)
        })();
        self.release_maintenance();
        result
    }

    pub(crate) fn release_view(
        &mut self,
        pin: crate::storage::DurablePin,
    ) -> Result<(), StoreError> {
        self.acquire_maintenance()?;
        let result = (|| {
            let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
            catalog.lock()?.release(pin)
        })();
        self.release_maintenance();
        result
    }

    pub(crate) fn retained_view(
        &self,
        name: &crate::storage::RetentionName,
    ) -> Result<crate::storage::PinnedView, StoreError> {
        let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
        let guard = catalog.lock()?;
        let pin = guard.read_root(name)?;
        guard.pin_retained(&pin)
    }

    pub(crate) fn gc_report(
        &mut self,
        budget: usize,
    ) -> Result<crate::storage::GcReport, StoreError> {
        let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
        catalog.lock()?.collect(budget)
    }

    pub(crate) fn background_flush_due(&self) -> bool {
        if self.has_pending() || self.head.txid < self.active.header.start_txid {
            return false;
        }
        let target = self.active.header.policy.rollover_bytes;
        if target != 0 && self.head.active_commit_end >= target {
            return true;
        }
        let now = unix_time();
        now.saturating_sub(self.head.last_dirty_unix)
            >= u64::from(self.active.header.policy.settle_seconds)
            || now.saturating_sub(self.head.oldest_dirty_unix)
                >= u64::from(self.active.header.policy.max_stale_seconds)
    }

    pub(crate) fn try_background_maintenance(&mut self) -> Result<(), StoreError> {
        if self.has_pending() || self.publication_owner.phase() == PublicationPhase::Checkpoint {
            return Ok(());
        }
        if self.background_flush_due() {
            self.acquire_maintenance()?;
            let result = self.flush_sidecars();
            self.release_maintenance();
            result?;
        }
        if self.layout.deletion_budget() > 0 {
            self.collect_garbage(self.layout.deletion_budget())?;
        }
        self.repack_once().map(|_| ())
    }

    pub(crate) fn repack_once(&mut self) -> Result<crate::storage::MaintenanceReport, StoreError> {
        if self.has_pending()
            || self.head.txid >= self.active.header.start_txid
            || self.view.is_none()
        {
            return Ok(crate::storage::MaintenanceReport::default());
        }
        // Idle readers must not repeatedly reserve SQLite publication when
        // there is no pack to rewrite. This advisory check holds only catalogue
        // exclusion; repack() selects again under both locks below.
        {
            let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
            let guard = catalog.lock()?;
            if crate::storage::eligible_pack(
                &guard,
                self.view.as_ref().ok_or(StoreError::Corrupt(0))?,
                self.layout,
            )?
            .is_none()
            {
                return Ok(crate::storage::MaintenanceReport::default());
            }
        }
        self.acquire_maintenance()?;
        let result = (|| {
            // Refresh under publication exclusion before creating the candidate.
            // An active write epoch cannot be discarded by a physical rewrite.
            if self.has_pending() || self.head.txid >= self.active.header.start_txid {
                return Ok(crate::storage::MaintenanceReport::default());
            }
            let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
            let guard = catalog.lock()?;
            let view = self.view.as_ref().ok_or(StoreError::Corrupt(0))?;
            let Some(candidate) =
                crate::storage::repack(&guard, view, self.layout, self.dictionary_policy())?
            else {
                return Ok(crate::storage::MaintenanceReport::default());
            };
            let (durable, copied) =
                candidate.revalidate(self.view.as_ref().ok_or(StoreError::Corrupt(0))?)?;
            let publication = self.prepare_view_publication(&guard, durable)?;
            let mut guard = self.finish_view_publication(guard, publication)?;
            let gc = guard.collect(self.layout.deletion_budget())?;
            Ok(crate::storage::MaintenanceReport {
                repacked_packs: 1,
                decoded_input: copied,
                gc,
            })
        })();
        self.release_maintenance();
        result
    }

    fn rollover_at_size_target(&mut self) -> Result<(), StoreError> {
        let target = self.active.header.policy.rollover_bytes;
        if target == 0
            || self.head.txid < self.active.header.start_txid
            || self.head.active_commit_end < target
        {
            return Ok(());
        }
        self.acquire_maintenance()?;
        let result = self.flush_sidecars();
        self.release_maintenance();
        result
    }

    pub(crate) fn acquire_maintenance(&mut self) -> Result<(), StoreError> {
        if self.publication_owner.phase() == PublicationPhase::Maintenance {
            return Ok(());
        }
        if self.publication_owner.phase() == PublicationPhase::Checkpoint {
            return Err(StoreError::Busy);
        }
        let acquired = if self.publication_owner.is_locked() {
            false
        } else {
            self.publication_owner = PublicationOwner::Transaction(
                crate::fs::ExclusiveLock::on_file(&self.publication, true)?,
            );
            true
        };
        if let Err(error) = self.refresh() {
            if acquired {
                self.release_publication();
            }
            return Err(error);
        }
        self.publication_owner
            .transition(PublicationPhase::Maintenance);
        Ok(())
    }

    pub(crate) fn release_maintenance(&mut self) {
        if self.publication_owner.phase() == PublicationPhase::Maintenance {
            self.publication_owner
                .transition(PublicationPhase::Transaction);
        }
        self.release_publication();
    }

    fn page_txid_map(&self) -> Result<Vec<u64>, StoreError> {
        let pages = page_count(self.head.logical_size, self.head.page_size)?;
        let mut map = vec![0; pages as usize];
        if let Some(view) = &self.view {
            for (page, txid) in view.versions() {
                if page.get() <= pages {
                    map[page.get() as usize - 1] = txid.get();
                }
            }
        }
        if let Some(preserved_pages) = self.active.truncate_pages {
            for txid in map.iter_mut().skip(preserved_pages as usize) {
                *txid = 0;
            }
        }
        for page_no in self.active.active_records.keys().copied() {
            if page_no > pages {
                continue;
            }
            let page = self.read_mutable_page(page_no)?;
            map[page_no as usize - 1] = if page.iter().all(|byte| *byte == 0) {
                0
            } else {
                self.head.txid
            };
        }
        Ok(map)
    }

    fn collect_garbage(&mut self, limit: usize) -> Result<(), StoreError> {
        // Payload reachability changes only under catalogue exclusion. Ordinary
        // main-image writes do not change the immutable header's base manifest.
        // Do not reserve SQLite publication for a read/GC-only catalogue pass.
        let catalog = crate::storage::Catalog::open(&self.sidecar_path, false)?;
        match catalog.try_lock() {
            Ok(mut guard) => {
                let _report = guard.collect(limit)?;
                Ok(())
            }
            Err(StoreError::Busy) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn release_publication(&mut self) {
        // A maintenance operation may call ordinary publication helpers more
        // than once (flush, seal, then compact). Keep one continuous critical
        // section until `release_maintenance()` so another process cannot
        // interleave between those phases.
        if self.publication_owner.phase() == PublicationPhase::Transaction {
            self.publication_owner = PublicationOwner::None;
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if self.has_pending() {
            self.discard_mutable_pending();
        }
        if self.publication_owner.is_locked() {
            self.publication_owner = PublicationOwner::None;
        }
    }
}

fn read_active_header(file: &File) -> Result<ActiveHeader, StoreError> {
    let mut encoded = [0; ACTIVE_HEADER_SIZE];
    read_exact_at(file, 0, &mut encoded).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            StoreError::NotZsqlite
        } else {
            error.into()
        }
    })?;
    ActiveHeader::decode(&encoded).map_err(StoreError::from)
}

fn active_state_offset(sequence: u64) -> u64 {
    if sequence.is_multiple_of(2) {
        ACTIVE_STATE_B_OFFSET
    } else {
        ACTIVE_STATE_A_OFFSET
    }
}

fn read_active_state(file: &File, database_id: DatabaseId) -> Result<ActiveState, StoreError> {
    let mut states = Vec::with_capacity(2);
    let mut empty = false;
    for offset in [ACTIVE_STATE_A_OFFSET, ACTIVE_STATE_B_OFFSET] {
        let mut encoded = [0; ACTIVE_STATE_SIZE];
        read_exact_at(file, offset, &mut encoded)?;
        if encoded.iter().all(|byte| *byte == 0) {
            empty = true;
            continue;
        }
        // Raw records are overwritten, so the older sector is NOT an older
        // page snapshot. Never silently attach its history to newer page bytes.
        let state = ActiveState::decode(&encoded).map_err(|_| StoreError::Corrupt(offset))?;
        if state.database_id != database_id {
            return Err(StoreError::IdentityMismatch);
        }
        states.push(state);
    }
    let newest = states
        .into_iter()
        .max_by_key(|state| state.sequence)
        .ok_or(StoreError::Corrupt(ACTIVE_STATE_A_OFFSET))?;
    if empty && newest.sequence != 1 {
        return Err(StoreError::Corrupt(ACTIVE_STATE_A_OFFSET));
    }
    Ok(newest)
}

fn load_active_records(
    file: &File,
    records_offset: u64,
    page_size: u32,
    record_count: u64,
) -> Result<(BTreeMap<u32, u64>, u64), StoreError> {
    if page_size == 0 {
        if record_count != 0 {
            return Err(StoreError::Corrupt(records_offset));
        }
        return Ok((BTreeMap::new(), records_offset));
    }
    let file_len = file.metadata()?.len();
    let mut cursor = records_offset;
    let mut records = BTreeMap::new();
    for _ in 0..record_count {
        let mut encoded = [0; FRAME_HEADER_SIZE];
        read_exact_at(file, cursor, &mut encoded)?;
        let header = FrameHeader::decode(&encoded)?;
        let end = cursor
            .checked_add(header.record_len())
            .ok_or(StoreError::Range)?;
        if end > file_len
            || header.codec != Codec::Raw
            || header.dictionary_index != u16::MAX
            || header.stored_len != page_size
            || header.raw_len != page_size
            || records.insert(header.page_no, cursor).is_some()
        {
            return Err(StoreError::Corrupt(cursor));
        }
        cursor = end;
    }
    Ok((records, cursor))
}

fn active_staging_path(path: &Path, id: [u8; 16]) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("database.zsqlite");
    path.with_file_name(format!(".{name}.{}.next", hex_active(id)))
}

pub(crate) fn validate_policy(value: StoragePolicyRecord) -> Result<(), StoreError> {
    if value.settle_seconds == 0
        || value.max_stale_seconds < value.settle_seconds
        || (value.rollover_bytes != 0 && value.rollover_bytes < 1024 * 1024)
        || DictionaryPolicy::new(
            value.dictionary.dictionary_bytes,
            value.dictionary.sample_bytes,
        )
        .is_err()
    {
        return Err(StoreError::InvalidConfiguration("invalid storage policy"));
    }
    Ok(())
}

fn page_count(size: u64, page_size: u32) -> Result<u32, StoreError> {
    if size == 0 {
        return Ok(0);
    }
    if !valid_page_size(page_size) || !size.is_multiple_of(u64::from(page_size)) {
        return Err(StoreError::InvalidPageSize(page_size));
    }
    u32::try_from(size / u64::from(page_size)).map_err(|_| StoreError::Range)
}

fn parse_page_size(page: &[u8]) -> Option<u32> {
    if page.get(..16)? != SQLITE_MAGIC {
        return None;
    }
    let encoded = u16::from_be_bytes(page.get(16..18)?.try_into().ok()?);
    let size = if encoded == 1 {
        65_536
    } else {
        u32::from(encoded)
    };
    valid_page_size(size).then_some(size)
}

fn random_bytes<const N: usize>() -> Result<[u8; N], StoreError> {
    let mut value = [0; N];
    getrandom::fill(&mut value).map_err(|error| std::io::Error::other(error.to_string()))?;
    if value.iter().all(|byte| *byte == 0) {
        value[0] = 1;
    }
    Ok(value)
}

fn hex_active(value: [u8; 16]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(32);
    for byte in value {
        output.push(TABLE[(byte >> 4) as usize] as char);
        output.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    output
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

fn open_lock(path: &Path, writable: bool, create: bool) -> Result<File, StoreError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(writable)
        .create(create && writable);
    options.open(path).map_err(StoreError::from)
}

#[cfg(unix)]
fn reject_aliased_active(file: &File) -> Result<(), StoreError> {
    use std::os::unix::fs::MetadataExt;
    if file.metadata()?.nlink() == 1 {
        Ok(())
    } else {
        Err(StoreError::Busy)
    }
}

#[cfg(not(unix))]
fn reject_aliased_active(_file: &File) -> Result<(), StoreError> {
    Ok(())
}

pub(crate) fn reject_auxiliary_files(path: &Path) -> Result<(), StoreError> {
    let mut auxiliary = Vec::with_capacity(5);
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        auxiliary.push(PathBuf::from(name));
    }
    // The parent VFS holds native byte locks and WAL shared memory on a
    // stable inode distinct from the replaceable Store file. SQLite still names the WAL
    // itself after the logical database, but the Unix VFS derives its SHM
    // path from the parent file descriptor's pathname.
    let sqlite_lock = sidecar_dir(path).join("locks").join("sqlite.lock");
    for suffix in ["-wal", "-shm"] {
        let mut name = sqlite_lock.as_os_str().to_os_string();
        name.push(suffix);
        auxiliary.push(PathBuf::from(name));
    }
    for name in auxiliary {
        match std::fs::metadata(name) {
            Ok(metadata) if metadata.len() != 0 => return Err(StoreError::Busy),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(fill: u8, size: u32) -> Vec<u8> {
        let mut page = vec![fill; size as usize];
        page[..16].copy_from_slice(SQLITE_MAGIC);
        let encoded = if size == 65_536 {
            1
        } else {
            u16::try_from(size).expect("test page size fits u16")
        };
        page[16..18].copy_from_slice(&encoded.to_be_bytes());
        page
    }

    fn dictionary_training_image(page_count: usize) -> Vec<u8> {
        const PAGE_SIZE: usize = 4096;

        let mut image = vec![0_u8; PAGE_SIZE * page_count];
        for (page_index, page) in image.chunks_exact_mut(PAGE_SIZE).enumerate() {
            for (word_index, word) in page.chunks_exact_mut(8).enumerate() {
                let value = u64::try_from(page_index)
                    .expect("test page index fits u64")
                    .wrapping_mul(31)
                    .wrapping_add(
                        u64::try_from(word_index).expect("test word index fits u64") % 17,
                    );
                word.copy_from_slice(&value.to_le_bytes());
            }
        }
        image[..16].copy_from_slice(SQLITE_MAGIC);
        image[16..18].copy_from_slice(
            &u16::try_from(PAGE_SIZE)
                .expect("test page size fits u16")
                .to_be_bytes(),
        );
        image
    }

    #[test]
    fn active_file_is_initialized_with_a_valid_header() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.zsqlite");
        let store = Store::open(&path, true)?;
        let header = read_active_header(&store.active.file)?;
        assert_eq!(header.database_id, store.active.header.database_id);
        assert_eq!(header.start_txid, 1);
        assert_eq!(ACTIVE_METADATA_END, store.active.file.metadata()?.len());
        drop(store);

        let active = OpenOptions::new().read(true).write(true).open(&path)?;
        let mut byte = [0_u8; 1];
        read_exact_at(&active, 300, &mut byte)?;
        byte[0] ^= 1;
        write_all_at(&active, 300, &byte)?;
        active.sync_all()?;
        assert!(matches!(
            Store::open_existing(&path),
            Err(StoreError::Format(crate::format::FormatError::Checksum))
        ));
        Ok(())
    }

    #[test]
    fn live_pages_are_raw_and_compression_happens_only_at_seal()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("seal.zsqlite");
        let image = dictionary_training_image(257);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &image)?;
        store.publish(true)?;
        assert!(store.view.is_none());
        assert_eq!(
            store.active.file.metadata()?.len(),
            ACTIVE_METADATA_END + 257 * (4096 + FRAME_HEADER_SIZE as u64)
        );
        store.flush_sidecars()?;
        assert!(store.active.active_records.is_empty());
        assert_eq!(store.active.file.metadata()?.len(), ACTIVE_METADATA_END);
        assert!(store.inspect()?.sealed_object_bytes < image.len() as u64);
        drop(store);
        let mut reopened = Store::open_existing(&path)?;
        let mut output = vec![0; image.len()];
        reopened.read_at(0, &mut output)?;
        assert_eq!(image, output);
        reopened.verify()?;
        Ok(())
    }

    #[test]
    fn small_run_retains_shared_dictionary_objects() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("dictionary.zsqlite");
        let image = dictionary_training_image(257);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &image)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        let dictionaries = store.inspect()?.dictionary_bytes;
        store.write_at(4096, &image[8192..12288])?;
        store.publish(true)?;
        store.flush_sidecars()?;
        assert_eq!(store.inspect()?.dictionary_bytes, dictionaries);
        store.verify()?;
        store.compact()?;
        store.verify()?;
        Ok(())
    }

    #[test]
    fn rolling_back_raw_overwrites_restores_the_committed_image()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("raw-rollback.zsqlite");
        let first = page(3, 4096);
        let second = page(7, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(0, &second)?;
        store.discard_pending();
        let mut output = vec![0; 4096];
        store.read_at(0, &mut output)?;
        assert_eq!(output, first);
        Ok(())
    }

    #[test]
    fn dropping_a_store_discards_uncommitted_raw_overwrites()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("raw-drop.zsqlite");
        let first = page(5, 4096);
        let second = page(9, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(0, &second)?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        let mut output = vec![0; 4096];
        reopened.read_at(0, &mut output)?;
        assert_eq!(output, first);
        Ok(())
    }

    #[test]
    fn active_records_seal_at_the_configured_size() -> Result<(), Box<dyn std::error::Error>> {
        const PAGE_SIZE: usize = 4096;
        const PAGE_COUNT: usize = 256;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("size-rollover.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.set_storage_policy(StoragePolicy {
            rollover_bytes: 1024 * 1024,
            ..StoragePolicy::default()
        })?;

        let mut image = vec![0_u8; PAGE_SIZE * PAGE_COUNT];
        for (page_index, page) in image.chunks_exact_mut(PAGE_SIZE).enumerate() {
            for (word_index, word) in page.chunks_exact_mut(8).enumerate() {
                let value = u64::try_from(page_index)?
                    .wrapping_mul(31)
                    .wrapping_add(u64::try_from(word_index)? % 17);
                word.copy_from_slice(&value.to_le_bytes());
            }
        }
        image[..16].copy_from_slice(SQLITE_MAGIC);
        image[16..18].copy_from_slice(&u16::try_from(PAGE_SIZE)?.to_be_bytes());

        store.write_at(0, &image)?;
        store.publish(true)?;

        assert_eq!(store.head.txid, 1);
        assert_eq!(store.inspect()?.pack_count, 1);
        assert_eq!(store.active.header.start_txid, 2);
        assert!(store.active.active_records.is_empty());
        Ok(())
    }

    #[test]
    fn incomplete_tail_is_ignored_on_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("incomplete-tail.zsqlite");
        let first = page(7, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        let committed_end = store.head.active_commit_end;
        drop(store);

        let active = OpenOptions::new().read(true).write(true).open(&path)?;
        write_all_at(&active, committed_end, crate::format::FRAME_MAGIC)?;
        active.set_len(committed_end + 4)?;
        active.sync_all()?;

        let mut reopened = Store::open_existing(&path)?;
        assert_eq!(reopened.head.active_commit_end, committed_end);
        let mut output = vec![0; first.len()];
        reopened.read_at(0, &mut output)?;
        assert_eq!(output, first);
        Ok(())
    }

    #[test]
    fn failed_writable_upgrade_leaves_store_read_only() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("upgrade.zsqlite");
        let first = page(7, 4096);
        let mut writer = Store::open(&path, true)?;
        writer.write_at(0, &first)?;
        writer.publish(true)?;
        drop(writer);

        let mut store = Store::open_existing_read_only(&path)?;
        let moved_path = path.with_extension("missing");
        std::fs::rename(&path, &moved_path)?;
        assert!(matches!(store.upgrade_writable(), Err(StoreError::Io(_))));
        assert!(!store.writable);
        assert!(matches!(store.write_at(0, &[1]), Err(StoreError::ReadOnly)));
        let mut output = vec![0; first.len()];
        store.read_at(0, &mut output)?;
        assert_eq!(output, first);
        std::fs::rename(&moved_path, &path)?;
        Ok(())
    }

    #[test]
    fn commit_reopen_seal_and_compact() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("test.zsqlite");
        let first = page(7, 4096);
        let second = vec![9; 4096];
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.write_at(4096, &second)?;
        store.publish(true)?;
        drop(store);
        let mut store = Store::open_existing(&path)?;
        store.verify()?;
        store.flush_sidecars()?;
        assert_eq!(store.inspect()?.pack_count, 1);
        let mut output = vec![0; 8192];
        assert_eq!(store.read_at(0, &mut output)?, output.len());
        assert_eq!(&output[..4096], first);
        assert_eq!(&output[4096..], second);
        Ok(())
    }

    #[test]
    fn later_seals_contain_only_changed_active_records() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("incremental-seal.zsqlite");
        let first = page(7, 4096);
        let second = vec![9; 4096];
        let replacement = page(11, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.write_at(4096, &second)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        assert_eq!(store.inspect()?.pack_count, 1);

        store.write_at(0, &replacement)?;
        store.publish(true)?;
        assert_eq!(store.active.active_records.len(), 1);
        store.flush_sidecars()?;
        assert_eq!(store.inspect()?.pack_count, 2);
        let versions = store.view.as_ref().ok_or(StoreError::Range)?.versions();
        assert_eq!(versions[0].1.get(), 2);
        assert_eq!(versions[1].1.get(), 1);

        let mut output = vec![0; 8192];
        store.read_at(0, &mut output)?;
        assert_eq!(&output[..4096], replacement);
        assert_eq!(&output[4096..], second);
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        reopened.read_at(0, &mut output)?;
        assert_eq!(&output[..4096], replacement);
        assert_eq!(&output[4096..], second);
        reopened.verify()?;
        Ok(())
    }

    #[test]
    fn corrupt_active_state_fails_closed_even_for_appends() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("fallback.zsqlite");
        let first = page(7, 4096);
        let second = vec![8; 4096];
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(4096, &second)?;
        store.publish(true)?;
        assert_eq!(store.head.txid, 2);

        let newest = active_state_offset(store.active.state_sequence);
        let active = &store.active.file;
        let mut byte = [0_u8; 1];
        read_exact_at(active, newest + 24, &mut byte)?;
        byte[0] ^= 0xff;
        write_all_at(active, newest + 24, &byte)?;
        active.sync_all()?;
        drop(store);

        assert!(matches!(
            Store::open_existing(&path),
            Err(StoreError::Corrupt(_))
        ));
        Ok(())
    }

    #[test]
    fn corrupt_manifest_is_rejected_on_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("corrupt-view.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &page(7, 4096))?;
        store.publish(true)?;
        store.flush_sidecars()?;
        let catalog = crate::storage::Catalog::open(&sidecar_dir(&path), false)?;
        let guard = catalog.lock()?;
        let id = store.view.as_ref().ok_or(StoreError::Range)?.id();
        let manifest_path = guard.path::<crate::storage::Manifest>(id);
        drop(guard);
        drop(store);
        let file = OpenOptions::new().write(true).open(manifest_path)?;
        write_all_at(&file, 12, &[0xa5])?;
        file.sync_all()?;
        assert!(Store::open_existing(&path).is_err());
        Ok(())
    }

    #[test]
    fn refresh_rejects_a_corrupt_state_without_changing_its_selected_view()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("refresh-failure.zsqlite");
        let first = page(7, 4096);
        let second = page(8, 4096);
        let mut writer = Store::open(&path, true)?;
        writer.write_at(0, &first)?;
        writer.publish(true)?;
        let mut stale_reader = Store::open_existing_read_only(&path)?;
        let selected_before_refresh = stale_reader.head;

        writer.write_at(4096, &second)?;
        writer.publish(true)?;
        let commit_offset = writer.head.active_commit_offset;
        let active = &writer.active.file;
        let mut byte = [0_u8; 1];
        read_exact_at(active, commit_offset + 24, &mut byte)?;
        byte[0] ^= 0x80;
        write_all_at(active, commit_offset + 24, &byte)?;
        active.sync_all()?;

        assert!(matches!(
            stale_reader.refresh(),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            stale_reader.refresh(),
            Err(StoreError::Corrupt(_))
        ));
        assert_eq!(stale_reader.head, selected_before_refresh);
        let mut output = vec![0; 4096];
        stale_reader.read_at(0, &mut output)?;
        assert_eq!(output, first, "the prior pinned snapshot was not restored");
        Ok(())
    }

    #[test]
    fn corrupt_state_cannot_pair_old_history_with_overwritten_pages()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("nondurable-fallback.zsqlite");
        let first = page(7, 4096);
        let second = page(8, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(0, &second)?;
        store.publish(false)?;

        let newest = active_state_offset(store.active.state_sequence);
        let active = &store.active.file;
        let mut byte = [0_u8; 1];
        read_exact_at(active, newest + 24, &mut byte)?;
        byte[0] ^= 0xff;
        write_all_at(active, newest + 24, &byte)?;
        drop(store);

        assert!(matches!(
            Store::open_existing(&path),
            Err(StoreError::Corrupt(_))
        ));
        Ok(())
    }

    #[test]
    fn sparse_zero_pages_survive_sealing_and_compaction() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("sparse.zsqlite");
        let first = page(7, 4096);
        let third = vec![9; 4096];
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.truncate(3 * 4096)?;
        store.publish(true)?;
        assert_eq!(store.page_txid_map()?, vec![2, 0, 0]);
        store.flush_sidecars()?;
        assert_eq!(store.page_txid_map()?, vec![2, 0, 0]);

        store.write_at(2 * 4096, &third)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        let before = store.page_txid_map()?;
        store.compact()?;
        assert_eq!(store.page_txid_map()?, before);
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        reopened.verify()?;
        let mut output = vec![1; 3 * 4096];
        reopened.read_at(0, &mut output)?;
        assert_eq!(&output[..4096], first);
        assert!(output[4096..8192].iter().all(|byte| *byte == 0));
        assert_eq!(&output[8192..], third);
        Ok(())
    }

    #[test]
    fn truncate_then_reextend_does_not_resurrect_old_pages()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("truncate-regrow.zsqlite");
        let first = page(1, 4096);
        let second = vec![2; 4096];
        let third = vec![3; 4096];
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.write_at(4096, &second)?;
        store.write_at(8192, &third)?;
        store.publish(true)?;

        store.truncate(4096)?;
        store.truncate(3 * 4096)?;
        store.write_at(2 * 4096 + 101, &[7, 8, 9, 10])?;
        store.publish(true)?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        let mut output = vec![0xff; 3 * 4096];
        reopened.read_at(0, &mut output)?;
        assert_eq!(&output[..4096], first);
        assert!(output[4096..8192].iter().all(|byte| *byte == 0));
        assert!(output[8192..8192 + 101].iter().all(|byte| *byte == 0));
        assert_eq!(&output[8192 + 101..8192 + 105], &[7, 8, 9, 10]);
        assert!(output[8192 + 105..].iter().all(|byte| *byte == 0));
        Ok(())
    }

    #[test]
    fn sealed_shrink_then_regrow_forgets_pages_from_older_segments()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("sealed-truncate-regrow.zsqlite");
        let first = page(1, 4096);
        let second = vec![2; 4096];
        let third = vec![3; 4096];
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.write_at(4096, &second)?;
        store.write_at(8192, &third)?;
        store.publish(true)?;
        store.flush_sidecars()?;

        store.truncate(4096)?;
        store.truncate(3 * 4096)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        let mut output = vec![0xff; 3 * 4096];
        reopened.read_at(0, &mut output)?;
        assert_eq!(&output[..4096], first);
        assert!(output[4096..].iter().all(|byte| *byte == 0));
        reopened.verify()?;
        Ok(())
    }

    #[test]
    fn ordinary_commits_overwrite_the_same_active_inode() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::MetadataExt as _;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("in-place.zsqlite");
        let first = page(1, 4096);
        let second = page(2, 4096);
        let third = page(3, 4096);
        let mut writer = Store::open(&path, true)?;
        writer.write_at(0, &first)?;
        writer.publish(true)?;
        let original = path.metadata()?;
        let mut stale_reader = Store::open_existing_read_only(&path)?;

        writer.write_at(0, &second)?;
        writer.publish(true)?;
        writer.write_at(0, &third)?;
        writer.publish(true)?;
        let current = path.metadata()?;
        assert_eq!(current.ino(), original.ino());
        assert_eq!(current.len(), original.len());

        stale_reader.refresh()?;
        assert_eq!(stale_reader.head.txid, 3);
        let mut output = vec![0; 4096];
        stale_reader.read_at(0, &mut output)?;
        assert_eq!(output, third);
        Ok(())
    }

    #[test]
    fn independent_bundle_leases_prevent_deletion() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("lifecycle.zsqlite");
        let first = Store::open(&path, true)?;
        let second = Store::open_existing_read_only(&path)?;
        assert!(matches!(Store::delete_bundle(&path), Err(StoreError::Busy)));
        drop(second);

        let probe = open_lock(&first.coordination.lock_path("lifecycle"), true, false)?;
        assert!(matches!(
            lock_exclusive(&probe, true),
            Err(StoreError::Busy)
        ));
        drop(first);
        lock_exclusive(&probe, true)?;
        unlock_file(&probe)?;
        Ok(())
    }

    #[test]
    fn recognizable_database_without_sidecars_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("missing-sidecar.zsqlite");
        drop(Store::open(&path, true)?);
        std::fs::remove_dir_all(sidecar_dir(&path))?;
        assert!(matches!(
            Store::open_existing(&path),
            Err(StoreError::MissingSidecar)
        ));
        Ok(())
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn randomized_store_state_machine_matches_a_byte_vector()
    -> Result<(), Box<dyn std::error::Error>> {
        const PAGE_SIZE: usize = 4096;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("state-machine.zsqlite");
        let first = page(1, u32::try_from(PAGE_SIZE)?);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        let mut committed = first;
        let mut working = committed.clone();
        let mut random = 0x6a09_e667_f3bc_c909_u64;

        let next = |state: &mut u64| {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            *state
        };

        for step in 0..240_u32 {
            let operation = next(&mut random) % 10;
            match operation {
                0 | 1 => {
                    let page_no = 2 + usize::try_from(next(&mut random) % 10)?;
                    let required = page_no * PAGE_SIZE;
                    if working.len() < required {
                        working.resize(required, 0);
                        store.truncate(required as u64)?;
                    }
                    let within = usize::try_from(next(&mut random) % 4000)?;
                    let amount = 1 + usize::try_from(next(&mut random) % 96)?;
                    let amount = amount.min(PAGE_SIZE - within);
                    let offset = (page_no - 1) * PAGE_SIZE + within;
                    let value = u8::try_from(next(&mut random) & 0xff)?;
                    let bytes = vec![value; amount];
                    store.write_at(offset as u64, &bytes)?;
                    working[offset..offset + amount].copy_from_slice(&bytes);
                }
                2 => {
                    let pages = 1 + usize::try_from(next(&mut random) % 12)?;
                    working.resize(pages * PAGE_SIZE, 0);
                    store.truncate(working.len() as u64)?;
                }
                3 => {
                    store.publish(true)?;
                    committed.clone_from(&working);
                }
                4 => {
                    store.publish(false)?;
                    committed.clone_from(&working);
                }
                5 => {
                    store.discard_pending();
                    working.clone_from(&committed);
                }
                6 => {
                    drop(store);
                    working.clone_from(&committed);
                    store = Store::open_existing(&path)?;
                }
                7 if step % 16 == 0 => {
                    store.flush_sidecars()?;
                    committed.clone_from(&working);
                }
                8 if step % 40 == 0 => {
                    store.compact()?;
                    committed.clone_from(&working);
                }
                _ => {
                    let page_no = 2 + usize::try_from(next(&mut random) % 8)?;
                    let required = page_no * PAGE_SIZE;
                    if working.len() < required {
                        working.resize(required, 0);
                        store.truncate(required as u64)?;
                    }
                    let offset = (page_no - 1) * PAGE_SIZE;
                    let first_value = u8::try_from(next(&mut random) & 0xff)?;
                    let second_value = first_value.wrapping_add(1);
                    store.write_at(offset as u64, &vec![first_value; PAGE_SIZE])?;
                    store.write_at(offset as u64, &vec![second_value; PAGE_SIZE])?;
                    working[offset..offset + PAGE_SIZE].fill(second_value);
                }
            }

            assert_eq!(store.logical_size(), working.len() as u64, "step {step}");
            let mut actual = vec![0xa5; working.len()];
            assert_eq!(store.read_at(0, &mut actual)?, actual.len(), "step {step}");
            if actual != working {
                let offset = actual
                    .iter()
                    .zip(&working)
                    .position(|(actual, expected)| actual != expected)
                    .unwrap_or(actual.len().min(working.len()));
                panic!(
                    "step {step} operation {operation}: first mismatch at {offset}, actual {}, expected {}",
                    actual.get(offset).copied().unwrap_or_default(),
                    working.get(offset).copied().unwrap_or_default()
                );
            }
        }

        store.publish(true)?;
        store.flush_sidecars()?;
        store.compact()?;
        store.verify()?;
        drop(store);
        let mut reopened = Store::open_existing(&path)?;
        let mut actual = vec![0; working.len()];
        reopened.read_at(0, &mut actual)?;
        assert_eq!(actual, working);
        reopened.verify()?;
        Ok(())
    }
}
