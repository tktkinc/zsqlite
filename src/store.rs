//! Transactional page storage built from mutable active and immutable sealed segments.

use crate::backend::{FsSegmentBackend, sidecar_dir};
use crate::format::{
    COMMIT_ENTRY_SIZE, COMMIT_HEADER_SIZE, Codec, CommitEntry, CommitHeader, DatabaseId,
    DictionaryEntry, DictionaryPolicyRecord, Digest, FRAME_HEADER_SIZE, FrameHeader,
    MAX_SECTION_BYTES, SECTOR_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_INDEX_ENTRY_SIZE, SEGMENT_MAGIC,
    SEGMENT_TRAILER_SIZE, SegmentHeader, SegmentId, SegmentIndexEntry, SegmentTrailer,
    StoragePolicyRecord, decode_dictionary_table, digest, encode_dictionary_table, genesis_history,
    valid_page_size,
};
use crate::fs::{absolute_path, allocated_bytes, read_exact_at, sync_parent_dir, write_all_at};
#[cfg(test)]
use crate::segment_codec::{MAP_BLOB_MAGIC, encode_blob};
use crate::segment_codec::{
    decode_index, decode_map, encode_index, encode_map, max_map_raw_len, validate_blob_section_len,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "macos")]
use std::os::fd::AsRawFd;

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const MIN_FRAME_SAVINGS: usize = 64;
const ZSTD_LEVEL: i32 = 3;
const PAGE_CACHE_BYTES: usize = 8 * 1024 * 1024;
const MIN_TRAINING_PAGES: usize = 256;
const MIN_TRAINING_BYTES: usize = 1024 * 1024;
const COPY_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
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
    #[error("database is not a V6 zsqlite database")]
    NotZsqlite,
    #[error("database was opened read-only")]
    ReadOnly,
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("input is not a complete, page-aligned SQLite database")]
    InvalidStandardDatabase,
    #[error("unsupported filesystem or platform operation")]
    Unsupported,
    #[error("invalid storage configuration: {0}")]
    InvalidConfiguration(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DictionaryPolicy {
    pub dictionary_bytes: u32,
    pub sample_bytes: u64,
    pub min_improvement_bps: u16,
    pub retrain_churn_bps: u16,
    pub promotion_cooldown: Duration,
}

impl Default for DictionaryPolicy {
    fn default() -> Self {
        Self {
            dictionary_bytes: 64 * 1024,
            sample_bytes: 32 * 1024 * 1024,
            min_improvement_bps: 500,
            retrain_churn_bps: 2_500,
            promotion_cooldown: Duration::from_secs(24 * 60 * 60),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoragePolicy {
    pub settle: Duration,
    pub max_stale: Duration,
    pub dictionary: DictionaryPolicy,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            settle: Duration::from_secs(5 * 60),
            max_stale: Duration::from_secs(60 * 60),
            dictionary: DictionaryPolicy::default(),
        }
    }
}

impl StoragePolicy {
    fn encode(self) -> Result<StoragePolicyRecord, StoreError> {
        let seconds = |value: Duration| {
            u32::try_from(value.as_secs())
                .map_err(|_| StoreError::InvalidConfiguration("duration is too large"))
        };
        let record = StoragePolicyRecord {
            settle_seconds: seconds(self.settle)?,
            max_stale_seconds: seconds(self.max_stale)?,
            dictionary: DictionaryPolicyRecord {
                dictionary_bytes: self.dictionary.dictionary_bytes,
                sample_bytes: self.dictionary.sample_bytes,
                min_improvement_bps: self.dictionary.min_improvement_bps,
                retrain_churn_bps: self.dictionary.retrain_churn_bps,
                promotion_cooldown_seconds: seconds(self.dictionary.promotion_cooldown)?,
            },
        };
        validate_policy(record)?;
        Ok(record)
    }

    fn decode(value: StoragePolicyRecord) -> Self {
        Self {
            settle: Duration::from_secs(u64::from(value.settle_seconds)),
            max_stale: Duration::from_secs(u64::from(value.max_stale_seconds)),
            dictionary: DictionaryPolicy {
                dictionary_bytes: value.dictionary.dictionary_bytes,
                sample_bytes: value.dictionary.sample_bytes,
                min_improvement_bps: value.dictionary.min_improvement_bps,
                retrain_churn_bps: value.dictionary.retrain_churn_bps,
                promotion_cooldown: Duration::from_secs(u64::from(
                    value.dictionary.promotion_cooldown_seconds,
                )),
            },
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
    pub head_history: Digest,
    pub generation: u64,
    pub sealed_segments: usize,
    pub active: bool,
    pub file_bytes: u64,
    pub file_allocated_bytes: u64,
    pub segment_bytes: u64,
    pub segment_allocated_bytes: u64,
    pub indexed_pages: usize,
    pub dictionary_bytes: usize,
    pub policy: StoragePolicy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameSource {
    Active,
    Segment(usize),
}

#[derive(Clone, Copy, Debug)]
struct PageLocation {
    source: FrameSource,
    last_txid: u64,
    frame_offset: u64,
    frame_record_len: u32,
    page_hash: Digest,
}

#[derive(Clone, Copy, Debug)]
struct PendingFrame {
    offset: u64,
    header: FrameHeader,
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
struct SegmentMeta {
    id: SegmentId,
    file: File,
    file_len: u64,
    trailer: SegmentTrailer,
    dictionaries: Vec<DictionaryEntry>,
}

#[derive(Debug)]
struct DiscoveredSegment {
    id: SegmentId,
    file: File,
    file_len: u64,
    header: SegmentHeader,
    trailer: SegmentTrailer,
}

#[derive(Debug)]
struct ActiveSegment {
    file: File,
    header: SegmentHeader,
    trailer: Option<SegmentTrailer>,
    dictionaries: Vec<DictionaryEntry>,
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

#[derive(Debug)]
struct PageCache {
    capacity: usize,
    bytes: usize,
    entries: VecDeque<(PageCacheKey, Arc<Vec<u8>>)>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationOwner {
    None,
    Transaction,
    Checkpoint,
    Maintenance,
}

impl PublicationOwner {
    fn is_locked(self) -> bool {
        self != Self::None
    }
}

type PageCacheKey = (u32, u64, Digest);

impl PageCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            bytes: 0,
            entries: VecDeque::new(),
        }
    }

    fn get(&mut self, key: PageCacheKey) -> Option<Arc<Vec<u8>>> {
        let position = self
            .entries
            .iter()
            .position(|(candidate, _)| *candidate == key)?;
        let entry = self.entries.remove(position)?;
        let value = Arc::clone(&entry.1);
        self.entries.push_front(entry);
        Some(value)
    }

    fn insert(&mut self, key: PageCacheKey, value: Vec<u8>) {
        if let Some(position) = self
            .entries
            .iter()
            .position(|(candidate, _)| *candidate == key)
            && let Some((_, old)) = self.entries.remove(position)
        {
            self.bytes = self.bytes.saturating_sub(old.len());
        }
        let value = Arc::new(value);
        self.bytes = self.bytes.saturating_add(value.len());
        self.entries.push_front((key, value));
        while self.bytes > self.capacity && self.entries.len() > 1 {
            if let Some((_, old)) = self.entries.pop_back() {
                self.bytes = self.bytes.saturating_sub(old.len());
            }
        }
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
}

#[derive(Debug, Default)]
struct DictionaryTrainer {
    pages: BTreeMap<u32, Vec<u8>>,
    order: VecDeque<u32>,
    bytes: usize,
    changed_pages: BTreeSet<u32>,
}

impl DictionaryTrainer {
    fn remember(&mut self, page_no: u32, page: Vec<u8>, limit: usize) {
        let page_bytes = page.len();
        if let Some(previous) = self.pages.insert(page_no, page) {
            self.bytes = self.bytes.saturating_sub(previous.len());
        } else {
            self.order.push_back(page_no);
        }
        self.bytes = self.bytes.saturating_add(page_bytes);
        while self.bytes > limit {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(page) = self.pages.remove(&oldest) {
                self.bytes = self.bytes.saturating_sub(page.len());
            }
        }
    }
}

/// Internal page store. `SQLite`'s ordinary lock file protocol surrounds access.
pub(crate) struct Store {
    path: PathBuf,
    sidecar_path: PathBuf,
    backend: FsSegmentBackend,
    publication: File,
    lifecycle: File,
    writable: bool,
    publication_owner: PublicationOwner,
    head: HeadState,
    segments: Vec<SegmentMeta>,
    locations: BTreeMap<u32, PageLocation>,
    active: ActiveSegment,
    pending_pages: BTreeMap<u32, PendingFrame>,
    pending_size: u64,
    pending_truncate_pages: Option<u32>,
    pending_dirty: bool,
    bootstrap: Option<BootstrapFile>,
    cache: PageCache,
    trainer: DictionaryTrainer,
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
        if existed && path.metadata()?.len() != 0 && !sidecar_dir(&path).exists() {
            let mut magic = [0; 8];
            let file = File::open(&path)?;
            if read_exact_at(&file, 0, &mut magic).is_ok() && magic == *SEGMENT_MAGIC {
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
        let header = read_segment_header(&file)?;
        reject_aliased_active(&file, header)?;
        let backend = FsSegmentBackend::open(sidecar_dir(&path), false)?;
        let publication = open_lock(&backend.lock_path("publication"), writable, false)?;
        let lifecycle = open_lock(&backend.lock_path("lifecycle"), writable, false)?;
        lock_shared(&lifecycle, false)?;
        let mut store = Self::blank(
            path,
            file,
            header,
            backend,
            publication,
            lifecycle,
            writable,
        );
        store.reload()?;
        Ok(store)
    }

    fn initialize(path: PathBuf, file: File) -> Result<Self, StoreError> {
        let database_id = random_bytes()?;
        let backend = FsSegmentBackend::open(sidecar_dir(&path), true)?;
        let publication = open_lock(&backend.lock_path("publication"), true, true)?;
        let lifecycle = open_lock(&backend.lock_path("lifecycle"), true, true)?;
        // SQLite's parent VFS uses a distinct, stable inode for its native
        // locking protocol. The database file is also opened by Store maintenance
        // APIs, and POSIX fcntl locks are process-associated: closing any fd
        // for the locked inode would otherwise release SQLite's locks.
        drop(open_lock(&backend.lock_path("sqlite"), true, true)?);
        File::open(backend.lock_dir())?.sync_all()?;
        lock_exclusive(&publication, false)?;
        if file.metadata()?.len() != 0 {
            unlock_file(&publication)?;
            drop(publication);
            drop(backend);
            drop(file);
            return Self::open_mode(&path, false, true);
        }
        let policy = StoragePolicy::default().encode()?;
        let dictionary_bytes = encode_dictionary_table(&[])?;
        let base_map = encode_map(&[])?;
        let dictionary_offset = SEGMENT_HEADER_SIZE as u64;
        let base_map_offset = dictionary_offset + dictionary_bytes.len() as u64;
        let records_offset = align_up(base_map_offset + base_map.len() as u64, SECTOR_SIZE as u64)?;
        let header = SegmentHeader {
            database_id,
            page_size: 0,
            start_txid: 1,
            base_history: genesis_history(),
            parent_physical_digest: [0; 32],
            base_logical_size: 0,
            generation: 1,
            last_dictionary_promotion_unix: 0,
            policy,
            dictionary_offset,
            dictionary_len: dictionary_bytes.len() as u64,
            base_map_offset,
            base_map_len: base_map.len() as u64,
            records_offset,
        };
        write_all_at(&file, 0, &header.encode())?;
        write_all_at(&file, dictionary_offset, &dictionary_bytes)?;
        write_all_at(&file, base_map_offset, &base_map)?;
        file.set_len(records_offset)?;
        file.sync_all()?;
        sync_parent_dir(&path)?;
        // Establish the lifetime lease before releasing publication. Without
        // this handoff, a racing xDelete could remove the freshly initialized
        // bundle in the gap and leave the successful opener attached to
        // unlinked files.
        lock_shared(&lifecycle, false)?;
        unlock_file(&publication)?;
        let mut store = Self::blank(path, file, header, backend, publication, lifecycle, true);
        store.reload()?;
        Ok(store)
    }

    fn blank(
        path: PathBuf,
        file: File,
        header: SegmentHeader,
        backend: FsSegmentBackend,
        publication: File,
        lifecycle: File,
        writable: bool,
    ) -> Self {
        Self {
            sidecar_path: sidecar_dir(&path),
            path,
            backend,
            publication,
            lifecycle,
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
            segments: Vec::new(),
            locations: BTreeMap::new(),
            active: ActiveSegment {
                file,
                header,
                trailer: None,
                dictionaries: Vec::new(),
            },
            pending_pages: BTreeMap::new(),
            pending_size: 0,
            pending_truncate_pages: None,
            pending_dirty: false,
            bootstrap: None,
            cache: PageCache::new(PAGE_CACHE_BYTES),
            trainer: DictionaryTrainer::default(),
        }
    }

    pub(crate) fn delete_bundle(path: impl AsRef<Path>) -> Result<(), StoreError> {
        let path = absolute_path(path.as_ref())?;
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(StoreError::NotZsqlite);
            }
            Err(error) => return Err(error.into()),
        };
        read_segment_header(&file)?;
        let sidecar = sidecar_dir(&path);
        let backend = match FsSegmentBackend::open(sidecar.clone(), false) {
            Ok(backend) => backend,
            Err(StoreError::MissingSidecar) => {
                // delete_bundle() removes the sidecar tree before the database file.
                // If that deleting process dies in between, the surviving
                // recognizable active file is a deletion tombstone: no opener can
                // use it without its identity-matched sidecar. Acquire any
                // lock files that survived recursive removal before finishing
                // the already-started deletion. This still protects a process
                // holding a generation whose sidecar was damaged externally.
                let _lifecycle =
                    lock_existing_for_delete(&sidecar.join("locks").join("lifecycle.lock"))?;
                let _publication =
                    lock_existing_for_delete(&sidecar.join("locks").join("publication.lock"))?;
                match std::fs::remove_dir_all(&sidecar) {
                    Ok(()) => {}
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                match std::fs::remove_file(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                sync_parent_dir(&path)?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let lifecycle = open_lock(&backend.lock_path("lifecycle"), true, false)?;
        lock_exclusive(&lifecycle, true)?;
        let publication = open_lock(&backend.lock_path("publication"), true, false)?;
        lock_exclusive(&publication, true)?;
        // Keep the recognizable active file in place until its storage is gone.
        // A racing creator will then fail closed instead of constructing a
        // new bundle in a sidecar directory the deleter is about to remove.
        std::fs::remove_dir_all(sidecar)?;
        std::fs::remove_file(&path)?;
        sync_parent_dir(&path)?;
        Ok(())
    }

    pub(crate) fn upgrade_writable(&mut self) -> Result<bool, StoreError> {
        if self.writable {
            return Ok(false);
        }

        // Stage every fallible open first so an error leaves this instance
        // consistently read-only.
        let active = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let publication = open_lock(&self.backend.lock_path("publication"), true, false)?;

        self.publication = publication;
        self.active.file = active;
        self.writable = true;
        Ok(true)
    }

    pub(crate) fn logical_size(&self) -> u64 {
        self.pending_size
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub(crate) fn sqlite_lock_path(&self) -> PathBuf {
        self.backend.lock_path("sqlite")
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
            Ok(match read_segment_header(&current_file) {
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
        self.pending_dirty || !self.pending_pages.is_empty() || self.bootstrap.is_some()
    }

    pub(crate) fn refresh(&mut self) -> Result<(), StoreError> {
        if self.has_pending() {
            return Ok(());
        }
        if self.current_inode_changed()?
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
            lock_exclusive(&self.publication, true)?;
            self.publication_owner = PublicationOwner::Transaction;
            if let Err(error) = self.reload().and_then(|()| {
                if self.active.trailer.is_some() {
                    self.finish_sealed_rollover()?;
                }
                self.active.file.set_len(self.head.active_commit_end)?;
                Ok(())
            }) {
                self.release_publication();
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn discard_pending(&mut self) {
        // Pending frames are never placed into committed free slots. They are
        // outside the last valid commit's end, so forgetting them is a
        // complete rollback; a later writer truncates the unreachable tail.
        self.pending_pages.clear();
        // Reload restores the last complete append-only commit, including the
        // pre-format state when a first-page bootstrap was abandoned.
        let _ = self.reload();
        self.pending_size = self.head.logical_size;
        self.pending_truncate_pages = None;
        self.pending_dirty = false;
        self.bootstrap = None;
        if self.publication_owner == PublicationOwner::Checkpoint {
            self.publication_owner = PublicationOwner::Transaction;
        }
        self.release_publication();
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
        self.write_pages(offset, input)?;
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
                self.stage_page(page_no, &page)?;
                page_offset = page_offset
                    .checked_add(u64::from(page_size))
                    .ok_or(StoreError::Range)?;
            }
        }
        Ok(())
    }

    fn write_pages(&mut self, offset: u64, input: &[u8]) -> Result<(), StoreError> {
        let page_size = self.head.page_size as usize;
        let page_size_u64 = u64::from(self.head.page_size);
        let mut consumed = 0_usize;
        while consumed < input.len() {
            let logical = offset
                .checked_add(u64::try_from(consumed).map_err(|_| StoreError::Range)?)
                .ok_or(StoreError::Range)?;
            let page_no = u32::try_from(
                logical
                    .checked_div(page_size_u64)
                    .and_then(|page| page.checked_add(1))
                    .ok_or(StoreError::Range)?,
            )
            .map_err(|_| StoreError::Range)?;
            let within = usize::try_from(logical % page_size_u64).map_err(|_| StoreError::Range)?;
            let amount = (input.len() - consumed).min(page_size - within);
            let mut page = if within == 0 && amount == page_size {
                input[consumed..consumed + amount].to_vec()
            } else {
                self.read_page(page_no)?
            };
            page[within..within + amount].copy_from_slice(&input[consumed..consumed + amount]);
            self.stage_page(page_no, &page)?;
            consumed += amount;
        }
        Ok(())
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
            self.pending_truncate_pages = Some(
                self.pending_truncate_pages
                    .map_or(max_page, |previous| previous.min(max_page)),
            );
        }
        let removed = max_page
            .checked_add(1)
            .map_or_else(Vec::new, |first_removed| {
                self.pending_pages
                    .range(first_removed..)
                    .map(|(page, _)| *page)
                    .collect::<Vec<_>>()
            });
        for page in removed {
            if let Some(frame) = self.pending_pages.remove(&page) {
                self.mark_frame_free(frame.offset, frame.header.capacity)?;
            }
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
    /// page data and the commit record that makes it visible.
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
        if self.publication_owner == PublicationOwner::Checkpoint {
            return Ok(());
        }
        if self.publication_owner.is_locked() || self.has_pending() {
            return Err(StoreError::Busy);
        }
        self.begin_write()?;
        self.publication_owner = PublicationOwner::Checkpoint;
        Ok(())
    }

    pub(crate) fn finish_checkpoint_publication(&mut self) {
        if self.publication_owner == PublicationOwner::Checkpoint {
            self.publication_owner = PublicationOwner::Transaction;
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
        self.ensure_active()?;
        let active = &self.active.file;
        let txid = self.head.txid.checked_add(1).ok_or(StoreError::Range)?;
        let entries = self
            .pending_pages
            .iter()
            .map(|(page_no, frame)| CommitEntry {
                page_no: *page_no,
                frame_offset: frame.offset,
                frame_record_len: u32::try_from(frame.header.record_len()).unwrap_or(u32::MAX),
                page_hash: frame.header.page_hash,
            })
            .collect::<Vec<_>>();
        if entries
            .iter()
            .any(|entry| entry.frame_record_len == u32::MAX)
        {
            return Err(StoreError::Range);
        }
        let entry_bytes = encode_commit_entries(&entries);
        let entries_digest = digest(&entry_bytes);
        let transaction_hash = transaction_hash(
            txid,
            self.pending_size,
            self.head.page_size,
            self.pending_truncate_pages,
            &entries,
        );
        let resulting_history = history_hash(self.head.history, transaction_hash);
        let unaligned_record_len = COMMIT_HEADER_SIZE
            .checked_add(entry_bytes.len())
            .ok_or(StoreError::Range)?;
        let record_len =
            usize::try_from(align_up(unaligned_record_len as u64, SECTOR_SIZE as u64)?)
                .map_err(|_| StoreError::Range)?;
        let frames_end = active.metadata()?.len();
        let commit_offset = align_up(frames_end, SECTOR_SIZE as u64)?;
        if commit_offset > frames_end {
            zero_range(
                active,
                frames_end,
                usize::try_from(commit_offset - frames_end).map_err(|_| StoreError::Range)?,
            )?;
        }
        let now = unix_time();
        let header = CommitHeader {
            record_len: u32::try_from(record_len).map_err(|_| StoreError::Range)?,
            entry_count: u32::try_from(entries.len()).map_err(|_| StoreError::Range)?,
            txid,
            previous_commit: self.head.active_commit_offset,
            logical_size: self.pending_size,
            page_size: self.head.page_size,
            truncate_pages: self.pending_truncate_pages,
            previous_history: self.head.history,
            transaction_hash,
            resulting_history,
            entries_digest,
            commit_unix: now,
        };
        // Write the commit body first and its checksummed header last. Until
        // that final sector is present, recovery sees only an uncommitted
        // zero/invalid tail.
        write_all_at(
            active,
            commit_offset + COMMIT_HEADER_SIZE as u64,
            &entry_bytes,
        )?;
        let commit_end = commit_offset
            .checked_add(record_len as u64)
            .ok_or(StoreError::Range)?;
        if record_len > unaligned_record_len {
            zero_range(
                active,
                commit_offset + unaligned_record_len as u64,
                record_len - unaligned_record_len,
            )?;
        }
        active.set_len(commit_end)?;
        write_all_at(active, commit_offset, &header.encode())?;

        let max_page = page_count(self.pending_size, self.head.page_size)?;
        let truncate_pages = self.pending_truncate_pages;
        if durable {
            sync_file(active, full_sync)?;
        }
        self.head.logical_size = self.pending_size;
        self.head.txid = txid;
        self.head.history = resulting_history;
        self.head.active_commit_offset = commit_offset;
        self.head.active_commit_end = commit_end;
        if self.head.oldest_dirty_unix == 0 {
            self.head.oldest_dirty_unix = now;
        }
        self.head.last_dirty_unix = now;

        if let Some(preserved_pages) = truncate_pages {
            self.locations.retain(|page, _| *page <= preserved_pages);
        }
        for entry in &entries {
            let frame = self
                .pending_pages
                .get(&entry.page_no)
                .ok_or(StoreError::Range)?;
            self.locations.insert(
                entry.page_no,
                PageLocation {
                    source: FrameSource::Active,
                    last_txid: txid,
                    frame_offset: frame.offset,
                    frame_record_len: entry.frame_record_len,
                    page_hash: entry.page_hash,
                },
            );
            self.trainer.changed_pages.insert(entry.page_no);
        }
        if truncate_pages.is_some() {
            self.locations.retain(|page, _| *page <= max_page);
        }
        let pending = std::mem::take(&mut self.pending_pages);
        for (page, frame) in pending {
            if let Ok(raw) = self.read_frame(
                FrameSource::Active,
                frame.offset,
                u32::try_from(frame.header.record_len()).unwrap_or(u32::MAX),
                page,
                txid,
                frame.header.page_hash,
            ) {
                self.remember_training_page(page, raw);
            }
        }
        self.pending_truncate_pages = None;
        self.pending_dirty = false;
        self.pending_size = self.head.logical_size;
        if retain_publication {
            self.publication_owner = PublicationOwner::Checkpoint;
        } else {
            self.release_publication();
        }
        Ok(())
    }

    fn stage_page(&mut self, page_no: u32, page: &[u8]) -> Result<(), StoreError> {
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
        self.ensure_active()?;
        let txid = self.head.txid.checked_add(1).ok_or(StoreError::Range)?;
        let page_hash = page_hash(txid, page_no, page);
        let (codec, dictionary_index, stored) = self.encode_page(page)?;
        let existing = self.pending_pages.remove(&page_no);
        let stored_len = u32::try_from(stored.len()).map_err(|_| StoreError::Range)?;
        let reusable = existing.filter(|frame| frame.header.capacity >= stored_len);
        let offset_and_capacity = if let Some(frame) = reusable {
            (frame.offset, frame.header.capacity)
        } else {
            if let Some(frame) = existing {
                self.mark_frame_free(frame.offset, frame.header.capacity)?;
            }
            self.allocate_frame(u32::try_from(stored.len()).map_err(|_| StoreError::Range)?)?
        };
        let (offset, capacity) = offset_and_capacity;
        let header = FrameHeader {
            free: false,
            page_no,
            txid,
            codec,
            dictionary_index,
            stored_len,
            raw_len: self.head.page_size,
            capacity,
            page_hash,
        };
        let active = &self.active.file;
        write_all_at(active, offset, &header.encode())?;
        write_all_at(active, offset + FRAME_HEADER_SIZE as u64, &stored)?;
        if capacity as usize > stored.len() {
            zero_range(
                active,
                offset + FRAME_HEADER_SIZE as u64 + stored.len() as u64,
                capacity as usize - stored.len(),
            )?;
        }
        self.pending_pages
            .insert(page_no, PendingFrame { offset, header });
        self.cache.insert((page_no, txid, page_hash), page.to_vec());
        Ok(())
    }

    fn encode_page(&self, page: &[u8]) -> Result<(Codec, u16, Vec<u8>), StoreError> {
        let Some(dictionary) = self.active.dictionaries.first() else {
            return Ok((Codec::Raw, u16::MAX, page.to_vec()));
        };
        let mut compressor = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, &dictionary.bytes)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        let compressed = compressor
            .compress(page)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        if compressed.len().saturating_add(MIN_FRAME_SAVINGS) < page.len() {
            Ok((Codec::Zstd, 0, compressed))
        } else {
            Ok((Codec::Raw, u16::MAX, page.to_vec()))
        }
    }

    fn allocate_frame(&mut self, needed: u32) -> Result<(u64, u32), StoreError> {
        let active = &self.active.file;
        let offset = active.metadata()?.len();
        let end = offset
            .checked_add(FRAME_HEADER_SIZE as u64 + u64::from(needed))
            .ok_or(StoreError::Range)?;
        active.set_len(end)?;
        Ok((offset, needed))
    }

    fn mark_frame_free(&mut self, offset: u64, capacity: u32) -> Result<(), StoreError> {
        if capacity == 0 {
            return Ok(());
        }
        let header = FrameHeader {
            free: true,
            page_no: 0,
            txid: 0,
            codec: Codec::Raw,
            dictionary_index: u16::MAX,
            stored_len: 0,
            raw_len: 0,
            capacity,
            page_hash: [0; 32],
        };
        let active = &self.active.file;
        write_all_at(active, offset, &header.encode())?;
        let _ = punch_payload(
            active,
            offset + FRAME_HEADER_SIZE as u64,
            u64::from(capacity),
        );
        Ok(())
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
        self.install_active(
            self.active
                .dictionaries
                .first()
                .map_or_else(Vec::new, |entry| entry.bytes.clone()),
            self.active.header.policy,
            self.active.header.last_dictionary_promotion_unix,
        )
    }

    fn install_active(
        &mut self,
        dictionary: Vec<u8>,
        policy: StoragePolicyRecord,
        last_dictionary_promotion_unix: u64,
    ) -> Result<(), StoreError> {
        let parent_physical_digest = self
            .segments
            .last()
            .map_or([0; 32], |segment| segment.id.physical_digest);
        let active = self.create_active(
            parent_physical_digest,
            dictionary,
            policy,
            last_dictionary_promotion_unix,
        )?;
        self.adopt_active(active);
        Ok(())
    }

    fn create_active(
        &self,
        parent_physical_digest: Digest,
        dictionary: Vec<u8>,
        policy: StoragePolicyRecord,
        last_dictionary_promotion_unix: u64,
    ) -> Result<ActiveSegment, StoreError> {
        let staging_id = random_bytes()?;
        let dictionaries = if dictionary.is_empty() {
            Vec::new()
        } else {
            let dictionary_digest = digest(&dictionary);
            vec![DictionaryEntry {
                digest: dictionary_digest,
                bytes: dictionary,
            }]
        };
        let dictionary_bytes = encode_dictionary_table(&dictionaries)?;
        let base_map = encode_map(&self.page_txid_map()?)?;
        let dictionary_offset = SEGMENT_HEADER_SIZE as u64;
        let base_map_offset = dictionary_offset
            .checked_add(dictionary_bytes.len() as u64)
            .ok_or(StoreError::Range)?;
        let unaligned_records_offset = base_map_offset
            .checked_add(base_map.len() as u64)
            .ok_or(StoreError::Range)?;
        let records_offset = align_up(unaligned_records_offset, SECTOR_SIZE as u64)?;
        let header = SegmentHeader {
            database_id: self.active.header.database_id,
            page_size: self.head.page_size,
            start_txid: self.head.txid.checked_add(1).ok_or(StoreError::Range)?,
            base_history: self.head.history,
            parent_physical_digest,
            base_logical_size: self.head.logical_size,
            generation: self
                .active
                .header
                .generation
                .checked_add(1)
                .ok_or(StoreError::Range)?,
            last_dictionary_promotion_unix,
            policy,
            dictionary_offset,
            dictionary_len: dictionary_bytes.len() as u64,
            base_map_offset,
            base_map_len: base_map.len() as u64,
            records_offset,
        };
        let path = active_staging_path(&self.path, staging_id);
        let _cleanup = CleanupFile(path.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        write_all_at(&file, 0, &header.encode())?;
        write_all_at(&file, dictionary_offset, &dictionary_bytes)?;
        write_all_at(&file, base_map_offset, &base_map)?;
        file.set_len(records_offset)?;
        file.sync_all()?;
        std::fs::rename(&path, &self.path)?;
        sync_parent_dir(&self.path)?;
        let current = OpenOptions::new()
            .read(true)
            .write(self.writable)
            .open(&self.path)?;
        Ok(ActiveSegment {
            file: current,
            header,
            trailer: None,
            dictionaries,
        })
    }

    fn adopt_active(&mut self, active: ActiveSegment) {
        let header = active.header;
        self.active = active;
        self.head.active_commit_offset = 0;
        self.head.active_commit_end = header.records_offset;
        self.head.oldest_dirty_unix = 0;
        self.head.last_dirty_unix = 0;
    }

    fn read_page(&mut self, page_no: u32) -> Result<Vec<u8>, StoreError> {
        if let Some(frame) = self.pending_pages.get(&page_no).copied() {
            return self.read_frame(
                FrameSource::Active,
                frame.offset,
                u32::try_from(frame.header.record_len()).map_err(|_| StoreError::Range)?,
                page_no,
                frame.header.txid,
                frame.header.page_hash,
            );
        }
        if self
            .pending_truncate_pages
            .is_some_and(|preserved_pages| page_no > preserved_pages)
        {
            return Ok(vec![0; self.head.page_size as usize]);
        }
        let Some(location) = self.locations.get(&page_no).copied() else {
            return Ok(vec![0; self.head.page_size as usize]);
        };
        let key = (page_no, location.last_txid, location.page_hash);
        if let Some(page) = self.cache.get(key) {
            return Ok((*page).clone());
        }
        let page = self.read_frame(
            location.source,
            location.frame_offset,
            location.frame_record_len,
            page_no,
            location.last_txid,
            location.page_hash,
        )?;
        self.cache.insert(key, page.clone());
        Ok(page)
    }

    fn read_frame(
        &self,
        source: FrameSource,
        offset: u64,
        expected_record_len: u32,
        page_no: u32,
        txid: u64,
        expected_hash: Digest,
    ) -> Result<Vec<u8>, StoreError> {
        let mut encoded = [0; FRAME_HEADER_SIZE];
        self.read_source(source, offset, &mut encoded)?;
        let header = FrameHeader::decode(&encoded)?;
        if header.free
            || header.record_len() != u64::from(expected_record_len)
            || header.page_no != page_no
            || header.txid != txid
            || header.page_hash != expected_hash
            || header.raw_len != self.head.page_size
        {
            return Err(StoreError::Corrupt(offset));
        }
        let mut stored = vec![0; header.stored_len as usize];
        self.read_source(source, offset + FRAME_HEADER_SIZE as u64, &mut stored)?;
        let dictionaries = match source {
            FrameSource::Active => &self.active.dictionaries,
            FrameSource::Segment(index) => {
                &self
                    .segments
                    .get(index)
                    .ok_or(StoreError::Corrupt(offset))?
                    .dictionaries
            }
        };
        decode_frame_payload(
            header,
            &stored,
            dictionaries,
            page_no,
            txid,
            expected_hash,
            offset,
        )
    }

    fn read_source(
        &self,
        source: FrameSource,
        offset: u64,
        output: &mut [u8],
    ) -> Result<(), StoreError> {
        match source {
            FrameSource::Active => read_exact_at(&self.active.file, offset, output)?,
            FrameSource::Segment(index) => read_exact_at(
                &self
                    .segments
                    .get(index)
                    .ok_or(StoreError::Corrupt(offset))?
                    .file,
                offset,
                output,
            )?,
        }
        Ok(())
    }

    fn reload(&mut self) -> Result<(), StoreError> {
        let mut options = OpenOptions::new();
        options.read(true).write(self.writable);
        let current = options.open(&self.path)?;
        let header = read_segment_header(&current)?;
        if header.database_id != self.active.header.database_id {
            return Err(StoreError::IdentityMismatch);
        }
        let discovered = discover_lineage(&self.backend, header)?;
        let page_size = header.page_size;
        let (segments, mut locations) = load_segments(discovered, header.database_id, page_size)?;
        let sealed_logical_size = segments
            .last()
            .map_or(0, |segment| segment.trailer.logical_size);
        let sealed_head_txid = segments
            .last()
            .map_or(0, |segment| segment.trailer.end_txid);
        let sealed_head_history = segments
            .last()
            .map_or_else(genesis_history, |segment| segment.trailer.end_history);
        if header.start_txid != sealed_head_txid.checked_add(1).ok_or(StoreError::Range)?
            || header.base_history != sealed_head_history
            || header.base_logical_size != sealed_logical_size
            || (page_size != 0 && header.page_size != page_size)
        {
            return Err(StoreError::IdentityMismatch);
        }
        let dictionary_end = checked_end(header.dictionary_offset, header.dictionary_len)?;
        let base_map_end = checked_end(header.base_map_offset, header.base_map_len)?;
        if dictionary_end > header.base_map_offset
            || base_map_end > header.records_offset
            || header.records_offset > current.metadata()?.len()
        {
            return Err(StoreError::Corrupt(header.records_offset));
        }
        let dictionaries =
            read_dictionary_table_file(&current, header.dictionary_offset, header.dictionary_len)?;
        let base_page_count = page_count(sealed_logical_size, page_size)?;
        validate_blob_section_len(header.base_map_len, max_map_raw_len(base_page_count)?)?;
        let base_map = decode_map(
            &read_range_file(&current, header.base_map_offset, header.base_map_len)?,
            base_page_count,
        )?;
        if base_map != full_page_txid_map(&locations, base_page_count)? {
            return Err(StoreError::Corrupt(header.base_map_offset));
        }
        let trailer = read_current_trailer(&current, header)?;
        let active = ActiveSegment {
            file: current,
            header,
            trailer,
            dictionaries,
        };
        let mut head = HeadState {
            page_size,
            logical_size: sealed_logical_size,
            txid: sealed_head_txid,
            history: sealed_head_history,
            active_commit_offset: 0,
            active_commit_end: header.records_offset,
            oldest_dirty_unix: 0,
            last_dirty_unix: 0,
        };
        Self::apply_active_commits(&active, &segments, &mut head, &mut locations)?;
        let _ = full_page_txid_map(&locations, page_count(head.logical_size, head.page_size)?)?;

        self.active = active;
        self.head = head;
        self.segments = segments;
        self.locations = locations;
        self.pending_size = self.head.logical_size;
        self.pending_truncate_pages = None;
        self.pending_dirty = false;
        self.pending_pages.clear();
        self.bootstrap = None;
        self.cache.clear();
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn apply_active_commits(
        active: &ActiveSegment,
        segments: &[SegmentMeta],
        head: &mut HeadState,
        locations: &mut BTreeMap<u32, PageLocation>,
    ) -> Result<(), StoreError> {
        let header = active.header;
        let file = &active.file;
        let file_len = file.metadata()?.len();
        let scan_end = active
            .trailer
            .map_or(file_len, |trailer| trailer.index_offset);
        let mut cursor = header.records_offset;
        let mut previous_commit = 0_u64;
        let mut txid = segments
            .last()
            .map_or(0, |segment| segment.trailer.end_txid);
        let mut history = segments
            .last()
            .map_or_else(genesis_history, |segment| segment.trailer.end_history);
        let mut logical_size = header.base_logical_size;
        let mut page_size = header.page_size;
        let mut committed_end = header.records_offset;
        // Commit entries can only reference frames written since the previous
        // commit. Keeping every historical frame header made reopen memory
        // scale with all writes in the active segment, including dead ones.
        let mut transaction_frames = BTreeMap::<u64, FrameHeader>::new();
        'scan: while cursor < scan_end {
            if scan_end - cursor < 4 {
                break;
            }
            let mut magic = [0; 4];
            read_exact_at(file, cursor, &mut magic)?;
            if &magic == crate::format::FRAME_MAGIC || &magic == crate::format::FREE_MAGIC {
                if scan_end - cursor < FRAME_HEADER_SIZE as u64 {
                    break;
                }
                let mut encoded = [0; FRAME_HEADER_SIZE];
                read_exact_at(file, cursor, &mut encoded)?;
                let frame = match FrameHeader::decode(&encoded) {
                    Ok(frame) => frame,
                    Err(error) if active.trailer.is_none() => {
                        let _ = error;
                        break;
                    }
                    Err(error) => return Err(error.into()),
                };
                let frame_end = cursor
                    .checked_add(frame.record_len())
                    .ok_or(StoreError::Range)?;
                if frame_end > scan_end {
                    break;
                }
                transaction_frames.insert(cursor, frame);
                cursor = frame_end;
            } else if magic == [0; 4] && !cursor.is_multiple_of(SECTOR_SIZE as u64) {
                let padding_end = align_up(cursor, SECTOR_SIZE as u64)?;
                if padding_end > scan_end {
                    break;
                }
                let mut padding =
                    vec![0; usize::try_from(padding_end - cursor).map_err(|_| StoreError::Range)?];
                read_exact_at(file, cursor, &mut padding)?;
                if padding.iter().any(|byte| *byte != 0) {
                    break;
                }
                cursor = padding_end;
            } else if &magic == crate::format::COMMIT_MAGIC {
                let (commit, entries) = match read_commit(file, cursor, scan_end) {
                    Ok(commit) => commit,
                    Err(error) if active.trailer.is_none() => {
                        let _ = error;
                        break;
                    }
                    Err(error) => return Err(error),
                };
                let expected_txid = txid.checked_add(1).ok_or(StoreError::Corrupt(cursor))?;
                let expected_page_size = if page_size == 0 {
                    commit.page_size
                } else {
                    page_size
                };
                if commit.previous_commit != previous_commit
                    || commit.txid != expected_txid
                    || commit.page_size != expected_page_size
                    || commit.previous_history != history
                    || commit.entries_digest != digest(&encode_commit_entries(&entries))
                    || commit.transaction_hash
                        != transaction_hash(
                            commit.txid,
                            commit.logical_size,
                            commit.page_size,
                            commit.truncate_pages,
                            &entries,
                        )
                    || commit.resulting_history != history_hash(history, commit.transaction_hash)
                {
                    if active.trailer.is_none() {
                        break 'scan;
                    }
                    return Err(StoreError::Corrupt(cursor));
                }
                let max_page = page_count(commit.logical_size, commit.page_size)?;
                let mut previous_page = 0_u32;
                for entry in &entries {
                    let frame = transaction_frames
                        .get(&entry.frame_offset)
                        .ok_or(StoreError::Corrupt(entry.frame_offset))?;
                    if entry.page_no <= previous_page
                        || entry.page_no > max_page
                        || frame.free
                        || frame.page_no != entry.page_no
                        || frame.txid != commit.txid
                        || frame.record_len() != u64::from(entry.frame_record_len)
                        || frame.page_hash != entry.page_hash
                        || frame.raw_len != commit.page_size
                    {
                        if active.trailer.is_none() {
                            break 'scan;
                        }
                        return Err(StoreError::Corrupt(entry.frame_offset));
                    }
                    let mut stored = vec![0; frame.stored_len as usize];
                    read_exact_at(
                        file,
                        entry.frame_offset + FRAME_HEADER_SIZE as u64,
                        &mut stored,
                    )?;
                    if let Err(error) = decode_frame_payload(
                        *frame,
                        &stored,
                        &active.dictionaries,
                        entry.page_no,
                        commit.txid,
                        entry.page_hash,
                        entry.frame_offset,
                    ) {
                        if active.trailer.is_none() {
                            let _ = error;
                            break 'scan;
                        }
                        return Err(error);
                    }
                    previous_page = entry.page_no;
                }
                if let Some(preserved_pages) = commit.truncate_pages {
                    locations.retain(|page, _| *page <= preserved_pages);
                }
                for entry in entries {
                    locations.insert(
                        entry.page_no,
                        PageLocation {
                            source: FrameSource::Active,
                            last_txid: commit.txid,
                            frame_offset: entry.frame_offset,
                            frame_record_len: entry.frame_record_len,
                            page_hash: entry.page_hash,
                        },
                    );
                }
                locations.retain(|page, _| *page <= max_page);
                previous_commit = cursor;
                txid = commit.txid;
                history = commit.resulting_history;
                logical_size = commit.logical_size;
                page_size = commit.page_size;
                if head.oldest_dirty_unix == 0 {
                    head.oldest_dirty_unix = commit.commit_unix;
                }
                head.last_dirty_unix = commit.commit_unix;
                transaction_frames.clear();
                cursor = cursor
                    .checked_add(u64::from(commit.record_len))
                    .ok_or(StoreError::Range)?;
                committed_end = cursor;
            } else {
                break;
            }
        }
        if active.trailer.is_some() && (cursor != scan_end || !transaction_frames.is_empty()) {
            return Err(StoreError::Corrupt(cursor));
        }
        head.page_size = page_size;
        head.logical_size = logical_size;
        head.txid = txid;
        head.history = history;
        head.active_commit_offset = previous_commit;
        head.active_commit_end = committed_end;
        if let Some(trailer) = active.trailer
            && (trailer.database_id != active.header.database_id
                || trailer.start_txid != header.start_txid
                || trailer.end_txid != txid
                || trailer.base_history != header.base_history
                || trailer.end_history != history
                || trailer.logical_size != logical_size
                || trailer.page_size != page_size
                || trailer.content_root != content_root(locations))
        {
            return Err(StoreError::Corrupt(scan_end));
        }
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

    fn seal_active(&mut self) -> Result<(), StoreError> {
        if self.active.trailer.is_some() {
            return self.finish_sealed_rollover();
        }
        let active = &self.active.file;
        let header = self.active.header;
        let index = self
            .locations
            .iter()
            .filter_map(|(page_no, location)| {
                (location.source == FrameSource::Active).then_some(SegmentIndexEntry {
                    page_no: *page_no,
                    last_txid: location.last_txid,
                    frame_offset: location.frame_offset,
                    frame_record_len: location.frame_record_len,
                    page_hash: location.page_hash,
                })
            })
            .collect::<Vec<_>>();
        let index_offset = self.head.active_commit_end;
        let trailer = finalize_segment(
            active,
            index_offset,
            &index,
            &self.page_txid_map()?,
            SegmentTrailer {
                database_id: self.active.header.database_id,
                start_txid: header.start_txid,
                end_txid: self.head.txid,
                base_history: header.base_history,
                end_history: self.head.history,
                logical_size: self.head.logical_size,
                page_size: self.head.page_size,
                index_offset: 0,
                index_len: 0,
                map_offset: 0,
                map_len: 0,
                content_root: content_root(&self.locations),
                physical_digest: [0; 32],
            },
        )?;
        self.active.trailer = Some(trailer);
        self.finish_sealed_rollover()?;
        self.collect_garbage(8)?;
        Ok(())
    }

    fn finish_sealed_rollover(&mut self) -> Result<(), StoreError> {
        let trailer = self.active.trailer.ok_or(StoreError::Corrupt(0))?;
        let active = &self.active.file;
        let id = SegmentId {
            start_txid: trailer.start_txid,
            end_txid: trailer.end_txid,
            end_history: trailer.end_history,
            physical_digest: trailer.physical_digest,
        };
        self.backend.put_segment(&id, &self.path)?;
        let segment_file = File::open(self.backend.segment_path(&id))?;
        let segment_index = self.segments.len();
        let segment = SegmentMeta {
            id: id.clone(),
            file: segment_file,
            file_len: active.metadata()?.len(),
            trailer,
            dictionaries: self.active.dictionaries.clone(),
        };
        let dictionary = self
            .active
            .dictionaries
            .first()
            .map_or_else(Vec::new, |entry| entry.bytes.clone());
        let policy = self.active.header.policy;
        let promotion = self.active.header.last_dictionary_promotion_unix;
        let next = match self.create_active(id.physical_digest, dictionary, policy, promotion) {
            Ok(active) => active,
            Err(error) => {
                // A directory-sync error can occur after rename, while earlier
                // errors leave the sealed file at `.zsqlite`. Reload whichever
                // complete pathname state is visible before returning the error.
                let _ = self.reload();
                return Err(error);
            }
        };
        self.segments.push(segment);
        for location in self.locations.values_mut() {
            if location.source == FrameSource::Active {
                location.source = FrameSource::Segment(segment_index);
            }
        }
        self.adopt_active(next);
        Ok(())
    }

    pub(crate) fn compact(&mut self) -> Result<(), StoreError> {
        self.acquire_maintenance()?;
        let result = self.compact_inner();
        self.release_maintenance();
        result
    }

    #[allow(clippy::too_many_lines)]
    fn compact_inner(&mut self) -> Result<(), StoreError> {
        let active_has_commits = self.head.txid >= self.active.header.start_txid;
        if self.has_pending() || active_has_commits || self.active.trailer.is_some() {
            self.flush_sidecars()?;
            self.begin_write()?;
        }
        if self.head.txid == 0 || self.segments.len() <= 1 {
            self.release_publication();
            return Ok(());
        }
        // Never make a corrupt source generation authoritative. This check
        // also covers bytes that are not represented in the logical index.
        for segment in &self.segments {
            verify_segment_physical(segment)?;
        }
        let staging_id = random_bytes()?;
        let path = active_staging_path(&self.path, staging_id).with_extension("compact");
        let _cleanup = CleanupFile(path.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let dictionaries = union_dictionaries(&self.segments);
        let dictionary_bytes = encode_dictionary_table(&dictionaries)?;
        let empty_map = encode_map(&[])?;
        let dictionary_offset = SEGMENT_HEADER_SIZE as u64;
        let base_map_offset = dictionary_offset + dictionary_bytes.len() as u64;
        let records_offset =
            align_up(base_map_offset + empty_map.len() as u64, SECTOR_SIZE as u64)?;
        let header = SegmentHeader {
            database_id: self.active.header.database_id,
            page_size: self.head.page_size,
            start_txid: 1,
            base_history: genesis_history(),
            parent_physical_digest: [0; 32],
            base_logical_size: 0,
            generation: self
                .active
                .header
                .generation
                .checked_add(1)
                .ok_or(StoreError::Range)?,
            last_dictionary_promotion_unix: self.active.header.last_dictionary_promotion_unix,
            policy: self.active.header.policy,
            dictionary_offset,
            dictionary_len: dictionary_bytes.len() as u64,
            base_map_offset,
            base_map_len: empty_map.len() as u64,
            records_offset,
        };
        write_all_at(&file, 0, &header.encode())?;
        write_all_at(&file, dictionary_offset, &dictionary_bytes)?;
        write_all_at(&file, base_map_offset, &empty_map)?;
        let dictionary_selectors = dictionaries
            .iter()
            .enumerate()
            .map(|(index, dictionary)| {
                Ok((
                    dictionary.digest,
                    u16::try_from(index).map_err(|_| StoreError::Range)?,
                ))
            })
            .collect::<Result<HashMap<_, _>, StoreError>>()?;
        let mut cursor = records_offset;
        let mut index = Vec::with_capacity(self.locations.len());
        for (page_no, location) in &self.locations {
            let mut encoded = [0; FRAME_HEADER_SIZE];
            self.read_source(location.source, location.frame_offset, &mut encoded)?;
            let mut frame = FrameHeader::decode(&encoded)?;
            let mut payload = vec![0; frame.stored_len as usize];
            self.read_source(
                location.source,
                location.frame_offset + FRAME_HEADER_SIZE as u64,
                &mut payload,
            )?;
            if frame.free
                || frame.page_no != *page_no
                || frame.txid != location.last_txid
                || frame.page_hash != location.page_hash
                || frame.raw_len != self.head.page_size
                || frame.record_len() != u64::from(location.frame_record_len)
            {
                return Err(StoreError::Corrupt(location.frame_offset));
            }
            let source_dictionaries = match location.source {
                FrameSource::Active => &self.active.dictionaries,
                FrameSource::Segment(segment) => &self.segments[segment].dictionaries,
            };
            // Validate the exact encoded bytes that will be copied. This is
            // intentionally before selector remapping so the source
            // dictionary identity is still available.
            let _ = decode_frame_payload(
                frame,
                &payload,
                source_dictionaries,
                *page_no,
                location.last_txid,
                location.page_hash,
                location.frame_offset,
            )?;
            if frame.codec == Codec::Zstd {
                let source_dictionary = source_dictionaries
                    .get(frame.dictionary_index as usize)
                    .ok_or(StoreError::Corrupt(location.frame_offset))?;
                frame.dictionary_index = *dictionary_selectors
                    .get(&source_dictionary.digest)
                    .ok_or(StoreError::Corrupt(location.frame_offset))?;
            }
            frame.capacity = frame.stored_len;
            let _ = decode_frame_payload(
                frame,
                &payload,
                &dictionaries,
                *page_no,
                location.last_txid,
                location.page_hash,
                cursor,
            )?;
            write_all_at(&file, cursor, &frame.encode())?;
            write_all_at(&file, cursor + FRAME_HEADER_SIZE as u64, &payload)?;
            index.push(SegmentIndexEntry {
                page_no: *page_no,
                last_txid: location.last_txid,
                frame_offset: cursor,
                frame_record_len: u32::try_from(frame.record_len())
                    .map_err(|_| StoreError::Range)?,
                page_hash: location.page_hash,
            });
            cursor += frame.record_len();
        }
        let index_offset = cursor;
        let trailer = finalize_segment(
            &file,
            index_offset,
            &index,
            &self.page_txid_map()?,
            SegmentTrailer {
                database_id: self.active.header.database_id,
                start_txid: 1,
                end_txid: self.head.txid,
                base_history: genesis_history(),
                end_history: self.head.history,
                logical_size: self.head.logical_size,
                page_size: self.head.page_size,
                index_offset: 0,
                index_len: 0,
                map_offset: 0,
                map_len: 0,
                content_root: content_root(&self.locations),
                physical_digest: [0; 32],
            },
        )?;
        let id = SegmentId {
            start_txid: 1,
            end_txid: self.head.txid,
            end_history: self.head.history,
            physical_digest: trailer.physical_digest,
        };
        self.backend.put_segment(&id, &path)?;
        let file_len = file.metadata()?.len();
        let new_segment = SegmentMeta {
            id,
            file,
            file_len,
            trailer,
            dictionaries,
        };
        verify_segment_physical(&new_segment)?;
        let mut new_locations = BTreeMap::new();
        for value in index {
            new_locations.insert(
                value.page_no,
                PageLocation {
                    source: FrameSource::Segment(0),
                    last_txid: value.last_txid,
                    frame_offset: value.frame_offset,
                    frame_record_len: value.frame_record_len,
                    page_hash: value.page_hash,
                },
            );
        }
        let parent_physical_digest = new_segment.id.physical_digest;
        let dictionary = self
            .active
            .dictionaries
            .first()
            .map_or_else(Vec::new, |entry| entry.bytes.clone());
        let next = match self.create_active(
            parent_physical_digest,
            dictionary,
            self.active.header.policy,
            self.active.header.last_dictionary_promotion_unix,
        ) {
            Ok(active) => active,
            Err(error) => {
                let _ = self.reload();
                return Err(error);
            }
        };
        self.segments = vec![new_segment];
        self.locations = new_locations;
        self.adopt_active(next);
        let _ = std::fs::remove_file(path);
        self.collect_garbage(usize::MAX)?;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn verify(&mut self) -> Result<(), StoreError> {
        let expected_pages = page_count(self.head.logical_size, self.head.page_size)? as usize;
        let _ = self.page_txid_map()?;
        for page_no in 1..=u32::try_from(expected_pages).map_err(|_| StoreError::Range)? {
            let page = self.read_page(page_no)?;
            if page_no == 1
                && (page.len() < 100
                    || page[..16] != *SQLITE_MAGIC
                    || parse_page_size(&page) != Some(self.head.page_size))
            {
                return Err(StoreError::Corrupt(0));
            }
        }
        for segment in &self.segments {
            verify_segment_physical(segment)?;
        }
        Ok(())
    }

    pub(crate) fn copy_logical_to(&mut self, destination: &File) -> Result<(), StoreError> {
        destination.set_len(self.head.logical_size)?;
        for page_no in 1..=page_count(self.head.logical_size, self.head.page_size)? {
            let page = self.read_page(page_no)?;
            write_all_at(
                destination,
                u64::from(page_no - 1) * u64::from(self.head.page_size),
                &page,
            )?;
        }
        Ok(())
    }

    pub(crate) fn inspect(&self) -> Result<Inspect, StoreError> {
        let mut segment_bytes = 0_u64;
        let mut segment_allocated_bytes = 0_u64;
        for segment in &self.segments {
            let path = self.backend.segment_path(&segment.id);
            segment_bytes = segment_bytes.saturating_add(path.metadata()?.len());
            segment_allocated_bytes =
                segment_allocated_bytes.saturating_add(allocated_bytes(&path.metadata()?));
        }
        let file_metadata = self.active.file.metadata()?;
        Ok(Inspect {
            path: self.path.clone(),
            sidecar_path: self.sidecar_path.clone(),
            page_size: self.head.page_size,
            page_count: page_count(self.head.logical_size, self.head.page_size)?,
            logical_size: self.head.logical_size,
            head_txid: self.head.txid,
            head_history: self.head.history,
            generation: self.active.header.generation,
            sealed_segments: self.segments.len(),
            active: self.head.txid >= self.active.header.start_txid,
            file_bytes: file_metadata.len(),
            file_allocated_bytes: allocated_bytes(&file_metadata),
            segment_bytes,
            segment_allocated_bytes,
            indexed_pages: self.locations.len(),
            dictionary_bytes: self
                .active
                .dictionaries
                .first()
                .map_or(0, |entry| entry.bytes.len()),
            policy: StoragePolicy::decode(self.active.header.policy),
        })
    }

    pub(crate) fn set_storage_policy(&mut self, policy: StoragePolicy) -> Result<(), StoreError> {
        self.begin_write()?;
        if self.head.txid >= self.active.header.start_txid {
            self.seal_active()?;
        }
        self.install_active(
            self.active
                .dictionaries
                .first()
                .map_or_else(Vec::new, |entry| entry.bytes.clone()),
            policy.encode()?,
            self.active.header.last_dictionary_promotion_unix,
        )?;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn install_initial_dictionary(
        &mut self,
        dictionary: Vec<u8>,
    ) -> Result<(), StoreError> {
        if self.head.txid != 0 || self.has_pending() || dictionary.is_empty() {
            return Err(StoreError::InvalidConfiguration(
                "an initial dictionary can only be installed into an empty database",
            ));
        }
        self.begin_write()?;
        let promoted = unix_time();
        self.install_active(dictionary, self.active.header.policy, promoted)?;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn background_flush_due(&self) -> bool {
        if self.has_pending() || self.head.txid < self.active.header.start_txid {
            return false;
        }
        let now = unix_time();
        now.saturating_sub(self.head.last_dirty_unix)
            >= u64::from(self.active.header.policy.settle_seconds)
            || now.saturating_sub(self.head.oldest_dirty_unix)
                >= u64::from(self.active.header.policy.max_stale_seconds)
    }

    pub(crate) fn try_background_maintenance(&mut self) -> Result<(), StoreError> {
        if self.has_pending() || self.publication_owner == PublicationOwner::Checkpoint {
            return Ok(());
        }
        if self.maybe_promote_dictionary()? || self.background_flush_due() {
            self.acquire_maintenance()?;
            let result = self.flush_sidecars();
            self.release_maintenance();
            result?;
        }
        self.collect_garbage(8)
    }

    pub(crate) fn acquire_maintenance(&mut self) -> Result<(), StoreError> {
        if self.publication_owner == PublicationOwner::Maintenance {
            return Ok(());
        }
        if self.publication_owner == PublicationOwner::Checkpoint {
            return Err(StoreError::Busy);
        }
        let acquired = if self.publication_owner.is_locked() {
            false
        } else {
            lock_exclusive(&self.publication, true)?;
            self.publication_owner = PublicationOwner::Transaction;
            true
        };
        if let Err(error) = self.refresh() {
            if acquired {
                self.release_publication();
            }
            return Err(error);
        }
        self.publication_owner = PublicationOwner::Maintenance;
        Ok(())
    }

    pub(crate) fn release_maintenance(&mut self) {
        if self.publication_owner == PublicationOwner::Maintenance {
            self.publication_owner = PublicationOwner::Transaction;
        }
        self.release_publication();
    }

    fn maybe_promote_dictionary(&mut self) -> Result<bool, StoreError> {
        let policy = StoragePolicy::decode(self.active.header.policy).dictionary;
        if self.trainer.pages.len() < MIN_TRAINING_PAGES || self.trainer.bytes < MIN_TRAINING_BYTES
        {
            return Ok(false);
        }
        let page_count = page_count(self.head.logical_size, self.head.page_size)?.max(1) as usize;
        let current_dictionary = self
            .active
            .dictionaries
            .first()
            .map_or(&[][..], |entry| entry.bytes.as_slice());
        if !current_dictionary.is_empty()
            && self.trainer.changed_pages.len().saturating_mul(10_000)
                < page_count.saturating_mul(policy.retrain_churn_bps as usize)
        {
            return Ok(false);
        }
        if self.active.header.last_dictionary_promotion_unix != 0
            && unix_time().saturating_sub(self.active.header.last_dictionary_promotion_unix)
                < policy.promotion_cooldown.as_secs()
        {
            return Ok(false);
        }
        let samples = self.trainer.pages.values().collect::<Vec<_>>();
        let split = samples.len().saturating_mul(4) / 5;
        let training = samples[..split]
            .iter()
            .map(|sample| sample.as_slice())
            .collect::<Vec<_>>();
        let candidate = zstd::dict::from_samples(&training, policy.dictionary_bytes as usize)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        let heldout = &samples[split..];
        let baseline = compressed_sample_size(heldout, current_dictionary)?;
        let proposed = compressed_sample_size(heldout, &candidate)?;
        if baseline == 0
            || baseline.saturating_sub(proposed).saturating_mul(10_000)
                < baseline.saturating_mul(policy.min_improvement_bps as usize)
            || baseline.saturating_sub(proposed) <= candidate.len()
        {
            return Ok(false);
        }
        self.begin_write()?;
        let result = (|| {
            if self.head.txid >= self.active.header.start_txid {
                self.flush_sidecars()?;
                self.begin_write()?;
            }
            let promoted = unix_time();
            self.install_active(candidate, self.active.header.policy, promoted)?;
            self.trainer.changed_pages.clear();
            self.release_publication();
            Ok(true)
        })();
        if result.is_err() {
            // Background-worker errors are currently best-effort. They must
            // never leave the cross-process publication lease attached to the
            // shared Store and wedge all future writers.
            self.release_publication();
        }
        result
    }

    fn remember_training_page(&mut self, page_no: u32, page: Vec<u8>) {
        let limit = usize::try_from(self.active.header.policy.dictionary.sample_bytes)
            .unwrap_or(usize::MAX);
        self.trainer.remember(page_no, page, limit);
    }

    fn page_txid_map(&self) -> Result<Vec<u64>, StoreError> {
        full_page_txid_map(
            &self.locations,
            page_count(self.head.logical_size, self.head.page_size)?,
        )
    }

    fn try_lifecycle_exclusive(&self) -> Result<bool, StoreError> {
        // Converting an existing flock from shared to exclusive is not atomic
        // on every supported platform. Explicitly drop it and always restore
        // the shared generation lease when the nonblocking acquisition loses
        // a race with another reader.
        unlock_file(&self.lifecycle)?;
        match lock_exclusive(&self.lifecycle, true) {
            Ok(()) => Ok(true),
            Err(error) => {
                lock_shared(&self.lifecycle, false)?;
                if matches!(error, StoreError::Busy) {
                    Ok(false)
                } else {
                    Err(error)
                }
            }
        }
    }

    fn collect_garbage(&mut self, limit: usize) -> Result<(), StoreError> {
        let acquired_publication = if self.publication_owner.is_locked() {
            false
        } else {
            match lock_exclusive(&self.publication, true) {
                Ok(()) => {
                    self.publication_owner = PublicationOwner::Transaction;
                    true
                }
                Err(StoreError::Busy) => return Ok(()),
                Err(error) => return Err(error),
            }
        };
        let result = (|| {
            self.refresh()?;
            if !self.try_lifecycle_exclusive()? {
                return Ok(());
            }
            let gc_result = self.collect_garbage_exclusive(limit);
            let relock_result = lock_shared(&self.lifecycle, false);
            gc_result?;
            relock_result
        })();
        if acquired_publication {
            self.release_publication();
        }
        result
    }

    fn collect_garbage_exclusive(&self, limit: usize) -> Result<(), StoreError> {
        let live_segments = self
            .segments
            .iter()
            .map(|segment| segment.id.clone())
            .collect::<BTreeSet<_>>();
        let mut removed = 0_usize;
        for segment in self.backend.list_segments()? {
            if removed >= limit {
                break;
            }
            if !live_segments.contains(&segment) {
                self.backend.delete_segment(&segment)?;
                removed += 1;
            }
        }
        Ok(())
    }

    fn release_publication(&mut self) {
        // A maintenance operation may call ordinary publication helpers more
        // than once (flush, seal, then compact). Keep one continuous critical
        // section until `release_maintenance()` so another process cannot
        // interleave between those phases.
        if self.publication_owner == PublicationOwner::Transaction {
            let _ = unlock_file(&self.publication);
            self.publication_owner = PublicationOwner::None;
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if self.publication_owner.is_locked() {
            let _ = unlock_file(&self.publication);
            self.publication_owner = PublicationOwner::None;
        }
    }
}

#[allow(clippy::too_many_lines)]
fn load_segments(
    discovered: Vec<DiscoveredSegment>,
    database_id: DatabaseId,
    page_size: u32,
) -> Result<(Vec<SegmentMeta>, BTreeMap<u32, PageLocation>), StoreError> {
    let mut segments = Vec::<SegmentMeta>::with_capacity(discovered.len());
    let mut locations = BTreeMap::new();
    for (segment_index, discovered) in discovered.into_iter().enumerate() {
        if discovered.file.metadata()?.len() != discovered.file_len
            || discovered.file_len < (SEGMENT_HEADER_SIZE + SEGMENT_TRAILER_SIZE) as u64
        {
            return Err(StoreError::Corrupt(0));
        }
        let header = discovered.header;
        let trailer = discovered.trailer;
        let trailer_offset = discovered.file_len - SEGMENT_TRAILER_SIZE as u64;
        let expected_parent = segments
            .last()
            .map_or([0; 32], |segment| segment.id.physical_digest);
        let expected_base_size = segments
            .last()
            .map_or(0, |segment| segment.trailer.logical_size);
        if header.database_id != database_id
            || trailer.database_id != database_id
            || header.start_txid != discovered.id.start_txid
            || header.page_size != trailer.page_size
            || trailer.start_txid != discovered.id.start_txid
            || trailer.end_txid != discovered.id.end_txid
            || trailer.base_history != header.base_history
            || trailer.end_history != discovered.id.end_history
            || trailer.physical_digest != discovered.id.physical_digest
            || header.parent_physical_digest != expected_parent
            || header.base_logical_size != expected_base_size
            || (page_size != 0 && trailer.page_size != page_size)
        {
            return Err(StoreError::IdentityMismatch);
        }
        let dictionary_end = checked_end(header.dictionary_offset, header.dictionary_len)?;
        let base_map_end = checked_end(header.base_map_offset, header.base_map_len)?;
        let index_end = checked_end(trailer.index_offset, trailer.index_len)?;
        let map_end = checked_end(trailer.map_offset, trailer.map_len)?;
        if dictionary_end > header.base_map_offset
            || base_map_end > header.records_offset
            || header.records_offset > trailer.index_offset
            || index_end != trailer.map_offset
            || map_end != trailer_offset
        {
            return Err(StoreError::Corrupt(0));
        }
        let dictionaries = read_dictionary_table_file(
            &discovered.file,
            header.dictionary_offset,
            header.dictionary_len,
        )?;
        let base_page_count = if segment_index == 0 {
            0
        } else {
            page_count(
                segments[segment_index - 1].trailer.logical_size,
                segments[segment_index - 1].trailer.page_size,
            )?
        };
        validate_blob_section_len(header.base_map_len, max_map_raw_len(base_page_count)?)?;
        let base_map = decode_map(
            &read_range_file(
                &discovered.file,
                header.base_map_offset,
                header.base_map_len,
            )?,
            base_page_count,
        )?;
        if base_map != full_page_txid_map(&locations, base_page_count)? {
            return Err(StoreError::Corrupt(header.base_map_offset));
        }
        let max_page = page_count(trailer.logical_size, trailer.page_size)?;
        let max_index_raw_len = u64::from(max_page)
            .checked_mul(SEGMENT_INDEX_ENTRY_SIZE as u64)
            .ok_or(StoreError::Range)?;
        validate_blob_section_len(trailer.index_len, max_index_raw_len)?;
        let index = decode_index(
            &read_range_file(&discovered.file, trailer.index_offset, trailer.index_len)?,
            max_page,
        )?;
        let mut previous_page = 0_u32;
        let mut frame_ranges = Vec::with_capacity(index.len());
        for value in &index {
            if value.page_no <= previous_page
                || value.page_no > max_page
                || value.last_txid < discovered.id.start_txid
                || value.last_txid > discovered.id.end_txid
            {
                return Err(StoreError::Corrupt(value.frame_offset));
            }
            previous_page = value.page_no;
            let frame_end = checked_end(value.frame_offset, u64::from(value.frame_record_len))?;
            if value.frame_offset < header.records_offset || frame_end > trailer.index_offset {
                return Err(StoreError::Corrupt(value.frame_offset));
            }
            frame_ranges.push((value.frame_offset, frame_end));
            locations.insert(
                value.page_no,
                PageLocation {
                    source: FrameSource::Segment(segment_index),
                    last_txid: value.last_txid,
                    frame_offset: value.frame_offset,
                    frame_record_len: value.frame_record_len,
                    page_hash: value.page_hash,
                },
            );
        }
        if let Some(offset) = overlapping_frame_offset(&mut frame_ranges) {
            return Err(StoreError::Corrupt(offset));
        }
        validate_blob_section_len(trailer.map_len, max_map_raw_len(max_page)?)?;
        let page_map = decode_map(
            &read_range_file(&discovered.file, trailer.map_offset, trailer.map_len)?,
            max_page,
        )?;
        // The ending map describes the complete logical database, not merely
        // pages contributed by this segment. In particular, a shrink followed
        // by regrowth can leave a zero map entry that tombstones a page still
        // present in an older segment. Treat the map as authoritative for
        // removals while requiring every indexed page to be selected by it.
        for value in &index {
            let map_txid = value
                .page_no
                .checked_sub(1)
                .and_then(|index| page_map.get(index as usize))
                .copied();
            if map_txid != Some(value.last_txid) {
                return Err(StoreError::Corrupt(trailer.map_offset));
            }
        }
        locations.retain(|page, location| {
            page.checked_sub(1)
                .and_then(|index| page_map.get(index as usize))
                .is_some_and(|txid| *txid == location.last_txid)
        });
        if page_map != full_page_txid_map(&locations, max_page)?
            || trailer.content_root != content_root(&locations)
        {
            return Err(StoreError::Corrupt(trailer.map_offset));
        }
        segments.push(SegmentMeta {
            id: discovered.id,
            file: discovered.file,
            file_len: discovered.file_len,
            trailer,
            dictionaries,
        });
    }
    Ok((segments, locations))
}

fn finalize_segment(
    file: &File,
    index_offset: u64,
    index: &[SegmentIndexEntry],
    page_map: &[u64],
    mut trailer: SegmentTrailer,
) -> Result<SegmentTrailer, StoreError> {
    let index_blob = encode_index(index)?;
    let map_blob = encode_map(page_map)?;
    trailer.index_offset = index_offset;
    trailer.index_len = index_blob.len() as u64;
    trailer.map_offset = checked_end(index_offset, trailer.index_len)?;
    trailer.map_len = map_blob.len() as u64;
    let trailer_offset = checked_end(trailer.map_offset, trailer.map_len)?;

    file.set_len(index_offset)?;
    write_all_at(file, index_offset, &index_blob)?;
    write_all_at(file, trailer.map_offset, &map_blob)?;
    write_all_at(file, trailer_offset, &trailer.encode(false))?;
    file.set_len(checked_end(trailer_offset, SEGMENT_TRAILER_SIZE as u64)?)?;
    trailer.physical_digest = hash_file(file)?;
    write_all_at(file, trailer_offset, &trailer.encode(true))?;
    file.sync_all()?;
    Ok(trailer)
}

fn read_commit(
    file: &File,
    offset: u64,
    committed_end: u64,
) -> Result<(CommitHeader, Vec<CommitEntry>), StoreError> {
    let mut encoded = [0; COMMIT_HEADER_SIZE];
    read_exact_at(file, offset, &mut encoded)?;
    let header = CommitHeader::decode(&encoded)?;
    let record_end = offset
        .checked_add(u64::from(header.record_len))
        .ok_or(StoreError::Range)?;
    if record_end > committed_end {
        return Err(StoreError::Corrupt(offset));
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(header.entry_count as usize)
        .map_err(|_| StoreError::Range)?;
    let mut cursor = offset + COMMIT_HEADER_SIZE as u64;
    for _ in 0..header.entry_count {
        let mut encoded = [0; COMMIT_ENTRY_SIZE];
        read_exact_at(file, cursor, &mut encoded)?;
        entries.push(CommitEntry::decode(&encoded)?);
        cursor += COMMIT_ENTRY_SIZE as u64;
    }
    if cursor < record_end {
        let padding = read_range_file(file, cursor, record_end - cursor)?;
        if padding.iter().any(|byte| *byte != 0) {
            return Err(StoreError::Corrupt(cursor));
        }
    }
    Ok((header, entries))
}

fn encode_commit_entries(entries: &[CommitEntry]) -> Vec<u8> {
    entries.iter().flat_map(|entry| entry.encode()).collect()
}

fn page_hash(txid: u64, page_no: u32, page: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zsqlite/page/v1\0");
    hasher.update(&txid.to_le_bytes());
    hasher.update(&page_no.to_le_bytes());
    hasher.update(page);
    *hasher.finalize().as_bytes()
}

fn decode_frame_payload(
    header: FrameHeader,
    stored: &[u8],
    dictionaries: &[DictionaryEntry],
    page_no: u32,
    txid: u64,
    expected_hash: Digest,
    offset: u64,
) -> Result<Vec<u8>, StoreError> {
    if stored.len() != header.stored_len as usize {
        return Err(StoreError::Corrupt(offset));
    }
    let raw = match header.codec {
        Codec::Raw => stored.to_vec(),
        Codec::Zstd => {
            let dictionary = dictionaries
                .get(header.dictionary_index as usize)
                .ok_or(StoreError::Corrupt(offset))?;
            let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dictionary.bytes)
                .map_err(|error| StoreError::Zstd(error.to_string()))?;
            decompressor
                .decompress(stored, header.raw_len as usize)
                .map_err(|error| StoreError::Zstd(error.to_string()))?
        }
    };
    if raw.len() != header.raw_len as usize || page_hash(txid, page_no, &raw) != expected_hash {
        return Err(StoreError::PageChecksum(page_no));
    }
    Ok(raw)
}

fn transaction_hash(
    txid: u64,
    logical_size: u64,
    page_size: u32,
    truncate_pages: Option<u32>,
    entries: &[CommitEntry],
) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zsqlite/transaction/v1\0");
    hasher.update(&txid.to_le_bytes());
    hasher.update(&logical_size.to_le_bytes());
    hasher.update(&page_size.to_le_bytes());
    if let Some(truncate_pages) = truncate_pages {
        hasher.update(b"truncate\0");
        hasher.update(&truncate_pages.to_le_bytes());
    }
    for entry in entries {
        hasher.update(&entry.page_no.to_le_bytes());
        hasher.update(&entry.page_hash);
    }
    *hasher.finalize().as_bytes()
}

fn history_hash(previous: Digest, transaction: Digest) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zsqlite/history/v1\0");
    hasher.update(&previous);
    hasher.update(&transaction);
    *hasher.finalize().as_bytes()
}

fn content_root(locations: &BTreeMap<u32, PageLocation>) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zsqlite/content/v1\0");
    for (page_no, location) in locations {
        hasher.update(&page_no.to_le_bytes());
        hasher.update(&location.last_txid.to_le_bytes());
        hasher.update(&location.page_hash);
    }
    *hasher.finalize().as_bytes()
}

fn full_page_txid_map(
    locations: &BTreeMap<u32, PageLocation>,
    page_count: u32,
) -> Result<Vec<u64>, StoreError> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(page_count as usize)
        .map_err(|_| StoreError::Range)?;
    output.resize(page_count as usize, 0);
    for (page_no, location) in locations {
        let index = page_no.checked_sub(1).ok_or(StoreError::Corrupt(0))?;
        let slot = output
            .get_mut(index as usize)
            .ok_or(StoreError::Corrupt(u64::from(*page_no)))?;
        *slot = location.last_txid;
    }
    Ok(output)
}

fn read_dictionary_table_file(
    file: &File,
    offset: u64,
    length: u64,
) -> Result<Vec<DictionaryEntry>, StoreError> {
    decode_dictionary_table(&read_range_file(file, offset, length)?).map_err(StoreError::from)
}

fn read_range_file(file: &File, offset: u64, length: u64) -> Result<Vec<u8>, StoreError> {
    if length > MAX_SECTION_BYTES {
        return Err(StoreError::Range);
    }
    let length = usize::try_from(length).map_err(|_| StoreError::Range)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| StoreError::Range)?;
    output.resize(length, 0);
    read_exact_at(file, offset, &mut output)?;
    Ok(output)
}

fn union_dictionaries(segments: &[SegmentMeta]) -> Vec<DictionaryEntry> {
    let mut output = BTreeMap::new();
    for dictionary in segments.iter().flat_map(|segment| &segment.dictionaries) {
        output
            .entry(dictionary.digest)
            .or_insert_with(|| dictionary.bytes.clone());
    }
    output
        .into_iter()
        .map(|(digest, bytes)| DictionaryEntry { digest, bytes })
        .collect()
}

fn compressed_sample_size(samples: &[&Vec<u8>], dictionary: &[u8]) -> Result<usize, StoreError> {
    if dictionary.is_empty() {
        return Ok(samples.iter().map(|sample| sample.len()).sum());
    }
    let mut compressor = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, dictionary)
        .map_err(|error| StoreError::Zstd(error.to_string()))?;
    let mut total = 0_usize;
    for sample in samples {
        total = total
            .checked_add(
                compressor
                    .compress(sample)
                    .map_err(|error| StoreError::Zstd(error.to_string()))?
                    .len(),
            )
            .ok_or(StoreError::Range)?;
    }
    Ok(total)
}

fn verify_segment_physical(segment: &SegmentMeta) -> Result<(), StoreError> {
    if segment.file.metadata()?.len() != segment.file_len
        || segment.trailer.physical_digest != segment.id.physical_digest
        || !verify_file_physical(&segment.file, segment.trailer)?
    {
        return Err(StoreError::Corrupt(
            segment.file_len - SEGMENT_TRAILER_SIZE as u64,
        ));
    }
    Ok(())
}

fn hash_file(file: &File) -> Result<Digest, StoreError> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0; COPY_BUFFER_SIZE];
    loop {
        let amount = file.read(&mut buffer)?;
        if amount == 0 {
            break;
        }
        hasher.update(&buffer[..amount]);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn overlapping_frame_offset(frame_ranges: &mut [(u64, u64)]) -> Option<u64> {
    frame_ranges.sort_unstable();
    frame_ranges
        .windows(2)
        .find(|pair| pair[0].1 > pair[1].0)
        .map(|pair| pair[1].0)
}

fn read_segment_header(file: &File) -> Result<SegmentHeader, StoreError> {
    let mut encoded = [0; SEGMENT_HEADER_SIZE];
    read_exact_at(file, 0, &mut encoded).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            StoreError::NotZsqlite
        } else {
            error.into()
        }
    })?;
    SegmentHeader::decode(&encoded).map_err(StoreError::from)
}

fn read_current_trailer(
    file: &File,
    header: SegmentHeader,
) -> Result<Option<SegmentTrailer>, StoreError> {
    let file_len = file.metadata()?.len();
    if file_len < header.records_offset + SEGMENT_TRAILER_SIZE as u64 {
        return Ok(None);
    }
    let offset = file_len - SEGMENT_TRAILER_SIZE as u64;
    let mut encoded = [0; SEGMENT_TRAILER_SIZE];
    read_exact_at(file, offset, &mut encoded)?;
    if &encoded[..8] != crate::format::TRAILER_MAGIC {
        return Ok(None);
    }
    let Ok(trailer) = SegmentTrailer::decode(&encoded) else {
        return Ok(None);
    };
    if trailer.database_id != header.database_id
        || trailer.start_txid != header.start_txid
        || trailer.base_history != header.base_history
        || trailer.map_offset.checked_add(trailer.map_len) != Some(offset)
        || !verify_file_physical(file, trailer)?
    {
        return Ok(None);
    }
    Ok(Some(trailer))
}

fn verify_file_physical(file: &File, trailer: SegmentTrailer) -> Result<bool, StoreError> {
    let trailer_offset = file
        .metadata()?
        .len()
        .checked_sub(SEGMENT_TRAILER_SIZE as u64)
        .ok_or(StoreError::Range)?;
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    let mut buffer = vec![0; COPY_BUFFER_SIZE];
    while offset < trailer_offset {
        let amount = usize::try_from((trailer_offset - offset).min(buffer.len() as u64))
            .map_err(|_| StoreError::Range)?;
        read_exact_at(file, offset, &mut buffer[..amount])?;
        hasher.update(&buffer[..amount]);
        offset += amount as u64;
    }
    let mut zeroed = trailer;
    zeroed.physical_digest = [0; 32];
    hasher.update(&zeroed.encode(false));
    Ok(*hasher.finalize().as_bytes() == trailer.physical_digest)
}

fn discover_lineage(
    backend: &FsSegmentBackend,
    active: SegmentHeader,
) -> Result<Vec<DiscoveredSegment>, StoreError> {
    let listed = backend.list_segments()?;
    let mut by_digest = HashMap::<Digest, Vec<SegmentId>>::with_capacity(listed.len());
    for id in listed {
        by_digest.entry(id.physical_digest).or_default().push(id);
    }
    let mut target = active.parent_physical_digest;
    let mut expected_end = active.start_txid - 1;
    let mut expected_history = active.base_history;
    let mut reversed = Vec::new();
    let mut seen = BTreeSet::new();
    while target != [0; 32] {
        if !seen.insert(target) {
            return Err(StoreError::Corrupt(0));
        }
        let id = by_digest
            .get(&target)
            .and_then(|candidates| {
                candidates
                    .iter()
                    .find(|id| id.end_txid == expected_end && id.end_history == expected_history)
            })
            .cloned()
            .ok_or(StoreError::MissingSidecar)?;
        let file = File::open(backend.segment_path(&id))?;
        let file_len = file.metadata()?.len();
        if file_len < (SEGMENT_HEADER_SIZE + SEGMENT_TRAILER_SIZE) as u64 {
            return Err(StoreError::Corrupt(0));
        }
        let mut header_bytes = [0; SEGMENT_HEADER_SIZE];
        read_exact_at(&file, 0, &mut header_bytes)?;
        let header = SegmentHeader::decode(&header_bytes)?;
        let mut trailer_bytes = [0; SEGMENT_TRAILER_SIZE];
        read_exact_at(
            &file,
            file_len - SEGMENT_TRAILER_SIZE as u64,
            &mut trailer_bytes,
        )?;
        let trailer = SegmentTrailer::decode(&trailer_bytes)?;
        if header.database_id != active.database_id
            || trailer.database_id != active.database_id
            || id.physical_digest != target
            || trailer.physical_digest != target
            || id.start_txid != header.start_txid
            || id.end_txid != trailer.end_txid
            || id.end_history != trailer.end_history
            || trailer.end_txid != expected_end
            || trailer.end_history != expected_history
        {
            return Err(StoreError::IdentityMismatch);
        }
        reversed.push(DiscoveredSegment {
            id,
            file,
            file_len,
            header,
            trailer,
        });
        target = header.parent_physical_digest;
        expected_end = header.start_txid - 1;
        expected_history = header.base_history;
    }
    if expected_end != 0 || expected_history != genesis_history() {
        return Err(StoreError::Corrupt(0));
    }
    reversed.reverse();
    Ok(reversed)
}

fn align_up(value: u64, alignment: u64) -> Result<u64, StoreError> {
    value
        .checked_add(alignment - 1)
        .map(|value| value / alignment * alignment)
        .ok_or(StoreError::Range)
}

fn active_staging_path(path: &Path, id: [u8; 16]) -> PathBuf {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("database.zsqlite");
    path.with_file_name(format!(".{name}.{}.next", hex_active(id)))
}

fn validate_policy(value: StoragePolicyRecord) -> Result<(), StoreError> {
    if value.settle_seconds == 0
        || value.max_stale_seconds < value.settle_seconds
        || !(8 * 1024..=112 * 1024).contains(&value.dictionary.dictionary_bytes)
        || value.dictionary.sample_bytes < MIN_TRAINING_BYTES as u64
        || value.dictionary.min_improvement_bps > 10_000
        || value.dictionary.retrain_churn_bps > 10_000
        || value.dictionary.promotion_cooldown_seconds == 0
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

fn checked_end(offset: u64, length: u64) -> Result<u64, StoreError> {
    offset.checked_add(length).ok_or(StoreError::Range)
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

fn sync_file(file: &File, full_sync: bool) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    if full_sync {
        // Match SQLite's Unix VFS: attempt the stronger drive-cache flush and
        // fall back to fsync when the filesystem does not support it.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC, 0) } == 0 {
            return Ok(());
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = full_sync;
    file.sync_all()
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

fn lock_existing_for_delete(path: &Path) -> Result<Option<File>, StoreError> {
    let file = match open_lock(path, true, false) {
        Ok(file) => file,
        Err(StoreError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    lock_exclusive(&file, true)?;
    Ok(Some(file))
}

#[cfg(unix)]
fn reject_aliased_active(file: &File, header: SegmentHeader) -> Result<(), StoreError> {
    use std::os::unix::fs::MetadataExt;
    if file.metadata()?.nlink() == 1 || read_current_trailer(file, header)?.is_some() {
        Ok(())
    } else {
        Err(StoreError::Busy)
    }
}

#[cfg(not(unix))]
fn reject_aliased_active(_file: &File, _header: SegmentHeader) -> Result<(), StoreError> {
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

fn zero_range(file: &File, offset: u64, length: usize) -> Result<(), StoreError> {
    let zeros = [0; 4096];
    let mut offset = offset;
    let mut remaining = length;
    while remaining > 0 {
        let amount = remaining.min(zeros.len());
        write_all_at(file, offset, &zeros[..amount])?;
        offset += amount as u64;
        remaining -= amount;
    }
    Ok(())
}

fn punch_payload(file: &File, offset: u64, length: u64) -> std::io::Result<()> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
    let start = offset.saturating_add(4095) / 4096 * 4096;
    let aligned_end = end / 4096 * 4096;
    if start >= aligned_end {
        return Ok(());
    }
    punch_aligned(file, start, aligned_end - start)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn punch_aligned(file: &File, offset: u64, length: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let offset =
        libc::off_t::try_from(offset).map_err(|_| std::io::Error::from(ErrorKind::InvalidInput))?;
    let length =
        libc::off_t::try_from(length).map_err(|_| std::io::Error::from(ErrorKind::InvalidInput))?;
    let result = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset,
            length,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(target_os = "macos")]
fn punch_aligned(file: &File, offset: u64, length: u64) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let offset =
        libc::off_t::try_from(offset).map_err(|_| std::io::Error::from(ErrorKind::InvalidInput))?;
    let length =
        libc::off_t::try_from(length).map_err(|_| std::io::Error::from(ErrorKind::InvalidInput))?;
    let mut range = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset: offset,
        fp_length: length,
    };
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &raw mut range) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
fn punch_aligned(_file: &File, _offset: u64, _length: u64) -> std::io::Result<()> {
    Err(std::io::Error::from(ErrorKind::Unsupported))
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
fn flock(file: &File, operation: libc::c_int) -> Result<(), StoreError> {
    use std::os::fd::AsRawFd;
    loop {
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == ErrorKind::WouldBlock {
            return Err(StoreError::Busy);
        }
        return Err(error.into());
    }
}

fn lock_exclusive(file: &File, nonblocking: bool) -> Result<(), StoreError> {
    flock(
        file,
        libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 },
    )
}
fn lock_shared(file: &File, nonblocking: bool) -> Result<(), StoreError> {
    flock(
        file,
        libc::LOCK_SH | if nonblocking { libc::LOCK_NB } else { 0 },
    )
}
fn unlock_file(file: &File) -> Result<(), StoreError> {
    flock(file, libc::LOCK_UN)
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

    #[test]
    fn active_file_is_initialized_with_a_valid_segment_header()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("active.zsqlite");
        let store = Store::open(&path, true)?;
        let header = read_segment_header(&store.active.file)?;
        assert_eq!(header.database_id, store.active.header.database_id);
        assert_eq!(header.start_txid, 1);
        assert_eq!(header.records_offset, store.active.file.metadata()?.len());
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
    fn overlapping_frame_offset_reports_the_actual_later_frame() {
        let mut ranges = [(100, 110), (40, 50), (20, 30), (45, 60)];
        assert_eq!(overlapping_frame_offset(&mut ranges), Some(45));
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
        assert_eq!(store.inspect()?.sealed_segments, 1);
        let mut output = vec![0; 8192];
        assert_eq!(store.read_at(0, &mut output)?, output.len());
        assert_eq!(&output[..4096], first);
        assert_eq!(&output[4096..], second);
        Ok(())
    }

    #[test]
    fn invalid_active_tail_recovers_the_last_valid_commit() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("fallback.zsqlite");
        let first = page(7, 4096);
        let second = page(8, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(0, &second)?;
        store.publish(true)?;
        assert_eq!(store.head.txid, 2);

        let newest = store.locations.get(&1).copied().ok_or(StoreError::Range)?;
        let active = &store.active.file;
        let payload_offset = newest.frame_offset + FRAME_HEADER_SIZE as u64;
        let mut byte = [0_u8; 1];
        read_exact_at(active, payload_offset, &mut byte)?;
        byte[0] ^= 0xff;
        write_all_at(active, payload_offset, &byte)?;
        active.sync_all()?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        assert_eq!(reopened.head.txid, 1);
        let mut output = vec![0; first.len()];
        reopened.read_at(0, &mut output)?;
        assert_eq!(output, first);
        Ok(())
    }

    #[test]
    fn page_reads_reject_an_index_frame_length_mismatch() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("frame-length.zsqlite");
        let first = page(7, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        let location = store.locations.get_mut(&1).ok_or(StoreError::Range)?;
        location.frame_record_len = location
            .frame_record_len
            .checked_sub(1)
            .ok_or(StoreError::Range)?;
        store.cache.clear();

        let mut output = vec![0; 4096];
        assert!(matches!(
            store.read_at(0, &mut output),
            Err(StoreError::Corrupt(_))
        ));
        Ok(())
    }

    #[test]
    fn refresh_ignores_an_invalid_new_tail() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("refresh-failure.zsqlite");
        let first = page(7, 4096);
        let second = page(8, 4096);
        let mut writer = Store::open(&path, true)?;
        writer.write_at(0, &first)?;
        writer.publish(true)?;
        let mut stale_reader = Store::open_existing_read_only(&path)?;
        let selected_before_refresh = stale_reader.head;

        writer.write_at(0, &second)?;
        writer.publish(true)?;
        let commit_offset = writer.head.active_commit_offset;
        let active = &writer.active.file;
        let mut byte = [0_u8; 1];
        read_exact_at(active, commit_offset + 24, &mut byte)?;
        byte[0] ^= 0x80;
        write_all_at(active, commit_offset + 24, &byte)?;
        active.sync_all()?;

        stale_reader.refresh()?;
        stale_reader.refresh()?;
        assert_eq!(stale_reader.head, selected_before_refresh);
        let mut output = vec![0; 4096];
        stale_reader.read_at(0, &mut output)?;
        assert_eq!(output, first, "the prior pinned snapshot was not restored");
        Ok(())
    }

    #[test]
    fn corrupt_uncommitted_tail_can_be_replaced_by_a_new_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("nondurable-fallback.zsqlite");
        let first = page(7, 4096);
        let second = page(8, 4096);
        let third = page(9, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(0, &second)?;
        store.publish(false)?;

        let newest = store.locations.get(&1).copied().ok_or(StoreError::Range)?;
        let active = &store.active.file;
        let payload_offset = newest.frame_offset + FRAME_HEADER_SIZE as u64;
        let mut byte = [0_u8; 1];
        read_exact_at(active, payload_offset, &mut byte)?;
        byte[0] ^= 0xff;
        write_all_at(active, payload_offset, &byte)?;
        drop(store);

        let mut store = Store::open_existing(&path)?;
        assert_eq!(store.head.txid, 1);
        let mut output = vec![0; 4096];
        store.read_at(0, &mut output)?;
        assert_eq!(output, first);

        store.write_at(0, &third)?;
        store.publish(true)?;
        assert_eq!(store.head.txid, 2);
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        reopened.read_at(0, &mut output)?;
        assert_eq!(output, third);
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
        assert_eq!(store.page_txid_map()?, vec![1, 0, 0]);
        store.flush_sidecars()?;
        assert_eq!(store.page_txid_map()?, vec![1, 0, 0]);

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
    fn a_corrupt_last_append_preserves_the_preceding_valid_append()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("durable-pin.zsqlite");
        let first = page(1, 4096);
        let second = page(2, 4096);
        let third = page(3, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(0, &second)?;
        store.publish(false)?;
        store.write_at(0, &third)?;
        store.publish(false)?;
        assert_eq!(store.head.txid, 3);

        let newest = store.locations.get(&1).copied().ok_or(StoreError::Range)?;
        let active = &store.active.file;
        let payload_offset = newest.frame_offset + FRAME_HEADER_SIZE as u64;
        let mut byte = [0_u8; 1];
        read_exact_at(active, payload_offset, &mut byte)?;
        byte[0] ^= 0x80;
        write_all_at(active, payload_offset, &byte)?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        assert_eq!(reopened.head.txid, 2);
        let mut output = vec![0; 4096];
        reopened.read_at(0, &mut output)?;
        assert_eq!(output, second);
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
    fn committed_active_prefix_is_append_only_and_survives_a_stale_reader()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("append-only.zsqlite");
        let first = page(1, 4096);
        let second = page(2, 4096);
        let third = page(3, 4096);
        let mut writer = Store::open(&path, true)?;
        writer.write_at(0, &first)?;
        writer.publish(true)?;
        let committed_end = writer.head.active_commit_end;
        let mut committed_prefix = vec![0; usize::try_from(committed_end)?];
        read_exact_at(&writer.active.file, 0, &mut committed_prefix)?;
        let mut stale_reader = Store::open_existing_read_only(&path)?;

        writer.write_at(0, &second)?;
        writer.publish(true)?;
        writer.write_at(0, &third)?;
        writer.publish(true)?;
        let mut current_prefix = vec![0; committed_prefix.len()];
        read_exact_at(&writer.active.file, 0, &mut current_prefix)?;
        assert_eq!(current_prefix, committed_prefix);

        let mut output = vec![0; 4096];
        stale_reader.read_at(0, &mut output)?;
        assert_eq!(output, first);
        Ok(())
    }

    #[test]
    fn failed_lifecycle_upgrade_restores_the_shared_lease() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("lifecycle.zsqlite");
        let first = Store::open(&path, true)?;
        let second = Store::open_existing_read_only(&path)?;
        assert!(!first.try_lifecycle_exclusive()?);
        drop(second);

        let probe = open_lock(&first.backend.lock_path("lifecycle"), true, false)?;
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
    fn malformed_maps_are_rejected_before_count_driven_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut huge_count = Vec::from(u64::MAX.to_le_bytes());
        huge_count.extend_from_slice(&[1, 0]);
        let encoded = encode_blob(*MAP_BLOB_MAGIC, &huge_count)?;
        assert!(matches!(
            decode_map(&encoded, 1),
            Err(StoreError::Corrupt(0))
        ));

        let mut noncanonical = Vec::from(1_u64.to_le_bytes());
        noncanonical.extend_from_slice(&[0x81, 0x00, 0x00]);
        let encoded = encode_blob(*MAP_BLOB_MAGIC, &noncanonical)?;
        assert!(matches!(
            decode_map(&encoded, 1),
            Err(StoreError::Corrupt(0))
        ));

        let mut overflow = Vec::from(1_u64.to_le_bytes());
        overflow.extend_from_slice(&[0xff; 9]);
        overflow.extend_from_slice(&[0x02, 0x00]);
        let encoded = encode_blob(*MAP_BLOB_MAGIC, &overflow)?;
        assert!(matches!(
            decode_map(&encoded, 1),
            Err(StoreError::Corrupt(0))
        ));

        let impossible_map = encode_map(&[1])?;
        assert!(matches!(
            decode_map(&impossible_map, 0),
            Err(StoreError::Range)
        ));

        let impossible_index = encode_index(&[SegmentIndexEntry {
            page_no: 1,
            last_txid: 1,
            frame_offset: SEGMENT_HEADER_SIZE as u64,
            frame_record_len: u32::try_from(FRAME_HEADER_SIZE)?,
            page_hash: [0; 32],
        }])?;
        assert!(matches!(
            decode_index(&impossible_index, 0),
            Err(StoreError::Range)
        ));
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
            match next(&mut random) % 10 {
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
            assert_eq!(actual, working, "step {step}");
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
