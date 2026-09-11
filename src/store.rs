//! Transactional page storage built from mutable active and immutable sealed segments.

use crate::backend::{FsSegmentBackend, sidecar_dir};
use crate::format::{
    ACTIVE_STATE_SIZE, ActiveState, Codec, DatabaseId, DictionaryEntry, DictionaryPolicyRecord,
    Digest, FRAME_HEADER_SIZE, FrameHeader, MAX_SECTION_BYTES, SECTOR_SIZE, SEGMENT_HEADER_SIZE,
    SEGMENT_INDEX_ENTRY_SIZE, SEGMENT_MAGIC, SEGMENT_TRAILER_SIZE, SegmentHeader, SegmentId,
    SegmentIndexEntry, SegmentTrailer, StoragePolicyRecord, decode_dictionary_table, digest,
    encode_dictionary_table, genesis_history, valid_page_size,
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
const ACTIVE_STATE_A_OFFSET: u64 = SEGMENT_HEADER_SIZE as u64;
const ACTIVE_STATE_B_OFFSET: u64 = ACTIVE_STATE_A_OFFSET + ACTIVE_STATE_SIZE as u64;
const ACTIVE_METADATA_END: u64 = ACTIVE_STATE_B_OFFSET + ACTIVE_STATE_SIZE as u64;

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
    /// Maximum size of the one dictionary trained while sealing a snapshot.
    pub dictionary_bytes: u32,
    /// Maximum raw image bytes sampled while training at seal time.
    pub sample_bytes: u64,
}

impl Default for DictionaryPolicy {
    fn default() -> Self {
        Self {
            dictionary_bytes: 64 * 1024,
            sample_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoragePolicy {
    pub settle: Duration,
    pub max_stale: Duration,
    /// Seal the active generation at a committed boundary once its raw records
    /// reach this many bytes. Zero disables size-triggered sealing.
    pub target_segment_bytes: u64,
    pub dictionary: DictionaryPolicy,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            settle: Duration::from_secs(5 * 60),
            max_stale: Duration::from_secs(60 * 60),
            target_segment_bytes: 0,
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
            target_segment_bytes: self.target_segment_bytes,
            dictionary: DictionaryPolicyRecord {
                dictionary_bytes: self.dictionary.dictionary_bytes,
                sample_bytes: self.dictionary.sample_bytes,
            },
        };
        validate_policy(record)?;
        Ok(record)
    }

    fn decode(value: StoragePolicyRecord) -> Self {
        Self {
            settle: Duration::from_secs(u64::from(value.settle_seconds)),
            max_stale: Duration::from_secs(u64::from(value.max_stale_seconds)),
            target_segment_bytes: value.target_segment_bytes,
            dictionary: DictionaryPolicy {
                dictionary_bytes: value.dictionary.dictionary_bytes,
                sample_bytes: value.dictionary.sample_bytes,
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

#[derive(Clone, Copy, Debug)]
struct PageLocation {
    segment_index: usize,
    last_txid: u64,
    frame_offset: u64,
    frame_record_len: u32,
    page_hash: Digest,
}

#[derive(Clone, Copy, Debug)]
struct PageChange {
    page_no: u32,
    page_hash: Digest,
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
    /// Dictionaries available at this point in the lineage. Small successor
    /// segments share this allocation when they add no dictionary.
    dictionaries: Arc<Vec<DictionaryEntry>>,
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
        self.insert_shared(key, Arc::new(value));
    }

    fn insert_shared(&mut self, key: PageCacheKey, value: Arc<Vec<u8>>) {
        if let Some(position) = self
            .entries
            .iter()
            .position(|(candidate, _)| *candidate == key)
            && let Some((_, old)) = self.entries.remove(position)
        {
            self.bytes = self.bytes.saturating_sub(old.len());
        }
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
    pending_raw_pages: BTreeSet<u32>,
    pending_raw_originals: BTreeMap<u32, Vec<u8>>,
    pending_size: u64,
    pending_truncate_pages: Option<u32>,
    pending_dirty: bool,
    bootstrap: Option<BootstrapFile>,
    cache: PageCache,
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
        reject_aliased_active(&file)?;
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
        let dictionary_offset = ACTIVE_METADATA_END;
        let base_map_offset = dictionary_offset + dictionary_bytes.len() as u64;
        let records_offset = align_up(base_map_offset + base_map.len() as u64, SECTOR_SIZE as u64)?;
        let header = SegmentHeader {
            mutable_snapshot: true,
            database_id,
            page_size: 0,
            start_txid: 1,
            base_history: genesis_history(),
            parent_physical_digest: [0; 32],
            base_logical_size: 0,
            generation: 1,
            policy,
            dictionary_offset,
            dictionary_len: dictionary_bytes.len() as u64,
            base_map_offset,
            base_map_len: base_map.len() as u64,
            records_offset,
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
            cache: PageCache::new(PAGE_CACHE_BYTES),
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
        self.pending_dirty
            || !self.pending_raw_pages.is_empty()
            || !self.pending_raw_originals.is_empty()
            || self.bootstrap.is_some()
    }

    pub(crate) fn refresh(&mut self) -> Result<(), StoreError> {
        if self.has_pending() {
            return Ok(());
        }
        let mutable_advanced = read_active_state(&self.active.file, self.active.header.database_id)
            .is_ok_and(|state| state.sequence > self.active.state_sequence);
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
            lock_exclusive(&self.publication, true)?;
            self.publication_owner = PublicationOwner::Transaction;
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
        if self.publication_owner == PublicationOwner::Checkpoint {
            self.publication_owner = PublicationOwner::Transaction;
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
        let mut entries = Vec::with_capacity(self.pending_raw_pages.len());
        for page_no in &self.pending_raw_pages {
            let page = self.read_mutable_page(*page_no)?;
            entries.push(PageChange {
                page_no: *page_no,
                page_hash: page_hash(txid, *page_no, &page),
            });
        }
        let transaction_hash = transaction_hash(
            txid,
            self.pending_size,
            self.head.page_size,
            self.pending_truncate_pages,
            &entries,
        );
        let resulting_history = history_hash(self.head.history, transaction_hash);
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
        if durable {
            sync_file(&self.active.file, full_sync)?;
        }

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
        self.pending_raw_pages.clear();
        self.pending_raw_originals.clear();
        self.pending_truncate_pages = None;
        self.pending_dirty = false;
        self.bootstrap = None;
        if retain_publication {
            self.publication_owner = PublicationOwner::Checkpoint;
        } else {
            self.release_publication();
            self.rollover_at_size_target()?;
        }
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
        self.install_active(self.active.header.policy)
    }

    fn install_active(&mut self, policy: StoragePolicyRecord) -> Result<(), StoreError> {
        let parent_physical_digest = self
            .segments
            .last()
            .map_or([0; 32], |segment| segment.id.physical_digest);
        let active = self.create_active(parent_physical_digest, policy)?;
        self.adopt_active(active);
        Ok(())
    }

    fn create_active(
        &self,
        parent_physical_digest: Digest,
        policy: StoragePolicyRecord,
    ) -> Result<ActiveSegment, StoreError> {
        let staging_id = random_bytes()?;
        let dictionary_bytes = encode_dictionary_table(&[])?;
        let base_map = encode_map(&self.page_txid_map()?)?;
        let dictionary_offset = ACTIVE_METADATA_END;
        let base_map_offset = dictionary_offset
            .checked_add(dictionary_bytes.len() as u64)
            .ok_or(StoreError::Range)?;
        let unaligned_records_offset = base_map_offset
            .checked_add(base_map.len() as u64)
            .ok_or(StoreError::Range)?;
        let records_offset = align_up(unaligned_records_offset, SECTOR_SIZE as u64)?;
        let header = SegmentHeader {
            mutable_snapshot: true,
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
        write_all_at(
            &file,
            ACTIVE_STATE_A_OFFSET,
            &ActiveState {
                database_id: header.database_id,
                sequence: 1,
                txid: self.head.txid,
                logical_size: self.head.logical_size,
                page_size: self.head.page_size,
                history: self.head.history,
                commit_unix: 0,
                record_count: 0,
                truncate_pages: None,
            }
            .encode(),
        )?;
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
            state_sequence: 1,
            active_records: BTreeMap::new(),
            truncate_pages: None,
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
        if self
            .pending_truncate_pages
            .is_some_and(|preserved_pages| page_no > preserved_pages)
            && !self.pending_raw_pages.contains(&page_no)
        {
            return Ok(vec![0; self.head.page_size as usize]);
        }
        if self.active.active_records.contains_key(&page_no) {
            return self.read_mutable_page(page_no);
        }
        if self
            .active
            .truncate_pages
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
            location.segment_index,
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
        segment_index: usize,
        offset: u64,
        expected_record_len: u32,
        page_no: u32,
        txid: u64,
        expected_hash: Digest,
    ) -> Result<Vec<u8>, StoreError> {
        let mut encoded = [0; FRAME_HEADER_SIZE];
        self.read_source(segment_index, offset, &mut encoded)?;
        let header = FrameHeader::decode(&encoded)?;
        if header.record_len() != u64::from(expected_record_len)
            || header.page_no != page_no
            || header.raw_len != self.head.page_size
        {
            return Err(StoreError::Corrupt(offset));
        }
        let mut stored = vec![0; header.stored_len as usize];
        self.read_source(
            segment_index,
            offset + FRAME_HEADER_SIZE as u64,
            &mut stored,
        )?;
        let dictionaries = &self
            .segments
            .get(segment_index)
            .ok_or(StoreError::Corrupt(offset))?
            .dictionaries;
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
        segment_index: usize,
        offset: u64,
        output: &mut [u8],
    ) -> Result<(), StoreError> {
        read_exact_at(
            &self
                .segments
                .get(segment_index)
                .ok_or(StoreError::Corrupt(offset))?
                .file,
            offset,
            output,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn reload(&mut self) -> Result<(), StoreError> {
        let mut options = OpenOptions::new();
        options.read(true).write(self.writable);
        let current = options.open(&self.path)?;
        let header = read_segment_header(&current)?;
        if header.database_id != self.active.header.database_id || !header.mutable_snapshot {
            return Err(StoreError::IdentityMismatch);
        }
        let discovered = discover_lineage(&self.backend, header)?;
        let page_size = header.page_size;
        let (segments, locations) = load_segments(discovered, header.database_id, page_size)?;
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
        if !dictionaries.is_empty() {
            return Err(StoreError::Corrupt(header.dictionary_offset));
        }
        let state = read_active_state(&current, header.database_id)?;
        if state.page_size != header.page_size
            || state.txid < sealed_head_txid
            || (state.txid == sealed_head_txid
                && (state.history != sealed_head_history
                    || state.logical_size != sealed_logical_size))
            || (state.txid > sealed_head_txid && state.txid < header.start_txid)
        {
            return Err(StoreError::IdentityMismatch);
        }
        let (active_records, expected_active_end) = load_active_records(
            &current,
            header.records_offset,
            state.page_size,
            state.record_count,
        )?;
        let file_len = current.metadata()?.len();
        if file_len < expected_active_end {
            return Err(StoreError::Corrupt(file_len));
        }
        self.active = ActiveSegment {
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
        self.segments = segments;
        self.locations = locations;
        self.pending_size = state.logical_size;
        self.pending_truncate_pages = None;
        self.pending_dirty = false;
        self.pending_raw_pages.clear();
        self.pending_raw_originals.clear();
        self.bootstrap = None;
        self.cache.clear();
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

    #[allow(clippy::too_many_lines)]
    fn seal_active(&mut self) -> Result<(), StoreError> {
        if self.head.txid < self.active.header.start_txid {
            return Ok(());
        }
        let logical_page_count = page_count(self.head.logical_size, self.head.page_size)?;
        let active_pages = self
            .active
            .active_records
            .keys()
            .copied()
            .filter(|page_no| *page_no <= logical_page_count)
            .collect::<Vec<_>>();
        let sample_limit = usize::try_from(self.active.header.policy.dictionary.sample_bytes)
            .unwrap_or(usize::MAX);
        let mut sample_bytes = 0_usize;
        let mut samples = Vec::<Vec<u8>>::new();
        for page_no in &active_pages {
            if sample_bytes.saturating_add(self.head.page_size as usize) > sample_limit {
                break;
            }
            let page = self.read_mutable_page(*page_no)?;
            sample_bytes = sample_bytes.saturating_add(page.len());
            samples.push(page);
        }
        let dictionary_size =
            usize::try_from(self.active.header.policy.dictionary.dictionary_bytes)
                .map_err(|_| StoreError::Range)?;
        let dictionary = if samples.len() >= MIN_TRAINING_PAGES
            && sample_bytes >= MIN_TRAINING_BYTES
            && dictionary_size != 0
        {
            let borrowed = samples.iter().map(Vec::as_slice).collect::<Vec<_>>();
            zstd::dict::from_samples(&borrowed, dictionary_size).unwrap_or_default()
        } else {
            Vec::new()
        };
        let mut dictionaries = self.segments.last().map_or_else(
            || Arc::new(Vec::new()),
            |segment| Arc::clone(&segment.dictionaries),
        );
        let mut dictionary_additions = Vec::new();
        let dictionary_index = if dictionary.is_empty() {
            dictionaries
                .len()
                .checked_sub(1)
                .map(u16::try_from)
                .transpose()
                .map_err(|_| StoreError::Range)?
        } else {
            let dictionary_digest = digest(&dictionary);
            if let Some(index) = dictionaries
                .iter()
                .position(|entry| entry.digest == dictionary_digest)
            {
                Some(u16::try_from(index).map_err(|_| StoreError::Range)?)
            } else {
                // u16::MAX is reserved as the raw-frame selector.
                if dictionaries.len() >= u16::MAX as usize {
                    return Err(StoreError::Range);
                }
                let index = u16::try_from(dictionaries.len()).map_err(|_| StoreError::Range)?;
                let entry = DictionaryEntry {
                    digest: dictionary_digest,
                    bytes: dictionary,
                };
                dictionary_additions.push(entry.clone());
                Arc::make_mut(&mut dictionaries).push(entry);
                Some(index)
            }
        };
        let dictionary_bytes = encode_dictionary_table(&dictionary_additions)?;
        let base_page_count = page_count(
            self.active.header.base_logical_size,
            self.active.header.page_size,
        )?;
        let base_map = encode_map(&full_page_txid_map(&self.locations, base_page_count)?)?;
        let dictionary_offset = SEGMENT_HEADER_SIZE as u64;
        let base_map_offset = dictionary_offset
            .checked_add(dictionary_bytes.len() as u64)
            .ok_or(StoreError::Range)?;
        let records_offset = align_up(
            base_map_offset
                .checked_add(base_map.len() as u64)
                .ok_or(StoreError::Range)?,
            SECTOR_SIZE as u64,
        )?;
        let staging_id = random_bytes()?;
        let path = active_staging_path(&self.path, staging_id).with_extension("seal");
        let cleanup = CleanupFile(path.clone());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let header = SegmentHeader {
            mutable_snapshot: false,
            database_id: self.active.header.database_id,
            page_size: self.head.page_size,
            start_txid: self.active.header.start_txid,
            base_history: self.active.header.base_history,
            parent_physical_digest: self.active.header.parent_physical_digest,
            base_logical_size: self.active.header.base_logical_size,
            generation: self.active.header.generation,
            policy: self.active.header.policy,
            dictionary_offset,
            dictionary_len: dictionary_bytes.len() as u64,
            base_map_offset,
            base_map_len: base_map.len() as u64,
            records_offset,
        };
        write_all_at(&file, 0, &header.encode())?;
        write_all_at(&file, dictionary_offset, &dictionary_bytes)?;
        write_all_at(&file, base_map_offset, &base_map)?;

        let mut compressor = dictionary_index
            .and_then(|index| dictionaries.get(index as usize))
            .map(|entry| zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, &entry.bytes))
            .transpose()
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        let mut cursor = records_offset;
        let mut index = Vec::with_capacity(active_pages.len());
        let segment_index = self.segments.len();
        let mut new_locations = self.locations.clone();
        if let Some(preserved_pages) = self.active.truncate_pages {
            new_locations.retain(|page_no, _| *page_no <= preserved_pages);
        }
        new_locations.retain(|page_no, _| *page_no <= logical_page_count);
        for page_no in active_pages {
            let page = self.read_mutable_page(page_no)?;
            if page.iter().all(|byte| *byte == 0) {
                new_locations.remove(&page_no);
                continue;
            }
            let compressed = compressor
                .as_mut()
                .map(|compressor| compressor.compress(&page))
                .transpose()
                .map_err(|error| StoreError::Zstd(error.to_string()))?;
            let (codec, frame_dictionary_index, payload) = compressed.map_or_else(
                || (Codec::Raw, u16::MAX, page.clone()),
                |compressed| {
                    if compressed.len().saturating_add(MIN_FRAME_SAVINGS) < page.len() {
                        (
                            Codec::Zstd,
                            dictionary_index.expect("a compressor has a dictionary selector"),
                            compressed,
                        )
                    } else {
                        (Codec::Raw, u16::MAX, page.clone())
                    }
                },
            );
            let page_hash = page_hash(self.head.txid, page_no, &page);
            let stored_len = u32::try_from(payload.len()).map_err(|_| StoreError::Range)?;
            let frame = FrameHeader {
                page_no,
                codec,
                dictionary_index: frame_dictionary_index,
                stored_len,
                raw_len: self.head.page_size,
            };
            write_all_at(&file, cursor, &frame.encode())?;
            write_all_at(&file, cursor + FRAME_HEADER_SIZE as u64, &payload)?;
            let record_len = u32::try_from(frame.record_len()).map_err(|_| StoreError::Range)?;
            index.push(SegmentIndexEntry {
                page_no,
                last_txid: self.head.txid,
                frame_offset: cursor,
                frame_record_len: record_len,
                page_hash,
            });
            new_locations.insert(
                page_no,
                PageLocation {
                    segment_index,
                    last_txid: self.head.txid,
                    frame_offset: cursor,
                    frame_record_len: record_len,
                    page_hash,
                },
            );
            cursor = cursor
                .checked_add(frame.record_len())
                .ok_or(StoreError::Range)?;
        }
        let page_map = full_page_txid_map(&new_locations, logical_page_count)?;
        let trailer = finalize_segment(
            &file,
            cursor,
            &index,
            &page_map,
            SegmentTrailer {
                database_id: header.database_id,
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
                content_root: content_root(&new_locations),
                physical_digest: [0; 32],
            },
        )?;
        let id = SegmentId {
            start_txid: header.start_txid,
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
        let parent_physical_digest = new_segment.id.physical_digest;
        self.segments.push(new_segment);
        self.locations = new_locations;
        let next = match self.create_active(parent_physical_digest, self.active.header.policy) {
            Ok(active) => active,
            Err(error) => {
                let _ = self.reload();
                return Err(error);
            }
        };
        self.adopt_active(next);
        drop(cleanup);
        self.collect_garbage(8)?;
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
        if self.has_pending() || active_has_commits {
            self.flush_sidecars()?;
            self.begin_write()?;
        }
        // Never make a corrupt source generation authoritative. This check
        // also covers bytes that are not represented in the logical index.
        for segment in &self.segments {
            verify_segment_physical(segment)?;
        }
        if self.head.txid == 0 || self.segments.len() <= 1 {
            self.release_publication();
            return Ok(());
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
            mutable_snapshot: false,
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
            self.read_source(location.segment_index, location.frame_offset, &mut encoded)?;
            let mut frame = FrameHeader::decode(&encoded)?;
            let mut payload = vec![0; frame.stored_len as usize];
            self.read_source(
                location.segment_index,
                location.frame_offset + FRAME_HEADER_SIZE as u64,
                &mut payload,
            )?;
            if frame.page_no != *page_no
                || frame.raw_len != self.head.page_size
                || frame.record_len() != u64::from(location.frame_record_len)
            {
                return Err(StoreError::Corrupt(location.frame_offset));
            }
            let source_dictionaries = &self.segments[location.segment_index].dictionaries;
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
            dictionaries: Arc::new(dictionaries),
        };
        verify_segment_physical(&new_segment)?;
        let mut new_locations = BTreeMap::new();
        for value in index {
            new_locations.insert(
                value.page_no,
                PageLocation {
                    segment_index: 0,
                    last_txid: value.last_txid,
                    frame_offset: value.frame_offset,
                    frame_record_len: value.frame_record_len,
                    page_hash: value.page_hash,
                },
            );
        }
        let parent_physical_digest = new_segment.id.physical_digest;
        let next = match self.create_active(parent_physical_digest, self.active.header.policy) {
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
        let logical_page_count = page_count(self.head.logical_size, self.head.page_size)?;
        Ok(Inspect {
            path: self.path.clone(),
            sidecar_path: self.sidecar_path.clone(),
            page_size: self.head.page_size,
            page_count: logical_page_count,
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
            indexed_pages: self
                .active
                .active_records
                .keys()
                .filter(|page_no| !self.locations.contains_key(page_no))
                .count()
                .saturating_add(self.locations.len()),
            dictionary_bytes: self
                .segments
                .last()
                .and_then(|segment| segment.dictionaries.first())
                .map_or(0, |entry| entry.bytes.len()),
            policy: StoragePolicy::decode(self.active.header.policy),
        })
    }

    pub(crate) fn set_storage_policy(&mut self, policy: StoragePolicy) -> Result<(), StoreError> {
        self.begin_write()?;
        if self.head.txid >= self.active.header.start_txid {
            self.seal_active()?;
        }
        self.install_active(policy.encode()?)?;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn background_flush_due(&self) -> bool {
        if self.has_pending() || self.head.txid < self.active.header.start_txid {
            return false;
        }
        let target = self.active.header.policy.target_segment_bytes;
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
        if self.has_pending() || self.publication_owner == PublicationOwner::Checkpoint {
            return Ok(());
        }
        if self.background_flush_due() {
            self.acquire_maintenance()?;
            let result = self.flush_sidecars();
            self.release_maintenance();
            result?;
        }
        self.collect_garbage(8)
    }

    fn rollover_at_size_target(&mut self) -> Result<(), StoreError> {
        let target = self.active.header.policy.target_segment_bytes;
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

    fn page_txid_map(&self) -> Result<Vec<u64>, StoreError> {
        let pages = page_count(self.head.logical_size, self.head.page_size)?;
        let mut map = full_page_txid_map(&self.locations, pages)?;
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
        if self.has_pending() {
            self.discard_mutable_pending();
        }
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
        let dictionary_additions = read_dictionary_table_file(
            &discovered.file,
            header.dictionary_offset,
            header.dictionary_len,
        )?;
        let mut dictionaries = segments.last().map_or_else(
            || Arc::new(Vec::new()),
            |segment| Arc::clone(&segment.dictionaries),
        );
        for dictionary in dictionary_additions {
            if dictionaries.len() >= u16::MAX as usize
                || dictionaries
                    .iter()
                    .any(|existing| existing.digest == dictionary.digest)
            {
                return Err(StoreError::Corrupt(header.dictionary_offset));
            }
            Arc::make_mut(&mut dictionaries).push(dictionary);
        }
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
                    segment_index,
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
    entries: &[PageChange],
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
    for dictionary in segments
        .iter()
        .flat_map(|segment| segment.dictionaries.iter())
    {
        output
            .entry(dictionary.digest)
            .or_insert_with(|| dictionary.bytes.clone());
    }
    output
        .into_iter()
        .map(|(digest, bytes)| DictionaryEntry { digest, bytes })
        .collect()
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

fn active_state_offset(sequence: u64) -> u64 {
    if sequence.is_multiple_of(2) {
        ACTIVE_STATE_B_OFFSET
    } else {
        ACTIVE_STATE_A_OFFSET
    }
}

fn read_active_state(file: &File, database_id: DatabaseId) -> Result<ActiveState, StoreError> {
    let mut states = Vec::with_capacity(2);
    for offset in [ACTIVE_STATE_A_OFFSET, ACTIVE_STATE_B_OFFSET] {
        let mut encoded = [0; ACTIVE_STATE_SIZE];
        if read_exact_at(file, offset, &mut encoded).is_ok()
            && let Ok(state) = ActiveState::decode(&encoded)
            && state.database_id == database_id
        {
            states.push(state);
        }
    }
    states
        .into_iter()
        .max_by_key(|state| state.sequence)
        .ok_or(StoreError::Corrupt(ACTIVE_STATE_A_OFFSET))
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
        || (value.target_segment_bytes != 0 && value.target_segment_bytes < 1024 * 1024)
        || !(8 * 1024..=112 * 1024).contains(&value.dictionary.dictionary_bytes)
        || value.dictionary.sample_bytes < MIN_TRAINING_BYTES as u64
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
    fn live_pages_are_raw_and_dictionary_compression_happens_at_seal()
    -> Result<(), Box<dyn std::error::Error>> {
        const PAGE_SIZE: usize = 4096;
        const PAGE_COUNT: usize = 257;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("forward-dictionary.zsqlite");
        let image = dictionary_training_image(PAGE_COUNT);

        let mut store = Store::open(&path, true)?;
        store.write_at(0, &image)?;
        store.publish(true)?;
        assert!(!store.active.active_records.is_empty());
        assert!(store.segments.is_empty());
        assert_eq!(
            store.active.file.metadata()?.len(),
            store.active.header.records_offset
                + u64::try_from(PAGE_COUNT)? * u64::try_from(PAGE_SIZE + FRAME_HEADER_SIZE)?
        );
        store.verify()?;

        store.flush_sidecars()?;
        assert!(store.active.active_records.is_empty());
        assert_eq!(
            store.active.file.metadata()?.len(),
            store.active.header.records_offset
        );
        assert_eq!(store.segments.len(), 1);
        assert_eq!(store.segments[0].dictionaries.len(), 1);
        assert!(store.segments[0].dictionaries[0].bytes.len() <= 64 * 1024);
        let compressed_pages = store
            .locations
            .values()
            .filter(|location| {
                let mut encoded = [0; FRAME_HEADER_SIZE];
                store
                    .read_source(location.segment_index, location.frame_offset, &mut encoded)
                    .is_ok()
                    && FrameHeader::decode(&encoded).is_ok_and(|frame| frame.codec == Codec::Zstd)
            })
            .count();
        assert!(compressed_pages > 0);
        drop(store);

        let mut store = Store::open_existing(&path)?;
        assert_eq!(store.segments[0].dictionaries.len(), 1);
        store.verify()?;
        Ok(())
    }

    #[test]
    fn small_delta_reuses_inherited_dictionary_without_copying_it()
    -> Result<(), Box<dyn std::error::Error>> {
        const PAGE_SIZE: usize = 4096;
        const PAGE_COUNT: usize = 257;

        let directory = tempfile::tempdir()?;
        let path = directory.path().join("inherited-dictionary.zsqlite");
        let image = dictionary_training_image(PAGE_COUNT);
        let replacement = image[2 * PAGE_SIZE..3 * PAGE_SIZE].to_vec();
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &image)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        assert_eq!(store.segments[0].dictionaries.len(), 1);

        store.write_at(PAGE_SIZE as u64, &replacement)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        assert_eq!(store.segments.len(), 2);
        let delta = &store.segments[1];
        assert_eq!(delta.dictionaries.len(), 1);
        let header = read_segment_header(&delta.file)?;
        assert!(
            read_dictionary_table_file(
                &delta.file,
                header.dictionary_offset,
                header.dictionary_len,
            )?
            .is_empty()
        );
        let index = decode_index(
            &read_range_file(
                &delta.file,
                delta.trailer.index_offset,
                delta.trailer.index_len,
            )?,
            u32::try_from(PAGE_COUNT)?,
        )?;
        assert_eq!(index.len(), 1);
        let mut encoded = [0; FRAME_HEADER_SIZE];
        read_exact_at(&delta.file, index[0].frame_offset, &mut encoded)?;
        let frame = FrameHeader::decode(&encoded)?;
        assert_eq!(frame.codec, Codec::Zstd);
        assert_eq!(frame.dictionary_index, 0);
        store.verify()?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        let mut output = vec![0; PAGE_SIZE];
        reopened.read_at(PAGE_SIZE as u64, &mut output)?;
        assert_eq!(output, replacement);
        reopened.verify()?;
        reopened.compact()?;
        reopened.verify()?;
        assert_eq!(reopened.segments.len(), 1);
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
            target_segment_bytes: 1024 * 1024,
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
        assert_eq!(store.segments.len(), 1);
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
        assert_eq!(store.segments.len(), 1);

        store.write_at(0, &replacement)?;
        store.publish(true)?;
        assert_eq!(store.active.active_records.len(), 1);
        store.flush_sidecars()?;
        assert_eq!(store.segments.len(), 2);
        let delta = &store.segments[1];
        let index = decode_index(
            &read_range_file(
                &delta.file,
                delta.trailer.index_offset,
                delta.trailer.index_len,
            )?,
            2,
        )?;
        assert_eq!(index.len(), 1);
        assert_eq!(index[0].page_no, 1);

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
    fn corrupt_active_state_slot_falls_back_to_the_previous_state()
    -> Result<(), Box<dyn std::error::Error>> {
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

        let mut reopened = Store::open_existing(&path)?;
        assert_eq!(reopened.head.txid, 1);
        assert_eq!(reopened.logical_size(), 4096);
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
        store.flush_sidecars()?;
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

        writer.write_at(4096, &second)?;
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
    fn corrupt_latest_state_can_be_replaced_by_a_new_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("nondurable-fallback.zsqlite");
        let first = page(7, 4096);
        let second = page(8, 4096);
        let third = page(9, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(4096, &second)?;
        store.publish(false)?;

        let newest = active_state_offset(store.active.state_sequence);
        let active = &store.active.file;
        let mut byte = [0_u8; 1];
        read_exact_at(active, newest + 24, &mut byte)?;
        byte[0] ^= 0xff;
        write_all_at(active, newest + 24, &byte)?;
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
