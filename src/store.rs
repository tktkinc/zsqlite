//! Transactional page storage built from mutable active and immutable sealed segments.

use crate::backend::{FsSegmentBackend, SegmentBackend, sidecar_dir};
use crate::format::{
    ANCHOR_HEADER_SIZE, ANCHOR_ROOT_A_OFFSET, ANCHOR_ROOT_B_OFFSET, ANCHOR_SIZE, AnchorHeader,
    AnchorRoot, COMMIT_ENTRY_SIZE, COMMIT_HEADER_SIZE, CatalogEntry, Codec, CommitEntry,
    CommitHeader, DatabaseId, DictionaryEntry, DictionaryPolicyRecord, Digest, FRAME_HEADER_SIZE,
    FrameHeader, MAX_SECTION_BYTES, RootCatalog, SECTOR_SIZE, SEGMENT_HEADER_SIZE,
    SEGMENT_INDEX_ENTRY_SIZE, SEGMENT_TRAILER_SIZE, SegmentHeader, SegmentId, SegmentIndexEntry,
    SegmentTrailer, StoragePolicyRecord, decode_dictionary_table, digest, encode_dictionary_table,
    genesis_history, valid_page_size,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const MIN_FRAME_SAVINGS: usize = 64;
const ZSTD_LEVEL: i32 = 3;
const PAGE_CACHE_BYTES: usize = 8 * 1024 * 1024;
const MIN_TRAINING_PAGES: usize = 256;
const MIN_TRAINING_BYTES: usize = 1024 * 1024;
const BLOB_HEADER_SIZE: usize = 80;
const BLOB_VERSION: u16 = 1;
const INDEX_BLOB_MAGIC: &[u8; 8] = b"ZIDX0001";
const MAP_BLOB_MAGIC: &[u8; 8] = b"ZMAP0001";
const COPY_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid zsqlite metadata: {0}")]
    Format(#[from] crate::format::FormatError),
    #[error("database anchor exists but its sidecar directory is missing")]
    MissingSidecar,
    #[error("database anchor and sidecar identities do not match")]
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
    #[error("database is not a V5 zsqlite database")]
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
    pub hot_horizon: Duration,
    pub admission_reads: u16,
    pub gc_dead_percent: u16,
    pub dictionary: DictionaryPolicy,
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            settle: Duration::from_secs(5 * 60),
            max_stale: Duration::from_secs(60 * 60),
            hot_horizon: Duration::from_secs(24 * 60 * 60),
            admission_reads: 2,
            gc_dead_percent: 25,
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
            hot_horizon_seconds: seconds(self.hot_horizon)?,
            admission_reads: self.admission_reads,
            gc_dead_percent: self.gc_dead_percent,
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
            hot_horizon: Duration::from_secs(u64::from(value.hot_horizon_seconds)),
            admission_reads: value.admission_reads,
            gc_dead_percent: value.gc_dead_percent,
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
    pub catalog_generation: u64,
    pub sealed_segments: usize,
    pub active: bool,
    pub anchor_bytes: u64,
    pub anchor_allocated_bytes: u64,
    pub segment_bytes: u64,
    pub segment_allocated_bytes: u64,
    pub active_bytes: u64,
    pub active_allocated_bytes: u64,
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

#[derive(Debug)]
struct SegmentMeta {
    entry: CatalogEntry,
    trailer: SegmentTrailer,
    dictionaries: Vec<DictionaryEntry>,
}

#[derive(Debug)]
struct PageCache {
    capacity: usize,
    bytes: usize,
    entries: VecDeque<(PageCacheKey, Arc<Vec<u8>>)>,
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

/// Internal page store. `SQLite`'s ordinary lock file protocol surrounds access.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct Store {
    path: PathBuf,
    sidecar_path: PathBuf,
    anchor: File,
    backend: Arc<FsSegmentBackend>,
    publication: File,
    lifecycle: File,
    writable: bool,
    publication_locked: bool,
    root_slots: [Option<AnchorRoot>; 2],
    active_root_slot: usize,
    root: AnchorRoot,
    catalog: RootCatalog,
    segments: Vec<SegmentMeta>,
    locations: BTreeMap<u32, PageLocation>,
    active: Option<File>,
    active_header: Option<SegmentHeader>,
    active_dictionaries: Vec<DictionaryEntry>,
    free_slots: BTreeMap<u64, u32>,
    protected_active_offsets: BTreeSet<u64>,
    pending_pages: BTreeMap<u32, PendingFrame>,
    pending_size: u64,
    pending_dirty: bool,
    bootstrap: Option<BootstrapFile>,
    cache: PageCache,
    training_pages: BTreeMap<u32, Vec<u8>>,
    training_order: VecDeque<u32>,
    changed_since_dictionary: BTreeSet<u32>,
    maintenance_locked: bool,
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
        let anchor = options.open(&path)?;
        reject_aliased_file(&anchor)?;
        if !existed || anchor.metadata()?.len() == 0 {
            if !create || !writable {
                return Err(StoreError::NotZsqlite);
            }
            return Self::initialize(path, anchor);
        }
        if anchor.metadata()?.len() != ANCHOR_SIZE as u64 {
            return Err(StoreError::NotZsqlite);
        }
        let header = read_anchor_header(&anchor)?;
        let backend = Arc::new(FsSegmentBackend::open(
            sidecar_dir(&path),
            header.database_id,
            false,
        )?);
        let publication = open_lock(&backend.lock_path("publication"), writable, false)?;
        let lifecycle = open_lock(&backend.lock_path("lifecycle"), writable, false)?;
        lock_shared(&lifecycle, false)?;
        let mut store = Self::blank(
            path,
            anchor,
            backend,
            publication,
            lifecycle,
            writable,
            header.database_id,
        )?;
        store.reload()?;
        Ok(store)
    }

    fn initialize(path: PathBuf, anchor: File) -> Result<Self, StoreError> {
        let database_id = random_bytes()?;
        let backend = Arc::new(FsSegmentBackend::open(
            sidecar_dir(&path),
            database_id,
            true,
        )?);
        let publication = open_lock(&backend.lock_path("publication"), true, true)?;
        lock_exclusive(&publication, false)?;
        if anchor.metadata()?.len() != 0 {
            unlock_file(&publication)?;
            drop(publication);
            drop(backend);
            drop(anchor);
            return Self::open_mode(&path, false, true);
        }
        let policy = StoragePolicy::default().encode()?;
        let catalog = RootCatalog {
            generation: 1,
            database_id,
            head_txid: 0,
            head_history: genesis_history(),
            current_dictionary: Vec::new(),
            segments: Vec::new(),
        };
        let catalog_bytes = catalog.encode()?;
        let catalog_digest = backend.put_catalog(&catalog, &catalog_bytes)?;
        let root = AnchorRoot {
            sequence: 1,
            durable: true,
            database_id,
            page_size: 0,
            logical_size: 0,
            head_txid: 0,
            head_history: genesis_history(),
            catalog_digest,
            active_id: [0; 16],
            active_commit_offset: 0,
            active_commit_end: 0,
            oldest_dirty_unix: 0,
            last_dirty_unix: 0,
            last_dictionary_promotion_unix: 0,
            policy,
        };
        write_all_at(&anchor, 0, &AnchorHeader { database_id }.encode())?;
        write_all_at(&anchor, ANCHOR_ROOT_A_OFFSET, &root.encode())?;
        anchor.set_len(ANCHOR_SIZE as u64)?;
        anchor.sync_all()?;
        sync_parent_dir(&path)?;
        unlock_file(&publication)?;
        let lifecycle = open_lock(&backend.lock_path("lifecycle"), true, true)?;
        lock_shared(&lifecycle, false)?;
        let mut store = Self::blank(
            path,
            anchor,
            backend,
            publication,
            lifecycle,
            true,
            database_id,
        )?;
        store.root_slots = [Some(root), None];
        store.root = root;
        store.catalog = catalog;
        store.pending_size = 0;
        Ok(store)
    }

    fn blank(
        path: PathBuf,
        anchor: File,
        backend: Arc<FsSegmentBackend>,
        publication: File,
        lifecycle: File,
        writable: bool,
        database_id: DatabaseId,
    ) -> Result<Self, StoreError> {
        let policy = StoragePolicy::default().encode()?;
        Ok(Self {
            sidecar_path: sidecar_dir(&path),
            path,
            anchor,
            backend,
            publication,
            lifecycle,
            writable,
            publication_locked: false,
            root_slots: [None, None],
            active_root_slot: 0,
            root: AnchorRoot {
                sequence: 0,
                durable: false,
                database_id,
                page_size: 0,
                logical_size: 0,
                head_txid: 0,
                head_history: genesis_history(),
                catalog_digest: [0; 32],
                active_id: [0; 16],
                active_commit_offset: 0,
                active_commit_end: 0,
                oldest_dirty_unix: 0,
                last_dirty_unix: 0,
                last_dictionary_promotion_unix: 0,
                policy,
            },
            catalog: RootCatalog {
                generation: 0,
                database_id,
                head_txid: 0,
                head_history: genesis_history(),
                current_dictionary: Vec::new(),
                segments: Vec::new(),
            },
            segments: Vec::new(),
            locations: BTreeMap::new(),
            active: None,
            active_header: None,
            active_dictionaries: Vec::new(),
            free_slots: BTreeMap::new(),
            protected_active_offsets: BTreeSet::new(),
            pending_pages: BTreeMap::new(),
            pending_size: 0,
            pending_dirty: false,
            bootstrap: None,
            cache: PageCache::new(PAGE_CACHE_BYTES),
            training_pages: BTreeMap::new(),
            training_order: VecDeque::new(),
            changed_since_dictionary: BTreeSet::new(),
            maintenance_locked: false,
        })
    }

    pub(crate) fn delete_bundle(path: impl AsRef<Path>) -> Result<(), StoreError> {
        let path = absolute_path(path.as_ref())?;
        let anchor = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Err(StoreError::NotZsqlite);
            }
            Err(error) => return Err(error.into()),
        };
        let header = read_anchor_header(&anchor)?;
        let backend = FsSegmentBackend::open(sidecar_dir(&path), header.database_id, false)?;
        let lifecycle = open_lock(&backend.lock_path("lifecycle"), true, false)?;
        lock_exclusive(&lifecycle, true)?;
        let publication = open_lock(&backend.lock_path("publication"), true, false)?;
        lock_exclusive(&publication, true)?;
        std::fs::remove_file(&path)?;
        std::fs::remove_dir_all(sidecar_dir(&path))?;
        sync_parent_dir(&path)?;
        Ok(())
    }

    pub(crate) fn upgrade_writable(&mut self) -> Result<bool, StoreError> {
        if self.writable {
            return Ok(false);
        }
        self.anchor = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.publication = open_lock(&self.backend.lock_path("publication"), true, false)?;
        self.writable = true;
        if self.root.active_id != [0; 16] {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.backend.active_path(self.root.active_id))?;
            self.active = Some(file);
        }
        Ok(true)
    }

    pub(crate) fn logical_size(&self) -> u64 {
        self.pending_size
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.pending_dirty || !self.pending_pages.is_empty() || self.bootstrap.is_some()
    }

    pub(crate) fn refresh(&mut self) -> Result<(), StoreError> {
        if self.has_pending() {
            return Ok(());
        }
        let (_, newest) = newest_root(&self.anchor, self.root.database_id)?;
        if newest.sequence > self.root.sequence {
            self.reload()?;
        }
        Ok(())
    }

    fn begin_write(&mut self) -> Result<(), StoreError> {
        if !self.writable {
            return Err(StoreError::ReadOnly);
        }
        if !self.publication_locked {
            lock_exclusive(&self.publication, false)?;
            self.publication_locked = true;
            if let Err(error) = self.reload().and_then(|()| {
                if let Some(active) = &self.active {
                    active.set_len(self.root.active_commit_end)?;
                }
                Ok(())
            }) {
                self.release_publication();
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn discard_pending(&mut self) -> Result<(), StoreError> {
        for frame in std::mem::take(&mut self.pending_pages).into_values() {
            self.mark_frame_free(frame.offset, frame.header.capacity)?;
        }
        self.pending_size = self.root.logical_size;
        self.pending_dirty = false;
        self.bootstrap = None;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn read_at(&mut self, offset: u64, output: &mut [u8]) -> Result<usize, StoreError> {
        output.fill(0);
        if output.is_empty() || offset >= self.pending_size {
            return Ok(0);
        }
        let actual = usize::try_from((self.pending_size - offset).min(output.len() as u64))
            .map_err(|_| StoreError::Range)?;
        if self.root.page_size == 0 {
            if let Some(bootstrap) = &self.bootstrap {
                let available = bootstrap.file.metadata()?.len().saturating_sub(offset);
                let copied = actual.min(usize::try_from(available).unwrap_or(usize::MAX));
                if copied != 0 {
                    read_exact_at(&bootstrap.file, offset, &mut output[..copied])?;
                }
            }
            return Ok(actual);
        }
        let page_size = self.root.page_size as usize;
        let mut copied = 0_usize;
        while copied < actual {
            let logical = usize::try_from(offset).map_err(|_| StoreError::Range)? + copied;
            let page_no = u32::try_from(logical / page_size + 1).map_err(|_| StoreError::Range)?;
            let within = logical % page_size;
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
        if self.root.page_size == 0 {
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
            let path = self
                .backend
                .active_dir()
                .join(format!("{}.zbootstrap", hex_active(id)));
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
            self.root.page_size = page_size;
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
                page_offset += u64::from(page_size);
            }
        }
        Ok(())
    }

    fn write_pages(&mut self, offset: u64, input: &[u8]) -> Result<(), StoreError> {
        let page_size = self.root.page_size as usize;
        let mut consumed = 0_usize;
        while consumed < input.len() {
            let logical = usize::try_from(offset).map_err(|_| StoreError::Range)? + consumed;
            let page_no = u32::try_from(logical / page_size + 1).map_err(|_| StoreError::Range)?;
            let within = logical % page_size;
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
        if self.root.page_size == 0 {
            if let Some(bootstrap) = &self.bootstrap {
                bootstrap.file.set_len(size)?;
            }
        } else if !size.is_multiple_of(u64::from(self.root.page_size)) {
            return Err(StoreError::InvalidPageSize(self.root.page_size));
        }
        let max_page = if self.root.page_size == 0 {
            0
        } else {
            page_count(size, self.root.page_size)?
        };
        let removed = self
            .pending_pages
            .range((max_page.saturating_add(1))..)
            .map(|(page, _)| *page)
            .collect::<Vec<_>>();
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
        if !self.has_pending() {
            self.release_publication();
            return Ok(());
        }
        if self.root.page_size == 0 {
            if self.pending_size == 0 {
                self.pending_dirty = false;
                self.bootstrap = None;
                self.release_publication();
                return Ok(());
            }
            return Err(StoreError::UnknownPageSize);
        }
        self.ensure_active()?;
        let active = self.active.as_ref().ok_or(StoreError::Corrupt(0))?;
        let txid = self
            .root
            .head_txid
            .checked_add(1)
            .ok_or(StoreError::Range)?;
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
        let transaction_hash =
            transaction_hash(txid, self.pending_size, self.root.page_size, &entries);
        let resulting_history = history_hash(self.root.head_history, transaction_hash);
        let record_len = COMMIT_HEADER_SIZE
            .checked_add(entry_bytes.len())
            .ok_or(StoreError::Range)?;
        let commit_offset = active.metadata()?.len();
        let header = CommitHeader {
            record_len: u32::try_from(record_len).map_err(|_| StoreError::Range)?,
            entry_count: u32::try_from(entries.len()).map_err(|_| StoreError::Range)?,
            txid,
            previous_commit: self.root.active_commit_offset,
            logical_size: self.pending_size,
            page_size: self.root.page_size,
            previous_history: self.root.head_history,
            transaction_hash,
            resulting_history,
            entries_digest,
        };
        write_all_at(active, commit_offset, &header.encode())?;
        write_all_at(
            active,
            commit_offset + COMMIT_HEADER_SIZE as u64,
            &entry_bytes,
        )?;
        let commit_end = commit_offset
            .checked_add(record_len as u64)
            .ok_or(StoreError::Range)?;
        active.set_len(commit_end)?;
        if durable {
            active.sync_all()?;
        }

        let now = unix_time();
        let max_page = page_count(self.pending_size, self.root.page_size)?;
        let reclaim_candidates = self.protected_active_offsets.clone();
        let new_protected = self
            .locations
            .values()
            .filter(|location| location.source == FrameSource::Active)
            .map(|location| location.frame_offset)
            .collect::<BTreeSet<_>>();
        let new_root = AnchorRoot {
            sequence: self.next_root_sequence()?,
            durable,
            database_id: self.root.database_id,
            page_size: self.root.page_size,
            logical_size: self.pending_size,
            head_txid: txid,
            head_history: resulting_history,
            catalog_digest: self.root.catalog_digest,
            active_id: self.active_header.ok_or(StoreError::Corrupt(0))?.active_id,
            active_commit_offset: commit_offset,
            active_commit_end: commit_end,
            oldest_dirty_unix: if self.root.oldest_dirty_unix == 0 {
                now
            } else {
                self.root.oldest_dirty_unix
            },
            last_dirty_unix: now,
            last_dictionary_promotion_unix: self.root.last_dictionary_promotion_unix,
            policy: self.root.policy,
        };
        self.publish_root(new_root, durable)?;

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
            self.changed_since_dictionary.insert(entry.page_no);
        }
        self.locations.retain(|page, _| *page <= max_page);
        let pending = std::mem::take(&mut self.pending_pages);
        for (page, frame) in pending {
            if let Ok(raw) = self.read_frame(
                FrameSource::Active,
                frame.offset,
                page,
                txid,
                frame.header.page_hash,
            ) {
                self.remember_training_page(page, raw);
            }
        }
        let current_offsets = self
            .locations
            .values()
            .filter(|location| location.source == FrameSource::Active)
            .map(|location| location.frame_offset)
            .collect::<BTreeSet<_>>();
        if lock_exclusive(&self.lifecycle, true).is_ok() {
            let reclaim_result = (|| {
                for offset in reclaim_candidates
                    .difference(&new_protected)
                    .filter(|offset| !current_offsets.contains(offset))
                {
                    let mut encoded = [0; FRAME_HEADER_SIZE];
                    read_exact_at(
                        self.active.as_ref().ok_or(StoreError::Corrupt(*offset))?,
                        *offset,
                        &mut encoded,
                    )?;
                    let frame = FrameHeader::decode(&encoded)?;
                    self.mark_frame_free(*offset, frame.capacity)?;
                }
                Ok::<(), StoreError>(())
            })();
            let relock_result = lock_shared(&self.lifecycle, false);
            reclaim_result?;
            relock_result?;
        }
        self.protected_active_offsets = new_protected;
        self.pending_dirty = false;
        self.pending_size = self.root.logical_size;
        self.release_publication();
        Ok(())
    }

    fn publish_root(&mut self, root: AnchorRoot, durable: bool) -> Result<(), StoreError> {
        let slot = 1 - self.active_root_slot;
        let offset = if slot == 0 {
            ANCHOR_ROOT_A_OFFSET
        } else {
            ANCHOR_ROOT_B_OFFSET
        };
        write_all_at(&self.anchor, offset, &root.encode())?;
        if durable {
            self.anchor.sync_all()?;
        }
        self.root_slots[slot] = Some(root);
        self.active_root_slot = slot;
        self.root = root;
        Ok(())
    }

    fn next_root_sequence(&self) -> Result<u64, StoreError> {
        self.root_slots
            .iter()
            .flatten()
            .map(|root| root.sequence)
            .chain(std::iter::once(self.root.sequence))
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StoreError::Range)
    }

    fn stage_page(&mut self, page_no: u32, page: &[u8]) -> Result<(), StoreError> {
        if page.len() != self.root.page_size as usize {
            return Err(StoreError::InvalidPageLength {
                page_no,
                actual: page.len(),
                expected: self.root.page_size as usize,
            });
        }
        if page_no == 1
            && self.root.head_txid != 0
            && parse_page_size(page) != Some(self.root.page_size)
        {
            return Err(StoreError::InvalidPageSize(
                parse_page_size(page).unwrap_or(0),
            ));
        }
        self.ensure_active()?;
        let txid = self
            .root
            .head_txid
            .checked_add(1)
            .ok_or(StoreError::Range)?;
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
            raw_len: self.root.page_size,
            capacity,
            page_hash,
        };
        let active = self.active.as_ref().ok_or(StoreError::Corrupt(0))?;
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
        let Some(dictionary) = self.active_dictionaries.first() else {
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
        let reusable = self
            .free_slots
            .iter()
            .filter(|(_, capacity)| **capacity >= needed)
            .min_by_key(|(_, capacity)| **capacity)
            .map(|(offset, capacity)| (*offset, *capacity));
        if let Some((offset, capacity)) = reusable {
            self.free_slots.remove(&offset);
            return Ok((offset, capacity));
        }
        let active = self.active.as_ref().ok_or(StoreError::Corrupt(0))?;
        let offset = active.metadata()?.len();
        let end = offset
            .checked_add(FRAME_HEADER_SIZE as u64 + u64::from(needed))
            .ok_or(StoreError::Range)?;
        active.set_len(end)?;
        Ok((offset, needed))
    }

    fn mark_frame_free(&mut self, offset: u64, capacity: u32) -> Result<(), StoreError> {
        if capacity == 0 || self.active.is_none() {
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
        let active = self.active.as_ref().ok_or(StoreError::Corrupt(0))?;
        write_all_at(active, offset, &header.encode())?;
        let _ = punch_payload(
            active,
            offset + FRAME_HEADER_SIZE as u64,
            u64::from(capacity),
        );
        self.free_slots.insert(offset, capacity);
        Ok(())
    }

    fn ensure_active(&mut self) -> Result<(), StoreError> {
        if self.active.is_some() {
            return Ok(());
        }
        if self.root.page_size == 0 {
            return Err(StoreError::UnknownPageSize);
        }
        let active_id = random_bytes()?;
        let dictionaries = if self.catalog.current_dictionary.is_empty() {
            Vec::new()
        } else {
            vec![DictionaryEntry {
                digest: digest(&self.catalog.current_dictionary),
                bytes: self.catalog.current_dictionary.clone(),
            }]
        };
        let dictionary_bytes = encode_dictionary_table(&dictionaries)?;
        let base_map = encode_map(&self.page_txid_map()?)?;
        let dictionary_offset = SEGMENT_HEADER_SIZE as u64;
        let base_map_offset = dictionary_offset
            .checked_add(dictionary_bytes.len() as u64)
            .ok_or(StoreError::Range)?;
        let records_offset = base_map_offset
            .checked_add(base_map.len() as u64)
            .ok_or(StoreError::Range)?;
        let header = SegmentHeader {
            database_id: self.root.database_id,
            active_id,
            page_size: self.root.page_size,
            start_txid: self
                .root
                .head_txid
                .checked_add(1)
                .ok_or(StoreError::Range)?,
            base_history: self.root.head_history,
            dictionary_offset,
            dictionary_len: dictionary_bytes.len() as u64,
            base_map_offset,
            base_map_len: base_map.len() as u64,
            records_offset,
        };
        let path = self.backend.active_path(active_id);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        write_all_at(&file, 0, &header.encode())?;
        write_all_at(&file, dictionary_offset, &dictionary_bytes)?;
        write_all_at(&file, base_map_offset, &base_map)?;
        file.set_len(records_offset)?;
        File::open(self.backend.active_dir())?.sync_all()?;
        self.active = Some(file);
        self.active_header = Some(header);
        self.active_dictionaries = dictionaries;
        self.free_slots.clear();
        self.protected_active_offsets.clear();
        Ok(())
    }

    fn read_page(&mut self, page_no: u32) -> Result<Vec<u8>, StoreError> {
        if let Some(frame) = self.pending_pages.get(&page_no).copied() {
            return self.read_frame(
                FrameSource::Active,
                frame.offset,
                page_no,
                frame.header.txid,
                frame.header.page_hash,
            );
        }
        let Some(location) = self.locations.get(&page_no).copied() else {
            return Ok(vec![0; self.root.page_size as usize]);
        };
        let key = (page_no, location.last_txid, location.page_hash);
        if let Some(page) = self.cache.get(key) {
            return Ok((*page).clone());
        }
        let page = self.read_frame(
            location.source,
            location.frame_offset,
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
        page_no: u32,
        txid: u64,
        expected_hash: Digest,
    ) -> Result<Vec<u8>, StoreError> {
        let mut encoded = [0; FRAME_HEADER_SIZE];
        self.read_source(source, offset, &mut encoded)?;
        let header = FrameHeader::decode(&encoded)?;
        if header.free
            || header.page_no != page_no
            || header.txid != txid
            || header.page_hash != expected_hash
            || header.raw_len != self.root.page_size
        {
            return Err(StoreError::Corrupt(offset));
        }
        let mut stored = vec![0; header.stored_len as usize];
        self.read_source(source, offset + FRAME_HEADER_SIZE as u64, &mut stored)?;
        let raw = match header.codec {
            Codec::Raw => stored,
            Codec::Zstd => {
                let dictionaries = match source {
                    FrameSource::Active => &self.active_dictionaries,
                    FrameSource::Segment(index) => {
                        &self
                            .segments
                            .get(index)
                            .ok_or(StoreError::Corrupt(offset))?
                            .dictionaries
                    }
                };
                let dictionary = dictionaries
                    .get(header.dictionary_index as usize)
                    .ok_or(StoreError::Corrupt(offset))?;
                let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&dictionary.bytes)
                    .map_err(|error| StoreError::Zstd(error.to_string()))?;
                decompressor
                    .decompress(&stored, header.raw_len as usize)
                    .map_err(|error| StoreError::Zstd(error.to_string()))?
            }
        };
        if raw.len() != header.raw_len as usize || page_hash(txid, page_no, &raw) != expected_hash {
            return Err(StoreError::PageChecksum(page_no));
        }
        Ok(raw)
    }

    fn read_source(
        &self,
        source: FrameSource,
        offset: u64,
        output: &mut [u8],
    ) -> Result<(), StoreError> {
        match source {
            FrameSource::Active => read_exact_at(
                self.active.as_ref().ok_or(StoreError::Corrupt(offset))?,
                offset,
                output,
            )?,
            FrameSource::Segment(index) => self.backend.read_segment_range(
                &self
                    .segments
                    .get(index)
                    .ok_or(StoreError::Corrupt(offset))?
                    .entry
                    .id,
                offset,
                output,
            )?,
        }
        Ok(())
    }

    fn reload(&mut self) -> Result<(), StoreError> {
        let slots = read_root_slots(&self.anchor, self.root.database_id)?;
        let mut candidates = slots
            .iter()
            .enumerate()
            .filter_map(|(slot, root)| root.map(|root| (slot, root)))
            .collect::<Vec<_>>();
        candidates.sort_by_key(|(_, root)| std::cmp::Reverse(root.sequence));
        let mut newest_error = None;
        for (slot, root) in candidates {
            match self.reload_root(slot, root, &slots) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    if newest_error.is_none() {
                        newest_error = Some(error);
                    }
                }
            }
        }
        Err(newest_error.unwrap_or(StoreError::Corrupt(ANCHOR_ROOT_A_OFFSET)))
    }

    fn reload_root(
        &mut self,
        slot: usize,
        root: AnchorRoot,
        slots: &[Option<AnchorRoot>; 2],
    ) -> Result<(), StoreError> {
        let catalog_bytes = self.backend.read_catalog(root.catalog_digest)?;
        let catalog = RootCatalog::decode(&catalog_bytes)?;
        if catalog.database_id != root.database_id || catalog.head_txid > root.head_txid {
            return Err(StoreError::IdentityMismatch);
        }
        let (segments, mut locations) = load_segments(&*self.backend, &catalog, root.page_size)?;
        self.active = None;
        self.active_header = None;
        self.active_dictionaries.clear();
        self.free_slots.clear();
        self.protected_active_offsets.clear();
        self.root_slots = *slots;
        self.active_root_slot = slot;
        self.root = root;
        self.catalog = catalog;
        self.segments = segments;
        self.locations.clear();
        self.locations.append(&mut locations);
        if root.active_id != [0; 16] {
            self.open_active(self.writable)?;
            self.apply_active_commits()?;
        } else if root.head_txid != self.catalog.head_txid
            || root.head_history != self.catalog.head_history
        {
            return Err(StoreError::Corrupt(0));
        }
        let _ = self.page_txid_map()?;
        self.pending_size = root.logical_size;
        self.pending_dirty = false;
        self.pending_pages.clear();
        self.bootstrap = None;
        self.cache.clear();
        Ok(())
    }

    fn open_active(&mut self, writable: bool) -> Result<(), StoreError> {
        let mut options = OpenOptions::new();
        options.read(true).write(writable);
        let file = options.open(self.backend.active_path(self.root.active_id))?;
        let mut encoded = [0; SEGMENT_HEADER_SIZE];
        read_exact_at(&file, 0, &mut encoded)?;
        let header = SegmentHeader::decode(&encoded)?;
        if header.database_id != self.root.database_id
            || header.active_id != self.root.active_id
            || header.start_txid != self.catalog.head_txid.saturating_add(1)
            || header.base_history != self.catalog.head_history
            || header.page_size != self.root.page_size
        {
            return Err(StoreError::IdentityMismatch);
        }
        let file_len = file.metadata()?.len();
        let dictionary_end = checked_end(header.dictionary_offset, header.dictionary_len)?;
        let base_map_end = checked_end(header.base_map_offset, header.base_map_len)?;
        if dictionary_end > header.base_map_offset
            || base_map_end > header.records_offset
            || header.records_offset > self.root.active_commit_end
            || self.root.active_commit_offset < header.records_offset
            || self.root.active_commit_offset >= self.root.active_commit_end
            || file_len < self.root.active_commit_end
        {
            return Err(StoreError::Corrupt(header.records_offset));
        }
        let dictionaries =
            read_dictionary_table_file(&file, header.dictionary_offset, header.dictionary_len)?;
        let base_map = decode_map(&read_range_file(
            &file,
            header.base_map_offset,
            header.base_map_len,
        )?)?;
        let base_page_count = self.segments.last().map_or(Ok(0), |segment| {
            page_count(segment.trailer.logical_size, segment.trailer.page_size)
        })?;
        if base_map != full_page_txid_map(&self.locations, base_page_count)? {
            return Err(StoreError::Corrupt(header.base_map_offset));
        }
        self.active = Some(file);
        self.active_header = Some(header);
        self.active_dictionaries = dictionaries;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn apply_active_commits(&mut self) -> Result<(), StoreError> {
        let header = self.active_header.ok_or(StoreError::Corrupt(0))?;
        let file = self.active.as_ref().ok_or(StoreError::Corrupt(0))?;
        let mut cursor = header.records_offset;
        let mut previous_commit = 0_u64;
        let mut txid = self.catalog.head_txid;
        let mut history = self.catalog.head_history;
        let mut seen_frames = BTreeMap::<u64, u32>::new();
        let protected_txid = self.root_slots[1 - self.active_root_slot]
            .filter(|root| {
                root.active_id == self.root.active_id
                    && root.catalog_digest == self.root.catalog_digest
                    && root.head_txid < self.root.head_txid
            })
            .map(|root| root.head_txid);
        while cursor < self.root.active_commit_end {
            let mut magic = [0; 4];
            read_exact_at(file, cursor, &mut magic)?;
            if &magic == crate::format::FRAME_MAGIC || &magic == crate::format::FREE_MAGIC {
                let mut encoded = [0; FRAME_HEADER_SIZE];
                read_exact_at(file, cursor, &mut encoded)?;
                let frame = FrameHeader::decode(&encoded)?;
                let frame_end = cursor
                    .checked_add(frame.record_len())
                    .ok_or(StoreError::Range)?;
                if frame_end > self.root.active_commit_end {
                    return Err(StoreError::Corrupt(cursor));
                }
                seen_frames.insert(cursor, frame.capacity);
                cursor = frame_end;
            } else if &magic == crate::format::COMMIT_MAGIC {
                let (commit, entries) = read_commit(file, cursor, self.root.active_commit_end)?;
                if commit.previous_commit != previous_commit
                    || commit.txid != txid.saturating_add(1)
                    || commit.previous_history != history
                    || commit.entries_digest != digest(&encode_commit_entries(&entries))
                    || commit.transaction_hash
                        != transaction_hash(
                            commit.txid,
                            commit.logical_size,
                            commit.page_size,
                            &entries,
                        )
                    || commit.resulting_history != history_hash(history, commit.transaction_hash)
                {
                    return Err(StoreError::Corrupt(cursor));
                }
                for entry in entries {
                    self.locations.insert(
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
                let max_page = page_count(commit.logical_size, commit.page_size)?;
                self.locations.retain(|page, _| *page <= max_page);
                if protected_txid == Some(commit.txid) {
                    self.protected_active_offsets = self
                        .locations
                        .values()
                        .filter(|location| location.source == FrameSource::Active)
                        .map(|location| location.frame_offset)
                        .collect();
                }
                previous_commit = cursor;
                txid = commit.txid;
                history = commit.resulting_history;
                cursor = cursor
                    .checked_add(u64::from(commit.record_len))
                    .ok_or(StoreError::Range)?;
            } else {
                return Err(StoreError::Corrupt(cursor));
            }
        }
        if cursor != self.root.active_commit_end
            || previous_commit != self.root.active_commit_offset
            || txid != self.root.head_txid
            || history != self.root.head_history
        {
            return Err(StoreError::Corrupt(cursor));
        }
        let live = self
            .locations
            .values()
            .filter(|location| location.source == FrameSource::Active)
            .map(|location| location.frame_offset)
            .collect::<BTreeSet<_>>();
        for (offset, capacity) in seen_frames {
            if live.contains(&offset) {
                let location = self
                    .locations
                    .values()
                    .find(|location| {
                        location.source == FrameSource::Active && location.frame_offset == offset
                    })
                    .ok_or(StoreError::Corrupt(offset))?;
                let mut frame_bytes = [0; FRAME_HEADER_SIZE];
                read_exact_at(file, offset, &mut frame_bytes)?;
                let frame = FrameHeader::decode(&frame_bytes)?;
                if frame.record_len() != u64::from(location.frame_record_len) {
                    return Err(StoreError::Corrupt(offset));
                }
                let _ = self.read_frame(
                    FrameSource::Active,
                    offset,
                    self.locations
                        .iter()
                        .find(|(_, candidate)| {
                            candidate.frame_offset == offset
                                && candidate.source == FrameSource::Active
                        })
                        .map(|(page, _)| *page)
                        .ok_or(StoreError::Corrupt(offset))?,
                    location.last_txid,
                    location.page_hash,
                )?;
            } else if !self.protected_active_offsets.contains(&offset) {
                self.free_slots.insert(offset, capacity);
            }
        }
        Ok(())
    }

    pub(crate) fn flush_sidecars(&mut self) -> Result<(), StoreError> {
        self.begin_write()?;
        if self.has_pending() {
            self.publish(true)?;
            self.begin_write()?;
        }
        if self.root.active_id == [0; 16] {
            self.release_publication();
            return Ok(());
        }
        self.seal_active()?;
        self.release_publication();
        Ok(())
    }

    fn seal_active(&mut self) -> Result<(), StoreError> {
        let active = self.active.as_ref().ok_or(StoreError::Corrupt(0))?;
        let header = self.active_header.ok_or(StoreError::Corrupt(0))?;
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
        let index_blob = encode_index(&index)?;
        let map_blob = encode_map(&self.page_txid_map()?)?;
        let index_offset = active.metadata()?.len();
        write_all_at(active, index_offset, &index_blob)?;
        let map_offset = index_offset
            .checked_add(index_blob.len() as u64)
            .ok_or(StoreError::Range)?;
        write_all_at(active, map_offset, &map_blob)?;
        let trailer_offset = map_offset
            .checked_add(map_blob.len() as u64)
            .ok_or(StoreError::Range)?;
        let mut trailer = SegmentTrailer {
            database_id: self.root.database_id,
            start_txid: header.start_txid,
            end_txid: self.root.head_txid,
            base_history: header.base_history,
            end_history: self.root.head_history,
            logical_size: self.root.logical_size,
            page_size: self.root.page_size,
            index_offset,
            index_len: index_blob.len() as u64,
            map_offset,
            map_len: map_blob.len() as u64,
            content_root: content_root(&self.locations),
            physical_digest: [0; 32],
        };
        write_all_at(active, trailer_offset, &trailer.encode(false))?;
        active.set_len(trailer_offset + SEGMENT_TRAILER_SIZE as u64)?;
        trailer.physical_digest = hash_file(active)?;
        write_all_at(active, trailer_offset, &trailer.encode(true))?;
        active.sync_all()?;
        let id = SegmentId {
            start_txid: trailer.start_txid,
            end_txid: trailer.end_txid,
            end_history: trailer.end_history,
            physical_digest: trailer.physical_digest,
        };
        self.backend
            .put_segment(&id, &self.backend.active_path(header.active_id))?;
        let entry = CatalogEntry {
            id: id.clone(),
            base_history: trailer.base_history,
            file_len: active.metadata()?.len(),
        };
        let mut catalog = self.catalog.clone();
        catalog.generation = catalog.generation.checked_add(1).ok_or(StoreError::Range)?;
        catalog.head_txid = self.root.head_txid;
        catalog.head_history = self.root.head_history;
        catalog.segments.push(entry);
        let catalog_bytes = catalog.encode()?;
        let catalog_digest = self.backend.put_catalog(&catalog, &catalog_bytes)?;
        let mut root = self.root;
        root.sequence = self.next_root_sequence()?;
        root.catalog_digest = catalog_digest;
        root.active_id = [0; 16];
        root.active_commit_offset = 0;
        root.active_commit_end = 0;
        root.oldest_dirty_unix = 0;
        root.last_dirty_unix = 0;
        root.durable = true;
        self.publish_root(root, true)?;
        self.active = None;
        self.catalog = catalog;
        self.reload()?;
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
        if self.has_pending() || self.root.active_id != [0; 16] {
            self.flush_sidecars()?;
            self.begin_write()?;
        }
        if self.root.head_txid == 0 || self.catalog.segments.len() <= 1 {
            self.release_publication();
            return Ok(());
        }
        let active_id = random_bytes()?;
        let path = self.backend.active_path(active_id);
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
        let records_offset = base_map_offset + empty_map.len() as u64;
        let header = SegmentHeader {
            database_id: self.root.database_id,
            active_id: [0; 16],
            page_size: self.root.page_size,
            start_txid: 1,
            base_history: genesis_history(),
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
            if frame.codec == Codec::Zstd {
                let source_dictionary = match location.source {
                    FrameSource::Active => self
                        .active_dictionaries
                        .get(frame.dictionary_index as usize),
                    FrameSource::Segment(segment) => self.segments[segment]
                        .dictionaries
                        .get(frame.dictionary_index as usize),
                }
                .ok_or(StoreError::Corrupt(location.frame_offset))?;
                frame.dictionary_index = *dictionary_selectors
                    .get(&source_dictionary.digest)
                    .ok_or(StoreError::Corrupt(location.frame_offset))?;
            }
            frame.capacity = frame.stored_len;
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
        let index_blob = encode_index(&index)?;
        let map_blob = encode_map(&self.page_txid_map()?)?;
        let index_offset = cursor;
        write_all_at(&file, index_offset, &index_blob)?;
        let map_offset = index_offset + index_blob.len() as u64;
        write_all_at(&file, map_offset, &map_blob)?;
        let trailer_offset = map_offset + map_blob.len() as u64;
        let mut trailer = SegmentTrailer {
            database_id: self.root.database_id,
            start_txid: 1,
            end_txid: self.root.head_txid,
            base_history: genesis_history(),
            end_history: self.root.head_history,
            logical_size: self.root.logical_size,
            page_size: self.root.page_size,
            index_offset,
            index_len: index_blob.len() as u64,
            map_offset,
            map_len: map_blob.len() as u64,
            content_root: content_root(&self.locations),
            physical_digest: [0; 32],
        };
        write_all_at(&file, trailer_offset, &trailer.encode(false))?;
        file.set_len(trailer_offset + SEGMENT_TRAILER_SIZE as u64)?;
        trailer.physical_digest = hash_file(&file)?;
        write_all_at(&file, trailer_offset, &trailer.encode(true))?;
        file.sync_all()?;
        let id = SegmentId {
            start_txid: 1,
            end_txid: self.root.head_txid,
            end_history: self.root.head_history,
            physical_digest: trailer.physical_digest,
        };
        self.backend.put_segment(&id, &path)?;
        let entry = CatalogEntry {
            id,
            base_history: genesis_history(),
            file_len: file.metadata()?.len(),
        };
        let catalog = RootCatalog {
            generation: self
                .catalog
                .generation
                .checked_add(1)
                .ok_or(StoreError::Range)?,
            database_id: self.root.database_id,
            head_txid: self.root.head_txid,
            head_history: self.root.head_history,
            current_dictionary: self.catalog.current_dictionary.clone(),
            segments: vec![entry],
        };
        let catalog_digest = self.backend.put_catalog(&catalog, &catalog.encode()?)?;
        let mut root = self.root;
        root.sequence = self.next_root_sequence()?;
        root.catalog_digest = catalog_digest;
        root.durable = true;
        self.publish_root(root, true)?;
        drop(file);
        let _ = std::fs::remove_file(path);
        self.catalog = catalog;
        self.reload()?;
        self.collect_garbage(usize::MAX)?;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn verify(&mut self) -> Result<(), StoreError> {
        let expected_pages = page_count(self.root.logical_size, self.root.page_size)? as usize;
        let _ = self.page_txid_map()?;
        for page_no in 1..=u32::try_from(expected_pages).map_err(|_| StoreError::Range)? {
            let page = self.read_page(page_no)?;
            if page_no == 1
                && (page.len() < 100
                    || page[..16] != *SQLITE_MAGIC
                    || parse_page_size(&page) != Some(self.root.page_size))
            {
                return Err(StoreError::Corrupt(0));
            }
        }
        for segment in &self.segments {
            verify_segment_physical(&*self.backend, segment)?;
        }
        Ok(())
    }

    pub(crate) fn copy_logical_to(&mut self, destination: &File) -> Result<(), StoreError> {
        destination.set_len(self.root.logical_size)?;
        for page_no in 1..=page_count(self.root.logical_size, self.root.page_size)? {
            let page = self.read_page(page_no)?;
            write_all_at(
                destination,
                u64::from(page_no - 1) * u64::from(self.root.page_size),
                &page,
            )?;
        }
        Ok(())
    }

    pub(crate) fn inspect(&self) -> Result<Inspect, StoreError> {
        let mut segment_bytes = 0_u64;
        let mut segment_allocated_bytes = 0_u64;
        for entry in &self.catalog.segments {
            let path = self.backend.segment_path(&entry.id);
            segment_bytes = segment_bytes.saturating_add(path.metadata()?.len());
            segment_allocated_bytes =
                segment_allocated_bytes.saturating_add(allocated_bytes(&path.metadata()?));
        }
        let (active_bytes, active_allocated_bytes) = if let Some(file) = &self.active {
            let metadata = file.metadata()?;
            (metadata.len(), allocated_bytes(&metadata))
        } else {
            (0, 0)
        };
        let anchor_metadata = self.anchor.metadata()?;
        Ok(Inspect {
            path: self.path.clone(),
            sidecar_path: self.sidecar_path.clone(),
            page_size: self.root.page_size,
            page_count: page_count(self.root.logical_size, self.root.page_size)?,
            logical_size: self.root.logical_size,
            head_txid: self.root.head_txid,
            head_history: self.root.head_history,
            catalog_generation: self.catalog.generation,
            sealed_segments: self.catalog.segments.len(),
            active: self.root.active_id != [0; 16],
            anchor_bytes: anchor_metadata.len(),
            anchor_allocated_bytes: allocated_bytes(&anchor_metadata),
            segment_bytes,
            segment_allocated_bytes,
            active_bytes,
            active_allocated_bytes,
            indexed_pages: self.locations.len(),
            dictionary_bytes: self.catalog.current_dictionary.len(),
            policy: StoragePolicy::decode(self.root.policy),
        })
    }

    pub(crate) fn set_storage_policy(&mut self, policy: StoragePolicy) -> Result<(), StoreError> {
        self.begin_write()?;
        let mut root = self.root;
        root.sequence = self.next_root_sequence()?;
        root.policy = policy.encode()?;
        root.durable = true;
        self.publish_root(root, true)?;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn install_initial_dictionary(
        &mut self,
        dictionary: Vec<u8>,
    ) -> Result<(), StoreError> {
        if self.root.head_txid != 0
            || self.root.active_id != [0; 16]
            || self.has_pending()
            || dictionary.is_empty()
        {
            return Err(StoreError::InvalidConfiguration(
                "an initial dictionary can only be installed into an empty database",
            ));
        }
        self.begin_write()?;
        let mut catalog = self.catalog.clone();
        catalog.generation = catalog.generation.checked_add(1).ok_or(StoreError::Range)?;
        catalog.current_dictionary = dictionary;
        let catalog_digest = self.backend.put_catalog(&catalog, &catalog.encode()?)?;
        let mut root = self.root;
        root.sequence = self.next_root_sequence()?;
        root.catalog_digest = catalog_digest;
        root.last_dictionary_promotion_unix = unix_time();
        root.durable = true;
        self.publish_root(root, true)?;
        self.catalog = catalog;
        self.release_publication();
        Ok(())
    }

    pub(crate) fn background_flush_due(&self) -> bool {
        if self.root.active_id == [0; 16] || self.has_pending() {
            return false;
        }
        let now = unix_time();
        now.saturating_sub(self.root.last_dirty_unix) >= u64::from(self.root.policy.settle_seconds)
            || now.saturating_sub(self.root.oldest_dirty_unix)
                >= u64::from(self.root.policy.max_stale_seconds)
    }

    pub(crate) fn try_background_maintenance(&mut self) -> Result<(), StoreError> {
        if self.has_pending() {
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
        if self.maintenance_locked {
            return Ok(());
        }
        if !self.publication_locked {
            lock_exclusive(&self.publication, true)?;
            self.publication_locked = true;
        }
        self.refresh()?;
        self.maintenance_locked = true;
        Ok(())
    }

    pub(crate) fn release_maintenance(&mut self) {
        self.maintenance_locked = false;
        self.release_publication();
    }

    fn maybe_promote_dictionary(&mut self) -> Result<bool, StoreError> {
        let policy = StoragePolicy::decode(self.root.policy).dictionary;
        let bytes = self.training_pages.values().map(Vec::len).sum::<usize>();
        if self.training_pages.len() < MIN_TRAINING_PAGES || bytes < MIN_TRAINING_BYTES {
            return Ok(false);
        }
        let page_count = page_count(self.root.logical_size, self.root.page_size)?.max(1) as usize;
        if !self.catalog.current_dictionary.is_empty()
            && self.changed_since_dictionary.len().saturating_mul(10_000)
                < page_count.saturating_mul(policy.retrain_churn_bps as usize)
        {
            return Ok(false);
        }
        if self.root.last_dictionary_promotion_unix != 0
            && unix_time().saturating_sub(self.root.last_dictionary_promotion_unix)
                < policy.promotion_cooldown.as_secs()
        {
            return Ok(false);
        }
        let samples = self.training_pages.values().collect::<Vec<_>>();
        let split = samples.len().saturating_mul(4) / 5;
        let training = samples[..split]
            .iter()
            .map(|sample| sample.as_slice())
            .collect::<Vec<_>>();
        let candidate = zstd::dict::from_samples(&training, policy.dictionary_bytes as usize)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        let heldout = &samples[split..];
        let baseline = compressed_sample_size(heldout, &self.catalog.current_dictionary)?;
        let proposed = compressed_sample_size(heldout, &candidate)?;
        if baseline == 0
            || baseline.saturating_sub(proposed).saturating_mul(10_000)
                < baseline.saturating_mul(policy.min_improvement_bps as usize)
            || baseline.saturating_sub(proposed) <= candidate.len()
        {
            return Ok(false);
        }
        self.begin_write()?;
        if self.root.active_id != [0; 16] {
            self.flush_sidecars()?;
            self.begin_write()?;
        }
        let mut catalog = self.catalog.clone();
        catalog.generation = catalog.generation.checked_add(1).ok_or(StoreError::Range)?;
        catalog.current_dictionary = candidate;
        let catalog_digest = self.backend.put_catalog(&catalog, &catalog.encode()?)?;
        let mut root = self.root;
        root.sequence = self.next_root_sequence()?;
        root.catalog_digest = catalog_digest;
        root.last_dictionary_promotion_unix = unix_time();
        root.durable = true;
        self.publish_root(root, true)?;
        self.catalog = catalog;
        self.changed_since_dictionary.clear();
        self.release_publication();
        Ok(true)
    }

    fn remember_training_page(&mut self, page_no: u32, page: Vec<u8>) {
        let limit = usize::try_from(self.root.policy.dictionary.sample_bytes).unwrap_or(usize::MAX);
        if self.training_pages.insert(page_no, page).is_none() {
            self.training_order.push_back(page_no);
        }
        while self.training_pages.values().map(Vec::len).sum::<usize>() > limit {
            if let Some(oldest) = self.training_order.pop_front() {
                self.training_pages.remove(&oldest);
            } else {
                break;
            }
        }
    }

    fn page_txid_map(&self) -> Result<Vec<u64>, StoreError> {
        full_page_txid_map(
            &self.locations,
            page_count(self.root.logical_size, self.root.page_size)?,
        )
    }

    fn collect_garbage(&self, limit: usize) -> Result<(), StoreError> {
        if lock_exclusive(&self.lifecycle, true).is_err() {
            return Ok(());
        }
        let result = self.collect_garbage_exclusive(limit);
        lock_shared(&self.lifecycle, false)?;
        result
    }

    fn collect_garbage_exclusive(&self, limit: usize) -> Result<(), StoreError> {
        let mut live_catalogs = BTreeMap::new();
        live_catalogs.insert(self.root.catalog_digest, self.catalog.clone());
        for root in self.root_slots.iter().flatten() {
            if let std::collections::btree_map::Entry::Vacant(entry) =
                live_catalogs.entry(root.catalog_digest)
            {
                let bytes = self.backend.read_catalog(root.catalog_digest)?;
                let catalog = RootCatalog::decode(&bytes)?;
                if catalog.database_id != self.root.database_id {
                    return Err(StoreError::IdentityMismatch);
                }
                entry.insert(catalog);
            }
        }
        let live_segments = live_catalogs
            .values()
            .flat_map(|catalog| catalog.segments.iter().map(|entry| entry.id.clone()))
            .collect::<BTreeSet<_>>();
        let live_active = self
            .root_slots
            .iter()
            .flatten()
            .map(|root| root.active_id)
            .filter(|id| *id != [0; 16])
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
        for catalog in self.backend.list_catalogs()? {
            if removed >= limit {
                break;
            }
            if catalog != self.root.catalog_digest
                && !self
                    .root_slots
                    .iter()
                    .flatten()
                    .any(|root| root.catalog_digest == catalog)
            {
                self.backend.delete_catalog(catalog)?;
                removed += 1;
            }
        }
        for entry in std::fs::read_dir(self.backend.active_dir())? {
            if removed >= limit {
                break;
            }
            let path = entry?.path();
            let keep = live_active
                .iter()
                .any(|id| path == self.backend.active_path(*id));
            if !keep {
                let _ = std::fs::remove_file(path);
                removed += 1;
            }
        }
        Ok(())
    }

    fn release_publication(&mut self) {
        if self.publication_locked {
            let _ = unlock_file(&self.publication);
            self.publication_locked = false;
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.release_publication();
    }
}

#[allow(clippy::too_many_lines)]
fn load_segments(
    backend: &dyn SegmentBackend,
    catalog: &RootCatalog,
    page_size: u32,
) -> Result<(Vec<SegmentMeta>, BTreeMap<u32, PageLocation>), StoreError> {
    let mut segments = Vec::with_capacity(catalog.segments.len());
    let mut locations = BTreeMap::new();
    for (segment_index, entry) in catalog.segments.iter().enumerate() {
        if backend.segment_len(&entry.id)? != entry.file_len
            || entry.file_len < (SEGMENT_HEADER_SIZE + SEGMENT_TRAILER_SIZE) as u64
        {
            return Err(StoreError::Corrupt(0));
        }
        let mut header_bytes = [0; SEGMENT_HEADER_SIZE];
        backend.read_segment_range(&entry.id, 0, &mut header_bytes)?;
        let header = SegmentHeader::decode(&header_bytes)?;
        let mut trailer_bytes = [0; SEGMENT_TRAILER_SIZE];
        let trailer_offset = entry.file_len - SEGMENT_TRAILER_SIZE as u64;
        backend.read_segment_range(&entry.id, trailer_offset, &mut trailer_bytes)?;
        let trailer = SegmentTrailer::decode(&trailer_bytes)?;
        if header.database_id != catalog.database_id
            || trailer.database_id != catalog.database_id
            || header.start_txid != entry.id.start_txid
            || header.base_history != entry.base_history
            || header.page_size != trailer.page_size
            || trailer.start_txid != entry.id.start_txid
            || trailer.end_txid != entry.id.end_txid
            || trailer.base_history != entry.base_history
            || trailer.end_history != entry.id.end_history
            || trailer.physical_digest != entry.id.physical_digest
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
        let dictionaries = read_dictionary_table_backend(
            backend,
            &entry.id,
            header.dictionary_offset,
            header.dictionary_len,
        )?;
        let index = decode_index(&read_range_backend(
            backend,
            &entry.id,
            trailer.index_offset,
            trailer.index_len,
        )?)?;
        let mut previous_page = 0_u32;
        for value in &index {
            if value.page_no <= previous_page
                || value.last_txid < entry.id.start_txid
                || value.last_txid > entry.id.end_txid
            {
                return Err(StoreError::Corrupt(value.frame_offset));
            }
            previous_page = value.page_no;
            let frame_end = checked_end(value.frame_offset, u64::from(value.frame_record_len))?;
            if value.frame_offset < header.records_offset || frame_end > trailer.index_offset {
                return Err(StoreError::Corrupt(value.frame_offset));
            }
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
        let max_page = page_count(trailer.logical_size, trailer.page_size)?;
        locations.retain(|page, _| *page <= max_page);
        let page_map = decode_map(&read_range_backend(
            backend,
            &entry.id,
            trailer.map_offset,
            trailer.map_len,
        )?)?;
        if page_map != full_page_txid_map(&locations, max_page)?
            || trailer.content_root != content_root(&locations)
        {
            return Err(StoreError::Corrupt(trailer.map_offset));
        }
        segments.push(SegmentMeta {
            entry: entry.clone(),
            trailer,
            dictionaries,
        });
    }
    Ok((segments, locations))
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
    let mut entries = Vec::with_capacity(header.entry_count as usize);
    let mut cursor = offset + COMMIT_HEADER_SIZE as u64;
    for _ in 0..header.entry_count {
        let mut encoded = [0; COMMIT_ENTRY_SIZE];
        read_exact_at(file, cursor, &mut encoded)?;
        entries.push(CommitEntry::decode(&encoded)?);
        cursor += COMMIT_ENTRY_SIZE as u64;
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

fn transaction_hash(
    txid: u64,
    logical_size: u64,
    page_size: u32,
    entries: &[CommitEntry],
) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zsqlite/transaction/v1\0");
    hasher.update(&txid.to_le_bytes());
    hasher.update(&logical_size.to_le_bytes());
    hasher.update(&page_size.to_le_bytes());
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
    let mut output = vec![0; page_count as usize];
    for (page_no, location) in locations {
        let index = page_no.checked_sub(1).ok_or(StoreError::Corrupt(0))?;
        let slot = output
            .get_mut(index as usize)
            .ok_or(StoreError::Corrupt(u64::from(*page_no)))?;
        *slot = location.last_txid;
    }
    Ok(output)
}

fn encode_index(entries: &[SegmentIndexEntry]) -> Result<Vec<u8>, StoreError> {
    let raw = entries
        .iter()
        .flat_map(|entry| entry.encode())
        .collect::<Vec<_>>();
    encode_blob(*INDEX_BLOB_MAGIC, &raw)
}

fn decode_index(encoded: &[u8]) -> Result<Vec<SegmentIndexEntry>, StoreError> {
    let raw = decode_blob(*INDEX_BLOB_MAGIC, encoded)?;
    if !raw.len().is_multiple_of(SEGMENT_INDEX_ENTRY_SIZE) {
        return Err(StoreError::Corrupt(0));
    }
    raw.chunks_exact(SEGMENT_INDEX_ENTRY_SIZE)
        .map(|chunk| {
            let encoded: [u8; SEGMENT_INDEX_ENTRY_SIZE] =
                chunk.try_into().expect("exact index chunk");
            SegmentIndexEntry::decode(&encoded).map_err(StoreError::from)
        })
        .collect()
}

fn encode_map(values: &[u64]) -> Result<Vec<u8>, StoreError> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&(values.len() as u64).to_le_bytes());
    let mut cursor = 0_usize;
    while cursor < values.len() {
        let value = values[cursor];
        let mut run = 1_usize;
        while cursor + run < values.len() && values[cursor + run] == value {
            run += 1;
        }
        put_varint(&mut raw, run as u64);
        put_varint(&mut raw, value);
        cursor += run;
    }
    encode_blob(*MAP_BLOB_MAGIC, &raw)
}

fn decode_map(encoded: &[u8]) -> Result<Vec<u64>, StoreError> {
    let raw = decode_blob(*MAP_BLOB_MAGIC, encoded)?;
    if raw.len() < 8 {
        return Err(StoreError::Corrupt(0));
    }
    let count = u64::from_le_bytes(raw[..8].try_into().expect("map count"));
    let count = usize::try_from(count).map_err(|_| StoreError::Range)?;
    let mut output = Vec::with_capacity(count);
    let mut cursor = 8_usize;
    while output.len() < count {
        let run = usize::try_from(get_varint(&raw, &mut cursor)?).map_err(|_| StoreError::Range)?;
        let value = get_varint(&raw, &mut cursor)?;
        if run == 0 || output.len().saturating_add(run) > count {
            return Err(StoreError::Corrupt(0));
        }
        output.resize(output.len() + run, value);
    }
    if cursor != raw.len() {
        return Err(StoreError::Corrupt(0));
    }
    Ok(output)
}

fn encode_blob(magic: [u8; 8], raw: &[u8]) -> Result<Vec<u8>, StoreError> {
    if raw.len() as u64 > MAX_SECTION_BYTES {
        return Err(StoreError::Range);
    }
    let compressed = zstd::bulk::compress(raw, ZSTD_LEVEL)
        .map_err(|error| StoreError::Zstd(error.to_string()))?;
    let (codec, payload) = if compressed.len().saturating_add(MIN_FRAME_SAVINGS) < raw.len() {
        (Codec::Zstd, compressed)
    } else {
        (Codec::Raw, raw.to_vec())
    };
    let mut output = vec![0; BLOB_HEADER_SIZE];
    output[..8].copy_from_slice(&magic);
    output[8..10].copy_from_slice(&BLOB_VERSION.to_le_bytes());
    output[10] = codec as u8;
    output[16..24].copy_from_slice(&(raw.len() as u64).to_le_bytes());
    output[24..32].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    output[32..64].copy_from_slice(&digest(raw));
    let checksum = crc32fast::hash(&output[..BLOB_HEADER_SIZE - 4]);
    output[BLOB_HEADER_SIZE - 4..].copy_from_slice(&checksum.to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

fn decode_blob(magic: [u8; 8], encoded: &[u8]) -> Result<Vec<u8>, StoreError> {
    if encoded.len() < BLOB_HEADER_SIZE
        || encoded[..8] != magic
        || u16::from_le_bytes(encoded[8..10].try_into().expect("blob version")) != BLOB_VERSION
        || u32::from_le_bytes(
            encoded[BLOB_HEADER_SIZE - 4..BLOB_HEADER_SIZE]
                .try_into()
                .expect("blob checksum"),
        ) != crc32fast::hash(&encoded[..BLOB_HEADER_SIZE - 4])
    {
        return Err(StoreError::Corrupt(0));
    }
    let raw_len = usize::try_from(u64::from_le_bytes(
        encoded[16..24].try_into().expect("raw len"),
    ))
    .map_err(|_| StoreError::Range)?;
    let stored_len = usize::try_from(u64::from_le_bytes(
        encoded[24..32].try_into().expect("stored len"),
    ))
    .map_err(|_| StoreError::Range)?;
    if raw_len as u64 > MAX_SECTION_BYTES || stored_len as u64 > MAX_SECTION_BYTES {
        return Err(StoreError::Range);
    }
    if encoded.len()
        != BLOB_HEADER_SIZE
            .checked_add(stored_len)
            .ok_or(StoreError::Range)?
    {
        return Err(StoreError::Corrupt(0));
    }
    let payload = &encoded[BLOB_HEADER_SIZE..];
    let raw = match Codec::try_from(encoded[10])? {
        Codec::Raw => payload.to_vec(),
        Codec::Zstd => zstd::bulk::decompress(payload, raw_len)
            .map_err(|error| StoreError::Zstd(error.to_string()))?,
    };
    if raw.len() != raw_len || digest(&raw) != encoded[32..64] {
        return Err(StoreError::Corrupt(0));
    }
    Ok(raw)
}

fn put_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push(u8::try_from(value & 0x7f).expect("varint chunk fits u8") | 0x80);
        value >>= 7;
    }
    output.push(u8::try_from(value).expect("terminal varint byte fits u8"));
}

fn get_varint(input: &[u8], cursor: &mut usize) -> Result<u64, StoreError> {
    let mut output = 0_u64;
    for shift in (0..=63).step_by(7) {
        let byte = *input.get(*cursor).ok_or(StoreError::Corrupt(0))?;
        *cursor += 1;
        output |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(output);
        }
    }
    Err(StoreError::Corrupt(0))
}

fn read_dictionary_table_file(
    file: &File,
    offset: u64,
    length: u64,
) -> Result<Vec<DictionaryEntry>, StoreError> {
    decode_dictionary_table(&read_range_file(file, offset, length)?).map_err(StoreError::from)
}

fn read_dictionary_table_backend(
    backend: &dyn SegmentBackend,
    id: &SegmentId,
    offset: u64,
    length: u64,
) -> Result<Vec<DictionaryEntry>, StoreError> {
    decode_dictionary_table(&read_range_backend(backend, id, offset, length)?)
        .map_err(StoreError::from)
}

fn read_range_file(file: &File, offset: u64, length: u64) -> Result<Vec<u8>, StoreError> {
    if length > MAX_SECTION_BYTES {
        return Err(StoreError::Range);
    }
    let mut output = vec![0; usize::try_from(length).map_err(|_| StoreError::Range)?];
    read_exact_at(file, offset, &mut output)?;
    Ok(output)
}

fn read_range_backend(
    backend: &dyn SegmentBackend,
    id: &SegmentId,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, StoreError> {
    if length > MAX_SECTION_BYTES {
        return Err(StoreError::Range);
    }
    let mut output = vec![0; usize::try_from(length).map_err(|_| StoreError::Range)?];
    backend.read_segment_range(id, offset, &mut output)?;
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

fn verify_segment_physical(
    backend: &dyn SegmentBackend,
    segment: &SegmentMeta,
) -> Result<(), StoreError> {
    let length = segment.entry.file_len;
    let trailer_offset = length - SEGMENT_TRAILER_SIZE as u64;
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0_u64;
    let mut buffer = vec![0; COPY_BUFFER_SIZE];
    while offset < trailer_offset {
        let amount = usize::try_from((trailer_offset - offset).min(buffer.len() as u64))
            .map_err(|_| StoreError::Range)?;
        backend.read_segment_range(&segment.entry.id, offset, &mut buffer[..amount])?;
        hasher.update(&buffer[..amount]);
        offset += amount as u64;
    }
    let mut trailer = segment.trailer;
    trailer.physical_digest = [0; 32];
    hasher.update(&trailer.encode(false));
    if *hasher.finalize().as_bytes() != segment.entry.id.physical_digest {
        return Err(StoreError::Corrupt(trailer_offset));
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

fn newest_root(file: &File, database_id: DatabaseId) -> Result<(usize, AnchorRoot), StoreError> {
    let slots = read_root_slots(file, database_id)?;
    slots
        .iter()
        .enumerate()
        .filter_map(|(index, root)| root.map(|root| (index, root)))
        .max_by_key(|(_, root)| root.sequence)
        .ok_or(StoreError::Corrupt(ANCHOR_ROOT_A_OFFSET))
}

fn read_root_slots(
    file: &File,
    database_id: DatabaseId,
) -> Result<[Option<AnchorRoot>; 2], StoreError> {
    let mut output = [None, None];
    for (index, offset) in [ANCHOR_ROOT_A_OFFSET, ANCHOR_ROOT_B_OFFSET]
        .into_iter()
        .enumerate()
    {
        let mut encoded = [0; SECTOR_SIZE];
        read_exact_at(file, offset, &mut encoded)?;
        if let Ok(root) = AnchorRoot::decode(&encoded) {
            if root.database_id != database_id {
                return Err(StoreError::IdentityMismatch);
            }
            output[index] = Some(root);
        }
    }
    Ok(output)
}

fn read_anchor_header(file: &File) -> Result<AnchorHeader, StoreError> {
    let mut encoded = [0; ANCHOR_HEADER_SIZE];
    read_exact_at(file, 0, &mut encoded).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            StoreError::NotZsqlite
        } else {
            error.into()
        }
    })?;
    AnchorHeader::decode(&encoded).map_err(StoreError::from)
}

fn validate_policy(value: StoragePolicyRecord) -> Result<(), StoreError> {
    if value.settle_seconds == 0
        || value.max_stale_seconds < value.settle_seconds
        || value.hot_horizon_seconds == 0
        || value.admission_reads == 0
        || value.gc_dead_percent > 100
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
fn reject_aliased_file(file: &File) -> Result<(), StoreError> {
    use std::os::unix::fs::MetadataExt;
    if file.metadata()?.nlink() == 1 {
        Ok(())
    } else {
        Err(StoreError::Busy)
    }
}

#[cfg(not(unix))]
fn reject_aliased_file(_file: &File) -> Result<(), StoreError> {
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<(), StoreError> {
    File::open(path.parent().ok_or(StoreError::Range)?)?.sync_all()?;
    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf, StoreError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

pub(crate) fn reject_auxiliary_files(path: &Path) -> Result<(), StoreError> {
    for suffix in ["-journal", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        match std::fs::metadata(PathBuf::from(name)) {
            Ok(metadata) if metadata.len() != 0 => return Err(StoreError::Busy),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
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

#[cfg(unix)]
fn write_all_at(file: &File, mut offset: u64, mut input: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !input.is_empty() {
        let amount = match file.write_at(input, offset) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            result => result?,
        };
        if amount == 0 {
            return Err(std::io::Error::from(ErrorKind::WriteZero));
        }
        offset = offset
            .checked_add(amount as u64)
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        input = &input[amount..];
    }
    Ok(())
}

#[cfg(not(unix))]
fn read_exact_at(_file: &File, _offset: u64, _output: &mut [u8]) -> std::io::Result<()> {
    Err(std::io::Error::from(ErrorKind::Unsupported))
}
#[cfg(not(unix))]
fn write_all_at(_file: &File, _offset: u64, _input: &[u8]) -> std::io::Result<()> {
    Err(std::io::Error::from(ErrorKind::Unsupported))
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

#[cfg(unix)]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks().saturating_mul(512)
}
#[cfg(not(unix))]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
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
    fn falls_back_from_corrupt_newest_root_and_supersedes_its_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("fallback.zsqlite");
        let first = page(7, 4096);
        let second = page(8, 4096);
        let third = page(9, 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &first)?;
        store.publish(true)?;
        store.write_at(0, &second)?;
        store.publish(true)?;
        assert_eq!(store.root.head_txid, 2);
        assert_eq!(store.root.sequence, 3);

        let newest = store.locations.get(&1).copied().ok_or(StoreError::Range)?;
        let active = store.active.as_ref().ok_or(StoreError::Range)?;
        let payload_offset = newest.frame_offset + FRAME_HEADER_SIZE as u64;
        let mut byte = [0_u8; 1];
        read_exact_at(active, payload_offset, &mut byte)?;
        byte[0] ^= 0xff;
        write_all_at(active, payload_offset, &byte)?;
        active.sync_all()?;
        drop(store);

        let mut store = Store::open_existing(&path)?;
        assert_eq!(store.root.head_txid, 1);
        let mut output = vec![0; 4096];
        store.read_at(0, &mut output)?;
        assert_eq!(output, first);

        store.write_at(0, &third)?;
        store.publish(true)?;
        assert_eq!(store.root.head_txid, 2);
        assert_eq!(store.root.sequence, 4);
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
}
