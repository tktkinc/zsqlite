//! V3 append-only compressed extent store.

use crate::format::{
    Anchor, COMMIT_SIZE, Codec, Commit, DatabaseId, EXTENT_HEADER_SIZE, ExtentHeader, HEADER_SIZE,
    Header, INDEX_HEADER_SIZE, IndexHeader, MAX_EXTENT_BYTES, MAX_INDEX_BYTES, SECTOR_SIZE,
    SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET, Superblock, digest, valid_page_size,
};
use crate::seekable;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const SQLITE_MAGIC: &[u8; 16] = b"SQLite format 3\0";
const MIN_SAVINGS: usize = 64;
const ZSTD_LEVEL: i32 = 3;
const INDEX_ENTRY_SIZE: usize = 24;
const DEFAULT_EXTENT_BYTES: u32 = MAX_EXTENT_BYTES;
const DEFAULT_SEEK_CHUNK_BYTES: u32 = 64 * 1024;
const EXTENT_CACHE_BYTES: usize = 8 * 1024 * 1024;
const PENDING_CHUNK_CAPACITY: usize = 8;
const PENDING_EXTENT_CACHE_BYTES: usize = 2 * 1024 * 1024;
const COPY_BUFFER_SIZE: usize = 64 * 1024;
const MAX_GENERATIONS: usize = 1_000_000;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid sidecar: {0}")]
    Format(#[from] crate::format::FormatError),
    #[error("database anchor exists but its zsqlite sidecar is missing")]
    MissingSidecar,
    #[error("database anchor and sidecar identities do not match")]
    IdentityMismatch,
    #[error("database is corrupt at byte {0}; no automatic repair was attempted")]
    Corrupt(u64),
    #[error("database is busy in another process")]
    Busy,
    #[error("cannot determine SQLite page size from the first write")]
    UnknownPageSize,
    #[error("invalid SQLite page size {0}")]
    InvalidPageSize(u32),
    #[error("page {page_no} has stored length {actual}, expected {expected}")]
    InvalidPageLength {
        page_no: u32,
        actual: usize,
        expected: usize,
    },
    #[error("extent containing page {0} has a digest mismatch")]
    PageChecksum(u32),
    #[error("numeric or allocation limit exceeded")]
    Range,
    #[error("zstd error: {0}")]
    Zstd(String),
    #[error("database is not a V3 zsqlite database")]
    NotZsqlite,
    #[error("database was opened read-only")]
    ReadOnly,
    #[error("destination already exists: {0}")]
    DestinationExists(PathBuf),
    #[error("input is not a complete, page-aligned SQLite database")]
    InvalidStandardDatabase,
    #[error("unsupported filesystem or platform operation")]
    Unsupported,
    #[error("invalid compression configuration: {0}")]
    InvalidConfiguration(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompressionConfig {
    extent_bytes: u32,
    seek_chunk_bytes: u32,
}

impl CompressionConfig {
    pub fn new(extent_bytes: u32, seek_chunk_bytes: u32) -> Result<Self, StoreError> {
        if !(512..=MAX_EXTENT_BYTES).contains(&extent_bytes) || !extent_bytes.is_power_of_two() {
            return Err(StoreError::InvalidConfiguration(
                "extent size must be a power of two between 512 bytes and 1 MiB",
            ));
        }
        if !(512..=extent_bytes).contains(&seek_chunk_bytes)
            || !seek_chunk_bytes.is_power_of_two()
            || !extent_bytes.is_multiple_of(seek_chunk_bytes)
        {
            return Err(StoreError::InvalidConfiguration(
                "seek chunk size must be a power-of-two divisor of the extent size",
            ));
        }
        Ok(Self {
            extent_bytes,
            seek_chunk_bytes,
        })
    }

    #[must_use]
    pub const fn extent_bytes(self) -> u32 {
        self.extent_bytes
    }

    #[must_use]
    pub const fn seek_chunk_bytes(self) -> u32 {
        self.seek_chunk_bytes
    }

    pub fn validate_page_size(self, page_size: u32) -> Result<(), StoreError> {
        if !valid_page_size(page_size)
            || self.extent_bytes < page_size
            || self.seek_chunk_bytes < page_size
            || !self.extent_bytes.is_multiple_of(page_size)
            || !self.seek_chunk_bytes.is_multiple_of(page_size)
        {
            return Err(StoreError::InvalidConfiguration(
                "extent and seek chunk sizes must contain whole SQLite pages",
            ));
        }
        Ok(())
    }
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            extent_bytes: DEFAULT_EXTENT_BYTES,
            seek_chunk_bytes: DEFAULT_SEEK_CHUNK_BYTES,
        }
    }
}

#[derive(Debug)]
struct ExtentLocation {
    header: ExtentHeader,
    record_offset: u64,
    payload_offset: u64,
    seekable: Option<seekable::Layout>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CacheKey {
    record_offset: u64,
    frame: u32,
}

const WHOLE_EXTENT_CACHE_FRAME: u32 = u32::MAX;

#[derive(Debug)]
struct ExtentCache {
    capacity: usize,
    bytes: usize,
    entries: VecDeque<(CacheKey, Arc<Vec<u8>>)>,
}

impl ExtentCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            bytes: 0,
            entries: VecDeque::new(),
        }
    }

    fn get(&mut self, key: CacheKey) -> Option<Arc<Vec<u8>>> {
        let position = self
            .entries
            .iter()
            .position(|(candidate, _)| *candidate == key)?;
        let entry = self
            .entries
            .remove(position)
            .expect("cache position exists");
        let value = Arc::clone(&entry.1);
        self.entries.push_front(entry);
        Some(value)
    }

    fn insert(&mut self, key: CacheKey, value: Arc<Vec<u8>>) {
        self.remove(key);
        self.bytes = self.bytes.saturating_add(value.len());
        self.entries.push_front((key, value));
        while self.bytes > self.capacity && self.entries.len() > 1 {
            if let Some((_, removed)) = self.entries.pop_back() {
                self.bytes = self.bytes.saturating_sub(removed.len());
            }
        }
    }

    fn remove(&mut self, key: CacheKey) {
        if let Some(position) = self
            .entries
            .iter()
            .position(|(candidate, _)| *candidate == key)
            && let Some((_, removed)) = self.entries.remove(position)
        {
            self.bytes = self.bytes.saturating_sub(removed.len());
        }
    }

    fn remove_record(&mut self, record_offset: u64) {
        self.entries.retain(|(key, value)| {
            let keep = key.record_offset != record_offset;
            if !keep {
                self.bytes = self.bytes.saturating_sub(value.len());
            }
            keep
        });
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }
}

type AppendedExtents = (Vec<Arc<ExtentLocation>>, u64, [u8; blake3::OUT_LEN]);

#[derive(Clone, Debug)]
struct PageRun {
    first_page: u32,
    page_count: u32,
    extent: Arc<ExtentLocation>,
    extent_page: u32,
}

#[derive(Debug, Default)]
struct PendingChunk {
    pages: BTreeMap<u32, Vec<u8>>,
}

#[derive(Debug)]
struct BootstrapLayer {
    file: File,
}

#[derive(Debug)]
struct PendingLayer {
    file: File,
    generation: u64,
    page_size: u32,
    config: CompressionConfig,
    next_offset: u64,
    chunks: BTreeMap<u32, PendingChunk>,
    chunk_lru: VecDeque<u32>,
    runs: BTreeMap<u32, PageRun>,
    extent_cache: ExtentCache,
}

impl PageRun {
    fn end(&self) -> Result<u32, StoreError> {
        self.first_page
            .checked_add(self.page_count)
            .ok_or(StoreError::Range)
    }

    fn contains(&self, page: u32) -> bool {
        self.end()
            .is_ok_and(|end| page >= self.first_page && page < end)
    }
}

impl BootstrapLayer {
    fn new(sidecar_path: &Path) -> Result<Self, StoreError> {
        Ok(Self {
            file: create_unlinked_temporary(sidecar_path)?,
        })
    }

    fn write_at(&self, offset: u64, input: &[u8]) -> Result<(), StoreError> {
        write_all_at(&self.file, offset, input)?;
        let end = offset
            .checked_add(input.len() as u64)
            .ok_or(StoreError::Range)?;
        if end > self.file.metadata()?.len() {
            self.file.set_len(end)?;
        }
        Ok(())
    }

    fn read_at(&self, offset: u64, output: &mut [u8]) -> Result<(), StoreError> {
        read_exact_at(&self.file, offset, output)?;
        Ok(())
    }

    fn truncate(&self, size: u64) -> Result<(), StoreError> {
        self.file.set_len(size)?;
        Ok(())
    }
}

impl PendingLayer {
    fn new(
        sidecar_path: &Path,
        generation: u64,
        page_size: u32,
        config: CompressionConfig,
    ) -> Result<Self, StoreError> {
        config.validate_page_size(page_size)?;
        Ok(Self {
            file: create_unlinked_temporary(sidecar_path)?,
            generation,
            page_size,
            config,
            next_offset: 0,
            chunks: BTreeMap::new(),
            chunk_lru: VecDeque::new(),
            runs: BTreeMap::new(),
            extent_cache: ExtentCache::new(PENDING_EXTENT_CACHE_BYTES),
        })
    }

    fn is_empty(&self) -> bool {
        self.chunks.values().all(|chunk| chunk.pages.is_empty()) && self.runs.is_empty()
    }

    fn contains_page(&self, page: u32) -> Result<bool, StoreError> {
        let chunk = pending_chunk_first(page, self.page_size, self.config.extent_bytes)?;
        if self
            .chunks
            .get(&chunk)
            .is_some_and(|chunk| chunk.pages.contains_key(&page))
        {
            return Ok(true);
        }
        Ok(self
            .runs
            .range(..=page)
            .next_back()
            .is_some_and(|(_, run)| run.contains(page)))
    }

    fn read_page(&mut self, page: u32) -> Result<Option<Vec<u8>>, StoreError> {
        let chunk = pending_chunk_first(page, self.page_size, self.config.extent_bytes)?;
        if let Some(value) = self
            .chunks
            .get(&chunk)
            .and_then(|chunk| chunk.pages.get(&page))
        {
            return Ok(Some(value.clone()));
        }
        let run = self
            .runs
            .range(..=page)
            .next_back()
            .map(|(_, run)| run.clone());
        let Some(run) = run.filter(|run| run.contains(page)) else {
            return Ok(None);
        };
        let within = run
            .extent_page
            .checked_add(page - run.first_page)
            .ok_or(StoreError::Range)?;
        read_extent_page(
            &self.file,
            &mut self.extent_cache,
            &run.extent,
            within,
            self.page_size,
        )
        .map(Some)
    }

    fn stage_page(&mut self, page: u32, data: Vec<u8>) -> Result<(), StoreError> {
        if page == 0 || data.len() != self.page_size as usize {
            return Err(StoreError::InvalidPageLength {
                page_no: page,
                actual: data.len(),
                expected: self.page_size as usize,
            });
        }
        let first = pending_chunk_first(page, self.page_size, self.config.extent_bytes)?;
        if !self.chunks.contains_key(&first) {
            while self.chunks.len() >= PENDING_CHUNK_CAPACITY {
                let oldest = self.chunk_lru.pop_front().ok_or(StoreError::Range)?;
                self.flush_chunk(oldest)?;
            }
            self.chunks.insert(first, PendingChunk::default());
        }
        self.touch_chunk(first);
        let chunk = self.chunks.get_mut(&first).ok_or(StoreError::Range)?;
        chunk.pages.insert(page, data);
        if chunk.pages.len()
            == max_pages_per_extent(self.page_size, self.config.extent_bytes)? as usize
        {
            self.flush_chunk(first)?;
        }
        Ok(())
    }

    fn truncate(&mut self, max_page: u32) -> Result<(), StoreError> {
        for chunk in self.chunks.values_mut() {
            chunk.pages.retain(|page, _| *page <= max_page);
        }
        self.chunks.retain(|_, chunk| !chunk.pages.is_empty());
        self.chunk_lru
            .retain(|first| self.chunks.contains_key(first));
        truncate_runs(&mut self.runs, max_page)
    }

    fn flush_all(&mut self) -> Result<(), StoreError> {
        let chunks: Vec<u32> = self.chunks.keys().copied().collect();
        for first in chunks {
            self.flush_chunk(first)?;
        }
        Ok(())
    }

    fn flush_chunk(&mut self, first: u32) -> Result<(), StoreError> {
        let Some(chunk) = self.chunks.remove(&first) else {
            return Ok(());
        };
        self.chunk_lru.retain(|candidate| *candidate != first);
        if chunk.pages.is_empty() {
            return Ok(());
        }
        let mut raw = Vec::new();
        raw.try_reserve_exact(self.config.extent_bytes as usize)
            .map_err(|_| StoreError::Range)?;
        let mut run_first = 0_u32;
        let mut previous = 0_u32;
        let mut count = 0_u32;
        for (page, data) in chunk.pages {
            if count != 0 && page != previous.checked_add(1).ok_or(StoreError::Range)? {
                self.flush_run(run_first, count, &raw)?;
                raw.clear();
                count = 0;
            }
            if count == 0 {
                run_first = page;
            }
            raw.extend_from_slice(&data);
            count = count.checked_add(1).ok_or(StoreError::Range)?;
            previous = page;
        }
        if count != 0 {
            self.flush_run(run_first, count, &raw)?;
        }
        Ok(())
    }

    fn flush_run(
        &mut self,
        first_page: u32,
        page_count: u32,
        raw: &[u8],
    ) -> Result<(), StoreError> {
        let (extent, next, _) = write_extent(
            &self.file,
            self.next_offset,
            self.generation,
            first_page,
            page_count,
            raw,
            self.page_size,
            self.config.seek_chunk_bytes,
        )?;
        self.next_offset = next;
        replace_run(
            &mut self.runs,
            PageRun {
                first_page,
                page_count,
                extent,
                extent_page: 0,
            },
        )
    }

    fn read_extent(&mut self, location: &Arc<ExtentLocation>) -> Result<Arc<Vec<u8>>, StoreError> {
        read_whole_extent(&self.file, &mut self.extent_cache, location, self.page_size)
    }

    fn touch_chunk(&mut self, first: u32) {
        self.chunk_lru.retain(|candidate| *candidate != first);
        self.chunk_lru.push_back(first);
    }

    #[cfg(test)]
    fn buffered_bytes(&self) -> usize {
        self.chunks
            .values()
            .flat_map(|chunk| chunk.pages.values())
            .map(Vec::len)
            .sum()
    }
}

#[derive(Clone, Debug)]
pub struct Inspect {
    pub path: PathBuf,
    pub sidecar_path: PathBuf,
    pub database_id: DatabaseId,
    pub page_size: u32,
    pub logical_size: u64,
    pub page_count: u32,
    pub generation: u64,
    pub index_generation: u64,
    pub indexed_pages: usize,
    pub live_extents: usize,
    pub index_runs: usize,
    pub base_bytes: u64,
    pub sidecar_bytes: u64,
    pub sidecar_allocated_bytes: u64,
    pub live_stored_bytes: u64,
    pub committed_end: u64,
    pub hole_punching: bool,
}

/// Internal page store. `SQLite` locking must surround logical access.
pub(crate) struct Store {
    path: PathBuf,
    sidecar_path: PathBuf,
    base: File,
    sidecar: File,
    lifecycle: File,
    publication: File,
    database_id: DatabaseId,
    config: CompressionConfig,
    page_size: u32,
    logical_size: u64,
    committed_size: u64,
    generation: u64,
    commit_offset: u64,
    committed_end: u64,
    runs: BTreeMap<u32, PageRun>,
    live_counts: HashMap<u64, u32>,
    pending: Option<PendingLayer>,
    bootstrap: Option<BootstrapLayer>,
    truncate_floor: Option<u64>,
    writable: bool,
    superblocks: [Option<Superblock>; 2],
    active_slot: usize,
    indexed_generation: u64,
    obsolete_ranges: Vec<(u64, u64, u64)>,
    extent_cache: ExtentCache,
    hole_punching: bool,
    allocation_granularity: u64,
    publication_locked: bool,
}

impl Store {
    pub(crate) fn open_existing(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_mode(path.as_ref(), false, true, CompressionConfig::default())
    }

    pub(crate) fn open_existing_read_only(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_mode(path.as_ref(), false, false, CompressionConfig::default())
    }

    pub(crate) fn open_existing_read_only_with_config(
        path: impl AsRef<Path>,
        config: CompressionConfig,
    ) -> Result<Self, StoreError> {
        Self::open_mode(path.as_ref(), false, false, config)
    }

    #[cfg(test)]
    pub(crate) fn open(path: impl AsRef<Path>, create: bool) -> Result<Self, StoreError> {
        Self::open_mode(path.as_ref(), create, true, CompressionConfig::default())
    }

    pub(crate) fn open_with_config(
        path: impl AsRef<Path>,
        create: bool,
        config: CompressionConfig,
    ) -> Result<Self, StoreError> {
        Self::open_mode(path.as_ref(), create, true, config)
    }

    #[allow(clippy::too_many_lines)]
    fn open_mode(
        path: &Path,
        create: bool,
        writable: bool,
        config: CompressionConfig,
    ) -> Result<Self, StoreError> {
        let path = absolute_path(path)?;
        if deletion_path(&path).exists() {
            return Err(StoreError::Busy);
        }
        let mut base_options = OpenOptions::new();
        base_options
            .read(true)
            .write(writable)
            .create(create && writable);
        let base = base_options.open(&path)?;
        reject_aliased_file(&base)?;
        let sidecar_path = sidecar_path(&path);
        let lifecycle_path = lifecycle_path(&path);
        let publication_path = publication_path(&path);
        let mut anchor = match read_anchor(&base) {
            Ok(anchor) => anchor,
            Err(_) if create && writable && lifecycle_path.exists() => None,
            Err(error) => return Err(error),
        };
        let mut lifecycle = None;
        let mut lifecycle_exclusive = false;

        // Database creation is serialized before an identity exists. A process
        // which finds an existing bootstrap file first waits for its creator to
        // downgrade the lock, then re-reads the authoritative anchor.
        if anchor.is_none()
            && create
            && writable
            && (base.metadata()?.len() == 0 || sidecar_path.exists() || lifecycle_path.exists())
        {
            let (file, _) = open_bootstrap_lock(&lifecycle_path)?;
            loop {
                lock_shared(&file, false)?;
                anchor = read_anchor(&base)?;
                if anchor.is_some() {
                    break;
                }
                unlock_file(&file)?;
                match lock_exclusive(&file, true) {
                    Ok(()) => {
                        anchor = read_anchor(&base)?;
                        if anchor.is_none() {
                            lifecycle_exclusive = true;
                            break;
                        }
                        unlock_file(&file)?;
                    }
                    Err(StoreError::Busy) => std::thread::yield_now(),
                    Err(error) => return Err(error),
                }
                if anchor.is_some() {
                    lock_shared(&file, false)?;
                    break;
                }
            }
            lifecycle = Some(file);
        }

        let base_len = base.metadata()?.len();
        let sidecar_exists = sidecar_path.exists();

        if anchor.is_some() && !sidecar_exists {
            return Err(StoreError::MissingSidecar);
        }
        if anchor.is_none() && base_len != 0 {
            return Err(StoreError::NotZsqlite);
        }
        if !sidecar_exists && (!create || !writable || base_len != 0) {
            return Err(StoreError::NotZsqlite);
        }

        let unanchored = anchor.is_none();
        let database_id = if sidecar_exists {
            let mut options = OpenOptions::new();
            options.read(true).write(writable);
            let sidecar = options.open(&sidecar_path)?;
            reject_aliased_file(&sidecar)?;
            let header = read_header(&sidecar)?;
            if let Some(anchor) = anchor {
                if anchor.database_id != header.database_id {
                    return Err(StoreError::IdentityMismatch);
                }
            } else {
                let slots = read_superblocks(&sidecar, header.database_id)?;
                if sidecar.metadata()?.len() != HEADER_SIZE as u64
                    || slots.iter().any(Option::is_some)
                    || !create
                    || !writable
                {
                    return Err(StoreError::IdentityMismatch);
                }
            }
            header.database_id
        } else {
            let database_id = random_database_id()?;
            create_sidecar(&sidecar_path, database_id)?;
            database_id
        };

        let mut sidecar_options = OpenOptions::new();
        sidecar_options.read(true).write(writable);
        let sidecar = sidecar_options.open(&sidecar_path)?;
        reject_aliased_file(&sidecar)?;
        let lifecycle = if let Some(file) = lifecycle {
            initialize_or_validate_lock_file(&file, database_id, unanchored)?;
            file
        } else {
            open_lock_file(&lifecycle_path, database_id, writable)?
        };
        let publication = if unanchored {
            ensure_lock_file(&publication_path, database_id)?
        } else {
            open_lock_file(&publication_path, database_id, writable)?
        };
        if unanchored {
            // The anchor is the authoritative final step. Any crash before this
            // point leaves an empty, recoverable bootstrap bundle, never a
            // database that can be mistaken for committed content.
            write_anchor(&base, database_id)?;
        }
        if lifecycle_exclusive {
            unlock_file(&lifecycle)?;
        }
        lock_shared(&lifecycle, false)?;

        let allocation_granularity = allocation_granularity(&sidecar);
        let mut store = Self {
            path,
            sidecar_path,
            base,
            sidecar,
            lifecycle,
            publication,
            database_id,
            config,
            page_size: 0,
            logical_size: 0,
            committed_size: 0,
            generation: 0,
            commit_offset: 0,
            committed_end: HEADER_SIZE as u64,
            runs: BTreeMap::new(),
            live_counts: HashMap::new(),
            pending: None,
            bootstrap: None,
            truncate_floor: None,
            writable,
            superblocks: [None, None],
            active_slot: 0,
            indexed_generation: 0,
            obsolete_ranges: Vec::new(),
            extent_cache: ExtentCache::new(EXTENT_CACHE_BYTES),
            hole_punching: cfg!(any(
                target_os = "linux",
                target_os = "android",
                target_os = "macos"
            )),
            allocation_granularity,
            publication_locked: false,
        };
        store.with_publication_snapshot(Self::reload)?;
        Ok(store)
    }

    pub(crate) fn ensure_config(&self, config: CompressionConfig) -> Result<(), StoreError> {
        if self.config == config {
            Ok(())
        } else {
            Err(StoreError::InvalidConfiguration(
                "connections sharing a database must use the same compression sizes",
            ))
        }
    }

    pub(crate) fn delete_bundle(path: impl AsRef<Path>) -> Result<(), StoreError> {
        let path = absolute_path(path.as_ref())?;
        let sidecar_path = sidecar_path(&path);
        let marker_path = deletion_path(&path);
        let anchor = match File::open(&path) {
            Ok(file) => read_anchor(&file)?,
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let marker_id = match File::open(&marker_path) {
            Ok(file) => {
                let mut id = [0; 16];
                read_exact_at(&file, 0, &mut id)?;
                Some(id)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let sidecar_id = match File::open(&sidecar_path) {
            Ok(file) => Some(read_header(&file)?.database_id),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let database_id = anchor
            .map(|value| value.database_id)
            .or(marker_id)
            .or(sidecar_id)
            .ok_or(StoreError::NotZsqlite)?;
        for found in [anchor.map(|value| value.database_id), marker_id, sidecar_id]
            .into_iter()
            .flatten()
        {
            if found != database_id {
                return Err(StoreError::IdentityMismatch);
            }
        }

        let lifecycle_path = lifecycle_path(&path);
        let lifecycle = ensure_lock_file(&lifecycle_path, database_id)?;
        lock_exclusive(&lifecycle, true)?;
        ensure_delete_marker(&marker_path, database_id)?;
        remove_if_exists(&sidecar_path)?;
        remove_if_exists(&publication_path(&path))?;
        remove_if_exists(&path)?;
        lifecycle.set_len(0)?;
        lifecycle.sync_all()?;
        sync_parent_dir(&path)?;
        unlock_file(&lifecycle)?;
        remove_if_exists(&lifecycle_path)?;
        sync_parent_dir(&path)?;
        remove_if_exists(&marker_path)?;
        sync_parent_dir(&path)?;
        Ok(())
    }

    pub(crate) fn logical_size(&self) -> u64 {
        self.logical_size
    }

    pub(crate) fn upgrade_writable(&mut self) -> Result<(), StoreError> {
        if self.writable {
            return Ok(());
        }
        let sidecar = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.sidecar_path)?;
        let base = OpenOptions::new().read(true).write(true).open(&self.path)?;
        reject_aliased_file(&base)?;
        self.sidecar = sidecar;
        self.base = base;
        self.writable = true;
        Ok(())
    }

    pub(crate) fn refresh(&mut self) -> Result<(), StoreError> {
        if self.has_pending() {
            return Ok(());
        }
        self.with_publication_snapshot(|store| {
            let slots = read_superblocks(&store.sidecar, store.database_id)?;
            let newest = newest_superblock(&slots);
            if newest.map_or(0, |(_, value)| value.sequence)
                > store.superblocks[store.active_slot].map_or(0, |value| value.sequence)
            {
                store.reload()?;
            }
            Ok(())
        })
    }

    pub(crate) fn begin_write(&mut self) -> Result<(), StoreError> {
        if !self.writable {
            return Err(StoreError::ReadOnly);
        }
        if !self.publication_locked {
            lock_exclusive(&self.publication, false)?;
            self.publication_locked = true;
            if let Err(error) = self.refresh() {
                self.release_publication();
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.pending
            .as_ref()
            .is_some_and(|pending| !pending.is_empty())
            || self.bootstrap.is_some()
            || self.truncate_floor.is_some()
            || self.logical_size != self.committed_size
    }

    pub(crate) fn discard_pending(&mut self) -> Result<(), StoreError> {
        if self.has_pending() {
            self.reload()?;
        }
        self.release_publication();
        Ok(())
    }

    pub(crate) fn read_at(&mut self, offset: u64, output: &mut [u8]) -> Result<usize, StoreError> {
        output.fill(0);
        if output.is_empty() || offset >= self.logical_size {
            return Ok(0);
        }
        let available = self.logical_size.saturating_sub(offset);
        let actual =
            usize::try_from(available.min(output.len() as u64)).map_err(|_| StoreError::Range)?;
        if self.page_size == 0 {
            if let Some(bootstrap) = &self.bootstrap {
                bootstrap.read_at(offset, &mut output[..actual])?;
            }
            return Ok(actual);
        }
        let page_size = self.page_size as usize;
        let mut copied = 0;
        while copied < actual {
            let absolute = usize::try_from(offset)
                .map_err(|_| StoreError::Range)?
                .checked_add(copied)
                .ok_or(StoreError::Range)?;
            let page_no = u32::try_from(absolute / page_size + 1).map_err(|_| StoreError::Range)?;
            let within = absolute % page_size;
            let amount = (page_size - within).min(actual - copied);
            let page = self.read_page(page_no)?;
            output[copied..copied + amount].copy_from_slice(&page[within..within + amount]);
            copied += amount;
        }
        Ok(actual)
    }

    pub(crate) fn write_at(&mut self, offset: u64, input: &[u8]) -> Result<usize, StoreError> {
        self.begin_write()?;
        if input.is_empty() {
            return Ok(0);
        }
        let declared =
            (offset == 0 && input.len() >= 18 && input[..SQLITE_MAGIC.len()] == *SQLITE_MAGIC)
                .then(|| parse_page_size(input))
                .flatten();
        if let Some(declared) = declared
            && self.page_size != 0
            && declared != self.page_size
        {
            return Err(StoreError::InvalidPageSize(declared));
        }
        if self.page_size == 0 {
            if self.bootstrap.is_none() {
                self.bootstrap = Some(BootstrapLayer::new(&self.sidecar_path)?);
            }
            self.bootstrap
                .as_ref()
                .ok_or(StoreError::Range)?
                .write_at(offset, input)?;
            self.logical_size = self.logical_size.max(
                offset
                    .checked_add(input.len() as u64)
                    .ok_or(StoreError::Range)?,
            );
            let Some(page_size) = declared else {
                return Ok(input.len());
            };
            if self.logical_size != 0 && !self.logical_size.is_multiple_of(u64::from(page_size)) {
                return Err(StoreError::InvalidPageLength {
                    page_no: 0,
                    actual: usize::try_from(self.logical_size).unwrap_or(usize::MAX),
                    expected: page_size as usize,
                });
            }
            self.page_size = page_size;
            let bootstrap = self.bootstrap.take().ok_or(StoreError::Range)?;
            self.config.validate_page_size(page_size)?;
            let mut buffer = allocate_zeroed(self.config.extent_bytes as usize)?;
            let mut staged_offset = 0_u64;
            while staged_offset < self.logical_size {
                let amount = usize::try_from(
                    (self.logical_size - staged_offset).min(u64::from(self.config.extent_bytes)),
                )
                .map_err(|_| StoreError::Range)?;
                bootstrap.read_at(staged_offset, &mut buffer[..amount])?;
                self.apply_known_page_write(staged_offset, &buffer[..amount])?;
                staged_offset = staged_offset
                    .checked_add(amount as u64)
                    .ok_or(StoreError::Range)?;
            }
            return Ok(input.len());
        }
        self.apply_known_page_write(offset, input)?;
        Ok(input.len())
    }

    fn apply_known_page_write(&mut self, offset: u64, input: &[u8]) -> Result<(), StoreError> {
        let page_size = self.page_size as usize;
        let start = usize::try_from(offset).map_err(|_| StoreError::Range)?;
        let mut copied = 0;
        while copied < input.len() {
            let absolute = start.checked_add(copied).ok_or(StoreError::Range)?;
            let page_no = u32::try_from(absolute / page_size + 1).map_err(|_| StoreError::Range)?;
            let within = absolute % page_size;
            let amount = (page_size - within).min(input.len() - copied);
            let mut page = self.read_page(page_no)?;
            page[within..within + amount].copy_from_slice(&input[copied..copied + amount]);
            self.stage_page(page_no, page)?;
            copied += amount;
        }
        self.logical_size = self.logical_size.max(
            offset
                .checked_add(input.len() as u64)
                .ok_or(StoreError::Range)?,
        );
        Ok(())
    }

    fn stage_page(&mut self, page: u32, data: Vec<u8>) -> Result<(), StoreError> {
        if self.pending.is_none() {
            let generation = self.generation.checked_add(1).ok_or(StoreError::Range)?;
            self.pending = Some(PendingLayer::new(
                &self.sidecar_path,
                generation,
                self.page_size,
                self.config,
            )?);
        }
        self.pending
            .as_mut()
            .ok_or(StoreError::Range)?
            .stage_page(page, data)
    }

    pub(crate) fn truncate(&mut self, size: u64) -> Result<(), StoreError> {
        self.begin_write()?;
        if self.page_size != 0 && !size.is_multiple_of(u64::from(self.page_size)) {
            return Err(StoreError::InvalidPageLength {
                page_no: 0,
                actual: usize::try_from(size).unwrap_or(usize::MAX),
                expected: self.page_size as usize,
            });
        }
        if size < self.logical_size {
            self.truncate_floor = Some(self.truncate_floor.map_or(size, |old| old.min(size)));
        }
        self.logical_size = size;
        if self.page_size != 0 {
            let last =
                u32::try_from(size / u64::from(self.page_size)).map_err(|_| StoreError::Range)?;
            if let Some(pending) = &mut self.pending {
                pending.truncate(last)?;
            }
        } else if let Some(bootstrap) = &self.bootstrap {
            bootstrap.truncate(size)?;
        }
        Ok(())
    }

    /// Publishes pending data. Durable publication uses data-before-superblock ordering.
    pub(crate) fn publish(&mut self, durable: bool) -> Result<(), StoreError> {
        if !self.has_pending() {
            let result = if durable {
                self.begin_write()
                    .and_then(|()| self.mark_current_durable())
            } else {
                Ok(())
            };
            self.release_publication();
            return result;
        }
        self.begin_write()?;
        if self.page_size == 0 {
            self.release_publication();
            return Err(StoreError::UnknownPageSize);
        }
        let page_count = logical_page_count(self.logical_size, self.page_size)?;
        self.materialize_truncated_tail(page_count)?;
        let generation = self.generation.checked_add(1).ok_or(StoreError::Range)?;
        let generation_start = align_up(self.sidecar.metadata()?.len(), SECTOR_SIZE as u64)?;
        if generation_start > self.sidecar.metadata()?.len() {
            self.sidecar.set_len(generation_start)?;
        }
        let mut cursor = generation_start;
        let (extents, next, generation_digest) = if let Some(pending) = &mut self.pending {
            append_pending_extents(pending, &self.sidecar, cursor, page_count, self.page_size)?
        } else {
            (
                Vec::new(),
                cursor,
                *blake3::Hasher::new().finalize().as_bytes(),
            )
        };
        cursor = next;
        let commit_offset = cursor;
        let commit = Commit {
            generation,
            previous_commit: self.commit_offset,
            generation_start,
            logical_size: self.logical_size,
            page_size: self.page_size,
            extent_count: u32::try_from(extents.len()).map_err(|_| StoreError::Range)?,
            generation_digest,
        };
        write_all_at(&self.sidecar, commit_offset, &commit.encode())?;
        let commit_end = commit_offset
            .checked_add(COMMIT_SIZE as u64)
            .ok_or(StoreError::Range)?;
        self.sidecar.set_len(commit_end)?;
        if durable {
            self.sidecar.sync_all()?;
        }
        let target = 1 - self.active_slot;
        let sequence = self
            .superblocks
            .iter()
            .flatten()
            .map(|value| value.sequence)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StoreError::Range)?;
        let superblock = Superblock {
            sequence,
            generation,
            durable,
            logical_size: self.logical_size,
            page_size: self.page_size,
            page_count,
            commit_offset,
            commit_end,
            index_offset: 0,
            index_end: 0,
            database_id: self.database_id,
        };
        write_superblock(&self.sidecar, target, superblock)?;
        if durable {
            self.sidecar.sync_all()?;
        }
        self.superblocks[target] = Some(superblock);
        self.active_slot = target;
        self.generation = generation;
        self.commit_offset = commit_offset;
        self.committed_end = commit_end;
        self.committed_size = self.logical_size;
        self.install_generation(extents, page_count)?;
        self.pending = None;
        self.bootstrap = None;
        self.truncate_floor = None;
        self.indexed_generation = 0;
        self.punch_obsolete();
        self.release_publication();
        Ok(())
    }

    fn materialize_truncated_tail(&mut self, page_count: u32) -> Result<(), StoreError> {
        let Some(floor) = self.truncate_floor else {
            return Ok(());
        };
        let first_page = u32::try_from(floor / u64::from(self.page_size))
            .map_err(|_| StoreError::Range)?
            .checked_add(1)
            .ok_or(StoreError::Range)?;
        if first_page > page_count {
            return Ok(());
        }
        let zero = vec![0; self.page_size as usize];
        for page in first_page..=page_count {
            let already_staged = match &self.pending {
                Some(pending) => pending.contains_page(page)?,
                None => false,
            };
            if !already_staged {
                self.stage_page(page, zero.clone())?;
            }
        }
        Ok(())
    }

    pub(crate) fn checkpoint_index(&mut self) -> Result<(), StoreError> {
        if self.generation == 0 || self.indexed_generation == self.generation || self.has_pending()
        {
            return Ok(());
        }
        self.begin_write()?;
        let raw = encode_index(&self.runs)?;
        let (codec, payload) = encode_bytes(&raw)?;
        let offset = align_up(self.sidecar.metadata()?.len(), SECTOR_SIZE as u64)?;
        if offset > self.sidecar.metadata()?.len() {
            self.sidecar.set_len(offset)?;
        }
        let header = IndexHeader {
            generation: self.generation,
            entry_count: u32::try_from(self.runs.len()).map_err(|_| StoreError::Range)?,
            codec,
            raw_len: raw.len() as u64,
            stored_len: payload.len() as u64,
            raw_digest: digest(&raw),
        };
        write_all_at(&self.sidecar, offset, &header.encode())?;
        write_all_at(&self.sidecar, offset + INDEX_HEADER_SIZE as u64, &payload)?;
        let end = offset + INDEX_HEADER_SIZE as u64 + payload.len() as u64;
        self.sidecar.set_len(end)?;
        self.sidecar.sync_all()?;
        let target = 1 - self.active_slot;
        let current = self.superblocks[self.active_slot].ok_or(StoreError::Corrupt(0))?;
        let next = Superblock {
            sequence: current.sequence.checked_add(1).ok_or(StoreError::Range)?,
            durable: true,
            index_offset: offset,
            index_end: end,
            ..current
        };
        write_superblock(&self.sidecar, target, next)?;
        self.sidecar.sync_all()?;
        self.superblocks[target] = Some(next);
        self.active_slot = target;
        self.indexed_generation = self.generation;
        self.punch_obsolete();
        self.release_publication();
        Ok(())
    }

    pub(crate) fn verify(&mut self) -> Result<(), StoreError> {
        self.refresh()?;
        let pages = logical_page_count(self.logical_size, self.page_size)?;
        for page in 1..=pages {
            let data = self.read_page(page)?;
            if page == 1 && data.get(..SQLITE_MAGIC.len()) != Some(SQLITE_MAGIC.as_slice()) {
                return Err(StoreError::Corrupt(0));
            }
        }
        Ok(())
    }

    pub(crate) fn copy_logical_to(&mut self, destination: &File) -> Result<(), StoreError> {
        let chunk_size = usize::try_from(MAX_EXTENT_BYTES).map_err(|_| StoreError::Range)?;
        let mut buffer = allocate_zeroed(chunk_size)?;
        let mut offset = 0_u64;
        while offset < self.logical_size {
            let amount = usize::try_from((self.logical_size - offset).min(chunk_size as u64))
                .map_err(|_| StoreError::Range)?;
            let read = self.read_at(offset, &mut buffer[..amount])?;
            if read != amount {
                return Err(StoreError::Corrupt(offset));
            }
            write_all_at(destination, offset, &buffer[..amount])?;
            offset = offset.checked_add(amount as u64).ok_or(StoreError::Range)?;
        }
        destination.set_len(self.logical_size)?;
        Ok(())
    }

    pub(crate) fn inspect(&self) -> Result<Inspect, StoreError> {
        let metadata = self.sidecar.metadata()?;
        let mut seen = HashSet::new();
        let live_stored_bytes = self
            .runs
            .values()
            .filter(|run| seen.insert(run.extent.record_offset))
            .map(|run| u64::from(run.extent.header.stored_len) + EXTENT_HEADER_SIZE as u64)
            .sum::<u64>();
        let indexed_pages = self.runs.values().try_fold(0_usize, |total, run| {
            total
                .checked_add(run.page_count as usize)
                .ok_or(StoreError::Range)
        })?;
        Ok(Inspect {
            path: self.path.clone(),
            sidecar_path: self.sidecar_path.clone(),
            database_id: self.database_id,
            page_size: self.page_size,
            logical_size: self.logical_size,
            page_count: logical_page_count(self.logical_size, self.page_size)?,
            generation: self.generation,
            index_generation: self.indexed_generation,
            indexed_pages,
            live_extents: self.live_counts.len(),
            index_runs: self.runs.len(),
            base_bytes: self.base.metadata()?.len(),
            sidecar_bytes: metadata.len(),
            sidecar_allocated_bytes: allocated_bytes(&metadata),
            live_stored_bytes,
            committed_end: self.committed_end,
            hole_punching: self.hole_punching,
        })
    }

    pub(crate) fn compact(&mut self) -> Result<(), StoreError> {
        if self.has_pending() {
            return Err(StoreError::Busy);
        }
        reject_auxiliary_files(&self.path)?;
        self.acquire_maintenance()?;
        let result = self.compact_locked();
        self.release_maintenance();
        result
    }

    pub(crate) fn acquire_maintenance(&mut self) -> Result<(), StoreError> {
        if self.has_pending() || self.publication_locked {
            return Err(StoreError::Busy);
        }
        if let Err(error) = lock_exclusive(&self.lifecycle, true) {
            let _ = lock_shared(&self.lifecycle, false);
            return Err(error);
        }
        if let Err(error) = lock_exclusive(&self.publication, true) {
            let _ = lock_shared(&self.lifecycle, false);
            return Err(error);
        }
        self.publication_locked = true;
        if let Err(error) = self.refresh() {
            self.release_maintenance();
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn release_maintenance(&mut self) {
        self.release_publication();
        let _ = lock_shared(&self.lifecycle, false);
    }

    fn compact_locked(&mut self) -> Result<(), StoreError> {
        self.refresh()?;
        let (temporary_path, temporary) = create_temporary(&self.sidecar_path)?;
        let mut cleanup = CleanupPath(Some(temporary_path.clone()));
        initialize_sidecar_file(&temporary, self.database_id)?;
        let pages = logical_page_count(self.logical_size, self.page_size)?;
        let mut cursor = HEADER_SIZE as u64;
        let mut extents = Vec::new();
        let mut hasher = blake3::Hasher::new();
        self.config.validate_page_size(self.page_size)?;
        let max = max_pages_per_extent(self.page_size, self.config.extent_bytes)?;
        let mut first = 1;
        while first <= pages {
            let count = max.min(pages - first + 1);
            let mut raw = Vec::new();
            raw.try_reserve_exact(count as usize * self.page_size as usize)
                .map_err(|_| StoreError::Range)?;
            for page in first..first + count {
                raw.extend_from_slice(&self.read_page(page)?);
            }
            let (extent, next, encoded) = write_extent(
                &temporary,
                cursor,
                1,
                first,
                count,
                &raw,
                self.page_size,
                self.config.seek_chunk_bytes,
            )?;
            hasher.update(&encoded);
            cursor = next;
            extents.push(extent);
            first += count;
        }
        let commit_offset = cursor;
        let commit = Commit {
            generation: 1,
            previous_commit: 0,
            generation_start: HEADER_SIZE as u64,
            logical_size: self.logical_size,
            page_size: self.page_size,
            extent_count: u32::try_from(extents.len()).map_err(|_| StoreError::Range)?,
            generation_digest: *hasher.finalize().as_bytes(),
        };
        write_all_at(&temporary, commit_offset, &commit.encode())?;
        let end = commit_offset + COMMIT_SIZE as u64;
        temporary.set_len(end)?;
        temporary.sync_all()?;
        let superblock = Superblock {
            sequence: 1,
            generation: 1,
            durable: true,
            logical_size: self.logical_size,
            page_size: self.page_size,
            page_count: pages,
            commit_offset,
            commit_end: end,
            index_offset: 0,
            index_end: 0,
            database_id: self.database_id,
        };
        write_superblock(&temporary, 0, superblock)?;
        temporary.sync_all()?;
        preserve_metadata(&self.sidecar, &temporary)?;
        std::fs::rename(&temporary_path, &self.sidecar_path)?;
        sync_parent_dir(&self.sidecar_path)?;
        cleanup.0 = None;
        self.sidecar = OpenOptions::new()
            .read(true)
            .write(self.writable)
            .open(&self.sidecar_path)?;
        self.reload()
    }

    fn reload(&mut self) -> Result<(), StoreError> {
        self.obsolete_ranges.clear();
        let header = read_header(&self.sidecar)?;
        if header.database_id != self.database_id {
            return Err(StoreError::IdentityMismatch);
        }
        let slots = read_superblocks(&self.sidecar, self.database_id)?;
        let file_len = self.sidecar.metadata()?.len();
        let mut candidates: Vec<(usize, Superblock)> = slots
            .iter()
            .enumerate()
            .filter_map(|(slot, value)| value.map(|value| (slot, value)))
            .collect();
        candidates.sort_unstable_by_key(|(_, value)| std::cmp::Reverse(value.sequence));
        let mut selected = None;
        for (slot, candidate) in candidates {
            match validate_latest_commit(&self.sidecar, candidate) {
                Ok(()) => {
                    selected = Some((slot, candidate));
                    break;
                }
                Err(StoreError::Corrupt(_)) if !candidate.durable => {}
                Err(error) => return Err(error),
            }
        }
        let Some((slot, superblock)) = selected else {
            if slots.iter().any(Option::is_some) {
                return Err(StoreError::Corrupt(file_len));
            }
            if self.sidecar.metadata()?.len() != HEADER_SIZE as u64 {
                return Err(StoreError::Corrupt(HEADER_SIZE as u64));
            }
            self.page_size = 0;
            self.logical_size = 0;
            self.committed_size = 0;
            self.generation = 0;
            self.commit_offset = 0;
            self.committed_end = HEADER_SIZE as u64;
            self.runs.clear();
            self.live_counts.clear();
            self.pending = None;
            self.bootstrap = None;
            self.truncate_floor = None;
            self.extent_cache.clear();
            self.superblocks = slots;
            self.active_slot = 0;
            return Ok(());
        };
        let (loaded, loaded_index) = if superblock.index_offset != 0 {
            match load_index(&self.sidecar, superblock) {
                Ok(index) => (index, true),
                Err(StoreError::Io(error)) => return Err(StoreError::Io(error)),
                Err(_) => (load_chain(&self.sidecar, superblock)?, false),
            }
        } else {
            (load_chain(&self.sidecar, superblock)?, false)
        };
        self.page_size = superblock.page_size;
        self.logical_size = superblock.logical_size;
        self.committed_size = superblock.logical_size;
        self.generation = superblock.generation;
        self.commit_offset = superblock.commit_offset;
        self.committed_end = superblock.commit_end;
        self.runs = loaded;
        self.superblocks = slots;
        self.active_slot = slot;
        self.indexed_generation = if loaded_index {
            superblock.generation
        } else {
            0
        };
        self.rebuild_live_counts();
        self.pending = None;
        self.bootstrap = None;
        self.truncate_floor = None;
        self.extent_cache.clear();
        Ok(())
    }

    fn read_page(&mut self, page: u32) -> Result<Vec<u8>, StoreError> {
        if let Some(pending) = &mut self.pending
            && let Some(value) = pending.read_page(page)?
        {
            return Ok(value);
        }
        if self.page_size == 0 {
            return Err(StoreError::UnknownPageSize);
        }
        let page_start = u64::from(page.saturating_sub(1)) * u64::from(self.page_size);
        if self.truncate_floor.is_some_and(|floor| page_start >= floor) {
            return Ok(vec![0; self.page_size as usize]);
        }
        let run = self
            .runs
            .range(..=page)
            .next_back()
            .map(|(_, run)| run.clone());
        let Some(run) = run.filter(|run| run.contains(page)) else {
            return Ok(vec![0; self.page_size as usize]);
        };
        let within = run
            .extent_page
            .checked_add(page - run.first_page)
            .ok_or(StoreError::Range)?;
        read_extent_page(
            &self.sidecar,
            &mut self.extent_cache,
            &run.extent,
            within,
            self.page_size,
        )
    }

    fn install_generation(
        &mut self,
        extents: Vec<Arc<ExtentLocation>>,
        max_page: u32,
    ) -> Result<(), StoreError> {
        let old: HashMap<u64, Arc<ExtentLocation>> = self
            .runs
            .values()
            .map(|run| (run.extent.record_offset, Arc::clone(&run.extent)))
            .collect();
        truncate_runs(&mut self.runs, max_page)?;
        for extent in extents {
            replace_run(
                &mut self.runs,
                PageRun {
                    first_page: extent.header.first_page,
                    page_count: extent.header.page_count,
                    extent: Arc::clone(&extent),
                    extent_page: 0,
                },
            )?;
        }
        self.rebuild_live_counts();
        for (offset, extent) in old {
            if !self.live_counts.contains_key(&offset) {
                self.obsolete_ranges.push((
                    extent.payload_offset,
                    u64::from(extent.header.stored_len),
                    self.generation,
                ));
                self.extent_cache.remove_record(offset);
            }
        }
        Ok(())
    }

    fn rebuild_live_counts(&mut self) {
        self.live_counts.clear();
        for run in self.runs.values() {
            *self
                .live_counts
                .entry(run.extent.record_offset)
                .or_insert(0) += run.page_count;
        }
    }

    fn punch_obsolete(&mut self) {
        if !self.hole_punching {
            self.obsolete_ranges.clear();
            return;
        }
        let safe_generation = self
            .superblocks
            .iter()
            .flatten()
            .map(|slot| slot.generation)
            .min()
            .unwrap_or(0);
        let (safe, deferred): (Vec<_>, Vec<_>) = std::mem::take(&mut self.obsolete_ranges)
            .into_iter()
            .partition(|(_, _, retired_generation)| *retired_generation <= safe_generation);
        self.obsolete_ranges = deferred;
        for (offset, length, _) in safe {
            let Some((offset, length)) =
                aligned_interior(offset, length, self.allocation_granularity)
            else {
                continue;
            };
            if punch_hole(&self.sidecar, offset, length).is_err() {
                self.hole_punching = false;
                break;
            }
        }
    }

    fn release_publication(&mut self) {
        if self.publication_locked {
            let _ = unlock_file(&self.publication);
            self.publication_locked = false;
        }
    }

    fn with_publication_snapshot<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let lock_here = !self.publication_locked;
        if lock_here {
            lock_shared(&self.publication, false)?;
        }
        let result = operation(self);
        if !lock_here {
            return result;
        }
        let unlock_result = unlock_file(&self.publication);
        match (result, unlock_result) {
            (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    fn mark_current_durable(&mut self) -> Result<(), StoreError> {
        self.sidecar.sync_all()?;
        let Some(current) = self.superblocks[self.active_slot] else {
            return Ok(());
        };
        if current.durable {
            return Ok(());
        }
        let target = 1 - self.active_slot;
        let sequence = self
            .superblocks
            .iter()
            .flatten()
            .map(|value| value.sequence)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StoreError::Range)?;
        let next = Superblock {
            sequence,
            durable: true,
            ..current
        };
        write_superblock(&self.sidecar, target, next)?;
        self.sidecar.sync_all()?;
        self.superblocks[target] = Some(next);
        self.active_slot = target;
        self.punch_obsolete();
        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.release_publication();
        let _ = unlock_file(&self.lifecycle);
    }
}

fn load_chain(file: &File, superblock: Superblock) -> Result<BTreeMap<u32, PageRun>, StoreError> {
    let mut commits = Vec::new();
    let mut offset = superblock.commit_offset;
    let mut expected_generation = superblock.generation;
    while offset != 0 {
        if commits.len() >= MAX_GENERATIONS {
            return Err(StoreError::Range);
        }
        let (commit, extents) = read_generation(file, offset)?;
        if commit.generation != expected_generation || commit.page_size != superblock.page_size {
            return Err(StoreError::Corrupt(offset));
        }
        commits.push((commit, extents));
        offset = commit.previous_commit;
        expected_generation = expected_generation
            .checked_sub(1)
            .ok_or(StoreError::Corrupt(offset))?;
    }
    if expected_generation != 0 {
        return Err(StoreError::Corrupt(superblock.commit_offset));
    }
    commits.reverse();
    let mut runs = BTreeMap::new();
    for (commit, extents) in commits {
        let pages = logical_page_count(commit.logical_size, commit.page_size)?;
        truncate_runs(&mut runs, pages)?;
        for extent in extents {
            replace_run(
                &mut runs,
                PageRun {
                    first_page: extent.header.first_page,
                    page_count: extent.header.page_count,
                    extent,
                    extent_page: 0,
                },
            )?;
        }
    }
    Ok(runs)
}

fn validate_latest_commit(file: &File, superblock: Superblock) -> Result<(), StoreError> {
    if superblock.commit_end > file.metadata()?.len() {
        return Err(StoreError::Corrupt(superblock.commit_offset));
    }
    let (commit, _) = read_generation(file, superblock.commit_offset)?;
    if commit.generation != superblock.generation
        || commit.logical_size != superblock.logical_size
        || commit.page_size != superblock.page_size
    {
        return Err(StoreError::Corrupt(superblock.commit_offset));
    }
    Ok(())
}

fn read_generation(
    file: &File,
    commit_offset: u64,
) -> Result<(Commit, Vec<Arc<ExtentLocation>>), StoreError> {
    let file_len = file.metadata()?.len();
    let commit_end = commit_offset
        .checked_add(COMMIT_SIZE as u64)
        .ok_or(StoreError::Range)?;
    if commit_offset < HEADER_SIZE as u64 || commit_end > file_len {
        return Err(StoreError::Corrupt(commit_offset));
    }
    let mut encoded_commit = [0; COMMIT_SIZE];
    read_exact_at(file, commit_offset, &mut encoded_commit)?;
    let commit = Commit::decode(&encoded_commit).map_err(|_| StoreError::Corrupt(commit_offset))?;
    let minimum_generation_bytes = u64::from(commit.extent_count)
        .checked_mul(EXTENT_HEADER_SIZE as u64 + 1)
        .and_then(|bytes| commit.generation_start.checked_add(bytes))
        .ok_or(StoreError::Range)?;
    if minimum_generation_bytes > commit_offset || commit.generation_start > commit_offset {
        return Err(StoreError::Corrupt(commit_offset));
    }
    let mut cursor = commit.generation_start;
    let mut extents = Vec::new();
    extents
        .try_reserve_exact(commit.extent_count as usize)
        .map_err(|_| StoreError::Range)?;
    let mut hasher = blake3::Hasher::new();
    let mut covered = HashSet::new();
    for _ in 0..commit.extent_count {
        if cursor
            .checked_add(EXTENT_HEADER_SIZE as u64)
            .is_none_or(|end| end > commit_offset)
        {
            return Err(StoreError::Corrupt(cursor));
        }
        let mut encoded = [0; EXTENT_HEADER_SIZE];
        read_exact_at(file, cursor, &mut encoded)?;
        let header = ExtentHeader::decode(&encoded).map_err(|_| StoreError::Corrupt(cursor))?;
        if header.generation != commit.generation
            || header.raw_len
                != header
                    .page_count
                    .checked_mul(commit.page_size)
                    .ok_or(StoreError::Range)?
        {
            return Err(StoreError::Corrupt(cursor));
        }
        let end_page = header
            .first_page
            .checked_add(header.page_count)
            .ok_or(StoreError::Range)?;
        let max_page = logical_page_count(commit.logical_size, commit.page_size)?;
        if end_page - 1 > max_page
            || !(header.first_page..end_page).all(|page| covered.insert(page))
        {
            return Err(StoreError::Corrupt(cursor));
        }
        hasher.update(&encoded);
        let payload_offset = cursor
            .checked_add(EXTENT_HEADER_SIZE as u64)
            .ok_or(StoreError::Range)?;
        let next = cursor
            .checked_add(u64::from(header.allocation_len))
            .ok_or(StoreError::Range)?;
        if next > commit_offset
            || payload_offset
                .checked_add(u64::from(header.stored_len))
                .is_none_or(|payload_end| payload_end > next)
        {
            return Err(StoreError::Corrupt(cursor));
        }
        extents.push(Arc::new(ExtentLocation {
            header,
            record_offset: cursor,
            payload_offset,
            seekable: load_seekable_layout(file, cursor, payload_offset, header, commit.page_size)?,
        }));
        cursor = next;
    }
    if cursor != commit_offset || *hasher.finalize().as_bytes() != commit.generation_digest {
        return Err(StoreError::Corrupt(commit_offset));
    }
    Ok((commit, extents))
}

fn encode_index(runs: &BTreeMap<u32, PageRun>) -> Result<Vec<u8>, StoreError> {
    let size = runs
        .len()
        .checked_mul(INDEX_ENTRY_SIZE)
        .ok_or(StoreError::Range)?;
    if size as u64 > MAX_INDEX_BYTES {
        return Err(StoreError::Range);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(size)
        .map_err(|_| StoreError::Range)?;
    for run in runs.values() {
        output.extend_from_slice(&run.first_page.to_le_bytes());
        output.extend_from_slice(&run.page_count.to_le_bytes());
        output.extend_from_slice(&run.extent.record_offset.to_le_bytes());
        output.extend_from_slice(&run.extent_page.to_le_bytes());
        output.extend_from_slice(&0_u32.to_le_bytes());
    }
    Ok(output)
}

fn load_index(file: &File, superblock: Superblock) -> Result<BTreeMap<u32, PageRun>, StoreError> {
    let header_end = superblock
        .index_offset
        .checked_add(INDEX_HEADER_SIZE as u64)
        .ok_or(StoreError::Range)?;
    if superblock.index_end > file.metadata()?.len() || header_end > superblock.index_end {
        return Err(StoreError::Corrupt(superblock.index_offset));
    }
    let mut encoded = [0; INDEX_HEADER_SIZE];
    read_exact_at(file, superblock.index_offset, &mut encoded)?;
    let header =
        IndexHeader::decode(&encoded).map_err(|_| StoreError::Corrupt(superblock.index_offset))?;
    if header.generation != superblock.generation
        || header.raw_len != u64::from(header.entry_count) * INDEX_ENTRY_SIZE as u64
        || header_end.checked_add(header.stored_len) != Some(superblock.index_end)
    {
        return Err(StoreError::Corrupt(superblock.index_offset));
    }
    let mut payload =
        allocate_zeroed(usize::try_from(header.stored_len).map_err(|_| StoreError::Range)?)?;
    read_exact_at(file, header_end, &mut payload)?;
    let raw_len = usize::try_from(header.raw_len).map_err(|_| StoreError::Range)?;
    let raw = match header.codec {
        Codec::Raw => payload,
        Codec::Zstd => zstd::bulk::decompress(&payload, raw_len)
            .map_err(|error| StoreError::Zstd(error.to_string()))?,
        Codec::ZstdSeekable => return Err(StoreError::Corrupt(superblock.index_offset)),
    };
    if raw.len() != raw_len || digest(&raw) != header.raw_digest {
        return Err(StoreError::Corrupt(superblock.index_offset));
    }
    let mut extent_cache = HashMap::new();
    let mut runs = BTreeMap::new();
    let mut previous_end = 1_u32;
    let (entries, remainder) = raw.as_chunks::<INDEX_ENTRY_SIZE>();
    debug_assert!(remainder.is_empty());
    for entry in entries {
        let first_page = u32::from_le_bytes(entry[0..4].try_into().expect("u32"));
        let page_count = u32::from_le_bytes(entry[4..8].try_into().expect("u32"));
        let extent_offset = u64::from_le_bytes(entry[8..16].try_into().expect("u64"));
        let extent_page = u32::from_le_bytes(entry[16..20].try_into().expect("u32"));
        let run_end = first_page
            .checked_add(page_count)
            .ok_or(StoreError::Range)?;
        if entry[20..24].iter().any(|byte| *byte != 0)
            || first_page == 0
            || page_count == 0
            || first_page < previous_end
            || run_end
                > superblock
                    .page_count
                    .checked_add(1)
                    .ok_or(StoreError::Range)?
        {
            return Err(StoreError::Corrupt(superblock.index_offset));
        }
        let extent = if let Some(extent) = extent_cache.get(&extent_offset) {
            Arc::clone(extent)
        } else {
            let extent = Arc::new(read_extent_location(
                file,
                extent_offset,
                superblock.page_size,
            )?);
            extent_cache.insert(extent_offset, Arc::clone(&extent));
            extent
        };
        if extent_page
            .checked_add(page_count)
            .is_none_or(|end| end > extent.header.page_count)
            || extent
                .header
                .first_page
                .checked_add(extent_page)
                .is_none_or(|mapped_page| mapped_page != first_page)
            || extent.header.generation > superblock.generation
            || extent.header.raw_len
                != extent
                    .header
                    .page_count
                    .checked_mul(superblock.page_size)
                    .ok_or(StoreError::Range)?
        {
            return Err(StoreError::Corrupt(superblock.index_offset));
        }
        replace_run(
            &mut runs,
            PageRun {
                first_page,
                page_count,
                extent,
                extent_page,
            },
        )?;
        previous_end = run_end;
    }
    Ok(runs)
}

fn read_extent_location(
    file: &File,
    offset: u64,
    page_size: u32,
) -> Result<ExtentLocation, StoreError> {
    let mut encoded = [0; EXTENT_HEADER_SIZE];
    read_exact_at(file, offset, &mut encoded)?;
    let header = ExtentHeader::decode(&encoded).map_err(|_| StoreError::Corrupt(offset))?;
    let payload_offset = offset
        .checked_add(EXTENT_HEADER_SIZE as u64)
        .ok_or(StoreError::Range)?;
    let file_len = file.metadata()?.len();
    if offset
        .checked_add(u64::from(header.allocation_len))
        .is_none_or(|end| end > file_len)
    {
        return Err(StoreError::Corrupt(offset));
    }
    Ok(ExtentLocation {
        header,
        record_offset: offset,
        payload_offset,
        seekable: load_seekable_layout(file, offset, payload_offset, header, page_size)?,
    })
}

fn load_seekable_layout(
    file: &File,
    record_offset: u64,
    payload_offset: u64,
    header: ExtentHeader,
    page_size: u32,
) -> Result<Option<seekable::Layout>, StoreError> {
    if header.codec != Codec::ZstdSeekable {
        return Ok(None);
    }
    let footer_offset = payload_offset
        .checked_add(u64::from(header.stored_len))
        .and_then(|end| end.checked_sub(seekable::SEEK_FOOTER_SIZE as u64))
        .ok_or(StoreError::Corrupt(record_offset))?;
    let mut footer = [0_u8; seekable::SEEK_FOOTER_SIZE];
    read_exact_at(file, footer_offset, &mut footer)?;
    let tail_size =
        seekable::tail_size_from_footer(&footer).map_err(|_| StoreError::Corrupt(record_offset))?;
    if tail_size > header.stored_len as usize {
        return Err(StoreError::Corrupt(record_offset));
    }
    let mut tail = allocate_zeroed(tail_size)?;
    let tail_offset = payload_offset
        .checked_add(u64::from(header.stored_len) - tail_size as u64)
        .ok_or(StoreError::Range)?;
    read_exact_at(file, tail_offset, &mut tail)?;
    seekable::decode_layout(
        &tail,
        header.stored_len,
        header.raw_len,
        page_size,
        header.raw_digest,
    )
    .map(Some)
    .map_err(|_| StoreError::Corrupt(record_offset))
}

fn read_extent_page(
    file: &File,
    cache: &mut ExtentCache,
    location: &ExtentLocation,
    extent_page: u32,
    page_size: u32,
) -> Result<Vec<u8>, StoreError> {
    let raw_offset = extent_page
        .checked_mul(page_size)
        .ok_or(StoreError::Range)?;
    let raw_end = raw_offset.checked_add(page_size).ok_or(StoreError::Range)?;
    if raw_end > location.header.raw_len {
        return Err(StoreError::Corrupt(location.record_offset));
    }
    if location.header.codec != Codec::ZstdSeekable {
        let extent = read_whole_extent(file, cache, location, page_size)?;
        return extent
            .get(raw_offset as usize..raw_end as usize)
            .map(<[u8]>::to_vec)
            .ok_or(StoreError::Corrupt(location.record_offset));
    }

    let layout = location
        .seekable
        .as_ref()
        .ok_or(StoreError::Corrupt(location.record_offset))?;
    let frame_index = layout
        .frames
        .partition_point(|frame| frame.raw_offset <= raw_offset)
        .checked_sub(1)
        .ok_or(StoreError::Corrupt(location.record_offset))?;
    let frame = layout
        .frames
        .get(frame_index)
        .ok_or(StoreError::Corrupt(location.record_offset))?;
    let frame_end = frame
        .raw_offset
        .checked_add(frame.raw_size)
        .ok_or(StoreError::Range)?;
    if raw_offset < frame.raw_offset || raw_end > frame_end {
        return Err(StoreError::Corrupt(location.record_offset));
    }
    let raw = read_seekable_frame(file, cache, location, frame_index, frame, page_size)?;
    let start = usize::try_from(raw_offset - frame.raw_offset).map_err(|_| StoreError::Range)?;
    let end = start
        .checked_add(page_size as usize)
        .ok_or(StoreError::Range)?;
    raw.get(start..end)
        .map(<[u8]>::to_vec)
        .ok_or(StoreError::Corrupt(location.record_offset))
}

fn read_whole_extent(
    file: &File,
    cache: &mut ExtentCache,
    location: &ExtentLocation,
    page_size: u32,
) -> Result<Arc<Vec<u8>>, StoreError> {
    let cache_key = CacheKey {
        record_offset: location.record_offset,
        frame: WHOLE_EXTENT_CACHE_FRAME,
    };
    if let Some(value) = cache.get(cache_key) {
        return Ok(value);
    }

    let raw = match location.header.codec {
        Codec::Raw | Codec::Zstd => {
            let mut payload = allocate_zeroed(location.header.stored_len as usize)?;
            read_exact_at(file, location.payload_offset, &mut payload)?;
            match location.header.codec {
                Codec::Raw => payload,
                Codec::Zstd => zstd::bulk::decompress(&payload, location.header.raw_len as usize)
                    .map_err(|error| StoreError::Zstd(error.to_string()))?,
                Codec::ZstdSeekable => unreachable!("matched legacy codec"),
            }
        }
        Codec::ZstdSeekable => {
            let layout = location
                .seekable
                .as_ref()
                .ok_or(StoreError::Corrupt(location.record_offset))?;
            let mut output = allocate_zeroed(location.header.raw_len as usize)?;
            for (frame_index, frame) in layout.frames.iter().enumerate() {
                let frame_raw =
                    read_seekable_frame(file, cache, location, frame_index, frame, page_size)?;
                let start = frame.raw_offset as usize;
                let end = start
                    .checked_add(frame.raw_size as usize)
                    .ok_or(StoreError::Range)?;
                output
                    .get_mut(start..end)
                    .ok_or(StoreError::Corrupt(location.record_offset))?
                    .copy_from_slice(&frame_raw);
            }
            output
        }
    };
    if raw.len() != location.header.raw_len as usize
        || (location.header.codec != Codec::ZstdSeekable
            && digest(&raw) != location.header.raw_digest)
    {
        return Err(StoreError::PageChecksum(location.header.first_page));
    }
    let value = Arc::new(raw);
    cache.insert(cache_key, Arc::clone(&value));
    Ok(value)
}

fn read_seekable_frame(
    file: &File,
    cache: &mut ExtentCache,
    location: &ExtentLocation,
    frame_index: usize,
    frame: &seekable::Frame,
    page_size: u32,
) -> Result<Arc<Vec<u8>>, StoreError> {
    let frame_number = u32::try_from(frame_index).map_err(|_| StoreError::Range)?;
    let cache_key = CacheKey {
        record_offset: location.record_offset,
        frame: frame_number,
    };
    if let Some(value) = cache.get(cache_key) {
        return Ok(value);
    }
    let mut compressed = allocate_zeroed(frame.compressed_size as usize)?;
    let compressed_offset = location
        .payload_offset
        .checked_add(u64::from(frame.compressed_offset))
        .ok_or(StoreError::Range)?;
    read_exact_at(file, compressed_offset, &mut compressed)?;
    let raw = zstd::bulk::decompress(&compressed, frame.raw_size as usize).map_err(|_| {
        StoreError::PageChecksum(location.header.first_page + frame.raw_offset / page_size)
    })?;
    if raw.len() != frame.raw_size as usize || digest(&raw) != frame.raw_digest {
        return Err(StoreError::PageChecksum(
            location.header.first_page + frame.raw_offset / page_size,
        ));
    }
    let value = Arc::new(raw);
    cache.insert(cache_key, Arc::clone(&value));
    Ok(value)
}

fn replace_run(runs: &mut BTreeMap<u32, PageRun>, replacement: PageRun) -> Result<(), StoreError> {
    let start = replacement.first_page;
    let end = replacement.end()?;
    let keys: Vec<u32> = runs
        .range(..end)
        .filter_map(|(&key, run)| (run.end().ok()? > start).then_some(key))
        .collect();
    let mut fragments = Vec::new();
    for key in keys {
        let old = runs.remove(&key).expect("selected run exists");
        let old_end = old.end()?;
        if old.first_page < start {
            fragments.push(PageRun {
                page_count: start - old.first_page,
                ..old.clone()
            });
        }
        if old_end > end {
            fragments.push(PageRun {
                first_page: end,
                page_count: old_end - end,
                extent_page: old.extent_page + (end - old.first_page),
                ..old
            });
        }
    }
    for fragment in fragments {
        runs.insert(fragment.first_page, fragment);
    }
    runs.insert(start, replacement);
    Ok(())
}

fn truncate_runs(runs: &mut BTreeMap<u32, PageRun>, max_page: u32) -> Result<(), StoreError> {
    let keys: Vec<u32> = runs
        .range((max_page.saturating_add(1))..)
        .map(|(&key, _)| key)
        .collect();
    for key in keys {
        runs.remove(&key);
    }
    if let Some((&key, run)) = runs.range(..=max_page).next_back() {
        let end = run.end()?;
        if end > max_page.saturating_add(1) {
            let mut shortened = run.clone();
            shortened.page_count = max_page.saturating_add(1) - shortened.first_page;
            if shortened.page_count == 0 {
                runs.remove(&key);
            } else {
                runs.insert(key, shortened);
            }
        }
    }
    Ok(())
}

fn max_pages_per_extent(page_size: u32, extent_bytes: u32) -> Result<u32, StoreError> {
    valid_page_size(page_size)
        .then_some((extent_bytes / page_size).max(1))
        .ok_or(StoreError::InvalidPageSize(page_size))
}

fn pending_chunk_first(page: u32, page_size: u32, extent_bytes: u32) -> Result<u32, StoreError> {
    if page == 0 {
        return Err(StoreError::Range);
    }
    let pages = max_pages_per_extent(page_size, extent_bytes)?;
    ((page - 1) / pages)
        .checked_mul(pages)
        .and_then(|value| value.checked_add(1))
        .ok_or(StoreError::Range)
}

fn append_pending_extents(
    pending: &mut PendingLayer,
    destination: &File,
    mut cursor: u64,
    max_page: u32,
    page_size: u32,
) -> Result<AppendedExtents, StoreError> {
    if pending.page_size != page_size {
        return Err(StoreError::InvalidPageSize(pending.page_size));
    }
    pending.truncate(max_page)?;
    pending.flush_all()?;
    let runs: Vec<PageRun> = pending.runs.values().cloned().collect();
    let mut extents = Vec::new();
    extents
        .try_reserve_exact(runs.len())
        .map_err(|_| StoreError::Range)?;
    let mut generation_hasher = blake3::Hasher::new();
    let mut copy_buffer = allocate_zeroed(COPY_BUFFER_SIZE)?;

    for run in runs {
        let covers_entire_extent = run.extent_page == 0
            && run.first_page == run.extent.header.first_page
            && run.page_count == run.extent.header.page_count;
        if covers_entire_extent {
            let allocation_len = u64::from(run.extent.header.allocation_len);
            copy_exact_between(
                &pending.file,
                run.extent.record_offset,
                destination,
                cursor,
                allocation_len,
                &mut copy_buffer,
            )?;
            let encoded = run.extent.header.encode();
            generation_hasher.update(&encoded);
            let payload_offset = cursor
                .checked_add(EXTENT_HEADER_SIZE as u64)
                .ok_or(StoreError::Range)?;
            extents.push(Arc::new(ExtentLocation {
                header: run.extent.header,
                record_offset: cursor,
                payload_offset,
                seekable: run.extent.seekable.clone(),
            }));
            cursor = cursor
                .checked_add(allocation_len)
                .ok_or(StoreError::Range)?;
            continue;
        }

        // A later write or truncate can leave only a fragment of a staged
        // extent live. Re-encode that bounded fragment so the published
        // generation remains non-overlapping and independently verifiable.
        let raw = pending.read_extent(&run.extent)?;
        let start = usize::try_from(run.extent_page)
            .map_err(|_| StoreError::Range)?
            .checked_mul(page_size as usize)
            .ok_or(StoreError::Range)?;
        let length = usize::try_from(run.page_count)
            .map_err(|_| StoreError::Range)?
            .checked_mul(page_size as usize)
            .ok_or(StoreError::Range)?;
        let end = start.checked_add(length).ok_or(StoreError::Range)?;
        let fragment = raw
            .get(start..end)
            .ok_or(StoreError::Corrupt(run.extent.record_offset))?;
        let (extent, next, encoded) = write_extent(
            destination,
            cursor,
            pending.generation,
            run.first_page,
            run.page_count,
            fragment,
            page_size,
            pending.config.seek_chunk_bytes,
        )?;
        generation_hasher.update(&encoded);
        extents.push(extent);
        cursor = next;
    }
    Ok((extents, cursor, *generation_hasher.finalize().as_bytes()))
}

fn copy_exact_between(
    source: &File,
    mut source_offset: u64,
    destination: &File,
    mut destination_offset: u64,
    mut length: u64,
    buffer: &mut [u8],
) -> Result<(), StoreError> {
    if buffer.is_empty() {
        return Err(StoreError::Range);
    }
    while length != 0 {
        let amount =
            usize::try_from(length.min(buffer.len() as u64)).map_err(|_| StoreError::Range)?;
        read_exact_at(source, source_offset, &mut buffer[..amount])?;
        write_all_at(destination, destination_offset, &buffer[..amount])?;
        source_offset = source_offset
            .checked_add(amount as u64)
            .ok_or(StoreError::Range)?;
        destination_offset = destination_offset
            .checked_add(amount as u64)
            .ok_or(StoreError::Range)?;
        length -= amount as u64;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_extent(
    file: &File,
    offset: u64,
    generation: u64,
    first_page: u32,
    page_count: u32,
    raw: &[u8],
    page_size: u32,
    seek_chunk_bytes: u32,
) -> Result<(Arc<ExtentLocation>, u64, [u8; EXTENT_HEADER_SIZE]), StoreError> {
    if raw.len()
        != usize::try_from(page_count)
            .map_err(|_| StoreError::Range)?
            .checked_mul(page_size as usize)
            .ok_or(StoreError::Range)?
        || !seek_chunk_bytes.is_multiple_of(page_size)
    {
        return Err(StoreError::InvalidConfiguration(
            "extent writes must contain whole seek chunks and SQLite pages",
        ));
    }
    let seekable = seekable::encode(raw, seek_chunk_bytes as usize, ZSTD_LEVEL).map_err(
        |error| match error {
            seekable::SeekableError::Zstd(message) => StoreError::Zstd(message),
            seekable::SeekableError::Range => StoreError::Range,
            seekable::SeekableError::Invalid => StoreError::Corrupt(offset),
        },
    )?;
    let allocation_len = (EXTENT_HEADER_SIZE as u64)
        .checked_add(seekable.payload.len() as u64)
        .ok_or(StoreError::Range)?;
    let header = ExtentHeader {
        generation,
        first_page,
        page_count,
        codec: Codec::ZstdSeekable,
        raw_len: u32::try_from(raw.len()).map_err(|_| StoreError::Range)?,
        stored_len: u32::try_from(seekable.payload.len()).map_err(|_| StoreError::Range)?,
        allocation_len: u32::try_from(allocation_len).map_err(|_| StoreError::Range)?,
        raw_digest: seekable.metadata_digest,
    };
    let encoded = header.encode();
    write_all_at(file, offset, &encoded)?;
    let payload_offset = offset + EXTENT_HEADER_SIZE as u64;
    write_all_at(file, payload_offset, &seekable.payload)?;
    let next = offset + allocation_len;
    file.set_len(next)?;
    Ok((
        Arc::new(ExtentLocation {
            header,
            record_offset: offset,
            payload_offset,
            seekable: Some(seekable.layout),
        }),
        next,
        encoded,
    ))
}

fn encode_bytes(data: &[u8]) -> Result<(Codec, Vec<u8>), StoreError> {
    let compressed = zstd::bulk::compress(data, ZSTD_LEVEL)
        .map_err(|error| StoreError::Zstd(error.to_string()))?;
    if compressed.len().saturating_add(MIN_SAVINGS) <= data.len() {
        Ok((Codec::Zstd, compressed))
    } else {
        Ok((Codec::Raw, data.to_vec()))
    }
}

fn read_header(file: &File) -> Result<Header, StoreError> {
    if file.metadata()?.len() < HEADER_SIZE as u64 {
        return Err(StoreError::Corrupt(0));
    }
    let mut encoded = [0; SECTOR_SIZE];
    read_exact_at(file, 0, &mut encoded)?;
    Header::decode(&encoded).map_err(StoreError::Format)
}

fn read_superblocks(file: &File, id: DatabaseId) -> Result<[Option<Superblock>; 2], StoreError> {
    let mut output = [None, None];
    let mut nonzero = false;
    for (slot, offset) in [SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET]
        .into_iter()
        .enumerate()
    {
        let mut encoded = [0; SECTOR_SIZE];
        read_exact_at(file, offset, &mut encoded)?;
        nonzero |= encoded.iter().any(|byte| *byte != 0);
        if let Ok(value) = Superblock::decode(&encoded) {
            if value.is_some_and(|value| value.database_id != id) {
                return Err(StoreError::IdentityMismatch);
            }
            output[slot] = value;
        }
    }
    if nonzero && output.iter().all(Option::is_none) {
        return Err(StoreError::Corrupt(SUPERBLOCK_A_OFFSET));
    }
    Ok(output)
}

fn newest_superblock(slots: &[Option<Superblock>; 2]) -> Option<(usize, Superblock)> {
    slots
        .iter()
        .enumerate()
        .filter_map(|(slot, value)| value.map(|value| (slot, value)))
        .max_by_key(|(_, value)| value.sequence)
}

fn write_superblock(file: &File, slot: usize, value: Superblock) -> Result<(), StoreError> {
    let offset = match slot {
        0 => SUPERBLOCK_A_OFFSET,
        1 => SUPERBLOCK_B_OFFSET,
        _ => return Err(StoreError::Range),
    };
    write_all_at(file, offset, &value.encode())?;
    Ok(())
}

fn create_sidecar(path: &Path, id: DatabaseId) -> Result<(), StoreError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    initialize_sidecar_file(&file, id)?;
    sync_parent_dir(path)?;
    Ok(())
}

fn initialize_sidecar_file(file: &File, id: DatabaseId) -> Result<(), StoreError> {
    write_all_at(file, 0, &Header { database_id: id }.encode())?;
    file.set_len(HEADER_SIZE as u64)?;
    file.sync_all()?;
    Ok(())
}

fn random_database_id() -> Result<DatabaseId, StoreError> {
    let mut id = [0; 16];
    getrandom::fill(&mut id).map_err(|error| std::io::Error::other(error.to_string()))?;
    if id == [0; 16] {
        id[0] = 1;
    }
    Ok(id)
}

fn read_anchor(file: &File) -> Result<Option<Anchor>, StoreError> {
    if file.metadata()?.len() == 0 {
        return Ok(None);
    }
    let mut encoded = [0; SECTOR_SIZE];
    let amount = read_prefix_at(file, 0, &mut encoded)?;
    if amount < 8 || &encoded[..8] != crate::format::ANCHOR_MAGIC {
        return Ok(None);
    }
    if amount != SECTOR_SIZE {
        return Err(StoreError::Corrupt(0));
    }
    Anchor::decode(&encoded)
        .map(Some)
        .map_err(StoreError::Format)
}

fn write_anchor(file: &File, id: DatabaseId) -> Result<(), StoreError> {
    write_all_at(file, 0, &Anchor { database_id: id }.encode())?;
    file.set_len(SECTOR_SIZE as u64)?;
    file.sync_all()?;
    Ok(())
}

fn open_bootstrap_lock(path: &Path) -> Result<(File, bool), StoreError> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => {
            sync_parent_dir(path)?;
            Ok((file, true))
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            let file = OpenOptions::new().read(true).write(true).open(path)?;
            reject_aliased_file(&file)?;
            Ok((file, false))
        }
        Err(error) => Err(error.into()),
    }
}

fn initialize_or_validate_lock_file(
    file: &File,
    id: DatabaseId,
    allow_initialize: bool,
) -> Result<(), StoreError> {
    match file.metadata()?.len() {
        0 if allow_initialize => {
            write_all_at(file, 0, &id)?;
            file.sync_all()?;
            Ok(())
        }
        16 => {
            let mut found = [0; 16];
            read_exact_at(file, 0, &mut found)?;
            if found == id {
                Ok(())
            } else {
                Err(StoreError::IdentityMismatch)
            }
        }
        _ if allow_initialize => Err(StoreError::IdentityMismatch),
        _ => Err(StoreError::MissingSidecar),
    }
}

fn ensure_lock_file(path: &Path, id: DatabaseId) -> Result<File, StoreError> {
    let (file, created) = open_bootstrap_lock(path)?;
    initialize_or_validate_lock_file(&file, id, true)?;
    if created {
        sync_parent_dir(path)?;
    }
    Ok(file)
}

fn open_lock_file(path: &Path, id: DatabaseId, writable: bool) -> Result<File, StoreError> {
    let file = OpenOptions::new()
        .read(true)
        .write(writable)
        .open(path)
        .map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                StoreError::MissingSidecar
            } else {
                StoreError::Io(error)
            }
        })?;
    reject_aliased_file(&file)?;
    initialize_or_validate_lock_file(&file, id, false)?;
    Ok(file)
}

fn lifecycle_path(path: &Path) -> PathBuf {
    append_suffix(path, "-zsqlite-lock")
}
fn publication_path(path: &Path) -> PathBuf {
    append_suffix(path, "-zsqlite-publish")
}
fn deletion_path(path: &Path) -> PathBuf {
    append_suffix(path, "-zsqlite-delete")
}
fn sidecar_path(path: &Path) -> PathBuf {
    append_suffix(path, "-zsqlite")
}
fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn ensure_delete_marker(path: &Path, id: DatabaseId) -> Result<(), StoreError> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => {
            write_all_at(&file, 0, &id)?;
            file.sync_all()?;
            sync_parent_dir(path)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            let file = File::open(path)?;
            let mut found = [0; 16];
            read_exact_at(&file, 0, &mut found)?;
            if found == id {
                Ok(())
            } else {
                Err(StoreError::IdentityMismatch)
            }
        }
        Err(error) => Err(error.into()),
    }
}

fn remove_if_exists(path: &Path) -> Result<(), StoreError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn reject_auxiliary_files(path: &Path) -> Result<(), StoreError> {
    for suffix in ["-journal", "-wal", "-shm"] {
        match std::fs::metadata(append_suffix(path, suffix)) {
            Ok(metadata) if metadata.len() != 0 => return Err(StoreError::Busy),
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn parse_page_size(page: &[u8]) -> Option<u32> {
    let encoded = u16::from_be_bytes(page.get(16..18)?.try_into().ok()?);
    let size = if encoded == 1 {
        65_536
    } else {
        u32::from(encoded)
    };
    valid_page_size(size).then_some(size)
}

fn logical_page_count(size: u64, page_size: u32) -> Result<u32, StoreError> {
    if size == 0 {
        return Ok(0);
    }
    if !valid_page_size(page_size) || !size.is_multiple_of(u64::from(page_size)) {
        return Err(StoreError::InvalidPageLength {
            page_no: 0,
            actual: usize::try_from(size).unwrap_or(usize::MAX),
            expected: page_size as usize,
        });
    }
    u32::try_from(size / u64::from(page_size)).map_err(|_| StoreError::Range)
}

fn align_up(value: u64, alignment: u64) -> Result<u64, StoreError> {
    value
        .checked_add(alignment - 1)
        .map(|value| value / alignment * alignment)
        .ok_or(StoreError::Range)
}

fn allocate_zeroed(length: usize) -> Result<Vec<u8>, StoreError> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| StoreError::Range)?;
    output.resize(length, 0);
    Ok(output)
}

fn read_prefix_at(file: &File, offset: u64, output: &mut [u8]) -> std::io::Result<usize> {
    let amount = usize::try_from(
        file.metadata()?
            .len()
            .saturating_sub(offset)
            .min(output.len() as u64),
    )
    .unwrap_or(output.len());
    if amount != 0 {
        read_exact_at(file, offset, &mut output[..amount])?;
    }
    Ok(amount)
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut offset: u64, mut output: &mut [u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !output.is_empty() {
        let read = match file.read_at(output, offset) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            result => result?,
        };
        if read == 0 {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }
        offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        output = &mut output[read..];
    }
    Ok(())
}

#[cfg(unix)]
fn write_all_at(file: &File, mut offset: u64, mut input: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !input.is_empty() {
        let written = match file.write_at(input, offset) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            result => result?,
        };
        if written == 0 {
            return Err(std::io::Error::from(ErrorKind::WriteZero));
        }
        offset = offset
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        input = &input[written..];
    }
    Ok(())
}

#[cfg(not(unix))]
compile_error!("zsqlite V3 currently supports Unix filesystem VFSes only");

fn absolute_path(path: &Path) -> Result<PathBuf, std::io::Error> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

#[cfg(unix)]
fn reject_aliased_file(file: &File) -> Result<(), StoreError> {
    use std::os::unix::fs::MetadataExt;
    if file.metadata()?.nlink() > 1 {
        Err(StoreError::Unsupported)
    } else {
        Ok(())
    }
}

fn preserve_metadata(source: &File, destination: &File) -> Result<(), StoreError> {
    destination.set_permissions(source.metadata()?.permissions())?;
    Ok(())
}

fn create_temporary(path: &Path) -> Result<(PathBuf, File), StoreError> {
    loop {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = append_suffix(path, &format!(".compact.{}.{sequence}", std::process::id()));
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
}

fn create_unlinked_temporary(path: &Path) -> Result<File, StoreError> {
    let (temporary_path, file) = create_temporary(path)?;
    // Transaction staging must not survive a process crash. Keeping only the
    // descriptor makes cleanup automatic and prevents stale names on reopen.
    std::fs::remove_file(temporary_path)?;
    Ok(file)
}

struct CleanupPath(Option<PathBuf>);
impl Drop for CleanupPath {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn sync_parent_dir(path: &Path) -> Result<(), StoreError> {
    File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks() * 512
}

#[cfg(unix)]
fn allocation_granularity(file: &File) -> u64 {
    use std::os::unix::fs::MetadataExt;
    file.metadata().map_or(4096, |m| m.blksize().max(512))
}

fn aligned_interior(offset: u64, length: u64, granularity: u64) -> Option<(u64, u64)> {
    let end = offset.checked_add(length)?;
    let start = align_up(offset, granularity).ok()?;
    let aligned_end = end / granularity * granularity;
    (start < aligned_end).then(|| (start, aligned_end - start))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn punch_hole(file: &File, offset: u64, length: u64) -> std::io::Result<()> {
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
fn punch_hole(file: &File, offset: u64, length: u64) -> std::io::Result<()> {
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

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
fn flock(file: &File, operation: libc::c_int) -> Result<(), StoreError> {
    use std::os::fd::AsRawFd;
    loop {
        let rc = unsafe { libc::flock(file.as_raw_fd(), operation) };
        if rc == 0 {
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

fn lock_shared(file: &File, nonblocking: bool) -> Result<(), StoreError> {
    flock(
        file,
        libc::LOCK_SH | if nonblocking { libc::LOCK_NB } else { 0 },
    )
}
fn lock_exclusive(file: &File, nonblocking: bool) -> Result<(), StoreError> {
    flock(
        file,
        libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 },
    )
}
fn unlock_file(file: &File) -> Result<(), StoreError> {
    flock(file, libc::LOCK_UN)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sqlite_page(fill: u8, page_size: u32) -> Vec<u8> {
        let mut page = vec![fill; page_size as usize];
        page[..16].copy_from_slice(SQLITE_MAGIC);
        let encoded = if page_size == 65_536 {
            1
        } else {
            u16::try_from(page_size).expect("test page size fits in u16")
        };
        page[16..18].copy_from_slice(&encoded.to_be_bytes());
        page
    }

    fn incompressible_sqlite_page(page_size: u32) -> Vec<u8> {
        let mut page = vec![0_u8; page_size as usize];
        let mut state = 0xc6a4_a793_5bd1_e995_u64;
        for chunk in page.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        page[..16].copy_from_slice(SQLITE_MAGIC);
        page[16..18].copy_from_slice(&u16::try_from(page_size).unwrap().to_be_bytes());
        page
    }

    #[test]
    fn v3_round_trip_ranges_index_and_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("round-trip.db");
        let mut store = Store::open(&path, true)?;
        let mut image = Vec::new();
        for fill in 0..40 {
            image.extend_from_slice(&sqlite_page(fill, 4096));
        }
        store.write_at(0, &image)?;
        store.publish(true)?;
        store.checkpoint_index()?;
        assert!(store.runs.len() < 40);
        let id = store.database_id;
        drop(store);
        let mut reopened = Store::open_existing(&path)?;
        assert_eq!(reopened.database_id, id);
        let mut actual = vec![0; image.len()];
        reopened.read_at(0, &mut actual)?;
        assert_eq!(actual, image);
        reopened.verify()?;
        Ok(())
    }

    #[test]
    fn opening_waits_for_an_inflight_publication_snapshot() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("open-during-publication.db");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &sqlite_page(b'a', 4096))?;
        store.publish(true)?;
        store.begin_write()?;

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let opener_barrier = Arc::clone(&barrier);
        let opener_path = path.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let opener = std::thread::spawn(move || {
            opener_barrier.wait();
            let _ = sender.send(Store::open_existing(opener_path));
        });
        barrier.wait();
        assert!(matches!(
            receiver.recv_timeout(std::time::Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));

        store.release_publication();
        let mut reopened = receiver.recv_timeout(std::time::Duration::from_secs(5))??;
        assert!(opener.join().is_ok());
        reopened.verify()?;
        Ok(())
    }

    #[test]
    fn missing_and_swapped_sidecars_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let a = directory.path().join("a.db");
        let b = directory.path().join("b.db");
        for path in [&a, &b] {
            let mut store = Store::open(path, true)?;
            store.write_at(0, &sqlite_page(b'x', 4096))?;
            store.publish(true)?;
        }
        let a_side = sidecar_path(&a);
        let b_side = sidecar_path(&b);
        let temporary = directory.path().join("swap");
        std::fs::rename(&a_side, &temporary)?;
        std::fs::rename(&b_side, &a_side)?;
        std::fs::rename(&temporary, &b_side)?;
        assert!(matches!(
            Store::open_existing(&a),
            Err(StoreError::IdentityMismatch)
        ));
        std::fs::remove_file(&b_side)?;
        assert!(matches!(
            Store::open(&b, true),
            Err(StoreError::MissingSidecar)
        ));
        Ok(())
    }

    #[test]
    fn shrink_then_expand_reads_zeroes() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("truncate.db");
        let mut store = Store::open(&path, true)?;
        let mut image = sqlite_page(b'a', 4096);
        image.extend_from_slice(&vec![b'b'; 4096]);
        store.write_at(0, &image)?;
        store.publish(true)?;
        store.truncate(4096)?;
        store.truncate(8192)?;
        let mut tail = vec![1; 4096];
        store.read_at(4096, &mut tail)?;
        assert!(tail.iter().all(|byte| *byte == 0));
        store.discard_pending()?;
        tail.fill(0);
        store.read_at(4096, &mut tail)?;
        assert!(tail.iter().all(|byte| *byte == b'b'));
        Ok(())
    }

    #[test]
    fn shrink_then_expand_persists_zeroes_after_publish_and_reopen()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("truncate-persisted.db");
        let mut store = Store::open(&path, true)?;
        let mut image = sqlite_page(b'a', 4096);
        image.extend_from_slice(&vec![b'b'; 4096]);
        store.write_at(0, &image)?;
        store.publish(true)?;
        let before_generation = store.generation;

        // SQLite is allowed to truncate and then grow the database again
        // without writing every byte in the regrown tail. Those bytes must not
        // resurrect content from the generation predating the truncate.
        store.truncate(4096)?;
        store.truncate(8192)?;
        store.publish(true)?;
        assert!(store.generation > before_generation);
        drop(store);

        let mut reopened = Store::open_existing_read_only(&path)?;
        assert_eq!(reopened.logical_size, 8192);
        let mut tail = vec![1; 4096];
        reopened.read_at(4096, &mut tail)?;
        assert!(tail.iter().all(|byte| *byte == 0));
        Ok(())
    }

    #[test]
    fn pending_layer_bounds_memory_and_survives_publish() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounded-pending.db");
        let mut store = Store::open(&path, true)?;
        let page_size = 4096_u32;
        let pages_per_chunk = max_pages_per_extent(page_size, DEFAULT_EXTENT_BYTES)?;
        let chunk_count = u32::try_from(PENDING_CHUNK_CAPACITY)? + 4;

        // Touch incomplete, widely separated chunks so the ninth chunk must
        // evict and compress the least-recently-used one.
        for chunk in 0..chunk_count {
            let page_no = chunk * pages_per_chunk + 1;
            let fill = u8::try_from(chunk)?;
            let mut page = vec![fill; page_size as usize];
            if page_no == 1 {
                page[..16].copy_from_slice(SQLITE_MAGIC);
                page[16..18].copy_from_slice(&u16::try_from(page_size)?.to_be_bytes());
            }
            store.write_at(u64::from(page_no - 1) * u64::from(page_size), &page)?;
            let pending = store.pending.as_ref().expect("pending layer");
            assert!(
                pending.buffered_bytes() <= PENDING_CHUNK_CAPACITY * DEFAULT_EXTENT_BYTES as usize
            );
        }
        let pending = store.pending.as_ref().expect("pending layer");
        assert_eq!(pending.chunks.len(), PENDING_CHUNK_CAPACITY);
        assert_eq!(pending.runs.len(), 4);
        assert!(pending.next_offset > 0);

        store.publish(true)?;
        store.verify()?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        for chunk in 0..chunk_count {
            let page_no = chunk * pages_per_chunk + 1;
            let fill = u8::try_from(chunk)?;
            let mut page = vec![0; page_size as usize];
            reopened.read_at(u64::from(page_no - 1) * u64::from(page_size), &mut page)?;
            if page_no == 1 {
                assert_eq!(&page[..16], SQLITE_MAGIC);
            } else {
                assert!(page.iter().all(|byte| *byte == fill));
            }
        }
        Ok(())
    }

    #[test]
    fn page_one_arriving_last_uses_a_positional_bootstrap_spool()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("late-page-one.db");
        let page_size = 4096_u32;
        let logical_size =
            (u64::try_from(PENDING_CHUNK_CAPACITY)? + 4) * u64::from(DEFAULT_EXTENT_BYTES);
        let tail_length = usize::try_from(logical_size - u64::from(page_size))?;
        let tail = vec![0x6d; tail_length];
        let mut store = Store::open(&path, true)?;

        store.write_at(u64::from(page_size), &tail)?;
        assert_eq!(store.page_size, 0);
        assert!(store.pending.is_none());
        assert_eq!(
            store
                .bootstrap
                .as_ref()
                .expect("bootstrap spool")
                .file
                .metadata()?
                .len(),
            logical_size
        );
        let mut before_header = [0_u8; 32];
        store.read_at(logical_size - 32, &mut before_header)?;
        assert_eq!(before_header, [0x6d; 32]);

        store.write_at(0, &sqlite_page(0x42, page_size))?;
        assert!(store.bootstrap.is_none());
        let pending = store.pending.as_ref().expect("compressed pending layer");
        assert!(pending.next_offset > 0);
        assert!(pending.buffered_bytes() <= PENDING_CHUNK_CAPACITY * DEFAULT_EXTENT_BYTES as usize);
        store.publish(true)?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        let mut tail_sample = [0_u8; 32];
        reopened.read_at(logical_size - 32, &mut tail_sample)?;
        assert_eq!(tail_sample, [0x6d; 32]);
        reopened.verify()?;
        Ok(())
    }

    #[test]
    fn rewriting_spilled_pages_publishes_non_overlapping_fragments()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("rewrite-spilled.db");
        let page_size = 4096_u32;
        let pages = max_pages_per_extent(page_size, DEFAULT_EXTENT_BYTES)?;
        let mut expected = Vec::with_capacity(DEFAULT_EXTENT_BYTES as usize);
        for page in 1..=pages {
            let mut value = vec![(page % 251) as u8; page_size as usize];
            if page == 1 {
                value[..16].copy_from_slice(SQLITE_MAGIC);
                value[16..18].copy_from_slice(&u16::try_from(page_size)?.to_be_bytes());
            }
            expected.extend_from_slice(&value);
        }

        let mut store = Store::open(&path, true)?;
        store.write_at(0, &expected)?;
        assert!(
            store
                .pending
                .as_ref()
                .is_some_and(|pending| { pending.chunks.is_empty() && pending.runs.len() == 1 })
        );

        // Overwrite one complete page and a range crossing the following page
        // boundary after the configured extent has already been spilled.
        let replacement_page = pages / 2;
        let replacement_offset = u64::from(replacement_page - 1) * u64::from(page_size);
        let replacement = vec![0xd7; page_size as usize];
        store.write_at(replacement_offset, &replacement)?;
        let replacement_start = usize::try_from(replacement_offset)?;
        expected[replacement_start..replacement_start + page_size as usize]
            .copy_from_slice(&replacement);
        let crossing_offset = replacement_offset + u64::from(page_size) - 13;
        let crossing = vec![0x3c; 37];
        store.write_at(crossing_offset, &crossing)?;
        let crossing_start = usize::try_from(crossing_offset)?;
        expected[crossing_start..crossing_start + crossing.len()].copy_from_slice(&crossing);

        store.publish(true)?;
        store.verify()?;
        let mut actual = vec![0; expected.len()];
        store.read_at(0, &mut actual)?;
        assert_eq!(actual, expected);
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        actual.fill(0);
        reopened.read_at(0, &mut actual)?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn discarding_a_spilled_layer_restores_the_committed_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("discard-spilled.db");
        let page_size = 4096_u32;
        let original = sqlite_page(b'a', page_size);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &original)?;
        store.publish(true)?;
        let generation = store.generation;

        let pages_per_chunk = max_pages_per_extent(page_size, DEFAULT_EXTENT_BYTES)?;
        for chunk in 0..=u32::try_from(PENDING_CHUNK_CAPACITY)? {
            let page_no = chunk * pages_per_chunk + 1;
            let page = vec![b'z'; page_size as usize];
            store.write_at(u64::from(page_no - 1) * u64::from(page_size), &page)?;
        }
        assert!(
            store
                .pending
                .as_ref()
                .is_some_and(|pending| { pending.next_offset != 0 && !pending.runs.is_empty() })
        );
        store.discard_pending()?;
        assert_eq!(store.generation, generation);
        assert!(store.pending.is_none());
        assert_eq!(store.logical_size, original.len() as u64);
        let mut actual = vec![0; original.len()];
        store.read_at(0, &mut actual)?;
        assert_eq!(actual, original);
        Ok(())
    }

    #[test]
    fn corrupt_published_extent_is_not_truncated() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("corrupt.db");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &sqlite_page(b'a', 4096))?;
        store.publish(true)?;
        let sidecar = sidecar_path(&path);
        let before = std::fs::metadata(&sidecar)?.len();
        let extent_offset = store.runs[&1].extent.record_offset;
        drop(store);
        let file = OpenOptions::new().read(true).write(true).open(&sidecar)?;
        write_all_at(&file, extent_offset, b"BAD!")?;
        drop(file);
        assert!(matches!(
            Store::open_existing(&path),
            Err(StoreError::Corrupt(_))
        ));
        assert_eq!(std::fs::metadata(sidecar)?.len(), before);
        Ok(())
    }

    #[test]
    fn incompressible_seekable_frame_corruption_is_detected_without_modifying_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("corrupt-raw-payload.db");
        let page = incompressible_sqlite_page(4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &page)?;
        store.publish(true)?;
        let extent = Arc::clone(&store.runs[&1].extent);
        assert_eq!(extent.header.codec, Codec::ZstdSeekable);
        let sidecar_path = sidecar_path(&path);
        let original_length = store.sidecar.metadata()?.len();
        drop(store);

        let sidecar = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sidecar_path)?;
        let frame = &extent.seekable.as_ref().expect("seek layout").frames[0];
        let damaged_offset =
            extent.payload_offset + u64::from(frame.compressed_offset + frame.compressed_size / 2);
        let mut byte = [0_u8; 1];
        read_exact_at(&sidecar, damaged_offset, &mut byte)?;
        byte[0] ^= 0x80;
        write_all_at(&sidecar, damaged_offset, &byte)?;
        drop(sidecar);

        let mut reopened = Store::open_existing_read_only(&path)?;
        let mut output = vec![0_u8; page.len()];
        assert!(matches!(
            reopened.read_at(0, &mut output),
            Err(StoreError::PageChecksum(1))
        ));
        drop(reopened);
        assert_eq!(std::fs::metadata(&sidecar_path)?.len(), original_length);
        Ok(())
    }

    #[test]
    fn legacy_raw_and_zstd_extents_remain_readable() -> Result<(), Box<dyn std::error::Error>> {
        for codec in [Codec::Raw, Codec::Zstd] {
            let file = tempfile::tempfile()?;
            let raw = sqlite_page(b'l', 4096);
            let payload = match codec {
                Codec::Raw => raw.clone(),
                Codec::Zstd => zstd::bulk::compress(&raw, ZSTD_LEVEL)?,
                Codec::ZstdSeekable => unreachable!(),
            };
            let header = ExtentHeader {
                generation: 1,
                first_page: 1,
                page_count: 1,
                codec,
                raw_len: u32::try_from(raw.len())?,
                stored_len: u32::try_from(payload.len())?,
                allocation_len: u32::try_from(SECTOR_SIZE)? * 2,
                raw_digest: digest(&raw),
            };
            write_all_at(&file, 0, &header.encode())?;
            write_all_at(&file, EXTENT_HEADER_SIZE as u64, &payload)?;
            file.set_len(u64::from(header.allocation_len))?;
            let location = read_extent_location(&file, 0, 4096)?;
            let mut cache = ExtentCache::new(8192);
            assert_eq!(
                read_extent_page(&file, &mut cache, &location, 0, 4096)?,
                raw
            );
        }
        Ok(())
    }

    #[test]
    fn compressible_seekable_frame_corruption_is_detected_without_modifying_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("corrupt-zstd-payload.db");
        let page = sqlite_page(b'z', 4096);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &page)?;
        store.publish(true)?;
        let extent = Arc::clone(&store.runs[&1].extent);
        assert_eq!(extent.header.codec, Codec::ZstdSeekable);
        let sidecar_path = sidecar_path(&path);
        let original_length = store.sidecar.metadata()?.len();
        drop(store);

        let sidecar = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sidecar_path)?;
        // Damage multiple payload bytes while leaving the authenticated
        // extent header and every generation record intact.
        let frame = &extent.seekable.as_ref().expect("seek layout").frames[0];
        let damaged_offset = extent.payload_offset + u64::from(frame.compressed_offset);
        let damage_len = usize::try_from(frame.compressed_size.min(8))?;
        let damage = vec![0xa5; damage_len];
        write_all_at(&sidecar, damaged_offset, &damage)?;
        drop(sidecar);

        let mut reopened = Store::open_existing_read_only(&path)?;
        let mut output = vec![0_u8; page.len()];
        assert!(matches!(
            reopened.read_at(0, &mut output),
            Err(StoreError::PageChecksum(1))
        ));
        drop(reopened);
        assert_eq!(std::fs::metadata(&sidecar_path)?.len(), original_length);
        Ok(())
    }

    #[test]
    fn point_read_decodes_only_the_requested_seek_frame() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("selective-frame-read.db");
        let mut image = sqlite_page(b'a', 4096);
        image.extend_from_slice(&vec![b'b'; 31 * 4096]);
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &image)?;
        store.publish(true)?;
        let extent = Arc::clone(&store.runs[&1].extent);
        let second = &extent.seekable.as_ref().expect("seek layout").frames[1];
        assert_eq!(second.raw_offset, DEFAULT_SEEK_CHUNK_BYTES);
        drop(store);

        let sidecar_path = sidecar_path(&path);
        let sidecar = OpenOptions::new()
            .read(true)
            .write(true)
            .open(sidecar_path)?;
        write_all_at(
            &sidecar,
            extent.payload_offset + u64::from(second.compressed_offset),
            b"BAD!",
        )?;
        drop(sidecar);

        let mut reopened = Store::open_existing_read_only(&path)?;
        let mut first = vec![0; 4096];
        reopened.read_at(0, &mut first)?;
        assert_eq!(first, image[..4096]);
        let mut seventeenth = vec![0; 4096];
        assert!(matches!(
            reopened.read_at(u64::from(DEFAULT_SEEK_CHUNK_BYTES), &mut seventeenth),
            Err(StoreError::PageChecksum(17))
        ));
        Ok(())
    }

    #[test]
    fn unpublished_tail_is_preserved_and_never_scanned_as_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("tail.db");
        let mut store = Store::open(&path, true)?;
        let first = sqlite_page(b'a', 4096);
        store.write_at(0, &first)?;
        store.publish(true)?;
        let sidecar_path = sidecar_path(&path);
        drop(store);
        let sidecar = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sidecar_path)?;
        let committed_length = sidecar.metadata()?.len();
        let tail_end = committed_length + 777;
        sidecar.set_len(tail_end)?;
        write_all_at(&sidecar, committed_length, &[0xa5; 777])?;
        drop(sidecar);

        let mut reopened = Store::open_existing(&path)?;
        let mut actual = vec![0; 4096];
        reopened.read_at(0, &mut actual)?;
        assert_eq!(actual, first);
        assert_eq!(std::fs::metadata(&sidecar_path)?.len(), tail_end);
        let second = sqlite_page(b'b', 4096);
        reopened.write_at(0, &second)?;
        reopened.publish(true)?;
        assert!(reopened.runs[&1].extent.record_offset >= align_up(tail_end, 4096)?);
        reopened.read_at(0, &mut actual)?;
        assert_eq!(actual, second);
        Ok(())
    }

    #[test]
    fn physically_truncated_newest_commit_falls_back_to_previous_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("torn-newest.db");
        let mut store = Store::open(&path, true)?;
        let first = sqlite_page(b'a', 4096);
        store.write_at(0, &first)?;
        store.publish(true)?;
        let first_generation = store.generation;
        store.write_at(0, &sqlite_page(b'b', 4096))?;
        store.publish(false)?;
        let truncated_length = store.committed_end - 1;
        store.sidecar.set_len(truncated_length)?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        assert_eq!(reopened.generation, first_generation);
        let mut actual = vec![0; 4096];
        reopened.read_at(0, &mut actual)?;
        assert_eq!(actual, first);
        Ok(())
    }

    #[test]
    fn durability_promotion_prevents_fallback_after_acknowledgement()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("promoted-durable.db");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &sqlite_page(b'a', 4096))?;
        store.publish(true)?;
        store.write_at(0, &sqlite_page(b'b', 4096))?;
        store.publish(false)?;
        let nondurable_slot = store.superblocks[store.active_slot].expect("current slot");
        assert!(!nondurable_slot.durable);

        // This is the xSync-without-new-writes path: it must turn the exact
        // current generation into a durable acknowledgement.
        store.publish(true)?;
        let durable_slot = store.superblocks[store.active_slot].expect("promoted slot");
        assert!(durable_slot.durable);
        assert_eq!(durable_slot.generation, nondurable_slot.generation);
        assert!(durable_slot.sequence > nondurable_slot.sequence);
        let truncated_length = store.committed_end - 1;
        store.sidecar.set_len(truncated_length)?;
        drop(store);

        assert!(matches!(
            Store::open_existing_read_only(&path),
            Err(StoreError::Corrupt(_))
        ));
        assert_eq!(
            std::fs::metadata(sidecar_path(&path))?.len(),
            truncated_length
        );
        Ok(())
    }

    #[test]
    fn damaged_index_falls_back_to_the_authenticated_commit_chain()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("index-fallback.db");
        let mut store = Store::open(&path, true)?;
        let page = sqlite_page(b'i', 4096);
        store.write_at(0, &page)?;
        store.publish(true)?;
        store.checkpoint_index()?;
        let index_offset = store.superblocks[store.active_slot]
            .expect("published index slot")
            .index_offset;
        write_all_at(&store.sidecar, index_offset, b"BAD!")?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        assert_eq!(reopened.indexed_generation, 0);
        let mut actual = vec![0; 4096];
        reopened.read_at(0, &mut actual)?;
        assert_eq!(actual, page);
        Ok(())
    }

    #[test]
    fn damaged_index_payload_falls_back_to_the_authenticated_commit_chain()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("index-payload-fallback.db");
        let mut store = Store::open(&path, true)?;
        let page = sqlite_page(b'i', 4096);
        store.write_at(0, &page)?;
        store.publish(true)?;
        store.checkpoint_index()?;
        let index_offset = store.superblocks[store.active_slot]
            .expect("published index slot")
            .index_offset;
        let payload_offset = index_offset + INDEX_HEADER_SIZE as u64;
        let mut byte = [0_u8; 1];
        read_exact_at(&store.sidecar, payload_offset, &mut byte)?;
        byte[0] ^= 0x40;
        write_all_at(&store.sidecar, payload_offset, &byte)?;
        drop(store);

        let mut reopened = Store::open_existing(&path)?;
        assert_eq!(reopened.indexed_generation, 0);
        let mut actual = vec![0; 4096];
        reopened.read_at(0, &mut actual)?;
        assert_eq!(actual, page);
        reopened.verify()?;
        Ok(())
    }

    #[test]
    fn every_byte_truncation_of_the_optional_index_falls_back_to_the_commit_chain()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("truncated-index-fallback.db");
        let mut store = Store::open(&path, true)?;
        let mut image = sqlite_page(b'h', 4096);
        for fill in b'i'..=b'p' {
            image.extend_from_slice(&vec![fill; 4096]);
        }
        store.write_at(0, &image)?;
        store.publish(true)?;
        store.checkpoint_index()?;
        let slot = store.superblocks[store.active_slot].expect("indexed slot");
        let sidecar_path = sidecar_path(&path);
        drop(store);
        let complete = std::fs::read(&sidecar_path)?;

        for truncated_at in slot.index_offset..slot.index_end {
            let sidecar = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&sidecar_path)?;
            sidecar.set_len(complete.len() as u64)?;
            write_all_at(&sidecar, 0, &complete)?;
            sidecar.set_len(truncated_at)?;
            drop(sidecar);

            let mut reopened = Store::open_existing_read_only(&path)?;
            assert_eq!(
                reopened.indexed_generation, 0,
                "truncated at {truncated_at}"
            );
            let mut actual = vec![0; image.len()];
            reopened.read_at(0, &mut actual)?;
            assert_eq!(actual, image, "truncated at {truncated_at}");
        }
        Ok(())
    }

    #[test]
    fn in_bounds_corruption_does_not_silently_fall_back() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("no-corruption-fallback.db");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &sqlite_page(b'a', 4096))?;
        store.publish(true)?;
        store.write_at(0, &sqlite_page(b'b', 4096))?;
        store.publish(true)?;
        let newest_extent = store.runs[&1].extent.record_offset;
        let length = store.sidecar.metadata()?.len();
        write_all_at(&store.sidecar, newest_extent, b"BAD!")?;
        drop(store);
        assert!(matches!(
            Store::open_existing(&path),
            Err(StoreError::Corrupt(_))
        ));
        assert_eq!(std::fs::metadata(sidecar_path(&path))?.len(), length);
        Ok(())
    }

    #[test]
    fn configured_extents_are_batched_and_obsolete_ranges_are_processed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("batch-and-reclaim.db");
        let config = CompressionConfig::new(64 * 1024, 16 * 1024)?;
        let mut store = Store::open_with_config(&path, true, config)?;
        let mut state = 0x9247_f31a_75d2_b689_u64;
        let mut image = vec![0_u8; config.extent_bytes() as usize];
        for chunk in image.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        image[..16].copy_from_slice(SQLITE_MAGIC);
        image[16..18].copy_from_slice(&4096_u16.to_be_bytes());
        store.write_at(0, &image)?;
        store.publish(true)?;
        assert_eq!(store.live_counts.len(), 1);
        assert_eq!(store.runs.len(), 1);
        assert_eq!(
            store.runs[&1].page_count,
            config.extent_bytes() / store.page_size
        );
        let first_extent = Arc::clone(&store.runs[&1].extent);

        for byte in &mut image[100..] {
            *byte = byte.wrapping_add(71);
        }
        store.write_at(0, &image)?;
        store.publish(true)?;
        assert!(
            store
                .obsolete_ranges
                .iter()
                .any(|(offset, _, _)| *offset == first_extent.payload_offset)
        );
        store.checkpoint_index()?;
        assert!(store.obsolete_ranges.is_empty() || !store.hole_punching);
        store.verify()?;
        Ok(())
    }

    #[test]
    fn every_byte_truncation_of_the_newest_generation_recovers_the_previous_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("every-torn-byte.db");
        let mut store = Store::open(&path, true)?;
        let first = sqlite_page(b'a', 4096);
        let second = sqlite_page(b'b', 4096);
        store.write_at(0, &first)?;
        store.publish(true)?;
        let previous_end = store.committed_end;
        store.write_at(0, &second)?;
        store.publish(false)?;
        let newest_end = store.committed_end;
        let sidecar_path = sidecar_path(&path);
        drop(store);
        let complete = std::fs::read(&sidecar_path)?;

        for truncated_at in previous_end..newest_end {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&sidecar_path)?;
            file.set_len(complete.len() as u64)?;
            write_all_at(&file, 0, &complete)?;
            file.set_len(truncated_at)?;
            drop(file);
            let mut recovered = Store::open_existing_read_only(&path)?;
            let mut actual = vec![0; 4096];
            recovered.read_at(0, &mut actual)?;
            assert_eq!(actual, first, "truncated at byte {truncated_at}");
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sidecar_path)?;
        file.set_len(complete.len() as u64)?;
        write_all_at(&file, 0, &complete)?;
        drop(file);
        let mut recovered = Store::open_existing_read_only(&path)?;
        let mut actual = vec![0; 4096];
        recovered.read_at(0, &mut actual)?;
        assert_eq!(actual, second);
        Ok(())
    }

    #[test]
    fn every_byte_truncation_of_a_durable_generation_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("every-durable-torn-byte.db");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &sqlite_page(b'a', 4096))?;
        store.publish(true)?;
        let previous_end = store.committed_end;
        store.write_at(0, &sqlite_page(b'b', 4096))?;
        store.publish(true)?;
        let newest_end = store.committed_end;
        let sidecar_path = sidecar_path(&path);
        drop(store);
        let complete = std::fs::read(&sidecar_path)?;

        for truncated_at in previous_end..newest_end {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&sidecar_path)?;
            file.set_len(complete.len() as u64)?;
            write_all_at(&file, 0, &complete)?;
            file.set_len(truncated_at)?;
            drop(file);
            assert!(
                matches!(
                    Store::open_existing_read_only(&path),
                    Err(StoreError::Corrupt(_))
                ),
                "durable generation truncated at byte {truncated_at} was accepted or rolled back"
            );
            assert_eq!(std::fs::metadata(&sidecar_path)?.len(), truncated_at);
        }
        Ok(())
    }
}
