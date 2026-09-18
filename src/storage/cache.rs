//! Disposable plaintext page slots. No decoded payload is retained in Rust RAM.
use super::frame::{PageVersion, checksum};
use super::view::{PinnedView, ResolvedPage};
use crate::StoreError;
use crate::domain::{
    CacheBytes, FileOffset, ManifestId, PageNumber, PageSize, StoredBytes, StoredRange,
};
use crate::fs::CacheFile;
use crate::layout::CacheCapacity;
use crate::statistics::{DatabaseCacheStats, HandleIoStats};
use std::collections::HashMap;

// Exclusive slot ownership. A successful write occupies a vacant slot;
// invalidation makes it reusable even if physical hole punching is unsupported.
struct VacantSlot(FileOffset);
struct OccupiedSlot(FileOffset);
struct Entry {
    version: PageVersion,
    slot: OccupiedSlot,
    used: bool,
    previous: Option<PageNumber>,
    next: Option<PageNumber>,
}
struct Pages {
    file: CacheFile,
    manifest: ManifestId,
    page_size: PageSize,
    stride: u64,
    capacity: usize,
    allocated: usize,
    free: Vec<VacantSlot>,
    entries: HashMap<PageNumber, Entry>,
    newest: Option<PageNumber>,
    oldest: Option<PageNumber>,
    #[cfg(test)]
    fail_punch: bool,
    #[cfg(test)]
    punch_attempts: std::cell::Cell<usize>,
}
enum Backing {
    Empty,
    Ready(Box<Pages>),
    /// Real cache read/write failure: bypass the cache, never fail the database.
    /// Hole-punch failures do NOT enter this state.
    Disabled,
}
pub(crate) struct PageCache {
    backing: Backing,
    budget: CacheCapacity,
    stats: DatabaseCacheStats,
}
impl Pages {
    fn new(view: &PinnedView, capacity: CacheCapacity) -> Result<Self, StoreError> {
        let file = CacheFile::new()?;
        let budget = capacity.resolve(view.logical_size().get(), file.available_bytes());
        let size = view.logical_size().page_size();
        let stride = u64::from(size.get())
            .div_ceil(file.block_bytes())
            .checked_mul(file.block_bytes())
            .ok_or(StoreError::Range)?;
        let capacity = usize::try_from(budget.get().min(i64::MAX as u64) / stride)
            .map_err(|_| StoreError::Range)?;
        Ok(Self {
            file,
            manifest: view.id(),
            page_size: size,
            stride,
            capacity,
            allocated: 0,
            free: Vec::new(),
            entries: HashMap::new(),
            newest: None,
            oldest: None,
            #[cfg(test)]
            fail_punch: false,
            #[cfg(test)]
            punch_attempts: std::cell::Cell::new(0),
        })
    }
    fn unlink(&mut self, id: PageNumber) {
        let entry = &self.entries[&id];
        let (previous, next) = (entry.previous, entry.next);
        if let Some(previous) = previous {
            self.entries.get_mut(&previous).expect("LRU link").next = next;
        } else {
            self.newest = next;
        }
        if let Some(next) = next {
            self.entries.get_mut(&next).expect("LRU link").previous = previous;
        } else {
            self.oldest = previous;
        }
    }
    fn promote(&mut self, id: PageNumber) {
        let entry = self.entries.get_mut(&id).expect("LRU entry");
        entry.previous = None;
        entry.next = self.newest;
        if let Some(newest) = self.newest {
            self.entries.get_mut(&newest).expect("LRU newest").previous = Some(id);
        } else {
            self.oldest = Some(id);
        }
        self.newest = Some(id);
    }
    fn remove(&mut self, id: PageNumber, stats: &mut DatabaseCacheStats) -> Option<Entry> {
        if !self.entries.contains_key(&id) {
            return None;
        }
        self.unlink(id);
        let entry = self.entries.remove(&id).expect("cached page");
        stats.extra_pages_evicted_unused = stats
            .extra_pages_evicted_unused
            .saturating_add(u64::from(!entry.used));
        Some(entry)
    }
    fn invalidate(&mut self, id: PageNumber, stats: &mut DatabaseCacheStats) {
        let Some(entry) = self.remove(id, stats) else {
            return;
        };
        // This is space reclamation only, never a validity/durability operation.
        // The entry is already unreachable; failure must still allow slot reuse.
        let _best_effort = self.punch(&entry.slot);
        self.free.push(VacantSlot(entry.slot.0));
    }
    fn punch(&self, slot: &OccupiedSlot) -> Result<(), StoreError> {
        #[cfg(test)]
        self.punch_attempts.set(self.punch_attempts.get() + 1);
        #[cfg(test)]
        if self.fail_punch {
            return Err(std::io::Error::from(std::io::ErrorKind::Unsupported).into());
        }
        self.file
            .punch(StoredRange::new(slot.0, StoredBytes::new(self.stride))?)?;
        Ok(())
    }
    fn read(&mut self, id: PageNumber) -> Result<Option<(Vec<u8>, bool)>, StoreError> {
        let Some(entry) = self.entries.get(&id) else {
            return Ok(None);
        };
        let mut bytes = vec![0; self.page_size.as_usize()];
        self.file.read(entry.slot.0, &mut bytes)?;
        if checksum(id, entry.version.txid, &bytes) != entry.version.checksum {
            return Err(StoreError::PageChecksum(id.get()));
        }
        let prefetched = !entry.used;
        self.unlink(id);
        self.promote(id);
        self.entries.get_mut(&id).expect("cached page").used = true;
        Ok(Some((bytes, prefetched)))
    }
    fn insert(
        &mut self,
        version: &PageVersion,
        bytes: &[u8],
        used: bool,
        stats: &mut DatabaseCacheStats,
    ) -> Result<(), StoreError> {
        if self.capacity == 0 || self.entries.contains_key(&version.page) {
            return Ok(());
        }
        if bytes.len() != self.page_size.as_usize() {
            return Err(StoreError::Range);
        }
        let vacant = if self.entries.len() == self.capacity {
            // Immediate replacement reuses allocated blocks. Punching just
            // before overwriting would add I/O without reclaiming any space.
            let evicted = self
                .remove(self.oldest.expect("full cache"), stats)
                .expect("oldest page");
            VacantSlot(evicted.slot.0)
        } else if let Some(slot) = self.free.pop() {
            slot
        } else {
            let offset = (self.allocated as u64)
                .checked_mul(self.stride)
                .ok_or(StoreError::Range)?;
            self.file
                .set_len(offset.checked_add(self.stride).ok_or(StoreError::Range)?)?;
            self.allocated += 1;
            VacantSlot(FileOffset::new(offset))
        };
        self.file.write(vacant.0, bytes)?;
        self.entries.insert(
            version.page,
            Entry {
                version: version.clone(),
                slot: OccupiedSlot(vacant.0),
                used,
                previous: None,
                next: None,
            },
        );
        self.promote(version.page);
        Ok(())
    }
}
impl PageCache {
    /// Bounded disk scratch cache for sequential export/verification/repacking.
    pub(crate) fn maintenance() -> Result<Self, StoreError> {
        Self::new(CacheBytes::new(9 * 1024 * 1024))
    }
    pub(crate) fn new(budget: impl Into<CacheCapacity>) -> Result<Self, StoreError> {
        let budget = budget.into();
        if let CacheCapacity::Fixed(bytes) = budget {
            bytes.as_usize()?;
        }
        Ok(Self {
            backing: Backing::Empty,
            budget,
            stats: DatabaseCacheStats::default(),
        })
    }
    pub(crate) fn stats(&self) -> DatabaseCacheStats {
        DatabaseCacheStats {
            resident_bytes: match &self.backing {
                Backing::Ready(pages) => pages.entries.len() as u64 * pages.stride,
                _ => 0,
            },
            ..self.stats
        }
    }
    fn discard(&mut self, replacement: Backing) {
        if let Backing::Ready(pages) = std::mem::replace(&mut self.backing, replacement) {
            self.stats.extra_pages_evicted_unused = self
                .stats
                .extra_pages_evicted_unused
                .saturating_add(pages.entries.values().filter(|entry| !entry.used).count() as u64);
            // Closing the private unlinked file releases its storage.
        }
    }
    pub(crate) fn set_budget(
        &mut self,
        budget: impl Into<CacheCapacity>,
    ) -> Result<(), StoreError> {
        let budget = budget.into();
        if let CacheCapacity::Fixed(bytes) = budget {
            bytes.as_usize()?;
        }
        if budget != self.budget {
            self.discard(Backing::Empty);
            self.budget = budget;
        }
        Ok(())
    }
    pub(crate) fn synchronize(
        &mut self,
        view: Option<&PinnedView>,
        shadowed: impl Fn(PageNumber) -> bool,
    ) {
        if let Backing::Ready(pages) = &self.backing
            && view.is_none_or(|view| view.id() != pages.manifest)
        {
            self.discard(Backing::Empty);
            return;
        }
        self.invalidate_where(shadowed);
    }
    pub(crate) fn invalidate(&mut self, number: PageNumber) {
        if let Backing::Ready(pages) = &mut self.backing {
            pages.invalidate(number, &mut self.stats);
        }
    }
    pub(crate) fn invalidate_where(&mut self, matches: impl Fn(PageNumber) -> bool) {
        if let Backing::Ready(pages) = &self.backing {
            let numbers: Vec<_> = pages
                .entries
                .keys()
                .copied()
                .filter(|page| matches(*page))
                .collect();
            for number in numbers {
                self.invalidate(number);
            }
        }
    }
    /// Lookup by page and pinned manifest BEFORE resolving any compressed frame.
    pub(crate) fn read(
        &mut self,
        view: &PinnedView,
        number: PageNumber,
        io: &mut HandleIoStats,
        shadowed: impl Fn(PageNumber) -> bool,
    ) -> Result<Vec<u8>, StoreError> {
        self.read_prefetched(view, number, io, shadowed, None)
    }
    pub(crate) fn read_many(
        &mut self,
        view: &PinnedView,
        numbers: &[PageNumber],
        io: &mut HandleIoStats,
        shadowed: impl Fn(PageNumber) -> bool,
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        if let Backing::Ready(pages) = &self.backing
            && pages.manifest != view.id()
        {
            self.discard(Backing::Empty);
        }
        let mut output = Vec::with_capacity(numbers.len());
        let mut start = 0;
        while start < numbers.len() {
            let mut end = start;
            let mut bytes = 0_u64;
            let mut frames = std::collections::BTreeSet::new();
            let mut misses = Vec::new();
            while end < numbers.len() && end - start < 256 {
                let number = numbers[end];
                let cached = matches!(&self.backing, Backing::Ready(pages) if pages.entries.contains_key(&number));
                if !cached && let ResolvedPage::Stored(page) = view.resolve(number)? {
                    if !frames.contains(&page.frame_id()) {
                        if end > start && bytes + page.stored_bytes() > 16 * 1024 * 1024 {
                            break;
                        }
                        bytes += page.stored_bytes();
                        frames.insert(page.frame_id());
                    }
                    misses.push(number);
                }
                end += 1;
            }
            let mut records = view.read_frame_records(&misses)?;
            for number in &numbers[start..end] {
                output.push(self.read_prefetched(
                    view,
                    *number,
                    io,
                    &shadowed,
                    Some(&mut records),
                )?);
            }
            start = end;
        }
        Ok(output)
    }
    fn read_prefetched(
        &mut self,
        view: &PinnedView,
        number: PageNumber,
        io: &mut HandleIoStats,
        shadowed: impl Fn(PageNumber) -> bool,
        records: Option<&mut std::collections::BTreeMap<crate::domain::FrameId, (Vec<u8>, bool)>>,
    ) -> Result<Vec<u8>, StoreError> {
        if let Backing::Ready(pages) = &self.backing
            && pages.manifest != view.id()
        {
            self.discard(Backing::Empty);
        }
        if let Backing::Ready(pages) = &mut self.backing {
            match pages.read(number) {
                Ok(Some((bytes, prefetched))) => {
                    self.stats.hits = self.stats.hits.saturating_add(1);
                    self.stats.extra_pages_requested = self
                        .stats
                        .extra_pages_requested
                        .saturating_add(u64::from(prefetched));
                    io.cache_hits = io.cache_hits.saturating_add(1);
                    return Ok(bytes);
                }
                Ok(None) => {}
                // A damaged disposable cache is a miss, not database corruption.
                Err(_) => self.discard(Backing::Disabled),
            }
        }
        let resolved = view.resolve(number)?;
        let ResolvedPage::Stored(page) = &resolved else {
            return resolved.read();
        };
        self.stats.misses = self.stats.misses.saturating_add(1);
        io.cache_misses = io.cache_misses.saturating_add(1);
        let (frame, nanos, fetched) = if let Some((bytes, counted)) =
            records.and_then(|records| records.get_mut(&page.frame_id()))
        {
            let (_, frame, nanos) = page.decode_record(bytes)?;
            let fetched = if *counted { 0 } else { page.stored_bytes() };
            *counted = true;
            (frame, nanos, fetched)
        } else {
            let (frame, nanos) = page.fetch_measured()?;
            (frame, nanos, page.stored_bytes())
        };
        io.decode_nanoseconds = io.decode_nanoseconds.saturating_add(nanos);
        io.fetched_bytes = io.fetched_bytes.saturating_add(fetched);
        io.inflated_bytes = io.inflated_bytes.saturating_add(frame.bytes().len() as u64);
        let output = page.extract(&frame)?;
        if matches!(self.backing, Backing::Empty)
            && !matches!(self.budget, CacheCapacity::Fixed(bytes) if bytes.get() == 0)
        {
            self.backing = match Pages::new(view, self.budget) {
                Ok(pages) => Backing::Ready(Box::new(pages)),
                Err(_) => Backing::Disabled,
            };
        }
        let mut extra_slots = match &self.backing {
            Backing::Ready(pages) => pages.capacity.saturating_sub(1),
            _ => 0,
        };
        // A neighbor read cannot repopulate an obsolete copy shadowed by active.
        for (version, bytes) in page
            .cache_pages(&frame)?
            .filter(|(version, _)| version.page != number && !shadowed(version.page))
        {
            if extra_slots == 0 {
                self.stats.extra_pages_evicted_unused =
                    self.stats.extra_pages_evicted_unused.saturating_add(1);
            } else {
                self.insert(version, bytes, false);
                extra_slots -= 1;
            }
        }
        // Requested page goes last: an oversized frame retains this page plus
        // a bounded subset of useful neighbors, without exceeding the file cap.
        if let Some((version, bytes)) = page
            .cache_pages(&frame)?
            .find(|(version, _)| version.page == number)
        {
            self.insert(version, bytes, true);
        }
        Ok(output)
    }
    fn insert(&mut self, version: &PageVersion, bytes: &[u8], used: bool) {
        if let Backing::Ready(pages) = &mut self.backing
            && pages.insert(version, bytes, used, &mut self.stats).is_err()
        {
            self.discard(Backing::Disabled);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::DecodedBytes;
    use crate::layout::LayoutPolicy;
    use crate::store::Store;
    use crate::{RetentionName, StoragePolicy};
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn page(number: u8, size: usize) -> Vec<u8> {
        let mut bytes = vec![number; size];
        bytes[..16].copy_from_slice(b"SQLite format 3\0");
        bytes[16..18].copy_from_slice(
            &u16::try_from(if size == 65536 { 1 } else { size })
                .unwrap()
                .to_be_bytes(),
        );
        bytes
    }
    fn fixture(
        size: usize,
    ) -> Result<(tempfile::TempDir, Store, PinnedView), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("cache.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.set_storage_policy(
            StoragePolicy::default()
                .with_layout(LayoutPolicy::default().fixed(DecodedBytes::new(256 * 1024))?),
        )?;
        store.write_at(0, &[page(1, size), page(2, size), page(3, size)].concat())?;
        store.publish(true)?;
        let name = RetentionName::new("cache-fixture")?;
        drop(store.retain_view(name.clone(), false)?);
        let view = crate::open_retained(&path, &name)?;
        Ok((dir, store, view))
    }
    fn pages(cache: &PageCache) -> &Pages {
        match &cache.backing {
            Backing::Ready(pages) => pages,
            _ => panic!("missing file cache"),
        }
    }
    fn pages_mut(cache: &mut PageCache) -> &mut Pages {
        match &mut cache.backing {
            Backing::Ready(pages) => pages,
            _ => panic!("missing file cache"),
        }
    }

    #[test]
    fn invalidation_recycles_only_its_slot_even_when_punching_fails() -> TestResult {
        for size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            let (_dir, _store, view) = fixture(size)?;
            for fail_punch in [false, true] {
                let mut cache = PageCache::new(CacheBytes::new(512 * 1024))?;
                let mut io = HandleIoStats::default();
                cache.read(&view, PageNumber::new(1)?, &mut io, |_| false)?;
                let two = PageNumber::new(2)?;
                let offset = pages(&cache).entries[&two].slot.0;
                let length = pages(&cache).file.metadata()?.len();
                pages_mut(&mut cache).fail_punch = fail_punch;
                cache.invalidate(two);
                assert_eq!(pages(&cache).punch_attempts.get(), 1);
                assert_eq!(pages(&cache).entries.len(), 2);
                assert_eq!(pages(&cache).free.len(), 1);
                assert_eq!(pages(&cache).file.metadata()?.len(), length);
                let hits = io.cache_hits;
                assert_eq!(
                    cache.read(&view, PageNumber::new(1)?, &mut io, |_| false)?,
                    page(1, size)
                );
                assert_eq!(
                    cache.read(&view, PageNumber::new(3)?, &mut io, |_| false)?,
                    page(3, size)
                );
                assert_eq!(
                    io.cache_hits - hits,
                    2,
                    "failed punching must keep neighboring cache entries"
                );
                assert_eq!(cache.read(&view, two, &mut io, |_| false)?, page(2, size));
                assert_eq!(pages(&cache).entries[&two].slot.0, offset);
                assert_eq!(pages(&cache).file.metadata()?.len(), length);
            }
        }
        Ok(())
    }

    #[test]
    fn oversized_frames_and_lru_reuse_never_exceed_the_file_cap() -> TestResult {
        let (_dir, _store, view) = fixture(4096)?;
        let mut cache = PageCache::new(CacheBytes::new(4096))?;
        let mut io = HandleIoStats::default();
        for number in [1, 2, 3, 2, 1] {
            let number = PageNumber::new(number)?;
            cache.read(&view, number, &mut io, |_| false)?;
            assert!(pages(&cache).file.metadata()?.len() <= 4096);
            assert!(cache.stats().resident_bytes <= 4096);
            let before = io.cache_hits;
            cache.read(&view, number, &mut io, |_| false)?;
            assert_eq!(io.cache_hits, before + 1);
        }
        assert_eq!(pages(&cache).punch_attempts.get(), 0);
        cache.set_budget(CacheBytes::new(0))?;
        assert!(matches!(cache.backing, Backing::Empty));
        assert_eq!(cache.stats().resident_bytes, 0);
        Ok(())
    }

    #[test]
    fn cached_payload_corruption_and_truncation_refetch_verified_source() -> TestResult {
        let (_dir, _store, view) = fixture(4096)?;
        for truncate in [false, true] {
            let mut cache = PageCache::new(CacheBytes::new(65536))?;
            let mut io = HandleIoStats::default();
            let number = PageNumber::new(1)?;
            cache.read(&view, number, &mut io, |_| false)?;
            let backing = pages(&cache);
            if truncate {
                backing.file.set_len(0)?;
            } else {
                backing
                    .file
                    .write(backing.entries[&number].slot.0, &vec![0; 4096])?;
            }
            assert_eq!(
                cache.read(&view, number, &mut io, |_| false)?,
                page(1, 4096)
            );
            assert_eq!(io.cache_misses, 2);
            assert!(matches!(cache.backing, Backing::Disabled));
        }
        Ok(())
    }

    #[test]
    fn frame_prefetch_excludes_obsolete_and_active_shadowed_pages() -> TestResult {
        let (dir, mut store, old) = fixture(4096)?;
        store.write_at(4096, &page(9, 4096))?;
        store.publish(true)?;
        drop(store.retain_view(RetentionName::new("current")?, false)?);
        let current = crate::open_retained(
            dir.path().join("cache.zsqlite"),
            &RetentionName::new("current")?,
        )?;
        let mut cache = PageCache::new(CacheBytes::new(65536))?;
        let mut io = HandleIoStats::default();
        let one = PageNumber::new(1)?;
        let two = PageNumber::new(2)?;
        let three = PageNumber::new(3)?;
        cache.read(&current, one, &mut io, |_| false)?;
        assert!(
            !pages(&cache).entries.contains_key(&two),
            "obsolete slot in the old frame must not be cached"
        );
        assert_eq!(
            cache.read(&current, two, &mut io, |_| false)?,
            page(9, 4096)
        );
        cache.read(&old, one, &mut io, |_| false)?;
        cache.invalidate(two);
        cache.invalidate(three);
        cache.read(&old, three, &mut io, |page| page == two)?;
        assert!(
            !pages(&cache).entries.contains_key(&two),
            "a neighbor miss must not refill an invalidated active page"
        );
        Ok(())
    }
}
