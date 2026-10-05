//! A new database's first dictionary, chosen from related databases' preferred
//! dictionaries. Selection is advisory: unreadable relatives are skipped, and
//! a missing or unusable seed never prevents a seal.
use super::frame::DecodingDictionary;
use super::objects::CatalogGuard;
use super::wire::{ReadDecoder, envelope, open_envelope};
use crate::StoreError;
use crate::dictionary::{DictionarySeed, MAX_DICTIONARY_BYTES, MAX_SAMPLE_BYTES};
use crate::domain::DictionaryId;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};

const SEED: &str = "dictionary.seed";
/// Distinct related samples scored per finalist.
const POOL_BYTES: usize = 8 * 1024 * 1024;
/// Digest-order prefix of the pool used to screen every candidate.
const SCREEN_BYTES: usize = 1024 * 1024;
const FINALISTS: usize = 4;

/// Record the dictionary the namespace's first seal adopts.
pub(crate) fn record(guard: &CatalogGuard, dictionary: &[u8]) -> Result<(), StoreError> {
    if guard.state().sealed.is_some() {
        return Err(StoreError::InvalidConfiguration(
            "dictionary seeds apply only before the first seal",
        ));
    }
    if dictionary.is_empty() || dictionary.len() > MAX_DICTIONARY_BYTES as usize {
        return Err(StoreError::Range);
    }
    super::retention::atomic_write(
        &guard.root().join(SEED),
        &envelope(b"ZSEED001", dictionary)?,
    )
}

/// The recorded seed, if intact and usable for both encoding and decoding.
pub(super) fn load(guard: &CatalogGuard, level: i32) -> Option<Vec<u8>> {
    let path = guard.root().join(SEED);
    if path.metadata().ok()?.len() > u64::from(MAX_DICTIONARY_BYTES) + 64 * 1024 {
        return None;
    }
    let bytes = open_envelope(
        b"ZSEED001",
        &std::fs::read(path).ok()?,
        MAX_DICTIONARY_BYTES as usize,
    )
    .ok()?;
    (!bytes.is_empty()
        && DecodingDictionary::new(bytes.clone()).is_ok()
        && zstd::bulk::Compressor::with_dictionary(level, &bytes).is_ok())
    .then_some(bytes)
}

/// Seeds only affect the first seal; later seals drop a leftover record.
pub(super) fn discard(guard: &CatalogGuard) {
    let _ = std::fs::remove_file(guard.root().join(SEED));
}

struct Sample {
    bytes: Vec<u8>,
    origins: BTreeSet<usize>,
    plain: u64,
}

#[derive(Clone, Copy, Debug)]
struct Score {
    payload: u128,
    plain: u128,
}

/// Every reservoir keeps its lowest content digests, so the lowest digests of
/// their union form one bounded uniform sample of all related databases.
/// Each page remembers which sample namespaces contributed it.
struct Pool {
    pages: BTreeMap<[u8; 32], Sample>,
    bytes: usize,
    budget: usize,
}
impl Pool {
    fn new(budget: usize) -> Self {
        Self {
            pages: BTreeMap::new(),
            bytes: 0,
            budget,
        }
    }
    /// Returns false once a digest can no longer enter the full pool.
    fn insert(&mut self, id: [u8; 32], bytes: Vec<u8>, origin: usize) -> bool {
        if let Some(sample) = self.pages.get_mut(&id) {
            sample.origins.insert(origin);
            return true;
        }
        if self.bytes + bytes.len() > self.budget
            && self
                .pages
                .last_key_value()
                .is_some_and(|(last, _)| id >= *last)
        {
            return false;
        }
        self.bytes += bytes.len();
        self.pages.insert(
            id,
            Sample {
                bytes,
                origins: BTreeSet::from([origin]),
                plain: 0,
            },
        );
        while self.bytes > self.budget
            && let Some((_, sample)) = self.pages.pop_last()
        {
            self.bytes -= sample.bytes.len();
        }
        true
    }
    /// Stream a `ZSAMPLE1` reservoir in its stored digest order and stop at the
    /// pool's cutoff. The skipped suffix is not hash-verified; pages are
    /// structurally validated and used only to score authenticated dictionaries.
    fn read_reservoir(&mut self, path: &Path, origin: usize) -> Result<(), StoreError> {
        let mut file = std::io::BufReader::new(std::fs::File::open(path)?);
        let mut header = [0; 48];
        file.read_exact(&mut header)?;
        if header[..8] != *b"ZSAMPLE1" {
            return Err(StoreError::Corrupt(0));
        }
        let length = usize::try_from(u64::from_le_bytes(
            header[8..16].try_into().map_err(|_| StoreError::Range)?,
        ))
        .map_err(|_| StoreError::Range)?;
        if length > MAX_SAMPLE_BYTES + 1024 * 1024 {
            return Err(StoreError::Range);
        }
        let reader = zstd::stream::read::Decoder::with_buffer(file)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        let mut wire =
            ReadDecoder::new(std::io::BufReader::with_capacity(64 * 1024, reader), length);
        let _fresh = wire.u64()?;
        let mut previous = None;
        for _ in 0..wire.u32()? {
            let length = wire.u32()?;
            crate::domain::PageSize::new(length)?;
            let bytes = wire.take(length as usize)?;
            let id = *blake3::hash(&bytes).as_bytes();
            if previous.is_some_and(|previous| previous >= id) {
                return Err(StoreError::Corrupt(0));
            }
            previous = Some(id);
            if !self.insert(id, bytes, origin) {
                break;
            }
        }
        Ok(())
    }
    fn measure(&mut self, level: i32) -> Option<()> {
        let mut compressor = zstd::bulk::Compressor::new(level).ok()?;
        for sample in self.pages.values_mut() {
            let compressed = compressor.compress(&sample.bytes).ok()?.len();
            sample.plain = compressed.min(sample.bytes.len()) as u64;
        }
        Some(())
    }
    /// Charge each page as `Evaluation::select` does. Pages contributed only by
    /// the dictionary's holders are skipped when others exist: a holder's
    /// dictionary may have been trained on them.
    fn score(
        &self,
        level: i32,
        dictionary: &[u8],
        holders: &BTreeSet<usize>,
        limit: usize,
    ) -> Option<Score> {
        let prefix = || {
            let mut used = 0;
            self.pages.values().take_while(move |sample| {
                let keep = used < limit;
                used += sample.bytes.len();
                keep
            })
        };
        let independent = prefix().any(|sample| !sample.origins.is_subset(holders));
        let mut compressor = zstd::bulk::Compressor::with_dictionary(level, dictionary).ok()?;
        let mut score = Score {
            payload: 0,
            plain: 0,
        };
        for sample in prefix().filter(|sample| !independent || !sample.origins.is_subset(holders)) {
            let compressed = compressor.compress(&sample.bytes).ok()?.len();
            score.payload += compressed.saturating_add(32).min(sample.bytes.len()) as u128;
            score.plain += u128::from(sample.plain);
        }
        (score.plain > 0).then_some(score)
    }
}

/// The logical size whose projected payload must repay a seed's own bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SeedTarget {
    /// A new, empty database expected to grow like its related databases.
    RelatedMedian,
    /// A database of known logical size, such as a conversion source.
    Logical(u64),
}

/// Projected stored bytes with a dictionary, when they beat plain Zstandard's
/// `baseline` by at least 5% after charging the dictionary itself once. A small
/// target cannot repay a large dictionary however well it compresses.
fn adoption_estimate(score: Score, baseline: u128, dictionary_bytes: usize) -> Option<u128> {
    let estimate = score.payload * baseline / score.plain + dictionary_bytes as u128;
    (estimate * 100 <= baseline * 95).then_some(estimate)
}

/// Choose the related dictionary with the lowest estimated stored size for a
/// database of the target logical size: projected payload plus the dictionary
/// itself, at least 5% below plain Zstandard. Every candidate is screened on a
/// small sample; the best few are scored in full.
pub(crate) fn choose(
    related: &[PathBuf],
    level: i32,
    target: SeedTarget,
) -> (Option<(DictionaryId, Vec<u8>)>, DictionarySeed) {
    let mut report = DictionarySeed {
        dictionary: None,
        dictionary_bytes: 0,
        related: 0,
        skipped: 0,
        candidates: 0,
        sample_bytes: 0,
    };
    // Forks share one namespace and one reservoir; read each reservoir once.
    let mut namespaces = BTreeMap::<PathBuf, usize>::new();
    let mut storages = Vec::new();
    for path in related {
        let Ok(storage) = super::Storage::for_sidecar(&crate::backend::sidecar_dir(path), false)
        else {
            report.skipped += 1;
            continue;
        };
        let next = namespaces.len();
        let origin = *namespaces
            .entry(storage.namespace_directory().to_path_buf())
            .or_insert(next);
        storages.push((storage, origin));
    }
    let mut pool = Pool::new(POOL_BYTES);
    for (namespace, origin) in &namespaces {
        let _advisory = pool.read_reservoir(&namespace.join("dictionary.samples"), *origin);
    }
    report.sample_bytes = pool.bytes;
    if pool.measure(level).is_none() {
        return (None, report);
    }

    let mut holders = BTreeMap::<DictionaryId, BTreeSet<usize>>::new();
    let mut finalists = Vec::<(u128, DictionaryId, Vec<u8>)>::new();
    let mut sizes = Vec::new();
    for (storage, origin) in &storages {
        let Ok(view) = storage.open_sealed() else {
            report.skipped += 1;
            continue;
        };
        report.related += 1;
        sizes.push(view.logical_size().get());
        for id in &view.metadata.preferred {
            let first = !holders.contains_key(id);
            let held = holders.entry(*id).or_default();
            held.insert(*origin);
            let Some(dictionary) = view.dictionaries().get(id).filter(|_| first) else {
                continue;
            };
            report.candidates += 1;
            if let Some(score) = pool.score(level, dictionary.bytes(), held, SCREEN_BYTES) {
                finalists.push((
                    score.payload * (1 << 32) / score.plain,
                    *id,
                    dictionary.bytes().to_vec(),
                ));
                finalists.sort_by_key(|(ratio, id, _)| (*ratio, *id));
                finalists.truncate(FINALISTS);
            }
        }
    }

    sizes.sort_unstable();
    let raw: u128 = pool
        .pages
        .values()
        .map(|sample| sample.bytes.len() as u128)
        .sum();
    let plain: u128 = pool
        .pages
        .values()
        .map(|sample| u128::from(sample.plain))
        .sum();
    let target = match target {
        SeedTarget::RelatedMedian => sizes.get(sizes.len() / 2).copied(),
        SeedTarget::Logical(bytes) => (!sizes.is_empty()).then_some(bytes),
    };
    let (Some(target), true) = (target, raw > 0) else {
        return (None, report);
    };
    let baseline = plain * u128::from(target) / raw;
    let chosen = finalists
        .into_iter()
        .filter_map(|(_, id, bytes)| {
            let score = pool.score(level, &bytes, &holders[&id], usize::MAX)?;
            let estimate = adoption_estimate(score, baseline, bytes.len())?;
            Some((estimate, id, bytes))
        })
        .min_by_key(|(estimate, id, _)| (*estimate, *id))
        .map(|(_, id, bytes)| (id, bytes));
    if let Some((id, bytes)) = &chosen {
        report.dictionary = Some(*id);
        report.dictionary_bytes = bytes.len();
    }
    (chosen, report)
}

#[cfg(test)]
mod tests {
    use super::*;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn random(state: &mut u64, length: usize) -> Vec<u8> {
        (0..length)
            .map(|_| {
                *state ^= *state << 13;
                *state ^= *state >> 7;
                *state ^= *state << 17;
                state.to_le_bytes()[0]
            })
            .collect()
    }

    /// Pages that share one structure-like prefix and differ in their tails.
    fn family(common: &[u8], state: &mut u64, count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|_| {
                let mut page = common.to_vec();
                page.extend(random(state, 4096 - common.len()));
                page
            })
            .collect()
    }

    fn database(path: &Path, pages: &[Vec<u8>]) -> TestResult {
        let mut image = pages.concat();
        image[..16].copy_from_slice(b"SQLite format 3\0");
        image[16..18].copy_from_slice(&4096_u16.to_be_bytes());
        let mut store = crate::store::Store::open(path, true)?;
        store.write_at(0, &image)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        Ok(())
    }

    fn preferred(path: &Path) -> Result<Vec<DictionaryId>, StoreError> {
        let storage =
            super::super::Storage::for_sidecar(&crate::backend::sidecar_dir(path), false)?;
        Ok(storage.open_sealed()?.metadata.preferred.clone())
    }

    #[test]
    fn merged_reservoirs_are_the_bounded_sample_of_their_union() {
        let pages: Vec<_> = (0_u32..512)
            .map(|index| {
                let mut page = vec![0; 4096];
                page[..4].copy_from_slice(&index.to_le_bytes());
                (*blake3::hash(&page).as_bytes(), page)
            })
            .collect();
        let mut whole = Pool::new(256 * 1024);
        for (id, page) in &pages {
            whole.insert(*id, page.clone(), 0);
        }
        // Each reservoir is itself already bounded and digest ordered.
        let mut merged = Pool::new(256 * 1024);
        for (origin, half) in pages.chunks(256).enumerate() {
            let mut half: Vec<_> = half.to_vec();
            half.sort();
            for (id, page) in half.into_iter().take(64) {
                if !merged.insert(id, page, origin) {
                    break;
                }
            }
        }
        assert_eq!(
            whole.pages.keys().collect::<Vec<_>>(),
            merged.pages.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn related_databases_seed_the_first_seal_with_a_shared_dictionary() -> TestResult {
        let directory = tempfile::tempdir()?;
        let mut state = 17;
        let common = random(&mut state, 3072);
        let related: Vec<_> = (0..3)
            .map(|index| directory.path().join(format!("related-{index}.zsqlite")))
            .collect();
        for path in &related {
            database(path, &family(&common, &mut state, 320))?;
            assert!(
                !preferred(path)?.is_empty(),
                "relative trained a dictionary"
            );
        }
        let mut candidates = related.clone();
        candidates.push(directory.path().join("missing.zsqlite"));
        let path = directory.path().join("new.zsqlite");
        let report = crate::create_with_dictionary_from(&path, &candidates)?;
        assert_eq!((report.related, report.skipped), (3, 1));
        assert!(report.candidates >= 3);
        let id = report.dictionary.ok_or("no dictionary adopted")?;
        assert!(
            related
                .iter()
                .any(|path| preferred(path).is_ok_and(|ids| ids.contains(&id)))
        );

        // A small first seal cannot train, but it adopts the seed as the general
        // fallback; later trained specialists never evict that slot.
        database(&path, &family(&common, &mut state, 8))?;
        assert_eq!(preferred(&path)?, [id]);
        let inspect = crate::inspect(&path)?;
        assert!(
            inspect
                .frame_distribution
                .iter()
                .any(|bin| bin.dictionary_frames > 0)
        );
        let mut store = crate::store::Store::open_existing(&path)?;
        store.write_at(4096, &family(&common, &mut state, 1)[0])?;
        store.publish(true)?;
        store.flush_sidecars()?;
        drop(store);
        assert!(!crate::backend::sidecar_dir(&path).join(SEED).exists());
        assert_eq!(preferred(&path)?[0], id);
        crate::verify(&path)?;

        assert!(matches!(
            crate::create_with_dictionary_from(&path, &related),
            Err(StoreError::DestinationExists(_))
        ));
        let catalog = super::super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
        assert!(matches!(
            record(&catalog.lock()?, b"late"),
            Err(StoreError::InvalidConfiguration(_))
        ));
        Ok(())
    }

    /// An ordinary `SQLite` file holding these pages; returns its bytes.
    fn sqlite_file(path: &Path, pages: &[Vec<u8>]) -> Result<Vec<u8>, std::io::Error> {
        let mut image = pages.concat();
        image[..16].copy_from_slice(b"SQLite format 3\0");
        image[16..18].copy_from_slice(&4096_u16.to_be_bytes());
        image[18] = 1;
        image[19] = 1;
        image[21..24].copy_from_slice(&[64, 32, 32]);
        std::fs::write(path, &image)?;
        Ok(image)
    }

    fn related_family(
        directory: &Path,
        state: &mut u64,
    ) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
        let common = random(state, 3072);
        let related: Vec<_> = (0..3)
            .map(|index| directory.join(format!("related-{index}.zsqlite")))
            .collect();
        for path in &related {
            database(path, &family(&common, state, 320))?;
        }
        Ok(related)
    }

    #[test]
    fn adoption_charges_the_dictionary_against_the_target_size() {
        // A dictionary that removes 70% of the payload still costs 768 KiB once.
        let score = Score {
            payload: 30,
            plain: 100,
        };
        let dictionary = 768 * 1024;
        let small = 64 * 1024;
        assert_eq!(adoption_estimate(score, small, dictionary), None);
        let large = 64 * 1024 * 1024;
        assert_eq!(
            adoption_estimate(score, large, dictionary),
            Some(large * 3 / 10 + 768 * 1024)
        );
        // Break-even needs the savings to cover the dictionary plus 5%.
        let break_even = 768 * 1024 * 100 / 65;
        assert!(adoption_estimate(score, break_even + 1, dictionary).is_some());
        assert!(adoption_estimate(score, break_even - 1024, dictionary).is_none());
    }

    #[test]
    fn a_tiny_target_rejects_a_dictionary_a_large_target_adopts() -> TestResult {
        let directory = tempfile::tempdir()?;
        let related = related_family(directory.path(), &mut 31)?;
        let (tiny, report) = choose(&related, 3, SeedTarget::Logical(4096));
        assert!(tiny.is_none(), "one page cannot repay the dictionary");
        assert_eq!((report.related, report.candidates), (3, 3));
        let (large, _) = choose(&related, 3, SeedTarget::Logical(64 * 1024 * 1024));
        let (median, _) = choose(&related, 3, SeedTarget::RelatedMedian);
        let (id, bytes) = large.ok_or("a large target adopts the dictionary")?;
        assert!(bytes.len() > 4096);
        assert_eq!(median.map(|(id, _)| id), Some(id));
        Ok(())
    }

    #[test]
    fn seeded_conversion_adopts_a_related_dictionary_on_its_first_seal() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = |name: &str| directory.path().join(name);
        let storage = |name: &str| crate::facade::storage_path(&path(name));
        let mut state = 29;
        let common = random(&mut state, 3072);
        // The largest database of the family converts first and trains its own
        // dictionary from its samples.
        sqlite_file(&path("large.sqlite"), &family(&common, &mut state, 320))?;
        crate::convert_to_zsqlite(path("large.sqlite"), path("large.db"))?;
        let trained = preferred(&storage("large.db"))?;
        assert!(!trained.is_empty(), "the large conversion trained");

        let small = sqlite_file(&path("small.sqlite"), &family(&common, &mut state, 8))?;
        crate::create_with_dictionary_from(path("unsealed.db"), std::iter::empty::<PathBuf>())?;
        // An unsealed database, an ordinary SQLite file and a missing path are
        // skipped; the source and destination are excluded.
        let related = [
            "large.db",
            "unsealed.db",
            "large.sqlite",
            "missing.db",
            "small.sqlite",
            "small.db",
        ]
        .map(path);
        let policy = crate::StoragePolicy::default();
        let (info, report) = crate::convert_to_zsqlite_with_dictionary_from(
            path("small.sqlite"),
            path("small.db"),
            policy,
            &related,
        )?;
        assert_eq!((report.related, report.skipped), (1, 3));
        let id = report.dictionary.ok_or("no seed chosen")?;
        assert!(trained.contains(&id));
        assert_eq!(preferred(&storage("small.db"))?, [id]);
        assert!(
            info.frame_distribution
                .iter()
                .any(|bin| bin.dictionary_frames > 0)
        );
        crate::export_to_sqlite(path("small.db"), path("small-export.sqlite"))?;
        assert_eq!(std::fs::read(path("small-export.sqlite"))?, small);

        let unseeded = crate::convert_to_zsqlite(path("small.sqlite"), path("unseeded.db"))?;
        assert_eq!(unseeded.preferred_dictionaries, 0);
        // Including its own copy of the dictionary, the seeded bundle is smaller.
        assert!(info.sealed_object_bytes < unseeded.sealed_object_bytes);

        // The source's own length is the target: one page cannot repay it.
        let tiny = sqlite_file(&path("tiny.sqlite"), &family(&common, &mut state, 1))?;
        let (info, report) = crate::convert_to_zsqlite_with_dictionary_from(
            path("tiny.sqlite"),
            path("tiny.db"),
            policy,
            [path("large.db")],
        )?;
        assert_eq!((report.related, report.dictionary), (1, None));
        assert_eq!(info.preferred_dictionaries, 0);
        crate::export_to_sqlite(path("tiny.db"), path("tiny-export.sqlite"))?;
        assert_eq!(std::fs::read(path("tiny-export.sqlite"))?, tiny);
        Ok(())
    }

    #[test]
    fn a_dictionary_is_not_credited_with_its_holders_own_pages() -> TestResult {
        let mut state = 23;
        let shared = family(&random(&mut state, 3072), &mut state, 192);
        let outlier = family(&random(&mut state, 3072), &mut state, 576);
        let mut pool = Pool::new(POOL_BYTES);
        // Origins 0 and 1 share a structure. Origin 2 is larger and unrelated.
        for (index, page) in shared.iter().enumerate() {
            pool.insert(*blake3::hash(page).as_bytes(), page.clone(), index % 2);
        }
        for page in &outlier {
            pool.insert(*blake3::hash(page).as_bytes(), page.clone(), 2);
        }
        pool.measure(3).ok_or("measure")?;
        let holder: Vec<_> = shared.iter().step_by(2).collect();
        let shared_dictionary = zstd::dict::from_samples(&holder, 16 * 1024)?;
        let outlier_dictionary = zstd::dict::from_samples(&outlier, 16 * 1024)?;
        let ratio = |dictionary: &[u8], holders: &BTreeSet<usize>| -> Result<u128, String> {
            let score = pool
                .score(3, dictionary, holders, usize::MAX)
                .ok_or("score")?;
            Ok(score.payload * 1000 / score.plain)
        };
        let everyone = BTreeSet::from([0, 1, 2]);
        // Credited with its own pages, the outlier's dictionary looks best.
        assert!(ratio(&outlier_dictionary, &everyone)? < ratio(&shared_dictionary, &everyone)?);
        // Scored on other databases, the shared structure generalizes.
        assert!(
            ratio(&shared_dictionary, &BTreeSet::from([0]))?
                < ratio(&outlier_dictionary, &BTreeSet::from([2]))?
        );
        Ok(())
    }
}
