//! Advisory bounded committed-page reservoir. No decoding or GC dependency.
use super::objects::CatalogGuard;
use super::wire::{Decoder, envelope, open_envelope, u32_bytes, u64_bytes};
use crate::StoreError;
use crate::dictionary::{DictionaryPolicy, MAX_SAMPLE_BYTES, SampleBudget};
use std::collections::BTreeMap;

pub(super) struct Samples {
    pages: BTreeMap<[u8; 32], Vec<u8>>,
    bytes: usize,
    fresh: usize,
    budget: SampleBudget,
    dirty: bool,
}
impl Samples {
    fn new(budget: SampleBudget) -> Self {
        Self {
            pages: BTreeMap::new(),
            bytes: 0,
            fresh: 0,
            budget,
            dirty: false,
        }
    }
    pub(super) fn load(guard: &CatalogGuard, budget: SampleBudget) -> Self {
        Self::decode(guard, budget).unwrap_or_else(|_| Self::new(budget))
    }
    fn decode(guard: &CatalogGuard, budget: SampleBudget) -> Result<Self, StoreError> {
        let path = guard.root().join("dictionary.samples");
        if path.metadata()?.len() > (MAX_SAMPLE_BYTES + 2 * 1024 * 1024) as u64 {
            return Err(StoreError::Range);
        }
        let raw = open_envelope(
            b"ZSAMPLE1",
            &std::fs::read(path)?,
            MAX_SAMPLE_BYTES + 1024 * 1024,
        )?;
        let mut wire = Decoder::new(&raw);
        let fresh = usize::try_from(wire.u64()?).map_err(|_| StoreError::Range)?;
        let count = wire.u32()?;
        if count as usize > MAX_SAMPLE_BYTES / 512 {
            return Err(StoreError::Range);
        }
        let mut result = Self {
            pages: BTreeMap::new(),
            bytes: 0,
            fresh: fresh.min(budget.get()),
            budget,
            dirty: fresh > budget.get(),
        };
        for _ in 0..count {
            let length = wire.u32()?;
            crate::domain::PageSize::new(length)?;
            let bytes = wire.take(length as usize)?.to_vec();
            result.bytes += bytes.len();
            if result.bytes > MAX_SAMPLE_BYTES
                || result
                    .pages
                    .insert(*blake3::hash(&bytes).as_bytes(), bytes)
                    .is_some()
            {
                return Err(StoreError::Corrupt(0));
            }
        }
        wire.finish()?;
        result.trim();
        Ok(result)
    }
    pub(super) fn insert(&mut self, bytes: Vec<u8>) {
        let Ok(length) = u32::try_from(bytes.len()) else {
            return;
        };
        if crate::domain::PageSize::new(length).is_err() {
            return;
        }
        let id = *blake3::hash(&bytes).as_bytes();
        if self.pages.contains_key(&id) {
            return;
        }
        // Digest order is a deterministic uniform priority reservoir. Repeated
        // rejected content does not count as newly available training data.
        if self.bytes + bytes.len() > self.budget.get()
            && self
                .pages
                .last_key_value()
                .is_some_and(|(last, _)| id >= *last)
        {
            return;
        }
        self.bytes += bytes.len();
        self.fresh = self
            .fresh
            .saturating_add(bytes.len())
            .min(self.budget.get());
        self.pages.insert(id, bytes);
        self.dirty = true;
        self.trim();
    }
    fn trim(&mut self) {
        while self.bytes > self.budget.get() {
            if let Some((_, bytes)) = self.pages.pop_last() {
                self.bytes -= bytes.len();
                self.dirty = true;
            }
        }
    }
    pub(super) fn evaluation(&mut self) -> Option<Evaluation<'_>> {
        if self.fresh < 1024 * 1024 || self.pages.len() < 32 {
            return None;
        }
        self.fresh = 0;
        self.dirty = true;
        let (held_out, training): (Vec<_>, Vec<_>) = self
            .pages
            .values()
            .enumerate()
            .partition(|(index, _)| index % 5 == 0);
        Some(Evaluation {
            training: training
                .into_iter()
                .map(|(_, bytes)| bytes.as_slice())
                .collect(),
            held_out: held_out
                .into_iter()
                .map(|(_, bytes)| bytes.as_slice())
                .collect(),
        })
    }
    pub(super) fn persist(&self, guard: &CatalogGuard) -> Result<(), StoreError> {
        if !self.dirty {
            return Ok(());
        }
        let mut raw = Vec::with_capacity(self.bytes + self.pages.len() * 4 + 12);
        u64_bytes(&mut raw, self.fresh as u64);
        u32_bytes(
            &mut raw,
            u32::try_from(self.pages.len()).map_err(|_| StoreError::Range)?,
        );
        for bytes in self.pages.values() {
            u32_bytes(
                &mut raw,
                u32::try_from(bytes.len()).map_err(|_| StoreError::Range)?,
            );
            raw.extend(bytes);
        }
        let bytes = envelope(b"ZSAMPLE1", &raw)?;
        super::retention::atomic_write(&guard.root().join("dictionary.samples"), &bytes)
    }
}

/// Disjoint training and held-out samples borrowed from a sufficiently refreshed
/// reservoir. Evaluation never consumes the pool: later seals can grow it.
pub(super) struct Evaluation<'a> {
    training: Vec<&'a [u8]>,
    held_out: Vec<&'a [u8]>,
}
impl Evaluation<'_> {
    pub(super) fn select(
        &self,
        policy: DictionaryPolicy,
        level: i32,
        logical_bytes: crate::domain::LogicalBytes,
        existing: impl Iterator<Item = impl AsRef<[u8]>>,
    ) -> Option<Vec<u8>> {
        let training_bytes = self.training.iter().map(|bytes| bytes.len()).sum();
        let held_out_bytes: usize = self.held_out.iter().map(|bytes| bytes.len()).sum();
        // Scale held-out payload costs to the current image and charge the full
        // new dictionary once. Existing dictionaries are already retained. Keep
        // this in integer units of (stored bytes * held-out bytes), without
        // rounding a small candidate's advantage away.
        let score = |dictionary: &[u8], added_bytes: usize| -> Option<u128> {
            let mut compressor = zstd::bulk::Compressor::with_dictionary(level, dictionary).ok()?;
            let payload = self.held_out.iter().try_fold(0_u128, |sum, bytes| {
                let compressed = compressor.compress(bytes).ok()?.len();
                let reference_bytes = if dictionary.is_empty() { 0 } else { 32 };
                Some(sum + compressed.saturating_add(reference_bytes).min(bytes.len()) as u128)
            })?;
            Some(
                payload * u128::from(logical_bytes.get())
                    + added_bytes as u128 * held_out_bytes as u128,
            )
        };
        let previous = existing
            .filter_map(|bytes| score(bytes.as_ref(), 0))
            .chain(score(&[], 0))
            .min()?;
        policy
            .candidates(training_bytes)
            .into_iter()
            .filter_map(|capacity| {
                // A failed training/evaluation attempt must never prevent a seal.
                let candidate =
                    zstd::dict::from_samples(&self.training, capacity.get() as usize).ok()?;
                let cost = score(&candidate, candidate.len())?;
                (cost * 100 <= previous * 95).then_some((cost, candidate))
            })
            .min_by_key(|(cost, _)| *cost)
            .map(|(_, candidate)| candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type TestResult = Result<(), Box<dyn std::error::Error>>;
    fn page(index: u32) -> Vec<u8> {
        let mut bytes = vec![0; 4096];
        bytes[..4].copy_from_slice(&index.to_le_bytes());
        bytes
    }

    #[test]
    fn bounded_reservoir_covers_the_whole_input_independent_of_order() -> TestResult {
        let budget = SampleBudget::new(1024 * 1024)?;
        let mut forward = Samples::new(budget);
        let mut reverse = Samples::new(budget);
        for index in 0..4096 {
            forward.insert(page(index));
        }
        for index in (0..4096).rev() {
            reverse.insert(page(index));
        }
        assert_eq!(forward.pages, reverse.pages);
        assert_eq!(forward.bytes, budget.get());
        assert!(
            forward
                .pages
                .values()
                .any(|bytes| u32::from_le_bytes(bytes[..4].try_into().unwrap()) >= 3072)
        );
        forward.evaluation().ok_or("missing evaluation")?;
        for index in 0..4096 {
            forward.insert(page(index));
        }
        assert_eq!(
            forward.fresh, 0,
            "rejected/duplicate samples must not retrigger training"
        );
        assert!(forward.evaluation().is_none());
        Ok(())
    }

    #[test]
    fn samples_survive_tiny_seals_evaluation_and_reopen_and_grow_past_old_limit() -> TestResult {
        let directory = tempfile::tempdir()?;
        let catalog = crate::storage::Catalog::open(directory.path(), true)?;
        let guard = catalog.lock()?;
        let budget = DictionaryPolicy::default().sample_budget();
        // Four separate 256 KiB seals accumulate the first useful training set.
        for seal_index in 0..4 {
            let mut pool = Samples::load(&guard, budget);
            for index in seal_index * 64..(seal_index + 1) * 64 {
                pool.insert(page(index));
            }
            if seal_index < 3 {
                assert!(pool.evaluation().is_none());
            } else {
                let evaluation = pool.evaluation().ok_or("no accumulated evaluation")?;
                assert!(
                    evaluation
                        .training
                        .iter()
                        .all(|page| !evaluation.held_out.contains(page))
                );
                let training_bytes = evaluation.training.iter().map(|bytes| bytes.len()).sum();
                assert_eq!(
                    DictionaryPolicy::default().candidates(training_bytes)[0].get(),
                    8192
                );
            }
            pool.persist(&guard)?;
        }
        let mut pool = Samples::load(&guard, budget);
        assert_eq!(pool.pages.len(), 256);
        assert_eq!(pool.fresh, 0);
        assert!(!pool.dirty);
        let sample_path = guard.root().join("dictionary.samples");
        let modified = sample_path.metadata()?.modified()?;
        pool.persist(&guard)?;
        assert_eq!(
            sample_path.metadata()?.modified()?,
            modified,
            "idle seals do not rewrite samples"
        );
        for index in 256..2560 {
            pool.insert(page(index));
        }
        assert_eq!(pool.bytes, 10 * 1024 * 1024);
        assert!(pool.evaluation().is_some());
        pool.persist(&guard)?;
        let loaded = Samples::load(&guard, budget);
        assert_eq!(loaded.pages, pool.pages);
        assert_eq!(loaded.fresh, 0);
        let trimmed = Samples::load(&guard, SampleBudget::new(1024 * 1024)?);
        assert_eq!(trimmed.bytes, 1024 * 1024);
        assert!(trimmed.dirty);
        Ok(())
    }

    #[test]
    fn corrupt_samples_are_only_advisory() -> TestResult {
        let directory = tempfile::tempdir()?;
        let catalog = crate::storage::Catalog::open(directory.path(), true)?;
        let guard = catalog.lock()?;
        std::fs::write(guard.root().join("dictionary.samples"), b"truncated")?;
        assert_eq!(
            Samples::load(&guard, DictionaryPolicy::default().sample_budget()).bytes,
            0
        );
        Ok(())
    }
}
