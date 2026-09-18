//! Validated dictionary training limits. Sizes are ceilings, not allocations.
#![forbid(unsafe_code)]

use crate::StoreError;
use std::num::{NonZeroU32, NonZeroUsize};

pub(crate) const MAX_DICTIONARY_BYTES: u32 = 768 * 1024;
pub(crate) const MAX_SAMPLE_BYTES: usize = 96 * 1024 * 1024;

/// A supported nonzero dictionary capacity, distinct from a sample budget.
///
/// ```compile_fail
/// use zsqlite::dictionary::{DictionaryCapacity, SampleBudget};
/// let capacity: DictionaryCapacity = SampleBudget::new(1024 * 1024).unwrap();
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DictionaryCapacity(NonZeroU32);
impl DictionaryCapacity {
    pub fn new(bytes: u32) -> Result<Self, StoreError> {
        if !(8192..=MAX_DICTIONARY_BYTES).contains(&bytes) {
            return Err(StoreError::InvalidConfiguration(
                "dictionary capacity must be 8–768 KiB",
            ));
        }
        Ok(Self(NonZeroU32::new(bytes).ok_or(StoreError::Range)?))
    }
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// A bounded, addressable committed-sample budget (1–96 MiB).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SampleBudget(NonZeroUsize);
impl SampleBudget {
    pub fn new(bytes: u64) -> Result<Self, StoreError> {
        let bytes = usize::try_from(bytes).map_err(|_| StoreError::Range)?;
        if !(1024 * 1024..=MAX_SAMPLE_BYTES).contains(&bytes) {
            return Err(StoreError::InvalidConfiguration(
                "sample budget must be 1–96 MiB",
            ));
        }
        Ok(Self(NonZeroUsize::new(bytes).ok_or(StoreError::Range)?))
    }
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DictionaryTraining {
    Disabled,
    UpTo(DictionaryCapacity),
}

/// Dictionary candidates grow with available distinct samples, up to this limit.
/// Zero in `new` disables training; the domain representation has no zero sentinel.
/// Existing shared dictionaries remain usable when training is disabled.
///
/// ```
/// use zsqlite::DictionaryPolicy;
/// use zsqlite::dictionary::{DictionaryCapacity, DictionaryTraining};
/// let policy = DictionaryPolicy::new(768 * 1024, 96 * 1024 * 1024)?;
/// assert_eq!(policy.training(), DictionaryTraining::UpTo(DictionaryCapacity::new(768 * 1024)?));
/// # Ok::<(), zsqlite::StoreError>(())
/// ```
/// ```compile_fail
/// use zsqlite::DictionaryPolicy;
/// let invalid = DictionaryPolicy { dictionary_bytes: 1, sample_bytes: 0 };
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DictionaryPolicy {
    training: DictionaryTraining,
    samples: SampleBudget,
}
impl Default for DictionaryPolicy {
    fn default() -> Self {
        Self::new(MAX_DICTIONARY_BYTES, MAX_SAMPLE_BYTES as u64)
            .expect("supported default dictionary policy")
    }
}
impl DictionaryPolicy {
    pub fn new(dictionary_bytes: u32, sample_bytes: u64) -> Result<Self, StoreError> {
        Ok(Self {
            training: if dictionary_bytes == 0 {
                DictionaryTraining::Disabled
            } else {
                DictionaryTraining::UpTo(DictionaryCapacity::new(dictionary_bytes)?)
            },
            samples: SampleBudget::new(sample_bytes)?,
        })
    }
    #[must_use]
    pub const fn training(self) -> DictionaryTraining {
        self.training
    }
    #[must_use]
    pub const fn sample_budget(self) -> SampleBudget {
        self.samples
    }
    #[must_use]
    pub const fn dictionary_bytes(self) -> u32 {
        match self.training {
            DictionaryTraining::Disabled => 0,
            DictionaryTraining::UpTo(capacity) => capacity.get(),
        }
    }
    #[must_use]
    pub const fn sample_bytes(self) -> u64 {
        self.samples.get() as u64
    }

    /// Evaluate the two largest supported tiers with at least 100 training bytes
    /// per dictionary byte. Held-out bytes must not be passed as training bytes.
    pub(crate) fn candidates(self, training_bytes: usize) -> Vec<DictionaryCapacity> {
        let limit = (training_bytes / 100).min(self.dictionary_bytes() as usize);
        let mut sizes: Vec<_> = [8, 16, 32, 64, 128, 256, 512, 768]
            .into_iter()
            .map(|kib| kib * 1024)
            .filter(|bytes| *bytes <= limit)
            .collect();
        let maximum = self.dictionary_bytes() as usize;
        if maximum >= 8192 && maximum <= limit && !sizes.contains(&maximum) {
            sizes.push(maximum);
            sizes.sort_unstable();
        }
        sizes
            .into_iter()
            .rev()
            .take(2)
            .filter_map(|bytes| DictionaryCapacity::new(u32::try_from(bytes).ok()?).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validated_limits_and_integer_constructor() {
        for capacity in [0, 8192, 65536, 512 * 1024, 700 * 1024, MAX_DICTIONARY_BYTES] {
            assert!(DictionaryPolicy::new(capacity, MAX_SAMPLE_BYTES as u64).is_ok());
        }
        for capacity in [1, 8191, MAX_DICTIONARY_BYTES + 1, u32::MAX] {
            assert!(DictionaryPolicy::new(capacity, 1024 * 1024).is_err());
        }
        for budget in [0, 1024 * 1024 - 1, MAX_SAMPLE_BYTES as u64 + 1, u64::MAX] {
            assert!(DictionaryPolicy::new(65536, budget).is_err());
        }
        assert!(DictionaryPolicy::new(65536, 8 * 1024 * 1024).is_ok());
    }
    #[test]
    fn sizes_follow_training_bytes_not_configured_capacity() {
        let policy = DictionaryPolicy::default();
        let sizes = |bytes| {
            policy
                .candidates(bytes)
                .iter()
                .map(|size| size.get())
                .collect::<Vec<_>>()
        };
        assert!(sizes(819_199).is_empty());
        assert_eq!(sizes(819_200), [8192]);
        assert_eq!(sizes(8 * 1024 * 1024 * 4 / 5), [65536, 32768]);
        assert_eq!(sizes(64 * 1024 * 1024 * 4 / 5), [512 * 1024, 256 * 1024]);
        assert_eq!(sizes(MAX_SAMPLE_BYTES * 4 / 5), [768 * 1024, 512 * 1024]);
        assert!(
            DictionaryPolicy::new(0, MAX_SAMPLE_BYTES as u64)
                .unwrap()
                .candidates(MAX_SAMPLE_BYTES)
                .is_empty()
        );
        assert_eq!(
            DictionaryPolicy::new(700 * 1024, MAX_SAMPLE_BYTES as u64)
                .unwrap()
                .candidates(MAX_SAMPLE_BYTES)[0]
                .get(),
            700 * 1024
        );
    }
}
