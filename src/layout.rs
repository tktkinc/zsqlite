//! Validated frame, pack, cache, codec, and maintenance policy.
#![forbid(unsafe_code)]

use crate::domain::{CacheBytes, DecodedBytes, PageSize, StoredBytes, ValueError};

/// Plaintext page-cache sizing. Automatic capacity is resolved when the
/// disposable cache file is created, using the database size and free space on
/// that file's filesystem.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheCapacity {
    /// Use 20% of the logical database size when the temporary filesystem has
    /// enough free space.
    Automatic,
    /// Use exactly this byte cap. Zero disables the cache.
    Fixed(CacheBytes),
}

impl From<CacheBytes> for CacheCapacity {
    fn from(bytes: CacheBytes) -> Self {
        Self::Fixed(bytes)
    }
}

impl CacheCapacity {
    pub(crate) fn resolve(self, logical_bytes: u64, available_bytes: Option<u64>) -> CacheBytes {
        match self {
            Self::Fixed(bytes) => bytes,
            Self::Automatic => {
                let target = logical_bytes / 5;
                // Do not let a disposable cache consume more than half of the
                // space currently available to its backing file.
                let disk_cap = available_bytes.map_or(target, |bytes| bytes / 2);
                CacheBytes::new(target.min(disk_cap))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameLayout {
    Fixed(DecodedBytes),
    Page,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LayoutPolicy {
    frames: FrameLayout,
    pack_target: StoredBytes,
    zstd_level: i32,
    cache: CacheCapacity,
    maintenance_input: DecodedBytes,
    deletion_budget: usize,
}

impl Default for LayoutPolicy {
    fn default() -> Self {
        Self {
            frames: FrameLayout::Page,
            pack_target: StoredBytes::new(4 * 1024 * 1024),
            zstd_level: 3,
            cache: CacheCapacity::Automatic,
            maintenance_input: DecodedBytes::new(64 * 1024 * 1024),
            deletion_budget: 8,
        }
    }
}

impl LayoutPolicy {
    pub(crate) fn encode(self) -> [u8; 128] {
        let mut bytes = [0; 128];
        let frame_bytes = match self.frames {
            FrameLayout::Page => 0,
            FrameLayout::Fixed(bytes) => bytes.get(),
        };
        let mut put = |offset: usize, value: u64| {
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        };
        put(0, frame_bytes);
        put(
            8,
            u64::try_from(self.zstd_level).expect("validated positive codec level"),
        );
        let (cache_mode, cache_bytes) = match self.cache {
            CacheCapacity::Automatic => (0, 0),
            CacheCapacity::Fixed(bytes) => (1, bytes.get()),
        };
        put(16, cache_bytes);
        put(24, self.maintenance_input.get());
        put(32, self.deletion_budget as u64);
        put(40, self.pack_target.get());
        put(48, cache_mode);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8; 128]) -> Result<Self, ValueError> {
        if bytes[56..].iter().any(|byte| *byte != 0) {
            return Err(ValueError);
        }
        let get = |offset: usize| {
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("fixed policy"))
        };
        let mut policy = Self::default()
            .with_level(i32::try_from(get(8)).map_err(|_| ValueError)?)?
            .with_maintenance(
                DecodedBytes::new(get(24)),
                usize::try_from(get(32)).map_err(|_| ValueError)?,
            )?
            .with_pack_target(StoredBytes::new(get(40)))?;
        policy.cache = match get(48) {
            0 if get(16) == 0 => CacheCapacity::Automatic,
            1 => CacheCapacity::Fixed(CacheBytes::new(get(16))),
            _ => return Err(ValueError),
        };
        if get(0) != 0 {
            policy = policy.fixed(DecodedBytes::new(get(0)))?;
        }
        Ok(policy)
    }

    pub fn fixed(mut self, bytes: DecodedBytes) -> Result<Self, ValueError> {
        if bytes.get() < 512 || bytes.get() > 8 * 1024 * 1024 || !bytes.get().is_power_of_two() {
            return Err(ValueError);
        }
        self.frames = FrameLayout::Fixed(bytes);
        Ok(self)
    }

    pub fn with_pack_target(mut self, bytes: StoredBytes) -> Result<Self, ValueError> {
        if bytes.get() == 0 {
            return Err(ValueError);
        }
        self.pack_target = bytes;
        Ok(self)
    }

    pub fn with_level(mut self, level: i32) -> Result<Self, ValueError> {
        if !(1..=22).contains(&level) {
            return Err(ValueError);
        }
        self.zstd_level = level;
        Ok(self)
    }

    /// Set a fixed cap for the private plaintext cache file, including slot
    /// alignment padding. Zero disables the cache. Metadata and transient frame
    /// decoding are separate RAM costs.
    pub fn with_cache(mut self, bytes: CacheBytes) -> Result<Self, ValueError> {
        bytes.as_usize()?;
        self.cache = CacheCapacity::Fixed(bytes);
        Ok(self)
    }

    #[must_use]
    /// Restore automatic sizing after selecting a fixed cache capacity.
    pub fn with_automatic_cache(mut self) -> Self {
        self.cache = CacheCapacity::Automatic;
        self
    }

    pub fn with_maintenance(
        mut self,
        input: DecodedBytes,
        deletions: usize,
    ) -> Result<Self, ValueError> {
        if input.get() > 64 * 1024 * 1024 {
            return Err(ValueError);
        }
        self.maintenance_input = input;
        self.deletion_budget = deletions;
        Ok(self)
    }

    #[must_use]
    pub const fn level(self) -> i32 {
        self.zstd_level
    }

    #[must_use]
    pub const fn cache(self) -> CacheCapacity {
        self.cache
    }

    #[must_use]
    pub const fn maintenance_input(self) -> DecodedBytes {
        self.maintenance_input
    }

    #[must_use]
    pub const fn deletion_budget(self) -> usize {
        self.deletion_budget
    }

    #[must_use]
    pub const fn frames(self) -> FrameLayout {
        self.frames
    }

    #[must_use]
    pub const fn pack_target(self) -> StoredBytes {
        self.pack_target
    }

    #[must_use]
    pub fn frame_bytes(self, size: PageSize) -> usize {
        let cap = match self.frames {
            FrameLayout::Page => u64::from(size.get()),
            FrameLayout::Fixed(bytes) => bytes.get().max(u64::from(size.get())),
        };
        usize::try_from(cap).expect("bounded frame cap")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_roundtrips() {
        let policy = LayoutPolicy::default()
            .fixed(DecodedBytes::new(1024 * 1024))
            .unwrap()
            .with_pack_target(StoredBytes::new(8 * 1024 * 1024))
            .unwrap();
        assert_eq!(LayoutPolicy::decode(&policy.encode()).unwrap(), policy);
        let disabled = LayoutPolicy::default()
            .with_cache(CacheBytes::new(0))
            .unwrap();
        assert_eq!(LayoutPolicy::decode(&disabled.encode()).unwrap(), disabled);
        assert_eq!(
            CacheCapacity::Automatic.resolve(1000, Some(10_000)),
            CacheBytes::new(200)
        );
        assert_eq!(
            CacheCapacity::Automatic.resolve(1000, Some(100)),
            CacheBytes::new(50)
        );
        assert_eq!(
            CacheCapacity::Automatic.resolve(1000, None),
            CacheBytes::new(200)
        );
        assert_eq!(
            CacheCapacity::Fixed(CacheBytes::new(17)).resolve(1000, Some(2)),
            CacheBytes::new(17)
        );
    }
}
