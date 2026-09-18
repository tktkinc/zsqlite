//! Validated storage values. Wire decoding establishes these facts once; private
//! fields preserve them. Object identity is deliberately distinct from integrity.
//!
//! IDs and units cannot be mixed:
//! ```compile_fail
//! use zsqlite::domain::{ManifestId, PackId};
//! fn wrong(pack: PackId) -> ManifestId { pack }
//! ```
//! ```compile_fail
//! use zsqlite::domain::{ManifestId, ViewHash};
//! fn wrong(physical: ManifestId) -> ViewHash { physical }
//! ```
//! ```compile_fail
//! use zsqlite::domain::{StoredBytes, DecodedBytes};
//! fn wrong(bytes: StoredBytes) -> DecodedBytes { bytes }
//! ```
//! Values cannot bypass validation:
//! ```compile_fail
//! use zsqlite::domain::PageNumber;
//! let page = PageNumber(0);
//! ```
//! ```
//! use zsqlite::domain::{PageNumber, PageSize, LogicalBytes};
//! let size = PageSize::new(4096)?;
//! let page = PageNumber::new(2)?;
//! assert_eq!(page.offset(size)?.get(), 4096);
//! assert_eq!(LogicalBytes::new(8192, size)?.pages(), 2);
//! # Ok::<(), zsqlite::domain::ValueError>(())
//! ```

#![forbid(unsafe_code)]

use std::num::{NonZeroU32, NonZeroU64};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("value is outside the validated storage domain")]
pub struct ValueError;

macro_rules! identities {
    ($visibility:vis, $($name:ident),+ $(,)?) => {$ (
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        $visibility struct $name([u8; 32]);
        impl $name {
            /// Parsing an identity does not verify or establish reachability.
            #[must_use]
            $visibility const fn from_bytes(bytes: [u8; 32]) -> Self { Self(bytes) }
            #[must_use]
            $visibility const fn as_bytes(&self) -> &[u8; 32] { &self.0 }
        }
    )+};
}

identities!(
    pub,
    DatabaseId,
    ManifestId,
    ViewHash,
    PackId,
    BlobId,
    IndexId,
    BackendId,
    DictionaryId
);
identities!(
    pub(crate),
    LineageId,
    RepresentationId,
    AttachmentId,
    FrameId,
    HistoryHash,
    PageChecksum,
    ContentRoot,
    RetentionId
);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) struct StagingId([u8; 32]);
impl StagingId {
    #[must_use]
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

/// `SQLite`'s power-of-two page-size domain, including the 64 KiB encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PageSize(NonZeroU32);
impl PageSize {
    pub fn new(bytes: u32) -> Result<Self, ValueError> {
        if !(512..=65536).contains(&bytes) || !bytes.is_power_of_two() {
            return Err(ValueError);
        }
        Ok(Self(NonZeroU32::new(bytes).ok_or(ValueError)?))
    }
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
    #[must_use]
    pub const fn as_usize(self) -> usize {
        self.get() as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PageNumber(NonZeroU32);
impl PageNumber {
    pub fn new(number: u32) -> Result<Self, ValueError> {
        // SQLite reserves 0xffffffff; zero is not a page.
        if number == u32::MAX {
            return Err(ValueError);
        }
        Ok(Self(NonZeroU32::new(number).ok_or(ValueError)?))
    }
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
    pub fn offset(self, size: PageSize) -> Result<FileOffset, ValueError> {
        u64::from(self.get() - 1)
            .checked_mul(u64::from(size.get()))
            .map(FileOffset)
            .ok_or(ValueError)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TransactionId(NonZeroU64);
impl TransactionId {
    pub fn new(value: u64) -> Result<Self, ValueError> {
        Ok(Self(NonZeroU64::new(value).ok_or(ValueError)?))
    }
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
    pub fn next(self) -> Result<Self, ValueError> {
        Self::new(self.get().checked_add(1).ok_or(ValueError)?)
    }
}

/// Inclusive version coverage, not a promise that intermediate snapshots exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransactionSpan {
    begin: TransactionId,
    end: TransactionId,
}

/// A segment's represented transactions within its fully resolved view span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentCoverage {
    full: TransactionSpan,
    begin: TransactionId,
}
impl SegmentCoverage {
    pub fn new(full: TransactionSpan, begin: TransactionId) -> Result<Self, ValueError> {
        if begin < full.begin() || begin > full.end() {
            return Err(ValueError);
        }
        Ok(Self { full, begin })
    }
    #[must_use]
    pub const fn full(self) -> TransactionSpan {
        self.full
    }
    #[must_use]
    pub const fn represented(self) -> TransactionSpan {
        TransactionSpan {
            begin: self.begin,
            end: self.full.end,
        }
    }
}

impl TransactionSpan {
    pub fn new(begin: TransactionId, end: TransactionId) -> Result<Self, ValueError> {
        if begin > end {
            return Err(ValueError);
        }
        Ok(Self { begin, end })
    }
    #[must_use]
    pub const fn begin(self) -> TransactionId {
        self.begin
    }
    #[must_use]
    pub const fn end(self) -> TransactionId {
        self.end
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryEndpoint {
    Genesis,
    Published(TransactionId),
}

macro_rules! byte_units {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(u64);
        impl $name {
            #[must_use]
            pub const fn new(value: u64) -> Self { Self(value) }
            #[must_use]
            pub const fn get(self) -> u64 { self.0 }
            pub fn as_usize(self) -> Result<usize, ValueError> {
                usize::try_from(self.0).map_err(|_| ValueError)
            }
        }
    )+};
}
byte_units!(
    FileOffset,
    PackOffset,
    BlobOffset,
    BlobBytes,
    StoredBytes,
    DecodedBytes,
    CacheBytes
);

/// A range whose end cannot overflow. Object-size validation remains dynamic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredRange {
    offset: FileOffset,
    length: StoredBytes,
    end: FileOffset,
}
impl StoredRange {
    pub fn new(offset: FileOffset, length: StoredBytes) -> Result<Self, ValueError> {
        let end = FileOffset(offset.get().checked_add(length.get()).ok_or(ValueError)?);
        Ok(Self {
            offset,
            length,
            end,
        })
    }
    pub fn within(self, object_size: StoredBytes) -> Result<Self, ValueError> {
        if self.end.get() > object_size.get() {
            Err(ValueError)
        } else {
            Ok(self)
        }
    }
    #[must_use]
    pub const fn offset(self) -> FileOffset {
        self.offset
    }
    #[must_use]
    pub const fn length(self) -> StoredBytes {
        self.length
    }
    #[must_use]
    pub const fn end(self) -> FileOffset {
        self.end
    }
}

/// An aligned logical size, including an empty image. Carries its page size.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalBytes {
    bytes: u64,
    page_size: PageSize,
}
impl LogicalBytes {
    pub fn new(bytes: u64, page_size: PageSize) -> Result<Self, ValueError> {
        if !bytes.is_multiple_of(u64::from(page_size.get()))
            || bytes / u64::from(page_size.get()) >= u64::from(u32::MAX)
        {
            return Err(ValueError);
        }
        Ok(Self { bytes, page_size })
    }
    #[must_use]
    pub const fn get(self) -> u64 {
        self.bytes
    }
    #[must_use]
    pub const fn page_size(self) -> PageSize {
        self.page_size
    }
    #[must_use]
    pub fn pages(self) -> u32 {
        u32::try_from(self.bytes / u64::from(self.page_size.get())).expect("validated size")
    }
}

/// A nonempty frame: decoded length is derived, not independently writable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameShape {
    page_size: PageSize,
    pages: NonZeroU32,
}
impl FrameShape {
    pub fn new(page_size: PageSize, pages: u32) -> Result<Self, ValueError> {
        let pages = NonZeroU32::new(pages).ok_or(ValueError)?;
        if u64::from(pages.get()) * u64::from(page_size.get()) > 8 * 1024 * 1024 {
            return Err(ValueError);
        }
        Ok(Self { page_size, pages })
    }
    #[must_use]
    pub const fn page_size(self) -> PageSize {
        self.page_size
    }
    #[must_use]
    pub const fn pages(self) -> u32 {
        self.pages.get()
    }
    #[must_use]
    pub fn decoded(self) -> DecodedBytes {
        DecodedBytes::new(u64::from(self.pages()) * u64::from(self.page_size.get()))
    }
    // Slots never leave their resolved view. No public constructor or accessor.
    pub(crate) fn slot(self, index: u32) -> Result<FrameSlot, ValueError> {
        if index < self.pages() {
            Ok(FrameSlot(index))
        } else {
            Err(ValueError)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrameSlot(u32);
impl FrameSlot {
    pub(crate) fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompressionDictionary {
    None,
    Shared(DictionaryId),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadEncoding {
    Raw,
    Zstandard(CompressionDictionary),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_boundaries() {
        for power in 9..=16 {
            assert!(PageSize::new(1 << power).is_ok());
        }
        for invalid in [0, 1, 511, 513, 131_072] {
            assert!(PageSize::new(invalid).is_err());
        }
        assert!(PageNumber::new(0).is_err());
        assert!(PageNumber::new(u32::MAX).is_err());
        assert!(TransactionId::new(u64::MAX).unwrap().next().is_err());
        assert!(StoredRange::new(FileOffset::new(u64::MAX), StoredBytes::new(1)).is_err());
        let size = PageSize::new(4096).unwrap();
        assert!(LogicalBytes::new(4097, size).is_err());
        assert!(FrameShape::new(size, 0).is_err());
        assert!(FrameShape::new(size, 2049).is_err());
        assert!(FrameShape::new(size, 1).unwrap().slot(1).is_err());
    }
}
