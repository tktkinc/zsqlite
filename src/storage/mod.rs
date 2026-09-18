//! Sealed metadata runs, immutable payload blobs, and publication capabilities.
//!
//! The catalogue lock serializes publication, root changes, and collection.
//! A durable receipt establishes installation, not liveness or content validity.
//! Only a validated root traversal can authorize object deletion.
#![forbid(unsafe_code)]

pub mod adapter;
#[cfg(test)]
mod adapter_tests;
mod cache;
mod catalog;
mod handle;
mod pack;
mod placement;
pub use adapter::{BackendError, FaultBackend, FilesystemBackend, MemoryBackend, StorageBackend};
pub use handle::{Database, Storage};
pub use placement::{
    BlobExtent, BlobIndex, LocatedRange, PackRange, PlacementPin, RelocationReport,
};
#[cfg(test)]
pub(crate) mod faults;
mod frame;
mod objects;
mod repack;
mod retention;
mod samples;
mod seal;
mod segment;
mod view;
pub(crate) mod wire;
pub(crate) use cache::PageCache;
pub use repack::PackOccupancy;
pub(crate) use repack::{eligible_pack, repack};
pub use retention::{DurablePin, GcReport, MaintenanceReport, RetentionName};
pub(crate) use seal::{ManifestMode, SealEndpoint, seal};
pub(crate) use view::DurableView;
pub(crate) use view::checkpoint_manifest;
pub use view::{FrameDistribution, ManifestStatistics, PinnedView, ResolvedPage};
#[cfg(test)]
mod delta_tests;
#[cfg(test)]
mod tests;
pub(crate) use objects::{Catalog, CatalogGuard};
#[cfg(test)]
pub(crate) use objects::{Dictionary, Manifest};
