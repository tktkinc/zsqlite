use super::frame::FrameMetadata;
use super::objects::CatalogGuard;
use super::seal::{SealEndpoint, seal_with_copies};
use super::view::PinnedView;
use crate::StoreError;
use crate::domain::{DecodedBytes, FrameId, ManifestId, PackId, StoredBytes};
use crate::layout::LayoutPolicy;
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct PackOccupancy {
    pub pack: PackId,
    pub stored_bytes: StoredBytes,
    pub decoded_bytes: DecodedBytes,
    pub total_pages: u64,
    pub live_pages: u64,
    pub frames: usize,
    pub fully_live_frames: usize,
    pub partially_obsolete_frames: usize,
    pub fully_obsolete_frames: usize,
    pub fully_live_frame_bytes: StoredBytes,
    pub partially_obsolete_frame_bytes: StoredBytes,
    pub fully_obsolete_frame_bytes: StoredBytes,
}
impl PackOccupancy {
    #[must_use]
    pub fn reclaimable_bytes(&self) -> u64 {
        if self.total_pages == 0 {
            return self.stored_bytes.get();
        }
        self.stored_bytes
            .get()
            .saturating_mul(self.total_pages.saturating_sub(self.live_pages))
            / self.total_pages
    }

    #[must_use]
    pub const fn obsolete_pages(&self) -> u64 {
        self.total_pages.saturating_sub(self.live_pages)
    }

    /// Bytes belonging to complete frame records that no current page uses.
    /// Pack headers and partially obsolete frames are deliberately excluded.
    #[must_use]
    pub const fn whole_frame_reclaimable_bytes(&self) -> StoredBytes {
        self.fully_obsolete_frame_bytes
    }
}

pub(super) fn inventory(
    _guard: &CatalogGuard,
    view: &PinnedView,
) -> Result<Vec<PackOccupancy>, StoreError> {
    let mut output = Vec::new();
    let live = view.metadata.live_pages_by_pack();
    for pack in view.metadata.packs() {
        let length = view.placement.length(pack)?.get();
        let mut offset = super::segment::HEADER_SIZE as u64;
        let payload_end = length;
        let read = |offset, bytes: &mut [u8]| -> Result<(), StoreError> {
            let fetched = view.placement.read(
                pack,
                crate::domain::StoredRange::new(
                    crate::domain::FileOffset::new(offset),
                    StoredBytes::new(bytes.len() as u64),
                )?,
            )?;
            bytes.copy_from_slice(&fetched);
            Ok(())
        };
        let mut total_pages = 0_u64;
        let mut decoded = 0_u64;
        let mut frames = 0;
        let mut fully_live_frames = 0;
        let mut partially_obsolete_frames = 0;
        let mut fully_obsolete_frames = 0;
        let mut fully_live_frame_bytes = 0_u64;
        let mut partially_obsolete_frame_bytes = 0_u64;
        let mut fully_obsolete_frame_bytes = 0_u64;
        while offset < payload_end {
            let frame_start = offset;
            let mut prefix = [0; 36];
            read(offset, &mut prefix)?;
            let metadata_length =
                u32::from_le_bytes(prefix[..4].try_into().expect("fixed frame length")) as usize;
            if metadata_length > 1024 * 1024 {
                return Err(StoreError::Range);
            }
            let id = FrameId::from_bytes(prefix[4..].try_into().expect("fixed frame ID"));
            let mut encoded = vec![0; metadata_length];
            read(offset + 36, &mut encoded)?;
            let metadata = FrameMetadata::decode(&encoded, id)?;
            let mut record = [0; crate::format::FRAME_HEADER_SIZE];
            read(offset + 36 + metadata_length as u64, &mut record)?;
            if crate::format::FrameHeader::decode(&record)? != metadata.record_header()? {
                return Err(StoreError::Corrupt(offset));
            }
            offset = offset
                .checked_add(36 + metadata_length as u64 + record.len() as u64)
                .and_then(|offset| offset.checked_add(metadata.payload_bytes().get()))
                .ok_or(StoreError::Range)?;
            if offset > payload_end {
                return Err(StoreError::Corrupt(offset));
            }
            decoded = decoded
                .checked_add(metadata.shape().decoded().get())
                .ok_or(StoreError::Range)?;
            total_pages += u64::from(metadata.shape().pages());
            frames += 1;
            let frame_pages = u64::from(metadata.shape().pages());
            let frame_live_pages = metadata
                .pages()
                .iter()
                .filter(|version| view.metadata.is_live(version, id))
                .count() as u64;
            let frame_bytes = offset.saturating_sub(frame_start);
            if frame_live_pages == frame_pages {
                fully_live_frames += 1;
                fully_live_frame_bytes = fully_live_frame_bytes.saturating_add(frame_bytes);
            } else if frame_live_pages == 0 {
                fully_obsolete_frames += 1;
                fully_obsolete_frame_bytes = fully_obsolete_frame_bytes.saturating_add(frame_bytes);
            } else {
                partially_obsolete_frames += 1;
                partially_obsolete_frame_bytes =
                    partially_obsolete_frame_bytes.saturating_add(frame_bytes);
            }
        }
        let live_pages = live.get(&pack).copied().unwrap_or(0);
        if live_pages > total_pages {
            return Err(StoreError::Corrupt(0));
        }
        output.push(PackOccupancy {
            pack,
            stored_bytes: StoredBytes::new(length),
            decoded_bytes: DecodedBytes::new(decoded),
            total_pages,
            live_pages,
            frames,
            fully_live_frames,
            partially_obsolete_frames,
            fully_obsolete_frames,
            fully_live_frame_bytes: StoredBytes::new(fully_live_frame_bytes),
            partially_obsolete_frame_bytes: StoredBytes::new(partially_obsolete_frame_bytes),
            fully_obsolete_frame_bytes: StoredBytes::new(fully_obsolete_frame_bytes),
        });
    }
    Ok(output)
}

/// Physical rewrite tied to a source view, not an unconditional new head.
#[must_use]
pub(crate) struct RepackCandidate<'g> {
    source: ManifestId,
    manifest: super::view::DurableView<'g>,
    copied: DecodedBytes,
}
impl<'g> RepackCandidate<'g> {
    /// Must be called while publication exclusion is held. Any intervening
    /// sealed view makes this candidate stale, even if it has the same history.
    pub(crate) fn revalidate(
        self,
        current: &PinnedView,
    ) -> Result<(super::view::DurableView<'g>, DecodedBytes), StoreError> {
        if current.id() != self.source {
            return Err(StoreError::Busy);
        }
        Ok((self.manifest, self.copied))
    }
}

pub(crate) fn repack<'g>(
    guard: &'g CatalogGuard,
    view: &PinnedView,
    policy: LayoutPolicy,
    dictionary: crate::DictionaryPolicy,
) -> Result<Option<RepackCandidate<'g>>, StoreError> {
    let Some(selected) = eligible_pack(guard, view, policy)? else {
        return Ok(None);
    };
    build_candidate(guard, view, policy, dictionary, &selected).map(Some)
}

/// Advisory preflight only: this does not authorize publication or deletion.
/// Repeat selection after acquiring publication exclusion before doing work.
pub(crate) fn eligible_pack(
    guard: &CatalogGuard,
    view: &PinnedView,
    policy: LayoutPolicy,
) -> Result<Option<PackOccupancy>, StoreError> {
    if policy.maintenance_input().get() == 0 {
        return Ok(None);
    }
    let retained = guard.retained_packs_except(view.id())?;
    Ok(inventory(guard, view)?
        .into_iter()
        .filter(|pack| {
            pack.live_pages.saturating_mul(2) < pack.total_pages
                && pack.decoded_bytes.get() <= policy.maintenance_input().get()
                && !retained.contains(&pack.pack)
        })
        .max_by(|left, right| {
            // Integer cross multiplication avoids float ordering/NaN domains.
            let a = u128::from(left.reclaimable_bytes()) * u128::from(right.live_pages.max(1));
            let b = u128::from(right.reclaimable_bytes()) * u128::from(left.live_pages.max(1));
            a.cmp(&b).then_with(|| left.pack.cmp(&right.pack))
        }))
}

fn build_candidate<'g>(
    guard: &'g CatalogGuard,
    view: &PinnedView,
    policy: LayoutPolicy,
    dictionary: crate::DictionaryPolicy,
    selected: &PackOccupancy,
) -> Result<RepackCandidate<'g>, StoreError> {
    let mut pages = BTreeMap::new();
    let mut versions = Vec::new();
    let mut copied = Vec::new();
    let mut decoded_input = 0_u64;
    // Decode each source frame only once. Input is bounded by this one pack.
    for location in view
        .metadata
        .frames
        .values()
        .filter(|frame| frame.pack == selected.pack)
    {
        let first = location
            .metadata
            .pages()
            .iter()
            .find(|version| view.metadata.is_live(version, location.metadata.id()));
        let Some(first) = first else {
            continue;
        };
        let super::view::ResolvedPage::Stored(reference) = view.resolve(first.page)? else {
            return Err(StoreError::Corrupt(0));
        };
        let (encoded, decoded, _decode_nanos) = reference.fetch_record()?;
        decoded_input = decoded_input.saturating_add(decoded.bytes().len() as u64);
        if location
            .metadata
            .pages()
            .iter()
            .all(|version| view.metadata.is_live(version, location.metadata.id()))
        {
            copied.push(encoded);
            continue;
        }
        for (index, version) in location.metadata.pages().iter().enumerate() {
            if view.metadata.is_live(version, location.metadata.id()) {
                let size = location.metadata.shape().page_size().as_usize();
                pages.insert(
                    version.page,
                    decoded.bytes()[index * size..(index + 1) * size].to_vec(),
                );
                versions.push((version.page, version.txid));
            }
        }
    }
    let endpoint = SealEndpoint {
        dictionary,
        database: view.metadata.database,
        lineage: view.metadata.lineage,
        size: view.metadata.size,
        txid: view.metadata.txid,
        history: view.metadata.history,
        truncate: None,
    };
    let manifest = seal_with_copies(
        guard,
        Some(view),
        endpoint,
        &versions,
        |page| pages.get(&page).cloned().ok_or(StoreError::Range),
        policy,
        copied,
        super::seal::ManifestMode::Rollup,
    )?;
    Ok(RepackCandidate {
        source: view.id(),
        manifest,
        copied: DecodedBytes::new(decoded_input),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::read_exact_at;
    use std::fs::File;
    #[test]
    fn candidate_rejects_a_different_publication_source() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("stale.zsqlite");
        let mut store = crate::store::Store::open(&path, true)?;
        let layout = LayoutPolicy::default().fixed(DecodedBytes::new(65536))?;
        store.set_storage_policy(crate::StoragePolicy::default().with_layout(layout))?;
        let page = |value| {
            let mut bytes = vec![value; 4096];
            bytes[..16].copy_from_slice(b"SQLite format 3\0");
            bytes[16..18].copy_from_slice(&4096_u16.to_be_bytes());
            bytes
        };
        store.write_at(0, &(1..=8).map(page).collect::<Vec<_>>().concat())?;
        store.publish(true)?;
        store.flush_sidecars()?;
        store.write_at(0, &(11..=16).map(page).collect::<Vec<_>>().concat())?;
        store.publish(true)?;
        store.flush_sidecars()?;
        let mut header = [0; crate::format::ACTIVE_HEADER_SIZE];
        read_exact_at(&File::open(&path)?, 0, &mut header)?;
        let id = ManifestId::from_bytes(
            crate::format::ActiveHeader::decode(&header)?.parent_physical_digest,
        );
        let catalog = super::super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
        let guard = catalog.lock()?;
        let source = guard.pin(id)?;
        let occupancy = inventory(&guard, &source)?;
        assert!(!occupancy.is_empty());
        for pack in &occupancy {
            assert_eq!(
                pack.frames,
                pack.fully_live_frames
                    + pack.partially_obsolete_frames
                    + pack.fully_obsolete_frames
            );
            assert_eq!(pack.obsolete_pages(), pack.total_pages - pack.live_pages);
            assert!(
                pack.fully_live_frame_bytes.get()
                    + pack.partially_obsolete_frame_bytes.get()
                    + pack.fully_obsolete_frame_bytes.get()
                    <= pack.stored_bytes.get()
            );
        }
        let candidate = repack(&guard, &source, layout, crate::DictionaryPolicy::default())?
            .ok_or("expected candidate")?;
        let different_source = guard.pin(candidate.manifest.id())?;
        assert!(matches!(
            candidate.revalidate(&different_source),
            Err(StoreError::Busy)
        ));
        Ok(())
    }
}
