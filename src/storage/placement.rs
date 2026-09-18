//! Physical placement, independent of logical database manifests.
use super::adapter::{BackendError, ObjectKey, ObjectRange};
use super::catalog::pack_key;
use super::objects::CatalogGuard;
use super::wire::{Decoder, u32_bytes, u64_bytes};
use crate::StoreError;
use crate::domain::{
    BlobBytes, BlobId, BlobOffset, FileOffset, PackId, PackOffset, RepresentationId, StoredBytes,
    StoredRange,
};
use crate::fs::SharedLock;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

const BLOB_HEADER: u64 = 32;

/// Pack-relative bytes cannot be passed as a blob-relative offset.
/// ```compile_fail
/// use zsqlite::domain::{BlobOffset, PackOffset};
/// let offset: BlobOffset = PackOffset::new(8);
/// ```
/// ```compile_fail
/// use zsqlite::domain::{BlobId, PackId};
/// let pack: PackId = BlobId::from_bytes([0; 32]);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PackRange {
    offset: PackOffset,
    length: StoredBytes,
}
impl PackRange {
    pub fn new(offset: PackOffset, length: StoredBytes) -> Result<Self, crate::domain::ValueError> {
        if length.get() == 0 {
            return Err(crate::domain::ValueError);
        }
        offset
            .get()
            .checked_add(length.get())
            .ok_or(crate::domain::ValueError)?;
        Ok(Self { offset, length })
    }
    #[must_use]
    pub const fn offset(self) -> PackOffset {
        self.offset
    }
    #[must_use]
    pub const fn length(self) -> StoredBytes {
        self.length
    }
    pub fn within(self, size: StoredBytes) -> Result<Self, crate::domain::ValueError> {
        if self
            .offset
            .get()
            .checked_add(self.length.get())
            .is_none_or(|end| end > size.get())
        {
            return Err(crate::domain::ValueError);
        }
        Ok(self)
    }
    #[must_use]
    pub fn end(self) -> PackOffset {
        PackOffset::new(self.offset.get() + self.length.get())
    }
    pub(super) fn stored(range: StoredRange) -> Result<Self, StoreError> {
        Ok(Self::new(
            PackOffset::new(range.offset().get()),
            range.length(),
        )?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobExtent {
    blob: BlobId,
    offset: BlobOffset,
    length: BlobBytes,
    blob_length: BlobBytes,
}
impl BlobExtent {
    pub fn new(
        blob: BlobId,
        offset: BlobOffset,
        length: BlobBytes,
        blob_length: BlobBytes,
        pack_length: StoredBytes,
    ) -> Result<Self, crate::domain::ValueError> {
        if length.get() == 0
            || length.get() != pack_length.get()
            || offset
                .get()
                .checked_add(length.get())
                .is_none_or(|end| end > blob_length.get())
        {
            return Err(crate::domain::ValueError);
        }
        Ok(Self {
            blob,
            offset,
            length,
            blob_length,
        })
    }
    #[must_use]
    pub const fn blob(self) -> BlobId {
        self.blob
    }
    #[must_use]
    pub const fn offset(self) -> BlobOffset {
        self.offset
    }
    #[must_use]
    pub const fn length(self) -> BlobBytes {
        self.length
    }
    #[must_use]
    pub const fn blob_length(self) -> BlobBytes {
        self.blob_length
    }
    fn locate(self, range: PackRange) -> Result<StoredRange, StoreError> {
        if range
            .offset
            .get()
            .checked_add(range.length.get())
            .is_none_or(|end| end > self.length.get())
        {
            return Err(StoreError::Range);
        }
        Ok(StoredRange::new(
            FileOffset::new(
                self.offset
                    .get()
                    .checked_add(range.offset.get())
                    .ok_or(StoreError::Range)?,
            ),
            range.length,
        )?)
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Representation {
    pub id: RepresentationId,
    pub extent: BlobExtent,
}
impl Representation {
    fn new(pack: PackId, extent: BlobExtent) -> Self {
        let mut bytes = b"zsqlite/representation/v1".to_vec();
        bytes.extend(pack.as_bytes());
        bytes.extend(extent.blob.as_bytes());
        bytes.extend(extent.offset.get().to_le_bytes());
        bytes.extend(extent.length.get().to_le_bytes());
        Self {
            id: RepresentationId::from_bytes(*blake3::hash(&bytes).as_bytes()),
            extent,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Placements {
    pub pack: PackId,
    pub length: StoredBytes,
    // Persist grouping evidence rather than asking a transport to understand churn.
    pub cohort: [u8; 32],
    pub representations: Vec<Representation>,
}
impl Placements {
    pub fn preferred(&self) -> &Representation {
        &self.representations[0]
    }
    pub fn add(&mut self, extent: BlobExtent) -> Result<(), StoreError> {
        if extent.length.get() != self.length.get() {
            return Err(StoreError::IdentityMismatch);
        }
        let representation = Representation::new(self.pack, extent);
        self.representations
            .retain(|old| old.id != representation.id);
        self.representations.insert(0, representation);
        if self.representations.len() > 64 {
            return Err(StoreError::Range);
        }
        Ok(())
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = b"ZPLACE01".to_vec();
        bytes.extend(self.pack.as_bytes());
        u64_bytes(&mut bytes, self.length.get());
        bytes.extend(self.cohort);
        u32_bytes(
            &mut bytes,
            u32::try_from(self.representations.len()).expect("bounded representations"),
        );
        for representation in &self.representations {
            bytes.extend(representation.id.as_bytes());
            bytes.extend(representation.extent.blob.as_bytes());
            u64_bytes(&mut bytes, representation.extent.offset.get());
            u64_bytes(&mut bytes, representation.extent.length.get());
            u64_bytes(&mut bytes, representation.extent.blob_length.get());
        }
        bytes
    }
    pub fn decode(pack: PackId, bytes: &[u8]) -> Result<Self, StoreError> {
        let mut wire = Decoder::new(bytes);
        if wire.take(8)? != b"ZPLACE01" || wire.array::<32>()? != *pack.as_bytes() {
            return Err(StoreError::Corrupt(0));
        }
        let length = StoredBytes::new(wire.u64()?);
        let cohort = wire.array()?;
        let count = wire.u32()?;
        if count == 0 || count > 64 {
            return Err(StoreError::Range);
        }
        let mut representations = Vec::new();
        let mut ids = BTreeSet::new();
        for _ in 0..count {
            let id = RepresentationId::from_bytes(wire.array()?);
            let extent = BlobExtent::new(
                BlobId::from_bytes(wire.array()?),
                BlobOffset::new(wire.u64()?),
                BlobBytes::new(wire.u64()?),
                BlobBytes::new(wire.u64()?),
                length,
            )?;
            if extent.offset.get() < BLOB_HEADER
                || Representation::new(pack, extent).id != id
                || !ids.insert(id)
            {
                return Err(StoreError::Corrupt(0));
            }
            representations.push(Representation { id, extent });
        }
        wire.finish()?;
        Ok(Self {
            pack,
            length,
            cohort,
            representations,
        })
    }
}

/// An exact physical read lease, independent of cache residency.
#[derive(Debug)]
pub struct PlacementPin {
    storage: super::Storage,
    placements: BTreeMap<PackId, Placements>,
    _leases: Vec<SharedLock>,
}
/// This range borrows the pin that resolved it; another placement snapshot cannot use it.
/// ```compile_fail
/// use zsqlite::storage::{PlacementPin, LocatedRange, PackRange};
/// use zsqlite::domain::PackId;
/// fn escape(pin: PlacementPin, pack: PackId, range: PackRange) -> LocatedRange<'static> {
///     pin.locate(pack, range).unwrap()
/// }
/// ```
pub struct LocatedRange<'pin> {
    pin: &'pin PlacementPin,
    extent: BlobExtent,
    range: StoredRange,
}
impl PlacementPin {
    pub(super) fn new(
        guard: &CatalogGuard,
        packs: impl IntoIterator<Item = PackId>,
    ) -> Result<Self, StoreError> {
        let mut placements = BTreeMap::new();
        let mut objects = guard
            .state()
            .placements
            .objects
            .iter()
            .copied()
            .map(ObjectKey::Index)
            .collect::<BTreeSet<_>>();
        for pack in packs {
            let placement = guard.placement(pack)?;
            objects.insert(ObjectKey::Blob(placement.preferred().extent.blob));
            placements.insert(pack, placement);
        }
        let leases = objects
            .into_iter()
            .map(|key| SharedLock::acquire(&guard.physical_lease_path(key)))
            .collect::<Result<_, _>>()?;
        Ok(Self {
            storage: guard.storage().clone(),
            placements,
            _leases: leases,
        })
    }
    pub fn locate(&self, pack: PackId, range: PackRange) -> Result<LocatedRange<'_>, StoreError> {
        let extent = self
            .placements
            .get(&pack)
            .ok_or(StoreError::Corrupt(0))?
            .preferred()
            .extent;
        Ok(LocatedRange {
            pin: self,
            extent,
            range: extent.locate(range)?,
        })
    }
    pub(super) fn length(&self, pack: PackId) -> Result<StoredBytes, StoreError> {
        Ok(self
            .placements
            .get(&pack)
            .ok_or(StoreError::Corrupt(0))?
            .length)
    }
    pub fn read_ranges(&self, requests: &[LocatedRange<'_>]) -> Result<Vec<Vec<u8>>, StoreError> {
        if requests.is_empty()
            || requests.len() > 4096
            || requests
                .iter()
                .any(|request| !std::ptr::eq(request.pin, self))
        {
            return Err(StoreError::Range);
        }
        let total = requests.iter().try_fold(0_u64, |total, request| {
            total
                .checked_add(request.range.length().get())
                .ok_or(StoreError::Range)
        })?;
        if total > super::adapter::MAX_BATCH_BYTES {
            return Err(StoreError::Range);
        }
        let mut order: Vec<_> = requests.iter().enumerate().collect();
        order.sort_by_key(|(_, request)| (request.extent.blob, request.range.offset()));
        let mut merged: Vec<ObjectRange> = Vec::new();
        let mut slots = Vec::new();
        for (index, request) in order {
            let key = ObjectKey::Blob(request.extent.blob);
            if let Some(last) = merged.last_mut()
                && last.key() == key
                && request.range.offset() <= last.range().end()
            {
                let start = last.range().offset();
                let end = last.range().end().get().max(request.range.end().get());
                *last = ObjectRange::new(
                    key,
                    StoredRange::new(start, StoredBytes::new(end - start.get()))?,
                )?;
                slots.push((index, merged.len() - 1));
                continue;
            }
            merged.push(ObjectRange::new(key, request.range)?);
            slots.push((index, merged.len() - 1));
        }
        let bytes = self.storage.backend().read_ranges(&merged)?;
        if bytes.len() != merged.len()
            || bytes
                .iter()
                .zip(&merged)
                .any(|(bytes, request)| bytes.len() as u64 != request.range().length().get())
        {
            return Err(BackendError::InvalidData.into());
        }
        let mut result = vec![Vec::new(); requests.len()];
        for (index, slot) in slots {
            let start = usize::try_from(
                requests[index].range.offset().get() - merged[slot].range().offset().get(),
            )
            .map_err(|_| StoreError::Range)?;
            let end = start
                .checked_add(requests[index].range.length().as_usize()?)
                .ok_or(StoreError::Range)?;
            result[index] = bytes[slot]
                .get(start..end)
                .ok_or(StoreError::Range)?
                .to_vec();
        }
        Ok(result)
    }
    pub(super) fn read(&self, pack: PackId, range: StoredRange) -> Result<Vec<u8>, StoreError> {
        if range.length().get() == 0 {
            return Ok(Vec::new());
        }
        let located = self.locate(pack, PackRange::stored(range)?)?;
        let mut result = self.read_ranges(&[located])?;
        Ok(result.remove(0))
    }
    pub(super) fn copy_pack(
        &self,
        pack: PackId,
        output: &mut dyn Write,
    ) -> Result<StoredBytes, StoreError> {
        let length = self.length(pack)?;
        let id = super::pack::copy(
            |offset, bytes| {
                let fetched = self.read(
                    pack,
                    StoredRange::new(
                        FileOffset::new(offset),
                        StoredBytes::new(bytes.len() as u64),
                    )?,
                )?;
                bytes.copy_from_slice(&fetched);
                Ok(())
            },
            length.get(),
            output,
            true,
        )?;
        if id != pack {
            return Err(StoreError::Corrupt(0));
        }
        Ok(length)
    }
    pub(super) fn object_lengths(&self) -> BTreeMap<ObjectKey, u64> {
        self.placements
            .values()
            .map(|placement| {
                let extent = placement.preferred().extent;
                (ObjectKey::Blob(extent.blob), extent.blob_length.get())
            })
            .collect()
    }
}

/// A self-describing extent inventory authenticated against the complete blob
/// and each complete logical pack. Normal frame reads use pinned placement
/// metadata and do not download this footer.
#[derive(Clone, Debug)]
pub struct BlobIndex {
    extents: BTreeMap<PackId, BlobExtent>,
}
impl BlobIndex {
    pub fn authenticate(blob: BlobId, bytes: &[u8]) -> Result<Self, StoreError> {
        if bytes.len() < 92
            || bytes.get(..8) != Some(b"ZBLOB001")
            || bytes[8..32].iter().any(|byte| *byte != 0)
        {
            return Err(StoreError::Corrupt(0));
        }
        let mut tail = Decoder::new(&bytes[bytes.len() - 16..]);
        let footer_offset = usize::try_from(tail.u64()?).map_err(|_| StoreError::Range)?;
        if tail.take(8)? != b"ZBEND001" || footer_offset < 32 {
            return Err(StoreError::Corrupt(0));
        }
        let footer = bytes
            .get(footer_offset..bytes.len() - 16)
            .ok_or(StoreError::Range)?;
        if footer.len() < 44
            || blake3::hash(&footer[..footer.len() - 32]).as_bytes() != &footer[footer.len() - 32..]
        {
            return Err(StoreError::Corrupt(0));
        }
        if blob_identity(&footer[..footer.len() - 32], bytes.len() as u64) != blob {
            return Err(StoreError::Corrupt(0));
        }
        let mut wire = Decoder::new(&footer[..footer.len() - 32]);
        if wire.take(8)? != b"ZBINDEX1" {
            return Err(StoreError::Corrupt(0));
        }
        let count = wire.u32()?;
        if count == 0 || count > 4096 {
            return Err(StoreError::Range);
        }
        let mut extents = BTreeMap::new();
        let mut previous = None;
        for _ in 0..count {
            let pack = PackId::from_bytes(wire.array()?);
            let offset = wire.u64()?;
            let length = wire.u64()?;
            let extent = BlobExtent::new(
                blob,
                BlobOffset::new(offset),
                BlobBytes::new(length),
                BlobBytes::new(bytes.len() as u64),
                StoredBytes::new(length),
            )?;
            if offset < BLOB_HEADER
                || offset + length > footer_offset as u64
                || previous.is_some_and(|previous| previous >= pack)
            {
                return Err(StoreError::Corrupt(0));
            }
            previous = Some(pack);
            let payload = &bytes[usize::try_from(offset).map_err(|_| StoreError::Range)?
                ..usize::try_from(offset + length).map_err(|_| StoreError::Range)?];
            let derived = super::pack::copy(
                |offset, target| {
                    let begin = usize::try_from(offset).map_err(|_| StoreError::Range)?;
                    let end = begin.checked_add(target.len()).ok_or(StoreError::Range)?;
                    target.copy_from_slice(payload.get(begin..end).ok_or(StoreError::Range)?);
                    Ok(())
                },
                payload.len() as u64,
                &mut std::io::sink(),
                true,
            )?;
            if derived != pack {
                return Err(StoreError::Corrupt(0));
            }
            extents.insert(pack, extent);
        }
        wire.finish()?;
        let mut ordered: Vec<_> = extents.values().collect();
        ordered.sort_by_key(|extent| extent.offset);
        let mut end = BLOB_HEADER;
        for extent in ordered {
            if extent.offset.get() != end {
                return Err(StoreError::Corrupt(0));
            }
            end += extent.length.get();
        }
        if end != footer_offset as u64 {
            return Err(StoreError::Corrupt(0));
        }
        Ok(Self { extents })
    }
    #[must_use]
    pub fn extents(&self) -> &BTreeMap<PackId, BlobExtent> {
        &self.extents
    }
}

fn blob_identity(footer: &[u8], length: u64) -> BlobId {
    let mut hash = blake3::Hasher::new();
    hash.update(b"zsqlite/blob/v1\0");
    hash.update(footer);
    hash.update(&length.to_le_bytes());
    BlobId::from_bytes(*hash.finalize().as_bytes())
}
/// Streams directly to adapter-owned staging. Only the bounded extent index is
/// held in memory; the adapter learns the immutable key at finalization.
pub(super) struct BlobWriter<'a> {
    writer: Box<dyn super::adapter::ObjectWriter + 'a>,
    offset: u64,
    ranges: BTreeMap<PackId, (u64, u64)>,
}
impl<'a> BlobWriter<'a> {
    pub fn new(guard: &'a CatalogGuard) -> Result<Self, StoreError> {
        let mut writer = guard.storage().backend().begin_write()?;
        let mut header = [0; 32];
        header[..8].copy_from_slice(b"ZBLOB001");
        writer.write_all(&header)?;
        Ok(Self {
            writer,
            offset: 32,
            ranges: BTreeMap::new(),
        })
    }
    pub fn position(&self) -> u64 {
        self.offset
    }
    pub fn complete_pack(&mut self, pack: PackId, start: u64) -> Result<(), StoreError> {
        if self.ranges.len() >= 4096 || self.ranges.contains_key(&pack) || start >= self.offset {
            return Err(StoreError::Range);
        }
        self.ranges.insert(pack, (start, self.offset - start));
        Ok(())
    }
    pub fn finish(mut self) -> Result<(BlobId, BTreeMap<PackId, BlobExtent>), StoreError> {
        if self.ranges.is_empty() {
            return Err(StoreError::Range);
        }
        let footer_offset = self.offset;
        let mut footer = b"ZBINDEX1".to_vec();
        u32_bytes(
            &mut footer,
            u32::try_from(self.ranges.len()).map_err(|_| StoreError::Range)?,
        );
        for (pack, (offset, length)) in &self.ranges {
            footer.extend(pack.as_bytes());
            u64_bytes(&mut footer, *offset);
            u64_bytes(&mut footer, *length);
        }
        let length = self
            .offset
            .checked_add(footer.len() as u64 + 48)
            .ok_or(StoreError::Range)?;
        let blob = blob_identity(&footer, length);
        footer.extend(blake3::hash(&footer).as_bytes());
        self.write_all(&footer)?;
        self.write_all(&footer_offset.to_le_bytes())?;
        self.write_all(b"ZBEND001")?;
        self.writer
            .finish(ObjectKey::Blob(blob), StoredBytes::new(length))?;
        let extents = self
            .ranges
            .into_iter()
            .map(|(pack, (offset, count))| {
                Ok((
                    pack,
                    BlobExtent::new(
                        blob,
                        BlobOffset::new(offset),
                        BlobBytes::new(count),
                        BlobBytes::new(length),
                        StoredBytes::new(count),
                    )?,
                ))
            })
            .collect::<Result<_, StoreError>>()?;
        Ok((blob, extents))
    }
}
impl Write for BlobWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let maximum = u64::try_from(bytes.len()).map_err(std::io::Error::other)?;
        self.offset
            .checked_add(maximum)
            .ok_or_else(|| std::io::Error::other("object length overflow"))?;
        let written = self.writer.write(bytes)?;
        self.offset += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Clone, Debug, Default)]
pub struct RelocationReport {
    pub packs: usize,
    pub copied_bytes: u64,
    pub blob: Option<BlobId>,
}
impl CatalogGuard {
    pub(super) fn placement(&self, pack: PackId) -> Result<Placements, StoreError> {
        let state = self.state();
        Placements::decode(
            pack,
            state
                .placements
                .entries
                .get(pack.as_bytes().as_slice())
                .ok_or(StoreError::Corrupt(0))?,
        )
    }
    pub(super) fn install_pack_extent(
        &self,
        pack: PackId,
        extent: BlobExtent,
    ) -> Result<super::objects::Durable<'_, super::objects::Pack>, StoreError> {
        let mut placement = if let Some(bytes) = self
            .state()
            .placements
            .entries
            .get(pack.as_bytes().as_slice())
        {
            Placements::decode(pack, bytes)?
        } else {
            Placements {
                pack,
                length: StoredBytes::new(extent.length.get()),
                cohort: [0; 32],
                representations: Vec::new(),
            }
        };
        placement.add(extent)?;
        self.state_mut()
            .placements
            .insert(pack_key(pack), placement.encode());
        Ok(super::objects::Durable::pack(
            self,
            pack,
            StoredBytes::new(extent.length.get()),
        ))
    }
    pub(super) fn set_cohort(&self, pack: PackId, cohort: [u8; 32]) -> Result<(), StoreError> {
        let mut placement = self.placement(pack)?;
        placement.cohort = cohort;
        self.state_mut()
            .placements
            .insert(pack_key(pack), placement.encode());
        Ok(())
    }
    pub(crate) fn relocate(
        &self,
        packs: &[PackId],
        budget: StoredBytes,
    ) -> Result<RelocationReport, StoreError> {
        if packs.is_empty()
            || packs.len() > 4096
            || packs.iter().copied().collect::<BTreeSet<_>>().len() != packs.len()
        {
            return Err(StoreError::Range);
        }
        let reachable = self.reachable_packs()?;
        let pin = PlacementPin::new(self, packs.iter().copied())?;
        let mut writer = BlobWriter::new(self)?;
        let mut total = 0_u64;
        let mut cohort = None;
        for pack in packs {
            if !reachable.contains(pack) {
                return Err(StoreError::Busy);
            }
            let placement = self.placement(*pack)?;
            if cohort.is_some_and(|cohort| cohort != placement.cohort) {
                return Err(StoreError::InvalidConfiguration(
                    "relocation packs must share a churn cohort",
                ));
            }
            cohort = Some(placement.cohort);
            total = total
                .checked_add(placement.length.get())
                .ok_or(StoreError::Range)?;
            if total > budget.get() {
                return Err(StoreError::Range);
            }
            let start = writer.position();
            pin.copy_pack(*pack, &mut writer)?;
            writer.complete_pack(*pack, start)?;
        }
        let expected = self.state().revision.clone();
        let (blob, extents) = writer.finish()?;
        if self
            .storage()
            .backend()
            .read_root()?
            .as_ref()
            .map(super::adapter::RootRecord::revision)
            != expected.as_ref()
        {
            return Err(BackendError::Stale.into());
        }
        for pack in packs {
            let mut placement = self.placement(*pack)?;
            placement.add(extents[pack])?;
            self.state_mut()
                .placements
                .insert(pack_key(*pack), placement.encode());
        }
        self.publish_catalog()?;
        Ok(RelocationReport {
            packs: packs.len(),
            copied_bytes: total,
            blob: Some(blob),
        })
    }
}
