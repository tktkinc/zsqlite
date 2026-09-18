use super::frame::{FrameMetadata, PageVersion, VerifiedFrame};
use super::objects::{CatalogGuard, Dictionary, Durable, Manifest, ObjectKey, Pack};
use super::wire::{Decoder, envelope, open_envelope, u32_bytes, u64_bytes};
use crate::StoreError;
use crate::domain::{
    CompressionDictionary, ContentRoot, DatabaseId, DecodedBytes, DictionaryId, FileOffset,
    FrameId, FrameSlot, HistoryHash, LineageId, LogicalBytes, ManifestId, PackId, PageNumber,
    PageSize, PayloadEncoding, StoredBytes, StoredRange, TransactionId,
};
use crate::fs::SharedLock;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

const VIEW_LIMIT: usize = 512 * 1024 * 1024;
const MAX_RUN_DEPTH: usize = 64;
const RUN_PREFIX: usize = 92;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FrameLocation {
    pub pack: PackId,
    pub payload: super::placement::PackRange,
    pub metadata: FrameMetadata,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StoredPage {
    frame: FrameId,
    slot: FrameSlot,
}

/// Mutable resolved view draft. Ancestry is not writable draft data: only a
/// validated checkpoint/run resolution may establish manifest dependencies.
#[derive(Clone, Debug)]
pub(super) struct ViewMetadata {
    pub database: DatabaseId,
    pub lineage: LineageId,
    pub size: LogicalBytes,
    pub txid: TransactionId,
    pub history: HistoryHash,
    pub parent_seal: Option<crate::domain::ViewHash>,
    pub frames: BTreeMap<FrameId, FrameLocation>,
    pages: BTreeMap<PageNumber, StoredPage>,
    pub preferred: Vec<DictionaryId>,
}
/// Structural proof with immutable access. It is neither a reachability proof
/// nor a payload-verification proof. There is deliberately no `DerefMut`.
#[derive(Clone, Debug)]
pub(super) struct ValidatedMetadata {
    view: ViewMetadata,
    span: crate::domain::TransactionSpan,
    ancestors: Vec<ManifestId>,
    decoded_bytes: DecodedBytes,
    parent: super::segment::Parent,
}
impl std::ops::Deref for ValidatedMetadata {
    type Target = ViewMetadata;
    fn deref(&self) -> &ViewMetadata {
        &self.view
    }
}
impl ValidatedMetadata {
    pub(super) fn logical_hash(&self) -> crate::domain::ViewHash {
        self.view.logical_hash()
    }
    pub(super) fn draft(&self) -> ViewMetadata {
        self.view.clone()
    }
    pub(super) fn dependencies(&self, id: ManifestId) -> BTreeSet<ObjectKey> {
        std::iter::once(ObjectKey::Manifest(id))
            .chain(self.ancestors.iter().copied().map(ObjectKey::Manifest))
            .chain(self.packs().into_iter().map(ObjectKey::Pack))
            .chain(self.dictionaries().into_iter().map(ObjectKey::Dictionary))
            .collect()
    }
}
impl ViewMetadata {
    pub(super) fn empty(
        database: DatabaseId,
        lineage: LineageId,
        size: LogicalBytes,
        txid: TransactionId,
        history: HistoryHash,
    ) -> Self {
        Self {
            database,
            lineage,
            size,
            txid,
            history,
            parent_seal: None,
            frames: BTreeMap::new(),
            pages: BTreeMap::new(),
            preferred: Vec::new(),
        }
    }
    pub(super) fn page_versions(&self) -> impl Iterator<Item = &PageVersion> {
        self.pages
            .values()
            .map(|page| &self.frames[&page.frame].metadata.pages()[page.slot.index()])
    }
    pub(super) fn live_pages_by_pack(&self) -> BTreeMap<PackId, u64> {
        let mut counts = BTreeMap::new();
        for page in self.pages.values() {
            *counts.entry(self.frames[&page.frame].pack).or_default() += 1;
        }
        counts
    }
    pub(super) fn is_live(&self, version: &PageVersion, frame: FrameId) -> bool {
        self.pages
            .get(&version.page)
            .is_some_and(|location| location.frame == frame)
    }
    pub(super) fn remove_above(&mut self, last: u32) {
        self.pages.retain(|page, _| page.get() <= last);
    }
    pub(super) fn remove(&mut self, page: PageNumber) {
        self.pages.remove(&page);
    }
    pub(super) fn retain_frames(&mut self) {
        let live: BTreeSet<_> = self.pages.values().map(|page| page.frame).collect();
        self.frames.retain(|frame, _| live.contains(frame));
    }
    pub(super) fn insert(&mut self, location: FrameLocation) -> Result<(), StoreError> {
        if location.metadata.shape().page_size() != self.size.page_size() {
            return Err(StoreError::IdentityMismatch);
        }
        for (index, page) in location.metadata.pages().iter().enumerate() {
            if page.page.get() > self.size.pages() || page.txid > self.txid {
                return Err(StoreError::Corrupt(0));
            }
            let slot = location
                .metadata
                .shape()
                .slot(u32::try_from(index).map_err(|_| StoreError::Range)?)?;
            self.pages.insert(
                page.page,
                StoredPage {
                    frame: location.metadata.id(),
                    slot,
                },
            );
        }
        self.frames.insert(location.metadata.id(), location);
        Ok(())
    }
    pub(super) fn packs(&self) -> BTreeSet<PackId> {
        self.frames.values().map(|frame| frame.pack).collect()
    }
    pub(super) fn dictionaries(&self) -> BTreeSet<DictionaryId> {
        self.frames
            .values()
            .filter_map(|frame| match frame.metadata.encoding() {
                PayloadEncoding::Zstandard(CompressionDictionary::Shared(id)) => Some(id),
                _ => None,
            })
            .chain(self.preferred.iter().copied())
            .collect()
    }
    fn content_root(&self) -> ContentRoot {
        let mut hash = blake3::Hasher::new();
        for version in self.page_versions() {
            hash.update(&version.page.get().to_le_bytes());
            hash.update(&version.txid.get().to_le_bytes());
            hash.update(version.checksum.as_bytes());
        }
        ContentRoot::from_bytes(*hash.finalize().as_bytes())
    }
    pub(super) fn sealed_lineage(&mut self, parent: Option<&PinnedView>) {
        self.parent_seal = parent.map(|parent| parent.metadata.logical_hash());
        let mut hash = blake3::Hasher::new();
        hash.update(b"zsqlite/sealed-lineage/v1\0");
        hash.update(self.database.as_bytes());
        hash.update(self.lineage.as_bytes());
        hash.update(&self.txid.get().to_le_bytes());
        hash.update(&self.size.page_size().get().to_le_bytes());
        hash.update(&self.size.get().to_le_bytes());
        hash.update(&self.parent_seal.map_or([0; 32], |id| *id.as_bytes()));
        hash.update(self.content_root().as_bytes());
        self.history = HistoryHash::from_bytes(*hash.finalize().as_bytes());
    }
    fn logical_hash_for(&self, root: ContentRoot) -> crate::domain::ViewHash {
        let mut hash = blake3::Hasher::new();
        hash.update(b"zsqlite/logical-view/v1");
        hash.update(self.database.as_bytes());
        hash.update(self.lineage.as_bytes());
        hash.update(&self.size.page_size().get().to_le_bytes());
        hash.update(&self.size.get().to_le_bytes());
        hash.update(&self.txid.get().to_le_bytes());
        hash.update(self.history.as_bytes());
        hash.update(&self.parent_seal.map_or([0; 32], |id| *id.as_bytes()));
        hash.update(root.as_bytes());
        crate::domain::ViewHash::from_bytes(*hash.finalize().as_bytes())
    }
    fn logical_hash(&self) -> crate::domain::ViewHash {
        self.logical_hash_for(self.content_root())
    }
    fn encode(&self) -> Result<Vec<u8>, StoreError> {
        envelope(b"ZVIEW001", &self.encode_raw()?)
    }
    fn encode_raw(&self) -> Result<Vec<u8>, StoreError> {
        let mut raw = Vec::new();
        raw.extend(self.database.as_bytes());
        raw.extend(self.lineage.as_bytes());
        u32_bytes(&mut raw, self.size.page_size().get());
        u64_bytes(&mut raw, self.size.get());
        u64_bytes(&mut raw, self.txid.get());
        raw.extend(self.history.as_bytes());
        raw.extend(self.parent_seal.map_or([0; 32], |id| *id.as_bytes()));
        raw.extend(self.content_root().as_bytes());
        u32_bytes(
            &mut raw,
            u32::try_from(self.preferred.len()).map_err(|_| StoreError::Range)?,
        );
        for id in &self.preferred {
            raw.extend(id.as_bytes());
        }
        u32_bytes(
            &mut raw,
            u32::try_from(self.frames.len()).map_err(|_| StoreError::Range)?,
        );
        for (id, location) in &self.frames {
            raw.extend(id.as_bytes());
            raw.extend(location.pack.as_bytes());
            u64_bytes(&mut raw, location.payload.offset().get());
            let encoded = location.metadata.encode();
            u32_bytes(
                &mut raw,
                u32::try_from(encoded.len()).map_err(|_| StoreError::Range)?,
            );
            raw.extend(encoded);
        }
        u32_bytes(
            &mut raw,
            u32::try_from(self.pages.len()).map_err(|_| StoreError::Range)?,
        );
        for (number, page) in &self.pages {
            u32_bytes(&mut raw, number.get());
            raw.extend(page.frame.as_bytes());
            u32_bytes(
                &mut raw,
                u32::try_from(page.slot.index()).map_err(|_| StoreError::Range)?,
            );
        }
        if raw.len() > VIEW_LIMIT {
            return Err(StoreError::Range);
        }
        Ok(raw)
    }
    fn decode(encoded: &[u8]) -> Result<ValidatedMetadata, StoreError> {
        Self::decode_raw(&open_envelope(b"ZVIEW001", encoded, VIEW_LIMIT)?)
    }
    fn decode_raw(raw: &[u8]) -> Result<ValidatedMetadata, StoreError> {
        let mut wire = Decoder::new(raw);
        let database = DatabaseId::from_bytes(wire.array()?);
        let lineage = LineageId::from_bytes(wire.array()?);
        let page_size = PageSize::new(wire.u32()?)?;
        let size = LogicalBytes::new(wire.u64()?, page_size)?;
        let txid = TransactionId::new(wire.u64()?)?;
        let history = HistoryHash::from_bytes(wire.array()?);
        let parent: [u8; 32] = wire.array()?;
        let root = ContentRoot::from_bytes(wire.array()?);
        let mut result = Self::empty(database, lineage, size, txid, history);
        result.parent_seal =
            (parent != [0; 32]).then(|| crate::domain::ViewHash::from_bytes(parent));
        let preferred = wire.u32()?;
        if preferred > 4 {
            return Err(StoreError::Corrupt(0));
        }
        for _ in 0..preferred {
            let id = DictionaryId::from_bytes(wire.array()?);
            if result.preferred.contains(&id) {
                return Err(StoreError::Corrupt(0));
            }
            result.preferred.push(id);
        }
        let frames = wire.u32()?;
        if frames as usize > raw.len() / 64 {
            return Err(StoreError::Range);
        }
        for _ in 0..frames {
            let id = FrameId::from_bytes(wire.array()?);
            let pack = PackId::from_bytes(wire.array()?);
            let offset = FileOffset::new(wire.u64()?);
            let length = wire.u32()? as usize;
            let metadata = FrameMetadata::decode(wire.take(length)?, id)?;
            let payload = super::placement::PackRange::stored(StoredRange::new(
                offset,
                metadata.payload_bytes(),
            )?)?;
            if result
                .frames
                .insert(
                    id,
                    FrameLocation {
                        pack,
                        payload,
                        metadata,
                    },
                )
                .is_some()
            {
                return Err(StoreError::Corrupt(0));
            }
        }
        let pages = wire.u32()?;
        if pages > size.pages() || pages as usize > raw.len() / 40 {
            return Err(StoreError::Range);
        }
        for _ in 0..pages {
            let page = PageNumber::new(wire.u32()?)?;
            let frame = FrameId::from_bytes(wire.array()?);
            let metadata = &result
                .frames
                .get(&frame)
                .ok_or(StoreError::Corrupt(0))?
                .metadata;
            let slot = metadata.shape().slot(wire.u32()?)?;
            if result
                .pages
                .insert(page, StoredPage { frame, slot })
                .is_some()
            {
                return Err(StoreError::Corrupt(0));
            }
        }
        wire.finish()?;
        let validated = result.validate()?;
        if validated.decoded_bytes.get() != raw.len() as u64 || root != validated.content_root() {
            return Err(StoreError::Corrupt(0));
        }
        Ok(validated)
    }

    /// Shared structural validation for decoded bytes and in-memory drafts.
    /// No encode/decode round trip is needed to establish this proof.
    fn validate(self) -> Result<ValidatedMetadata, StoreError> {
        if self.preferred.len() > 4
            || self.preferred.iter().collect::<BTreeSet<_>>().len() != self.preferred.len()
        {
            return Err(StoreError::Corrupt(0));
        }
        let mut used = BTreeSet::new();
        for (page, stored) in &self.pages {
            let frame = &self
                .frames
                .get(&stored.frame)
                .ok_or(StoreError::Corrupt(0))?
                .metadata;
            let version = frame
                .pages()
                .get(stored.slot.index())
                .ok_or(StoreError::Corrupt(0))?;
            if version.page != *page || version.txid > self.txid || page.get() > self.size.pages() {
                return Err(StoreError::Corrupt(0));
            }
            used.insert(stored.frame);
        }
        if used.len() != self.frames.len() {
            return Err(StoreError::Corrupt(0));
        }
        // The current encoding has 192 fixed bytes, 32 per preferred dictionary,
        // 40 per page-map entry, and 76 plus encoded metadata per frame.
        let mut length = 192_u64
            .checked_add(
                (self.preferred.len() as u64)
                    .checked_mul(32)
                    .ok_or(StoreError::Range)?,
            )
            .and_then(|length| length.checked_add((self.pages.len() as u64).checked_mul(40)?))
            .ok_or(StoreError::Range)?;
        for (id, frame) in &self.frames {
            if *id != frame.metadata.id()
                || frame.metadata.shape().page_size() != self.size.page_size()
                || frame.payload.length() != frame.metadata.payload_bytes()
            {
                return Err(StoreError::Corrupt(0));
            }
            length = length
                .checked_add(76 + frame.metadata.encoded_len() as u64)
                .ok_or(StoreError::Range)?;
        }
        if length > VIEW_LIMIT as u64 {
            return Err(StoreError::Range);
        }
        validate_frame_ranges(&self)?;
        Ok(ValidatedMetadata {
            span: crate::domain::TransactionSpan::new(self.txid, self.txid)?,
            ancestors: Vec::new(),
            decoded_bytes: DecodedBytes::new(length),
            parent: super::segment::Parent::Checkpoint,
            view: self,
        })
    }
}

/// A validated sparse patch is not a resolved view. Missing entries inherit;
/// explicit removals and logical truncation stop fallback.
#[derive(Debug)]
struct MetadataRun {
    parent: super::segment::ParentRef,
    root: ContentRoot,
    span: crate::domain::TransactionSpan,
    patch: ValidatedMetadata,
    removed: BTreeSet<PageNumber>,
}
enum ManifestRecord {
    Checkpoint(ValidatedMetadata),
    Run(MetadataRun),
}
impl ManifestRecord {
    fn from_segment(
        authenticated: &super::objects::AuthenticatedSegment,
        budget: u64,
    ) -> Result<Self, StoreError> {
        let container = authenticated.container();
        let raw = open_envelope(
            b"ZFOOT001",
            &container.footer,
            usize::try_from(budget)
                .map_err(|_| StoreError::Range)?
                .min(VIEW_LIMIT),
        )?;
        let mut wire = Decoder::new(&raw);
        let root = ContentRoot::from_bytes(wire.array()?);
        let length = wire.u32()? as usize;
        let encoded = wire.take(length)?;
        let prefix = encoded.get(8..16).ok_or(StoreError::Range)?;
        let inner_size = u64::from_le_bytes(prefix.try_into().map_err(|_| StoreError::Range)?);
        let total = (raw.len() as u64)
            .checked_add(inner_size)
            .and_then(|size| size.checked_add(RUN_PREFIX as u64))
            .ok_or(StoreError::Range)?;
        if total > budget {
            return Err(StoreError::Range);
        }
        let mut record = Self::decode(encoded)?;
        wire.finish()?;
        let patch = match &mut record {
            Self::Checkpoint(patch) => {
                if container.header.parent() != super::segment::Parent::Checkpoint {
                    return Err(StoreError::IdentityMismatch);
                }
                patch
            }
            Self::Run(run) => {
                if container.header.parent() != super::segment::Parent::Previous(run.parent)
                    || container.header.coverage().full() != run.span
                    || run.root != root
                {
                    return Err(StoreError::IdentityMismatch);
                }
                &mut run.patch
            }
        };
        validate_frame_ranges(patch)?;
        if patch.txid != container.header.coverage().full().end() {
            return Err(StoreError::IdentityMismatch);
        }
        if patch.logical_hash_for(root) != container.header.logical_hash() {
            return Err(StoreError::IdentityMismatch);
        }
        patch.span = container.header.coverage().full();
        patch.decoded_bytes = DecodedBytes::new(total - RUN_PREFIX as u64);
        match &record {
            Self::Checkpoint(view) if view.content_root() != root => {
                return Err(StoreError::Corrupt(0));
            }
            Self::Run(run)
                if run
                    .removed
                    .iter()
                    .any(|page| run.patch.pages.contains_key(page)) =>
            {
                return Err(StoreError::Corrupt(0));
            }
            _ => {}
        }
        Ok(record)
    }
    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        match bytes.get(..8) {
            Some(b"ZVIEW001") => ViewMetadata::decode(bytes).map(Self::Checkpoint),
            Some(b"ZRUN0001") => MetadataRun::decode(bytes).map(Self::Run),
            _ => Err(StoreError::Corrupt(0)),
        }
    }
}
impl MetadataRun {
    fn decoded_bytes(&self) -> u64 {
        RUN_PREFIX as u64 + self.removed.len() as u64 * 4 + self.patch.decoded_bytes.get()
    }
    fn between(source: &PinnedView, target: &ViewMetadata) -> Result<Self, StoreError> {
        let mut patch = ViewMetadata::empty(
            target.database,
            target.lineage,
            target.size,
            target.txid,
            target.history,
        );
        patch.parent_seal = target.parent_seal;
        patch.preferred.clone_from(&target.preferred);
        for (number, page) in &target.pages {
            if source.metadata.pages.get(number) != Some(page)
                || source.metadata.frames.get(&page.frame) != target.frames.get(&page.frame)
            {
                patch.pages.insert(*number, *page);
            }
        }
        for page in patch.pages.values() {
            patch
                .frames
                .entry(page.frame)
                .or_insert_with(|| target.frames[&page.frame].clone());
        }
        let removed = source
            .metadata
            .pages
            .keys()
            .copied()
            .filter(|page| page.get() <= target.size.pages() && !target.pages.contains_key(page))
            .collect();
        Ok(Self {
            parent: super::segment::ParentRef {
                hash: source.metadata.logical_hash(),
                txid: source.metadata.txid,
            },
            root: target.content_root(),
            span: crate::domain::TransactionSpan::new(source.metadata.span.begin(), target.txid)?,
            patch: patch.validate()?,
            removed,
        })
    }
    fn encode(&self) -> Result<Vec<u8>, StoreError> {
        let mut raw = Vec::new();
        raw.extend(self.parent.hash.as_bytes());
        u64_bytes(&mut raw, self.parent.txid.get());
        raw.extend(self.root.as_bytes());
        u64_bytes(&mut raw, self.span.begin().get());
        u64_bytes(&mut raw, self.span.end().get());
        u32_bytes(
            &mut raw,
            u32::try_from(self.removed.len()).map_err(|_| StoreError::Range)?,
        );
        for page in &self.removed {
            u32_bytes(&mut raw, page.get());
        }
        raw.extend(self.patch.encode_raw()?);
        if raw.len() > VIEW_LIMIT {
            return Err(StoreError::Range);
        }
        envelope(b"ZRUN0001", &raw)
    }
    fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        let raw = open_envelope(b"ZRUN0001", bytes, VIEW_LIMIT)?;
        let mut wire = Decoder::new(&raw);
        let parent = super::segment::ParentRef {
            hash: crate::domain::ViewHash::from_bytes(wire.array()?),
            txid: TransactionId::new(wire.u64()?)?,
        };
        let root = ContentRoot::from_bytes(wire.array()?);
        let span = crate::domain::TransactionSpan::new(
            TransactionId::new(wire.u64()?)?,
            TransactionId::new(wire.u64()?)?,
        )?;
        let count = wire.u32()? as usize;
        if count > raw.len().saturating_sub(RUN_PREFIX) / 4 {
            return Err(StoreError::Range);
        }
        let mut removed = BTreeSet::new();
        for _ in 0..count {
            let page = PageNumber::new(wire.u32()?)?;
            if removed.last().is_some_and(|last| *last >= page) {
                return Err(StoreError::Corrupt(0));
            }
            removed.insert(page);
        }
        let patch = ViewMetadata::decode_raw(wire.take(raw.len() - RUN_PREFIX - count * 4)?)?;
        wire.finish()?;
        if span.end() != patch.txid
            || removed
                .iter()
                .any(|page| page.get() > patch.size.pages() || patch.pages.contains_key(page))
        {
            return Err(StoreError::Corrupt(0));
        }
        Ok(Self {
            parent,
            root,
            span,
            patch,
            removed,
        })
    }
    fn apply(
        self,
        parent_id: ManifestId,
        mut base: ValidatedMetadata,
    ) -> Result<ValidatedMetadata, StoreError> {
        if self.parent.hash != base.logical_hash()
            || self.parent.txid != base.txid
            || base.database != self.patch.database
            || base.lineage != self.patch.lineage
            || base.size.page_size() != self.patch.size.page_size()
            || self.span.end() != self.patch.txid
            || self.patch.txid < base.txid
            || (self.patch.txid == base.txid
                && (self.patch.size != base.size
                    || self.patch.history != base.history
                    || self.root != base.content_root()))
        {
            return Err(StoreError::IdentityMismatch);
        }
        let decoded_bytes = base
            .decoded_bytes
            .get()
            .checked_add(self.decoded_bytes())
            .ok_or(StoreError::Range)?;
        if base.ancestors.len() >= MAX_RUN_DEPTH
            || base.ancestors.contains(&parent_id)
            || decoded_bytes > VIEW_LIMIT as u64
        {
            return Err(StoreError::Range);
        }
        base.view.size = self.patch.size;
        base.parent = super::segment::Parent::Previous(self.parent);
        base.view.txid = self.patch.txid;
        base.view.history = self.patch.history;
        base.view.parent_seal = self.patch.parent_seal;
        base.view.preferred = self.patch.view.preferred;
        base.view.remove_above(base.view.size.pages());
        for page in self.removed {
            base.view.remove(page);
        }
        base.view.frames.extend(self.patch.view.frames);
        base.view.pages.extend(self.patch.view.pages);
        base.view.retain_frames();
        validate_frame_ranges(&base.view)?;
        if base.content_root() != self.root {
            return Err(StoreError::Corrupt(0));
        }
        base.ancestors.insert(0, parent_id);
        base.span = self.span;
        base.decoded_bytes = DecodedBytes::new(decoded_bytes);
        Ok(base)
    }
}

fn validate_frame_ranges(metadata: &ViewMetadata) -> Result<(), StoreError> {
    let mut ranges: BTreeMap<PackId, Vec<super::placement::PackRange>> = BTreeMap::new();
    for frame in metadata.frames.values() {
        ranges.entry(frame.pack).or_default().push(frame.payload);
    }
    for ranges in ranges.values_mut() {
        ranges.sort_unstable_by_key(|range| range.offset());
        if ranges
            .windows(2)
            .any(|pair| pair[0].end() > pair[1].offset())
        {
            return Err(StoreError::Corrupt(0));
        }
    }
    Ok(())
}

/// An owned manifest lease. A resolved read cannot outlive it.
/// ```compile_fail
/// use zsqlite::storage::{PinnedView, ResolvedPage};
/// use zsqlite::domain::PageNumber;
/// fn escape(pin: PinnedView, page: PageNumber) -> ResolvedPage<'static> {
///     pin.resolve(page).unwrap()
/// }
/// ```
#[derive(Debug)]
pub struct PinnedView {
    id: ManifestId,
    root: PathBuf,
    _lease: SharedLock,
    _bundle_lease: Option<SharedLock>,
    // Logical parent resolution may choose a newer equivalent rollup later.
    // These leases retain the exact physical objects this reader resolved.
    _object_leases: Vec<SharedLock>,
    pub(super) metadata: ValidatedMetadata,
    dictionaries: BTreeMap<DictionaryId, super::frame::DecodingDictionary>,
    lengths: BTreeMap<ObjectKey, u64>,
    storage: super::Storage,
    pub(super) placement: super::placement::PlacementPin,
}

pub enum ResolvedPage<'view> {
    Zero(ZeroPage<'view>),
    Stored(StoredPageRef<'view>),
}
pub struct ZeroPage<'view> {
    view: &'view PinnedView,
}
pub struct StoredPageRef<'view> {
    view: &'view PinnedView,
    location: &'view FrameLocation,
    slot: FrameSlot,
}

#[derive(Clone, Debug)]
pub struct FrameDistribution {
    pub decoded_bytes: crate::domain::DecodedBytes,
    pub frames: u64,
    pub raw_frames: u64,
    pub dictionary_frames: u64,
    pub stored_payload_bytes: StoredBytes,
}

/// On-disk manifest costs, separate from payload packs and in-memory maps.
#[derive(Clone, Debug)]
pub struct ManifestStatistics {
    id: ManifestId,
    logical_hash: crate::domain::ViewHash,
    parent_hash: Option<crate::domain::ViewHash>,
    sealed_parent: Option<crate::domain::ViewHash>,
    ancestors: Vec<ManifestId>,
    head_bytes: StoredBytes,
    ancestor_bytes: StoredBytes,
    span: crate::domain::TransactionSpan,
}
impl ManifestStatistics {
    #[must_use]
    pub fn logical_hash(&self) -> crate::domain::ViewHash {
        self.logical_hash
    }
    #[must_use]
    pub fn transaction_span(&self) -> crate::domain::TransactionSpan {
        self.span
    }
    #[must_use]
    pub fn id(&self) -> ManifestId {
        self.id
    }
    #[must_use]
    pub fn parent(&self) -> Option<crate::domain::ViewHash> {
        self.parent_hash
    }
    /// Original sealed parent, preserved through metadata compaction and relocation.
    #[must_use]
    pub fn sealed_parent(&self) -> Option<crate::domain::ViewHash> {
        self.sealed_parent
    }
    /// Physical parent chosen by this pinned resolution, which may be a rollup.
    #[must_use]
    pub fn resolved_parent(&self) -> Option<ManifestId> {
        self.ancestors.first().copied()
    }
    #[must_use]
    pub fn run_depth(&self) -> usize {
        self.ancestors.len()
    }
    #[must_use]
    pub fn head_bytes(&self) -> StoredBytes {
        self.head_bytes
    }
    #[must_use]
    pub fn ancestor_bytes(&self) -> StoredBytes {
        self.ancestor_bytes
    }
}

impl PinnedView {
    pub(crate) fn is_checkpoint(&self) -> bool {
        matches!(self.metadata.parent, super::segment::Parent::Checkpoint)
    }
    #[must_use]
    pub const fn id(&self) -> ManifestId {
        self.id
    }
    #[must_use]
    pub fn logical_size(&self) -> LogicalBytes {
        self.metadata.size
    }
    pub(crate) fn endpoint(&self) -> (DatabaseId, TransactionId, HistoryHash) {
        (
            self.metadata.database,
            self.metadata.txid,
            self.metadata.history,
        )
    }
    pub(super) fn read_frame_records(
        &self,
        numbers: &[PageNumber],
    ) -> Result<BTreeMap<FrameId, (Vec<u8>, bool)>, StoreError> {
        let mut frames = BTreeMap::new();
        for number in numbers {
            if let ResolvedPage::Stored(page) = self.resolve(*number)? {
                frames.entry(page.frame_id()).or_insert(page);
            }
        }
        if frames.is_empty() {
            return Ok(BTreeMap::new());
        }
        let mut ranges = Vec::new();
        for page in frames.values() {
            let offset = page
                .location
                .payload
                .offset()
                .get()
                .checked_sub(crate::format::FRAME_HEADER_SIZE as u64)
                .ok_or(StoreError::Range)?;
            ranges.push(self.placement.locate(
                page.location.pack,
                crate::storage::PackRange::new(
                    crate::domain::PackOffset::new(offset),
                    StoredBytes::new(page.stored_bytes()),
                )?,
            )?);
        }
        let records = self.placement.read_ranges(&ranges)?;
        Ok(frames
            .into_keys()
            .zip(records.into_iter().map(|bytes| (bytes, false)))
            .collect())
    }
    pub(crate) fn versions(&self) -> Vec<(PageNumber, TransactionId)> {
        self.metadata
            .page_versions()
            .map(|version| (version.page, version.txid))
            .collect()
    }
    pub(crate) fn pack_count(&self) -> usize {
        self.metadata.packs().len()
    }
    pub(crate) fn manifest_statistics(&self) -> Result<ManifestStatistics, StoreError> {
        let length = |id| -> Result<u64, StoreError> {
            self.lengths
                .get(&ObjectKey::Manifest(id))
                .copied()
                .ok_or(StoreError::Corrupt(0))
        };
        let mut ancestor_bytes = 0_u64;
        for ancestor in &self.metadata.ancestors {
            ancestor_bytes = ancestor_bytes
                .checked_add(length(*ancestor)?)
                .ok_or(StoreError::Range)?;
        }
        Ok(ManifestStatistics {
            id: self.id,
            logical_hash: self.metadata.logical_hash(),
            sealed_parent: self.metadata.parent_seal,
            parent_hash: match self.metadata.parent {
                super::segment::Parent::Checkpoint => None,
                super::segment::Parent::Previous(parent) => Some(parent.hash),
            },
            ancestors: self.metadata.ancestors.clone(),
            head_bytes: StoredBytes::new(length(self.id)?),
            ancestor_bytes: StoredBytes::new(ancestor_bytes),
            span: self.metadata.span,
        })
    }
    pub(crate) fn frame_distribution(&self) -> Vec<FrameDistribution> {
        let mut bins = BTreeMap::new();
        for location in self.metadata.frames.values() {
            let metadata = &location.metadata;
            let bin = bins
                .entry(metadata.shape().decoded())
                .or_insert(FrameDistribution {
                    decoded_bytes: metadata.shape().decoded(),
                    frames: 0,
                    raw_frames: 0,
                    dictionary_frames: 0,
                    stored_payload_bytes: StoredBytes::new(0),
                });
            bin.frames += 1;
            bin.raw_frames += u64::from(matches!(metadata.encoding(), PayloadEncoding::Raw));
            bin.dictionary_frames += u64::from(matches!(
                metadata.encoding(),
                PayloadEncoding::Zstandard(CompressionDictionary::Shared(_))
            ));
            bin.stored_payload_bytes = StoredBytes::new(
                bin.stored_payload_bytes
                    .get()
                    .saturating_add(metadata.payload_bytes().get()),
            );
        }
        bins.into_values().collect()
    }
    pub(crate) fn occupancy(
        &self,
        guard: &CatalogGuard,
    ) -> Result<Vec<super::repack::PackOccupancy>, StoreError> {
        super::repack::inventory(guard, self)
    }
    pub(crate) fn preferred_dictionary_count(&self) -> usize {
        self.metadata.preferred.len()
    }
    pub(crate) fn dictionary_bytes(&self) -> usize {
        self.dictionaries
            .values()
            .map(|dictionary| dictionary.bytes().len())
            .sum()
    }
    pub(crate) fn object_bytes(&self) -> Result<(u64, u64), StoreError> {
        let mut physical = self.placement.object_lengths();
        for (key, length) in &self.lengths {
            let key = match key {
                ObjectKey::Manifest(id) => super::adapter::ObjectKey::Manifest(*id),
                ObjectKey::Dictionary(id) => super::adapter::ObjectKey::Dictionary(*id),
                _ => continue,
            };
            physical.insert(key, *length);
        }
        let bytes = physical.values().try_fold(0_u64, |total, length| {
            total.checked_add(*length).ok_or(StoreError::Range)
        })?;
        Ok((bytes, bytes))
    }
    pub(crate) fn verify(&self, guard: &CatalogGuard) -> Result<(), StoreError> {
        if !self.same_catalog(guard) {
            return Err(StoreError::IdentityMismatch);
        }
        for pack in self.metadata.packs() {
            self.placement.copy_pack(pack, &mut std::io::sink())?;
        }
        Ok(())
    }
    pub fn resolve(&self, page: PageNumber) -> Result<ResolvedPage<'_>, StoreError> {
        if page.get() > self.metadata.size.pages() {
            return Err(StoreError::Range);
        }
        match self.metadata.pages.get(&page) {
            None => Ok(ResolvedPage::Zero(ZeroPage { view: self })),
            Some(stored) => Ok(ResolvedPage::Stored(StoredPageRef {
                view: self,
                location: &self.metadata.frames[&stored.frame],
                slot: stored.slot,
            })),
        }
    }
    pub(super) fn open(guard: &CatalogGuard, id: ManifestId) -> Result<Self, StoreError> {
        Self::open_inner(guard, id, true)
    }
    pub(super) fn open_for_bootstrap(
        guard: &CatalogGuard,
        id: ManifestId,
        _lifecycle: &crate::fs::ExclusiveLock,
    ) -> Result<Self, StoreError> {
        Self::open_inner(guard, id, false)
    }
    fn open_inner(
        guard: &CatalogGuard,
        id: ManifestId,
        lease_bundle: bool,
    ) -> Result<Self, StoreError> {
        let metadata = load_metadata(guard, id)?;
        let lease = SharedLock::acquire(
            &guard
                .root()
                .join("readers")
                .join(super::objects::hex(*id.as_bytes())),
        )?;
        let bundle_lease = if lease_bundle {
            Some(SharedLock::acquire(
                &guard
                    .storage()
                    .coordination_directory()
                    .join("locks/lifecycle.lock"),
            )?)
        } else {
            None
        };
        let mut dictionaries = BTreeMap::new();
        for dictionary in metadata.dictionaries() {
            let durable = guard.validate::<Dictionary>(
                dictionary,
                u64::from(crate::dictionary::MAX_DICTIONARY_BYTES),
            )?;
            let bytes = guard.read::<Dictionary>(
                dictionary,
                StoredRange::new(FileOffset::new(0), durable.length())?,
            )?;
            dictionaries.insert(dictionary, super::frame::DecodingDictionary::new(bytes)?);
        }
        validate_ranges(guard, &metadata)?;
        let lengths: BTreeMap<_, _> = metadata
            .dependencies(id)
            .into_iter()
            .map(|key| Ok((key, guard.logical_length(key)?)))
            .collect::<Result<_, StoreError>>()?;
        std::fs::create_dir_all(guard.root().join("object-readers"))?;
        let object_leases = lengths
            .keys()
            .map(|key| SharedLock::acquire(&key.lease_path(guard)?))
            .collect::<Result<Vec<_>, StoreError>>()?;
        let placement = super::placement::PlacementPin::new(guard, metadata.packs())?;
        Ok(Self {
            id,
            root: guard.root().to_path_buf(),
            _lease: lease,
            _bundle_lease: bundle_lease,
            _object_leases: object_leases,
            metadata,
            dictionaries,
            lengths,
            storage: guard.storage().clone(),
            placement,
        })
    }
    pub(super) fn dictionaries(&self) -> &BTreeMap<DictionaryId, super::frame::DecodingDictionary> {
        &self.dictionaries
    }
    pub(super) fn same_catalog(&self, guard: &CatalogGuard) -> bool {
        self.root == guard.root()
            && self.storage.backend().identity() == guard.storage().backend().identity()
    }
}
impl ResolvedPage<'_> {
    pub fn read(&self) -> Result<Vec<u8>, StoreError> {
        match self {
            Self::Zero(page) => Ok(vec![0; page.view.metadata.size.page_size().as_usize()]),
            Self::Stored(page) => page.read(),
        }
    }
}
impl StoredPageRef<'_> {
    pub(super) fn stored_bytes(&self) -> u64 {
        self.location.payload.length().get() + crate::format::FRAME_HEADER_SIZE as u64
    }
    pub(super) fn frame_id(&self) -> FrameId {
        self.location.metadata.id()
    }
    pub(super) fn fetch(&self) -> Result<VerifiedFrame, StoreError> {
        Ok(self.fetch_record()?.1)
    }
    pub(super) fn fetch_measured(&self) -> Result<(VerifiedFrame, u64), StoreError> {
        let (_, frame, nanos) = self.fetch_record()?;
        Ok((frame, nanos))
    }
    pub(super) fn fetch_record(
        &self,
    ) -> Result<(super::frame::EncodedFrame, VerifiedFrame, u64), StoreError> {
        let prefix_offset = self
            .location
            .payload
            .offset()
            .get()
            .checked_sub(crate::format::FRAME_HEADER_SIZE as u64)
            .ok_or(StoreError::Range)?;
        let length = self
            .location
            .payload
            .length()
            .get()
            .checked_add(crate::format::FRAME_HEADER_SIZE as u64)
            .ok_or(StoreError::Range)?;
        let record = self.view.placement.read(
            self.location.pack,
            StoredRange::new(FileOffset::new(prefix_offset), StoredBytes::new(length))?,
        )?;
        self.decode_record(&record)
    }
    pub(super) fn decode_record(
        &self,
        record: &[u8],
    ) -> Result<(super::frame::EncodedFrame, VerifiedFrame, u64), StoreError> {
        let prefix_offset = self
            .location
            .payload
            .offset()
            .get()
            .checked_sub(crate::format::FRAME_HEADER_SIZE as u64)
            .ok_or(StoreError::Range)?;
        if record.len() as u64 != self.stored_bytes() {
            return Err(StoreError::Corrupt(prefix_offset));
        }
        let prefix = record
            .get(..crate::format::FRAME_HEADER_SIZE)
            .ok_or(StoreError::Corrupt(prefix_offset))?;
        if crate::format::FrameHeader::decode(prefix.try_into().map_err(|_| StoreError::Range)?)?
            != self.location.metadata.record_header()?
        {
            return Err(StoreError::Corrupt(prefix_offset));
        }
        let stored = record[crate::format::FRAME_HEADER_SIZE..].to_vec();
        let decode_start = std::time::Instant::now();
        let (encoded, verified) = super::frame::EncodedFrame::verified(
            self.location.metadata.clone(),
            stored,
            &self.view.dictionaries,
        )?;
        Ok((
            encoded,
            verified,
            u64::try_from(decode_start.elapsed().as_nanos()).unwrap_or(u64::MAX),
        ))
    }
    pub(super) fn extract(&self, frame: &VerifiedFrame) -> Result<Vec<u8>, StoreError> {
        if frame.id() != self.frame_id() {
            return Err(StoreError::IdentityMismatch);
        }
        let size = self.location.metadata.shape().page_size().as_usize();
        let start = self.slot.index() * size;
        Ok(frame
            .bytes()
            .get(start..start + size)
            .ok_or(StoreError::Range)?
            .to_vec())
    }
    fn read(&self) -> Result<Vec<u8>, StoreError> {
        self.extract(&self.fetch()?)
    }
    /// Only live slots from this pinned view, after whole-frame verification.
    /// The slices and versions cannot escape their frame/view borrows.
    pub(super) fn cache_pages<'a>(
        &'a self,
        frame: &'a VerifiedFrame,
    ) -> Result<impl Iterator<Item = (&'a PageVersion, &'a [u8])>, StoreError> {
        if frame.id() != self.frame_id() {
            return Err(StoreError::IdentityMismatch);
        }
        Ok(self
            .location
            .metadata
            .pages()
            .iter()
            .zip(
                frame
                    .bytes()
                    .chunks_exact(self.view.logical_size().page_size().as_usize()),
            )
            .filter(|(version, _)| self.view.metadata.is_live(version, frame.id())))
    }
}

pub(super) fn load_metadata(
    guard: &CatalogGuard,
    id: ManifestId,
) -> Result<ValidatedMetadata, StoreError> {
    let mut next = id;
    let mut seen = BTreeSet::new();
    let mut runs = Vec::new();
    let mut decoded_bytes = 0_u64;
    let (mut resolved_id, mut resolved) = loop {
        if !seen.insert(next) {
            return Err(StoreError::Corrupt(0));
        }
        let container = guard.read_manifest(next, VIEW_LIMIT as u64)?;
        let remaining = (VIEW_LIMIT as u64)
            .checked_sub(decoded_bytes)
            .ok_or(StoreError::Range)?;
        let record = ManifestRecord::from_segment(&container, remaining)?;
        let used = match &record {
            ManifestRecord::Checkpoint(view) => view.decoded_bytes.get(),
            ManifestRecord::Run(run) => run.decoded_bytes(),
        };
        decoded_bytes = decoded_bytes.checked_add(used).ok_or(StoreError::Range)?;
        if decoded_bytes > VIEW_LIMIT as u64 {
            return Err(StoreError::Range);
        }
        match record {
            ManifestRecord::Checkpoint(metadata) => break (next, metadata),
            ManifestRecord::Run(run) => {
                if runs.len() >= MAX_RUN_DEPTH {
                    return Err(StoreError::Range);
                }
                let parent = run.parent;
                runs.push((next, run));
                next = super::segment::resolve_parent(guard, parent, &seen)?;
            }
        }
    };
    for (id, run) in runs.into_iter().rev() {
        resolved = run.apply(resolved_id, resolved)?;
        resolved_id = id;
    }
    Ok(resolved)
}

fn validate_ranges(guard: &CatalogGuard, metadata: &ViewMetadata) -> Result<(), StoreError> {
    let mut sizes = BTreeMap::new();
    for pack in metadata.packs() {
        sizes.insert(pack, guard.placement(pack)?.length);
    }
    for frame in metadata.frames.values() {
        frame.payload.within(sizes[&frame.pack])?;
    }
    Ok(())
}

pub(crate) fn checkpoint_manifest<'g>(
    guard: &'g CatalogGuard,
    source: &PinnedView,
) -> Result<DurableView<'g>, StoreError> {
    ManifestBuilder::new(guard, source.metadata.draft(), Some(source))?
        .checkpoint()
        .finalize()
}

/// Dependencies must be durable in this catalogue or already reachable through
/// the source lease. A stale/unpinned list of IDs cannot build a new manifest.
pub(super) struct ManifestBuilder<'g, 'source> {
    guard: &'g CatalogGuard,
    metadata: ViewMetadata,
    packs: BTreeSet<PackId>,
    dictionaries: BTreeSet<DictionaryId>,
    encoding: ManifestEncoding<'source>,
    source_span: crate::domain::TransactionSpan,
    represented_begin: TransactionId,
}
enum ManifestEncoding<'source> {
    Checkpoint,
    Parent(&'source PinnedView),
}

/// A durably installed manifest whose complete metadata has also been validated.
/// Generic durable bytes alone cannot authorize active-head publication.
#[must_use]
pub(crate) struct DurableView<'g> {
    object: Durable<'g, Manifest>,
    metadata: ValidatedMetadata,
}
impl DurableView<'_> {
    pub(crate) fn id(&self) -> ManifestId {
        self.object.id()
    }
    pub(crate) fn endpoint(&self) -> (DatabaseId, LogicalBytes, TransactionId, HistoryHash) {
        (
            self.metadata.database,
            self.metadata.size,
            self.metadata.txid,
            self.metadata.history,
        )
    }
}
impl<'g, 'source> ManifestBuilder<'g, 'source> {
    pub(super) fn new(
        guard: &'g CatalogGuard,
        metadata: ViewMetadata,
        source: Option<&'source PinnedView>,
    ) -> Result<Self, StoreError> {
        let source_span = source.map_or(
            crate::domain::TransactionSpan::new(metadata.txid, metadata.txid)?,
            |source| source.metadata.span,
        );
        let represented_begin = match source {
            Some(source) if source.metadata.txid < metadata.txid => source.metadata.txid.next()?,
            _ => metadata.txid,
        };
        let encoding = source.map_or(ManifestEncoding::Checkpoint, ManifestEncoding::Parent);
        let mut result = Self {
            guard,
            metadata,
            packs: BTreeSet::new(),
            dictionaries: BTreeSet::new(),
            encoding,
            source_span,
            represented_begin,
        };
        if let Some(source) = source {
            if !source.same_catalog(guard) {
                return Err(StoreError::IdentityMismatch);
            }
            result.packs = source.metadata.packs();
            result.dictionaries = source.metadata.dictionaries();
        }
        Ok(result)
    }
    /// Explicit checkpoint construction. Dependencies still require durable
    /// receipts/source authority; only the serialized representation changes.
    pub(super) fn checkpoint(mut self) -> Self {
        self.encoding = ManifestEncoding::Checkpoint;
        self
    }
    pub(super) fn pack(&mut self, receipt: &Durable<'g, Pack>) -> Result<(), StoreError> {
        if !receipt.belongs_to(self.guard) {
            return Err(StoreError::IdentityMismatch);
        }
        self.packs.insert(receipt.id());
        Ok(())
    }
    pub(super) fn dictionary(
        &mut self,
        receipt: &Durable<'g, Dictionary>,
    ) -> Result<(), StoreError> {
        if !receipt.belongs_to(self.guard) {
            return Err(StoreError::IdentityMismatch);
        }
        self.dictionaries.insert(receipt.id());
        Ok(())
    }
    pub(super) fn finalize(self) -> Result<DurableView<'g>, StoreError> {
        if !self.metadata.packs().is_subset(&self.packs)
            || !self.metadata.dictionaries().is_subset(&self.dictionaries)
        {
            return Err(StoreError::Corrupt(0));
        }
        validate_ranges(self.guard, &self.metadata)?;
        let root = self.metadata.content_root();
        let logical_hash = self.metadata.logical_hash_for(root);
        let source = match self.encoding {
            ManifestEncoding::Parent(source) => Some(source),
            ManifestEncoding::Checkpoint => None,
        };
        let mut run = if let Some(source) = source {
            let run = MetadataRun::between(source, &self.metadata)?;
            if source.metadata.ancestors.len() < MAX_RUN_DEPTH
                && run.patch.pages.len() + run.removed.len() < self.metadata.pages.len()
                && source
                    .metadata
                    .decoded_bytes
                    .get()
                    .checked_add(
                        run.decoded_bytes()
                            .checked_mul(2)
                            .ok_or(StoreError::Range)?,
                    )
                    .and_then(|bytes| bytes.checked_add(1024))
                    .is_some_and(|bytes| bytes <= VIEW_LIMIT as u64)
            {
                Some(run)
            } else {
                None
            }
        } else {
            None
        };
        let span =
            crate::domain::TransactionSpan::new(self.source_span.begin(), self.metadata.txid)?;
        let (parent, encoded) = if let Some(run) = run.take() {
            (super::segment::Parent::Previous(run.parent), run.encode()?)
        } else {
            (super::segment::Parent::Checkpoint, self.metadata.encode()?)
        };
        let mut raw = Vec::new();
        raw.extend(root.as_bytes());
        u32_bytes(
            &mut raw,
            u32::try_from(encoded.len()).map_err(|_| StoreError::Range)?,
        );
        raw.extend(encoded);
        if raw.len() > VIEW_LIMIT {
            return Err(StoreError::Range);
        }
        let footer = envelope(b"ZFOOT001", &raw)?;
        let header = super::segment::Header::new(
            crate::domain::SegmentCoverage::new(
                span,
                if matches!(parent, super::segment::Parent::Checkpoint) {
                    span.begin()
                } else {
                    self.represented_begin
                },
            )?,
            logical_hash,
            parent,
        )?;
        let mut builder = self.guard.build::<Manifest>()?;
        builder.append(&[0; super::segment::HEADER_SIZE])?;
        builder.segment_header(header)?;
        builder.append(&footer)?;
        builder.append(&(super::segment::HEADER_SIZE as u64).to_le_bytes())?;
        builder.append(b"ZEND0001")?;
        let object = builder.finalize()?.install()?;
        let metadata = load_metadata(self.guard, object.id())?;
        let durable = DurableView { object, metadata };
        #[cfg(test)]
        super::faults::check(super::faults::Point::ManifestReady)?;
        Ok(durable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::frame::{FrameEncoder, checksum};
    #[test]
    fn chain_depth_is_bounded_and_checkpoint_preserves_full_coverage()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounded.zsqlite");
        let mut store = crate::store::Store::open(&path, true)?;
        store.set_storage_policy(
            crate::StoragePolicy::default()
                .with_dictionary(crate::DictionaryPolicy::new(0, 1024 * 1024)?),
        )?;
        store.write_at(
            0,
            &[
                super::super::tests::page(1),
                super::super::tests::page(2),
                super::super::tests::page(3),
            ]
            .concat(),
        )?;
        store.publish(true)?;
        store.flush_sidecars()?;
        let begin = store
            .inspect()?
            .manifest
            .unwrap()
            .transaction_span()
            .begin();
        for depth in 1..=MAX_RUN_DEPTH + 1 {
            store.write_at(4096, &super::super::tests::page(u8::try_from(depth + 4)?))?;
            store.publish(true)?;
            store.flush_sidecars()?;
            let info = store.inspect()?.manifest.unwrap();
            assert_eq!(info.run_depth(), depth % (MAX_RUN_DEPTH + 1));
            assert_eq!(info.transaction_span().begin(), begin);
        }
        drop(store);
        crate::store::Store::open_existing(&path)?.verify()?;
        Ok(())
    }
    fn encoded_view() -> Vec<u8> {
        let size = PageSize::new(4096).unwrap();
        let number = PageNumber::new(1).unwrap();
        let txid = TransactionId::new(1).unwrap();
        let bytes = vec![7; 4096];
        let version = PageVersion {
            page: number,
            txid,
            checksum: checksum(number, txid, &bytes),
        };
        let frame = FrameEncoder::new(3, &BTreeMap::new())
            .unwrap()
            .build(size, vec![(version, bytes)])
            .unwrap();
        let (metadata, _payload) = frame.into_parts();
        let mut view = ViewMetadata::empty(
            DatabaseId::from_bytes([1; 32]),
            LineageId::from_bytes([2; 32]),
            LogicalBytes::new(4096, size).unwrap(),
            txid,
            HistoryHash::from_bytes([3; 32]),
        );
        view.insert(FrameLocation {
            pack: PackId::from_bytes([4; 32]),
            payload: crate::storage::PackRange::new(
                crate::domain::PackOffset::new(4096),
                metadata.payload_bytes(),
            )
            .unwrap(),
            metadata,
        })
        .unwrap();
        view.encode().unwrap()
    }
    #[test]
    fn drafts_validate_without_serialization_and_standalone_manifests_are_rejected() {
        let encoded = encoded_view();
        let decoded = ViewMetadata::decode(&encoded).unwrap();
        let draft = decoded.draft();
        let length = draft.encode_raw().unwrap().len();
        assert_eq!(
            draft.clone().validate().unwrap().decoded_bytes.get(),
            length as u64
        );
        let mut invalid = draft;
        let frame = invalid.frames.values_mut().next().unwrap();
        frame.payload =
            crate::storage::PackRange::new(frame.payload.offset(), StoredBytes::new(1)).unwrap();
        assert!(invalid.validate().is_err());

        let directory = tempfile::tempdir().unwrap();
        let catalog = super::super::Catalog::open(directory.path(), true).unwrap();
        let guard = catalog.lock().unwrap();
        let mut builder = guard.build::<Manifest>().unwrap();
        builder.append(&encoded).unwrap();
        assert!(builder.finalize().is_err());
        let mut invalid = encoded;
        invalid[..8].copy_from_slice(b"BADVIEW!");
        assert!(ViewMetadata::decode(&invalid).is_err());
        assert!(
            super::super::objects::ObjectKey::parse(&format!("{}.view", "00".repeat(32))).is_err()
        );
    }
    #[test]
    fn decoder_rejects_hostile_counts_references_slots_and_trailing_data() {
        let encoded = encoded_view();
        assert!(ViewMetadata::decode(&encoded).is_ok());
        let original = open_envelope(b"ZVIEW001", &encoded, VIEW_LIMIT).unwrap();
        for offset in [148, 152, original.len() - 44, original.len() - 4] {
            let mut raw = original.clone();
            raw[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(
                ViewMetadata::decode(&envelope(b"ZVIEW001", &raw).unwrap()).is_err(),
                "offset {offset}"
            );
        }
        let mut raw = original.clone();
        let length = raw.len();
        raw[length - 36] ^= 1; // page map references an unknown frame
        assert!(ViewMetadata::decode(&envelope(b"ZVIEW001", &raw).unwrap()).is_err());
        let mut raw = original;
        raw.push(0);
        assert!(ViewMetadata::decode(&envelope(b"ZVIEW001", &raw).unwrap()).is_err());
        for length in [0, 8, 16, encoded.len() - 1] {
            assert!(ViewMetadata::decode(&encoded[..length]).is_err());
        }
    }
}
