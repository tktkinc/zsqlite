use super::frame::{EncodedFrame, FrameEncoder, FrameMetadata, PageVersion, install_dictionary};
use super::objects::{CatalogGuard, Dictionary, Durable, Pack};
use super::view::{FrameLocation, ManifestBuilder, PinnedView, ViewMetadata};
use crate::StoreError;
use crate::domain::{
    DatabaseId, FileOffset, HistoryHash, LineageId, LogicalBytes, PageNumber, StoredRange,
    TransactionId,
};
use crate::layout::LayoutPolicy;
use std::collections::BTreeMap;
use std::io::Write;

#[derive(Clone, Copy, Debug)]
pub(crate) struct SealEndpoint {
    pub dictionary: crate::DictionaryPolicy,
    pub database: DatabaseId,
    pub lineage: LineageId,
    pub size: LogicalBytes,
    pub txid: TransactionId,
    pub history: HistoryHash,
    pub truncate: Option<u32>,
}

#[derive(Clone, Copy)]
pub(crate) enum ManifestMode {
    Incremental,
    Rollup,
}

pub(super) struct PackWriter<'g> {
    guard: &'g CatalogGuard,
    builder: super::placement::BlobWriter<'g>,
    identity: super::pack::Identity,
    offset: u64,
    metadata_bytes: u64,
    frames: Vec<(FileOffset, FrameMetadata)>,
    cohort: [u8; 32],
}
impl<'g> PackWriter<'g> {
    pub(super) fn new(guard: &'g CatalogGuard) -> Result<Self, StoreError> {
        let mut builder = super::placement::BlobWriter::new(guard)?;
        builder.write_all(&super::pack::HEADER)?;
        Ok(Self {
            builder,
            guard,
            identity: super::pack::Identity::new(),
            offset: super::segment::HEADER_SIZE as u64,
            metadata_bytes: 0,
            frames: Vec::new(),
            cohort: [0; 32],
        })
    }
    fn append(&mut self, frame: EncodedFrame) -> Result<(), StoreError> {
        let (metadata, payload) = frame.into_parts();
        let metadata_bytes = metadata.encoded_len() as u64 + 64;
        self.metadata_bytes = self
            .metadata_bytes
            .checked_add(metadata_bytes)
            .ok_or(StoreError::Range)?;
        if self.metadata_bytes > 16 * 1024 * 1024 {
            return Err(StoreError::Range);
        }
        let encoded = metadata.encode();
        self.builder.write_all(
            &(u32::try_from(encoded.len()).map_err(|_| StoreError::Range)?).to_le_bytes(),
        )?;
        self.builder.write_all(metadata.id().as_bytes())?;
        self.builder.write_all(&encoded)?;
        self.builder
            .write_all(&metadata.record_header()?.encode())?;
        self.offset += 36 + encoded.len() as u64 + crate::format::FRAME_HEADER_SIZE as u64;
        let offset = FileOffset::new(self.offset);
        self.builder.write_all(&payload)?;
        self.offset += payload.len() as u64;
        // Do not retain the payload a second time while constructing the pack.
        self.identity.frame(&metadata)?;
        self.frames.push((offset, metadata));
        Ok(())
    }
    fn finish(mut self, metadata: &mut ViewMetadata) -> Result<Durable<'g, Pack>, StoreError> {
        let pack = self.identity.finish(self.offset)?;
        self.builder.complete_pack(pack, 32)?;
        let (_, extents) = self.builder.finish()?;
        let durable = self.guard.install_pack_extent(pack, extents[&pack])?;
        durable.set_pack_cohort(self.cohort)?;
        for (offset, frame) in self.frames {
            metadata.insert(FrameLocation {
                pack: durable.id(),
                payload: super::placement::PackRange::stored(StoredRange::new(
                    offset,
                    frame.payload_bytes(),
                )?)?,
                metadata: frame,
            })?;
        }
        Ok(durable)
    }
}

/// Changed pages only. The source lease retains unchanged dependencies until the
/// durable complete manifest is published. Empty and first seals share this path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn seal<'g>(
    guard: &'g CatalogGuard,
    source: Option<&PinnedView>,
    endpoint: SealEndpoint,
    pages: &[(PageNumber, TransactionId)],
    read: impl FnMut(PageNumber) -> Result<Vec<u8>, StoreError>,
    policy: LayoutPolicy,
    mode: ManifestMode,
) -> Result<super::view::DurableView<'g>, StoreError> {
    seal_with_copies(
        guard,
        source,
        endpoint,
        pages,
        read,
        policy,
        Vec::new(),
        mode,
    )
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn seal_with_copies<'g>(
    guard: &'g CatalogGuard,
    source: Option<&PinnedView>,
    endpoint: SealEndpoint,
    pages: &[(PageNumber, TransactionId)],
    mut read: impl FnMut(PageNumber) -> Result<Vec<u8>, StoreError>,
    policy: LayoutPolicy,
    copied: Vec<EncodedFrame>,
    mode: ManifestMode,
) -> Result<super::view::DurableView<'g>, StoreError> {
    let mut metadata = source.map_or_else(
        || {
            ViewMetadata::empty(
                endpoint.database,
                endpoint.lineage,
                endpoint.size,
                endpoint.txid,
                endpoint.history,
            )
        },
        |source| source.metadata.draft(),
    );
    if metadata.database != endpoint.database || metadata.lineage != endpoint.lineage {
        return Err(StoreError::IdentityMismatch);
    }
    metadata.size = endpoint.size;
    metadata.txid = endpoint.txid;
    metadata.history = endpoint.history;
    metadata.remove_above(endpoint.size.pages());
    if let Some(truncate) = endpoint.truncate {
        metadata.remove_above(truncate);
    }
    let mut dictionaries = BTreeMap::new();
    if let Some(source) = source {
        for id in &source.metadata.preferred {
            if let Some(bytes) = source.dictionaries().get(id) {
                dictionaries.insert(*id, bytes.bytes().to_vec());
            }
        }
    }
    let mut dictionary_receipts = Vec::<Durable<'g, Dictionary>>::new();
    if let crate::dictionary::DictionaryTraining::UpTo(_) = endpoint.dictionary.training() {
        let mut reservoir =
            super::samples::Samples::load(guard, endpoint.dictionary.sample_budget());
        // Offer every committed page, not just a prefix of the database. The
        // bounded digest-priority reservoir samples across the complete image
        // during conversion and carries samples across incremental seals.
        for (page, _) in pages {
            reservoir.insert(read(*page)?);
        }
        if let Some(samples) = reservoir.evaluation()
            && let Some(candidate) = samples.select(
                endpoint.dictionary,
                policy.level(),
                endpoint.size,
                dictionaries.values(),
            )
            && let Ok(receipt) = install_dictionary(guard, &candidate)
        {
            let id = receipt.id();
            dictionaries.insert(id, candidate);
            if !metadata.preferred.contains(&id) {
                // Preserve the general fallback; evict the oldest specialist.
                if metadata.preferred.len() == 4 {
                    metadata.preferred.remove(1);
                }
                metadata.preferred.push(id);
            }
            dictionary_receipts.push(receipt);
        }
        let _advisory_result = reservoir.persist(guard);
    }
    dictionaries.retain(|id, _| metadata.preferred.contains(id));
    let mut encoder = FrameEncoder::new(policy.level(), &dictionaries)?;
    let mut versions = Vec::new();
    for (page, txid) in pages {
        if page.get() <= endpoint.size.pages() {
            versions.push((*page, *txid));
        }
    }
    versions.sort_by_key(|(page, _)| *page);
    let mut pack_receipts = Vec::new();
    let mut writer = PackWriter::new(guard)?;
    for frame in copied {
        writer.append(frame)?;
        if writer.offset >= policy.pack_target().get() || writer.metadata_bytes >= 8 * 1024 * 1024 {
            pack_receipts.push(writer.finish(&mut metadata)?);
            writer = PackWriter::new(guard)?;
        }
    }
    let mut frame_pages = Vec::new();
    let frame_cap = policy.frame_bytes(endpoint.size.page_size());
    for (page, txid) in versions {
        let bytes = read(page)?;
        if bytes.len() != endpoint.size.page_size().as_usize() {
            return Err(StoreError::Range);
        }
        if bytes.iter().all(|byte| *byte == 0) {
            metadata.remove(page);
            continue;
        }
        if !frame_pages.is_empty()
            && (frame_pages.len() + 1) * endpoint.size.page_size().as_usize() > frame_cap
        {
            writer.append(
                encoder.build(endpoint.size.page_size(), std::mem::take(&mut frame_pages))?,
            )?;
            if writer.offset >= policy.pack_target().get()
                || writer.metadata_bytes >= 8 * 1024 * 1024
            {
                pack_receipts.push(writer.finish(&mut metadata)?);
                writer = PackWriter::new(guard)?;
            }
        }
        frame_pages.push((PageVersion::verified(page, txid, &bytes), bytes));
    }
    if !frame_pages.is_empty() {
        writer.append(encoder.build(endpoint.size.page_size(), frame_pages)?)?;
    }
    // Payload packs and metadata runs are independent immutable objects. Even a
    // tiny seal installs its final pack before building the metadata segment.
    if !writer.frames.is_empty() {
        pack_receipts.push(writer.finish(&mut metadata)?);
    }
    metadata.retain_frames();
    if source.is_none_or(|source| endpoint.txid > source.endpoint().1) {
        metadata.sealed_lineage(source);
    }
    let mut builder = ManifestBuilder::new(guard, metadata, source)?;
    for receipt in &pack_receipts {
        builder.pack(receipt)?;
    }
    for receipt in &dictionary_receipts {
        builder.dictionary(receipt)?;
    }
    if matches!(mode, ManifestMode::Rollup) {
        builder = builder.checkpoint();
    }
    builder.finalize()
}
