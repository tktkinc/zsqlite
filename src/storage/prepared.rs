//! Page payloads and dictionary training prepared without database locks.
use super::frame::{EncodedFrame, FrameMetadata, PageVersion, install_dictionary};
use super::objects::CatalogGuard;
use super::parallel::{BATCH_BYTES, FramePages, encode_batch};
use super::samples::Samples;
use super::seal::{PackWriter, SealEndpoint};
use super::view::{ManifestBuilder, PinnedView, ViewMetadata};
use crate::domain::{DecodedBytes, DictionaryId, PackId, PageNumber, StoredBytes, TransactionId};
use crate::fs::{read_exact_at, write_all_at};
use crate::{StoreError, layout::LayoutPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::PathBuf;

/// The source view owns reader leases; the page file is immutable private
/// scratch or the adopted source. No publication/catalogue guard escapes here.
pub(crate) struct SealInput {
    pub(crate) endpoint: SealEndpoint,
    source: Option<PinnedView>,
    pages: Vec<(PageNumber, TransactionId)>,
    file: File,
    layout: LayoutPolicy,
    dictionaries: BTreeMap<DictionaryId, Vec<u8>>,
    preferred: Vec<DictionaryId>,
    samples: Samples,
    source_prefix: bool,
    repack: Option<BTreeSet<PackId>>,
    control_root: PathBuf,
}

pub(crate) struct PreparedSeal {
    endpoint: SealEndpoint,
    source: Option<PinnedView>,
    dictionaries: BTreeMap<DictionaryId, Vec<u8>>,
    preferred: Vec<DictionaryId>,
    samples: Option<Vec<u8>>,
    zeros: Vec<PageNumber>,
    frames: Vec<(FrameMetadata, u64)>,
    payloads: File,
    layout: LayoutPolicy,
    source_prefix: bool,
    report: Option<super::MaintenanceReport>,
}

impl SealInput {
    pub(crate) fn new(
        guard: &CatalogGuard,
        source: Option<&PinnedView>,
        endpoint: SealEndpoint,
        pages: Vec<(PageNumber, TransactionId)>,
        file: File,
        layout: LayoutPolicy,
    ) -> Result<Self, StoreError> {
        let source = source.map(|view| guard.pin(view.id())).transpose()?;
        let mut dictionaries = BTreeMap::new();
        let mut preferred = Vec::new();
        if let Some(source) = &source {
            for id in &source.metadata.preferred {
                if let Some(dictionary) = source.dictionaries().get(id) {
                    preferred.push(*id);
                    dictionaries.insert(*id, dictionary.bytes().to_vec());
                }
            }
        } else if let Some(seed) = super::seed::load(guard, layout.level()) {
            let id = DictionaryId::from_bytes(*blake3::hash(&seed).as_bytes());
            preferred.push(id);
            dictionaries.insert(id, seed);
        }
        Ok(Self {
            source,
            endpoint,
            pages,
            file,
            layout,
            dictionaries,
            preferred,
            samples: if matches!(
                endpoint.dictionary.training(),
                crate::dictionary::DictionaryTraining::Disabled
            ) {
                Samples::new(endpoint.dictionary.sample_budget())
            } else {
                Samples::load(guard, endpoint.dictionary.sample_budget())
            },
            source_prefix: false,
            repack: None,
            control_root: guard.root().to_owned(),
        })
    }
    pub(crate) fn source_prefix(mut self) -> Self {
        self.source_prefix = true;
        self
    }
    pub(crate) fn repack(mut self, packs: BTreeSet<PackId>) -> Self {
        self.repack = Some(packs);
        self
    }

    fn read_repack(
        &mut self,
        payloads: &File,
        frames: &mut Vec<(FrameMetadata, u64)>,
        offset: &mut u64,
    ) -> Result<Option<super::MaintenanceReport>, StoreError> {
        let Some(selected) = &self.repack else {
            return Ok(None);
        };
        let view = self.source.as_ref().ok_or(StoreError::Corrupt(0))?;
        let mut locations: Vec<_> = view
            .metadata
            .frames
            .values()
            .filter(|frame| selected.contains(&frame.pack))
            .collect();
        locations.sort_unstable_by_key(|frame| (frame.pack, frame.payload.offset()));
        let mut reads = super::maintenance_read::MaintenanceReads::new(
            &view.placement,
            locations.iter().copied(),
        )?;
        let mut report = super::MaintenanceReport {
            repacked_packs: selected.len(),
            ..Default::default()
        };
        for location in locations {
            let first = location
                .metadata
                .pages()
                .iter()
                .find(|version| view.metadata.is_live(version, location.metadata.id()));
            let Some(first) = first else {
                continue;
            };
            let super::ResolvedPage::Stored(reference) = view.resolve(first.page)? else {
                return Err(StoreError::Corrupt(0));
            };
            let record = reads.read(location)?;
            if location
                .metadata
                .pages()
                .iter()
                .all(|version| view.metadata.is_live(version, location.metadata.id()))
            {
                let (metadata, bytes) = reference.authenticate_record(&record)?.into_parts();
                write_all_at(payloads, *offset, &bytes)?;
                frames.push((metadata, *offset));
                *offset += bytes.len() as u64;
                report.copied_frames += 1;
                report.copied_bytes =
                    StoredBytes::new(report.copied_bytes.get() + bytes.len() as u64);
            } else {
                let (_, decoded, _) = reference.decode_record(&record)?;
                report.decoded_input =
                    DecodedBytes::new(report.decoded_input.get() + decoded.bytes().len() as u64);
                let size = self.endpoint.size.page_size();
                for (version, bytes) in location
                    .metadata
                    .pages()
                    .iter()
                    .zip(decoded.bytes().chunks_exact(size.as_usize()))
                {
                    if view.metadata.is_live(version, location.metadata.id()) {
                        write_all_at(
                            &self.file,
                            u64::from(version.page.get() - 1) * u64::from(size.get()),
                            bytes,
                        )?;
                        self.pages.push((version.page, version.txid));
                    }
                }
            }
        }
        self.pages.sort_unstable_by_key(|(page, _)| *page);
        Ok(Some(report))
    }

    fn read(&self, page: PageNumber) -> Result<Vec<u8>, StoreError> {
        let size = self.endpoint.size.page_size();
        let mut bytes = vec![0; size.as_usize()];
        read_exact_at(
            &self.file,
            u64::from(page.get() - 1) * u64::from(size.get()),
            &mut bytes,
        )?;
        Ok(bytes)
    }

    fn check_paused(&self) -> Result<(), StoreError> {
        if self.source_prefix && crate::conversion::paused(&self.control_root) {
            return Err(StoreError::Busy);
        }
        Ok(())
    }

    fn train_dictionary(&mut self) -> Result<(), StoreError> {
        if let crate::dictionary::DictionaryTraining::UpTo(_) = self.endpoint.dictionary.training()
        {
            for (page, _) in &self.pages {
                self.samples.insert(self.read(*page)?);
            }
            if let Some(evaluation) = self.samples.evaluation()
                && let Some(bytes) = evaluation.select(
                    self.endpoint.dictionary,
                    self.layout.level(),
                    self.endpoint.size,
                    self.dictionaries.values(),
                )
            {
                let id = DictionaryId::from_bytes(*blake3::hash(&bytes).as_bytes());
                if !self.preferred.contains(&id) {
                    if self.preferred.len() == 4 {
                        self.preferred.remove(1);
                    }
                    self.preferred.push(id);
                }
                self.dictionaries.insert(id, bytes);
            }
        }
        self.dictionaries
            .retain(|id, _| self.preferred.contains(id));
        Ok(())
    }

    pub(crate) fn prepare(mut self) -> Result<PreparedSeal, StoreError> {
        self.check_paused()?;
        let payloads = tempfile::tempfile()?;
        let mut frames = Vec::new();
        let mut offset = 0;
        let report = self.read_repack(&payloads, &mut frames, &mut offset)?;
        crate::compression::run(|| self.train_dictionary())?;
        self.check_paused()?;
        let mut zeros = Vec::new();
        let mut group = Vec::new();
        let size = self.endpoint.size.page_size();
        let capacity = self.layout.frame_bytes(size);
        let mut batch = Vec::new();
        let mut batch_bytes = 0;
        let mut encode = |batch| -> Result<(), StoreError> {
            self.check_paused()?;
            let encoded = encode_batch(
                batch,
                size,
                self.layout.level(),
                &self.dictionaries,
                crate::compression_options(),
            )?;
            self.check_paused()?;
            for frame in encoded {
                let (metadata, bytes) = frame.into_parts();
                write_all_at(&payloads, offset, &bytes)?;
                frames.push((metadata, offset));
                offset += bytes.len() as u64;
            }
            Ok(())
        };
        let mut spill = |pages: FramePages| -> Result<(), StoreError> {
            // Flush before adding another frame so even 8 MiB frames obey the
            // batch bound. Keep zero-page elision and frame grouping unchanged.
            let bytes = size.as_usize() * pages.len();
            if batch_bytes + bytes > BATCH_BYTES {
                encode(std::mem::take(&mut batch))?;
                batch_bytes = 0;
            }
            batch.push(pages);
            batch_bytes += bytes;
            Ok(())
        };
        for (page, txid) in &self.pages {
            let bytes = self.read(*page)?;
            if bytes.iter().all(|byte| *byte == 0) {
                zeros.push(*page);
                continue;
            }
            if !group.is_empty() && (group.len() + 1) * size.as_usize() > capacity {
                spill(std::mem::take(&mut group))?;
            }
            group.push((PageVersion::verified(*page, *txid, &bytes), bytes));
        }
        if !group.is_empty() {
            spill(group)?;
        }
        if !batch.is_empty() {
            encode(batch)?;
        }
        let samples = crate::compression::run(|| self.samples.encode())?;
        Ok(PreparedSeal {
            endpoint: self.endpoint,
            source: self.source,
            dictionaries: self.dictionaries,
            preferred: self.preferred,
            samples,
            zeros,
            frames,
            payloads,
            layout: self.layout,
            source_prefix: self.source_prefix,
            report,
        })
    }
}

impl PreparedSeal {
    pub(crate) fn report(&self) -> Option<super::MaintenanceReport> {
        self.report.clone()
    }
    pub(crate) fn install<'g>(
        &self,
        guard: &'g CatalogGuard,
    ) -> Result<super::DurableView<'g>, StoreError> {
        let endpoint = self.endpoint;
        let mut metadata = self.source.as_ref().map_or_else(
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
        for page in &self.zeros {
            metadata.remove(*page);
        }
        let mut receipts = Vec::new();
        for bytes in self.dictionaries.values() {
            receipts.push(install_dictionary(guard, bytes)?);
        }
        metadata.preferred.clone_from(&self.preferred);
        let mut packs = Vec::new();
        let mut writer = PackWriter::new(guard)?;
        for (frame, offset) in &self.frames {
            let mut payload = vec![
                0;
                usize::try_from(frame.payload_bytes().get())
                    .map_err(|_| StoreError::Range)?
            ];
            read_exact_at(&self.payloads, *offset, &mut payload)?;
            writer.append(EncodedFrame::authenticated(frame.clone(), payload)?)?;
            if writer.offset >= self.layout.pack_target().get()
                || writer.metadata_bytes >= 8 * 1024 * 1024
            {
                packs.push(writer.finish(&mut metadata)?);
                writer = PackWriter::new(guard)?;
            }
        }
        if !writer.frames.is_empty() {
            packs.push(writer.finish(&mut metadata)?);
        }
        metadata.retain_frames();
        if self.source_prefix {
            metadata.sealed_lineage(None);
        } else if self
            .source
            .as_ref()
            .is_none_or(|source| endpoint.txid > source.endpoint().1)
        {
            metadata.sealed_lineage(self.source.as_ref());
        }
        let mut builder = ManifestBuilder::new(guard, metadata, self.source.as_ref())?;
        if self.source_prefix || self.report.is_some() {
            builder = builder.checkpoint();
        }
        for receipt in &packs {
            builder.pack(receipt)?;
        }
        for receipt in &receipts {
            builder.dictionary(receipt)?;
        }
        let durable = builder.finalize()?;
        if let Some(bytes) = &self.samples {
            let _ = super::retention::atomic_write(&guard.root().join("dictionary.samples"), bytes);
        }
        super::seed::discard(guard);
        Ok(durable)
    }
}
