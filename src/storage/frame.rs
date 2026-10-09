//! Independently authenticated frame metadata and payloads.
use super::objects::{CatalogGuard, Dictionary, Durable};
use super::wire::{Decoder, u32_bytes, u64_bytes};
use crate::StoreError;
use crate::domain::{
    CompressionDictionary, DictionaryId, FrameId, FrameShape, PageChecksum, PageNumber, PageSize,
    PayloadEncoding, StoredBytes, TransactionId,
};
use std::collections::BTreeMap;

/// Authenticated dictionary bytes and an owned reusable decoding context. The
/// owning pinned view bounds its lifetime; concurrent reads borrow it under RAII.
pub(super) struct DecodingDictionary {
    bytes: Vec<u8>,
    decoder: std::sync::Mutex<zstd::bulk::Decompressor<'static>>,
}
impl std::fmt::Debug for DecodingDictionary {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DecodingDictionary")
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
impl DecodingDictionary {
    pub(super) fn new(bytes: Vec<u8>) -> Result<Self, StoreError> {
        let decoder = zstd::bulk::Decompressor::with_dictionary(&bytes)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        Ok(Self {
            bytes,
            decoder: std::sync::Mutex::new(decoder),
        })
    }
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    fn decode(&self, payload: &[u8], capacity: usize) -> Result<Vec<u8>, StoreError> {
        self.decoder
            .lock()
            .map_err(|_| StoreError::Zstd("dictionary decoder lock poisoned".into()))?
            .decompress(payload, capacity)
            .map_err(|error| StoreError::Zstd(error.to_string()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PageVersion {
    pub page: PageNumber,
    pub txid: TransactionId,
    pub checksum: PageChecksum,
}
impl PageVersion {
    pub(super) fn verified(page: PageNumber, txid: TransactionId, bytes: &[u8]) -> Self {
        Self {
            page,
            txid,
            checksum: checksum(page, txid, bytes),
        }
    }
    pub(super) fn encode(&self, output: &mut Vec<u8>) {
        u32_bytes(output, self.page.get());
        u64_bytes(output, self.txid.get());
        output.extend(self.checksum.as_bytes());
    }
    pub(super) fn decode(wire: &mut Decoder<'_>) -> Result<Self, StoreError> {
        Ok(Self {
            page: PageNumber::new(wire.u32()?)?,
            txid: TransactionId::new(wire.u64()?)?,
            checksum: PageChecksum::from_bytes(wire.array()?),
        })
    }
}

pub(super) fn checksum(page: PageNumber, txid: TransactionId, bytes: &[u8]) -> PageChecksum {
    let mut hash = blake3::Hasher::new();
    hash.update(b"zsqlite/page/v1");
    hash.update(&page.get().to_le_bytes());
    hash.update(&txid.get().to_le_bytes());
    hash.update(bytes);
    PageChecksum::from_bytes(*hash.finalize().as_bytes())
}

/// Metadata validated structurally and authenticated by its `FrameId`. This does
/// not assert that the payload has been fetched or checksum-verified.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FrameMetadata {
    id: FrameId,
    shape: FrameShape,
    encoding: PayloadEncoding,
    payload_bytes: StoredBytes,
    payload_hash: [u8; 32],
    pages: Vec<PageVersion>,
}
impl FrameMetadata {
    pub(super) fn id(&self) -> FrameId {
        self.id
    }
    pub(super) fn shape(&self) -> FrameShape {
        self.shape
    }
    pub(super) fn pages(&self) -> &[PageVersion] {
        &self.pages
    }
    pub(super) fn encoding(&self) -> PayloadEncoding {
        self.encoding
    }
    pub(super) fn payload_hash(&self) -> &[u8; 32] {
        &self.payload_hash
    }
    pub(super) fn payload_bytes(&self) -> StoredBytes {
        self.payload_bytes
    }
    pub(super) fn record_header(&self) -> Result<crate::format::FrameHeader, StoreError> {
        let (codec, dictionary_index) = match self.encoding {
            PayloadEncoding::Raw => (crate::format::Codec::Raw, u16::MAX),
            PayloadEncoding::Zstandard(CompressionDictionary::None) => {
                (crate::format::Codec::Zstd, u16::MAX)
            }
            PayloadEncoding::Zstandard(CompressionDictionary::Shared(_)) => {
                (crate::format::Codec::Zstd, 0)
            }
        };
        Ok(crate::format::FrameHeader {
            page_no: self.pages.first().ok_or(StoreError::Range)?.page.get(),
            codec,
            dictionary_index,
            stored_len: u32::try_from(self.payload_bytes.get()).map_err(|_| StoreError::Range)?,
            raw_len: u32::try_from(self.shape.decoded().get()).map_err(|_| StoreError::Range)?,
        })
    }
    pub(super) fn encoded_len(&self) -> usize {
        57 + self.pages.len() * 44
            + if matches!(
                self.encoding,
                PayloadEncoding::Zstandard(CompressionDictionary::Shared(_))
            ) {
                32
            } else {
                0
            }
    }
    pub(super) fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.encoded_len());
        output.extend(b"ZFRAME01");
        u32_bytes(&mut output, self.shape.page_size().get());
        u32_bytes(&mut output, self.shape.pages());
        match self.encoding {
            PayloadEncoding::Raw => output.push(0),
            PayloadEncoding::Zstandard(CompressionDictionary::None) => output.push(1),
            PayloadEncoding::Zstandard(CompressionDictionary::Shared(id)) => {
                output.push(2);
                output.extend(id.as_bytes());
            }
        }
        u64_bytes(&mut output, self.payload_bytes.get());
        output.extend(self.payload_hash);
        for page in &self.pages {
            page.encode(&mut output);
        }
        output
    }
    pub(super) fn decode(bytes: &[u8], expected: FrameId) -> Result<Self, StoreError> {
        if blake3::hash(bytes).as_bytes() != expected.as_bytes() {
            return Err(StoreError::Corrupt(0));
        }
        let mut wire = Decoder::new(bytes);
        if wire.take(8)? != b"ZFRAME01" {
            return Err(StoreError::Corrupt(0));
        }
        let shape = FrameShape::new(PageSize::new(wire.u32()?)?, wire.u32()?)?;
        let encoding = match wire.u8()? {
            0 => PayloadEncoding::Raw,
            1 => PayloadEncoding::Zstandard(CompressionDictionary::None),
            2 => PayloadEncoding::Zstandard(CompressionDictionary::Shared(
                DictionaryId::from_bytes(wire.array()?),
            )),
            _ => return Err(StoreError::Corrupt(0)),
        };
        let payload_bytes = StoredBytes::new(wire.u64()?);
        if payload_bytes.get() == 0
            || payload_bytes.get() > shape.decoded().get()
            || (encoding == PayloadEncoding::Raw && payload_bytes.get() != shape.decoded().get())
        {
            return Err(StoreError::Corrupt(0));
        }
        let payload_hash = wire.array()?;
        let mut pages = Vec::with_capacity(shape.pages() as usize);
        let mut distinct = std::collections::BTreeSet::new();
        for _ in 0..shape.pages() {
            let page = PageVersion::decode(&mut wire)?;
            if !distinct.insert(page.page) {
                return Err(StoreError::Corrupt(0));
            }
            pages.push(page);
        }
        wire.finish()?;
        Ok(Self {
            id: expected,
            shape,
            encoding,
            payload_bytes,
            payload_hash,
            pages,
        })
    }
    fn authenticate_payload(&self, payload: &[u8]) -> Result<(), StoreError> {
        if payload.len() as u64 != self.payload_bytes.get()
            || *blake3::hash(payload).as_bytes() != self.payload_hash
        {
            return Err(StoreError::Corrupt(0));
        }
        Ok(())
    }
    pub(super) fn verify_payload(
        &self,
        payload: &[u8],
        dictionaries: &BTreeMap<DictionaryId, DecodingDictionary>,
    ) -> Result<VerifiedFrame, StoreError> {
        self.authenticate_payload(payload)?;
        let raw = match self.encoding {
            PayloadEncoding::Raw => payload.to_vec(),
            PayloadEncoding::Zstandard(dictionary) => {
                let capacity = self.shape.decoded().as_usize()?;
                match dictionary {
                    CompressionDictionary::None => zstd::bulk::decompress(payload, capacity)
                        .map_err(|error| StoreError::Zstd(error.to_string()))?,
                    CompressionDictionary::Shared(id) => dictionaries
                        .get(&id)
                        .ok_or(StoreError::Corrupt(0))?
                        .decode(payload, capacity)?,
                }
            }
        };
        if raw.len() as u64 != self.shape.decoded().get() {
            return Err(StoreError::Corrupt(0));
        }
        for (version, page) in self
            .pages
            .iter()
            .zip(raw.chunks_exact(self.shape.page_size().as_usize()))
        {
            if checksum(version.page, version.txid, page) != version.checksum {
                return Err(StoreError::PageChecksum(version.page.get()));
            }
        }
        Ok(VerifiedFrame {
            id: self.id,
            bytes: raw,
        })
    }
}

/// Bytes admitted to the frame cache only after payload and every slot verify.
#[derive(Debug)]
pub(super) struct VerifiedFrame {
    id: FrameId,
    bytes: Vec<u8>,
}
impl VerifiedFrame {
    pub(super) fn id(&self) -> FrameId {
        self.id
    }
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Stored bytes authenticated against their frame metadata. This permits an
/// unchanged copy; decoding and page-checksum validation require `VerifiedFrame`.
pub(super) struct EncodedFrame {
    metadata: FrameMetadata,
    payload: Vec<u8>,
}
impl EncodedFrame {
    pub(super) fn into_parts(self) -> (FrameMetadata, Vec<u8>) {
        (self.metadata, self.payload)
    }
    pub(super) fn authenticated(
        metadata: FrameMetadata,
        payload: Vec<u8>,
    ) -> Result<Self, StoreError> {
        metadata.authenticate_payload(&payload)?;
        Ok(Self { metadata, payload })
    }
    pub(super) fn verified(
        metadata: FrameMetadata,
        payload: Vec<u8>,
        dictionaries: &BTreeMap<DictionaryId, DecodingDictionary>,
    ) -> Result<(Self, VerifiedFrame), StoreError> {
        let decoded = metadata.verify_payload(&payload, dictionaries)?;
        Ok((Self { metadata, payload }, decoded))
    }
}

/// Owns one prepared compression context for the active dictionary of a seal.
/// Reuse does not link frames: every compress call remains independently decodable.
pub(super) struct FrameEncoder {
    dictionary: CompressionDictionary,
    compressor: zstd::bulk::Compressor<'static>,
    sample: Vec<u8>,
    probe_output: Vec<u8>,
}
impl FrameEncoder {
    pub(super) fn new(
        level: i32,
        dictionary: Option<(DictionaryId, &[u8])>,
    ) -> Result<Self, StoreError> {
        let (dictionary, bytes) = dictionary
            .map_or((CompressionDictionary::None, &[][..]), |(id, bytes)| {
                (CompressionDictionary::Shared(id), bytes)
            });
        // with_dictionary copies the bytes into an owned context.
        let compressor = zstd::bulk::Compressor::with_dictionary(level, bytes)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        Ok(Self {
            dictionary,
            compressor,
            sample: Vec::with_capacity(4096),
            probe_output: Vec::with_capacity(zstd::zstd_safe::compress_bound(4096)),
        })
    }

    /// Sample four regions, including both ends, using the active dictionary.
    /// Never spend more than 4 KiB or a quarter of the frame on this heuristic.
    /// Small frames go straight to compression; a failed probe stores raw bytes.
    fn worth_compressing(&mut self, raw: &[u8]) -> Result<bool, StoreError> {
        if raw.len() < 4096 {
            return Ok(true);
        }
        let window = (raw.len() / 16).min(1024);
        self.sample.clear();
        for index in 0..4 {
            let start = (raw.len() - window) * index / 3;
            self.sample.extend_from_slice(&raw[start..start + window]);
        }
        let compressed = self
            .compressor
            .compress_to_buffer(&self.sample, &mut self.probe_output)
            .map_err(|error| StoreError::Zstd(error.to_string()))?;
        Ok(compressed < self.sample.len())
    }
    pub(super) fn build(
        &mut self,
        size: PageSize,
        pages: Vec<(PageVersion, Vec<u8>)>,
    ) -> Result<EncodedFrame, StoreError> {
        let shape = FrameShape::new(
            size,
            u32::try_from(pages.len()).map_err(|_| StoreError::Range)?,
        )?;
        let mut raw = Vec::with_capacity(shape.decoded().as_usize()?);
        let mut versions = Vec::with_capacity(pages.len());
        for (version, bytes) in pages {
            if bytes.len() != size.as_usize()
                || checksum(version.page, version.txid, &bytes) != version.checksum
            {
                return Err(StoreError::PageChecksum(version.page.get()));
            }
            versions.push(version);
            raw.extend(bytes);
        }
        let mut encoding = PayloadEncoding::Raw;
        let mut payload = raw;
        if self.worth_compressing(&payload)? {
            let compressed = self
                .compressor
                .compress(&payload)
                .map_err(|error| StoreError::Zstd(error.to_string()))?;
            let overhead = if matches!(self.dictionary, CompressionDictionary::Shared(_)) {
                32
            } else {
                0
            };
            let cost = compressed.len().saturating_add(overhead);
            if cost < payload.len() {
                encoding = PayloadEncoding::Zstandard(self.dictionary);
                payload = compressed;
            }
        }
        let mut metadata = FrameMetadata {
            id: FrameId::from_bytes([0; 32]),
            shape,
            encoding,
            payload_bytes: StoredBytes::new(payload.len() as u64),
            payload_hash: *blake3::hash(&payload).as_bytes(),
            pages: versions,
        };
        metadata.id = FrameId::from_bytes(*blake3::hash(&metadata.encode()).as_bytes());
        Ok(EncodedFrame { metadata, payload })
    }
}

pub(super) fn install_dictionary<'g>(
    guard: &'g CatalogGuard,
    bytes: &[u8],
) -> Result<Durable<'g, Dictionary>, StoreError> {
    if bytes.is_empty() || bytes.len() > crate::dictionary::MAX_DICTIONARY_BYTES as usize {
        return Err(StoreError::Range);
    }
    let mut builder = guard.build::<Dictionary>()?;
    builder.append(bytes)?;
    builder.finalize()?.install()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_skips_noise_but_keeps_frames_with_compressible_later_regions()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut raw: Vec<_> = (0..2048_u32)
            .flat_map(|index| *blake3::hash(&index.to_le_bytes()).as_bytes())
            .collect();
        let mut encoder = FrameEncoder::new(3, None)?;
        assert!(!encoder.worth_compressing(&raw)?);
        let size = PageSize::new(65_536)?;
        let version = PageVersion::verified(PageNumber::new(1)?, TransactionId::new(1)?, &raw);
        let (metadata, payload) = encoder
            .build(size, vec![(version, raw.clone())])?
            .into_parts();
        assert_eq!(metadata.encoding(), PayloadEncoding::Raw);
        let (_, decoded) = EncodedFrame::verified(metadata, payload, &BTreeMap::new())?;
        assert_eq!(decoded.bytes(), raw);

        raw[48 * 1024..].fill(7);
        assert!(encoder.worth_compressing(&raw)?);
        let version = PageVersion::verified(PageNumber::new(1)?, TransactionId::new(1)?, &raw);
        let (metadata, payload) = encoder
            .build(size, vec![(version, raw.clone())])?
            .into_parts();
        assert_eq!(
            metadata.encoding(),
            PayloadEncoding::Zstandard(CompressionDictionary::None)
        );
        let (_, decoded) = EncodedFrame::verified(metadata, payload, &BTreeMap::new())?;
        assert_eq!(decoded.bytes(), raw);
        Ok(())
    }

    #[test]
    fn authenticated_copy_preserves_dictionary_frames_without_decoding()
    -> Result<(), Box<dyn std::error::Error>> {
        let size = PageSize::new(4096)?;
        let mut state = 17_u32;
        let raw: Vec<_> = (0..size.as_usize())
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state.to_le_bytes()[0]
            })
            .collect();
        let dictionary = DictionaryId::from_bytes(*blake3::hash(&raw).as_bytes());
        let mut encoder = FrameEncoder::new(3, Some((dictionary, &raw)))?;
        let frame = encoder.build(
            size,
            vec![(
                PageVersion::verified(PageNumber::new(1)?, TransactionId::new(1)?, &raw),
                raw.clone(),
            )],
        )?;
        let (metadata, payload) = frame.into_parts();
        assert_eq!(
            metadata.encoding(),
            PayloadEncoding::Zstandard(CompressionDictionary::Shared(dictionary))
        );

        // A copy needs no decoding dictionary, unlike a verified plaintext read.
        let copied = EncodedFrame::authenticated(metadata.clone(), payload.clone())?;
        assert_eq!(copied.into_parts(), (metadata.clone(), payload.clone()));
        assert!(matches!(
            EncodedFrame::verified(metadata.clone(), payload.clone(), &BTreeMap::new()),
            Err(StoreError::Corrupt(_))
        ));
        let dictionaries = BTreeMap::from([(dictionary, DecodingDictionary::new(raw.clone())?)]);
        let (_, verified) =
            EncodedFrame::verified(metadata.clone(), payload.clone(), &dictionaries)?;
        assert_eq!(verified.bytes(), raw);

        let mut damaged = payload.clone();
        damaged[0] ^= 1;
        assert!(matches!(
            EncodedFrame::authenticated(metadata.clone(), damaged),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            EncodedFrame::authenticated(metadata, payload[..payload.len() - 1].to_vec()),
            Err(StoreError::Corrupt(_))
        ));
        Ok(())
    }
}
