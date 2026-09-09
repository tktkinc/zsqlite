//! Binary records for the LTX-style segment format.

use std::fmt::Write as _;

pub const FORMAT_VERSION: u16 = 6;
pub const SECTOR_SIZE: usize = 4096;
pub const SEGMENT_HEADER_SIZE: usize = SECTOR_SIZE;
pub const SEGMENT_TRAILER_SIZE: usize = SECTOR_SIZE;
pub const FRAME_HEADER_SIZE: usize = 80;
pub const COMMIT_HEADER_SIZE: usize = 192;
pub const COMMIT_ENTRY_SIZE: usize = 56;
pub const SEGMENT_INDEX_ENTRY_SIZE: usize = 60;
pub const MAX_SECTION_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FRAME_CAPACITY: u32 = 65_536;

pub const SEGMENT_MAGIC: &[u8; 8] = b"ZSQLSE06";
pub const TRAILER_MAGIC: &[u8; 8] = b"ZSQLST06";
pub const FRAME_MAGIC: &[u8; 4] = b"PFR6";
pub const FREE_MAGIC: &[u8; 4] = b"FRE6";
pub const COMMIT_MAGIC: &[u8; 4] = b"CMT6";

pub type Digest = [u8; 32];
pub type DatabaseId = [u8; 32];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Codec {
    Raw = 0,
    Zstd = 1,
}

impl TryFrom<u8> for Codec {
    type Error = FormatError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Zstd),
            _ => Err(FormatError::Invalid("unknown page codec")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DictionaryPolicyRecord {
    pub dictionary_bytes: u32,
    pub sample_bytes: u64,
    pub min_improvement_bps: u16,
    pub retrain_churn_bps: u16,
    pub promotion_cooldown_seconds: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoragePolicyRecord {
    pub settle_seconds: u32,
    pub max_stale_seconds: u32,
    pub dictionary: DictionaryPolicyRecord,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SegmentId {
    pub start_txid: u64,
    pub end_txid: u64,
    pub end_history: Digest,
    pub physical_digest: Digest,
}

impl SegmentId {
    #[must_use]
    pub fn filename(&self) -> String {
        format!(
            "{:016x}-{:016x}-{}-{}.zseg",
            self.start_txid,
            self.end_txid,
            hex(&self.end_history),
            hex(&self.physical_digest)
        )
    }

    pub fn parse_filename(value: &str) -> Result<Self, FormatError> {
        let stem = value
            .strip_suffix(".zseg")
            .ok_or(FormatError::Invalid("invalid segment suffix"))?;
        let fields = stem.split('-').collect::<Vec<_>>();
        if fields.len() != 4 || fields[0].len() != 16 || fields[1].len() != 16 {
            return Err(FormatError::Invalid("invalid segment filename"));
        }
        let value = Self {
            start_txid: u64::from_str_radix(fields[0], 16)
                .map_err(|_| FormatError::Invalid("invalid start TXID"))?,
            end_txid: u64::from_str_radix(fields[1], 16)
                .map_err(|_| FormatError::Invalid("invalid end TXID"))?,
            end_history: parse_hex_digest(fields[2])?,
            physical_digest: parse_hex_digest(fields[3])?,
        };
        if value.start_txid == 0 || value.end_txid < value.start_txid {
            return Err(FormatError::Invalid("invalid segment TXID range"));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DictionaryEntry {
    pub digest: Digest,
    pub bytes: Vec<u8>,
}

pub fn encode_dictionary_table(entries: &[DictionaryEntry]) -> Result<Vec<u8>, FormatError> {
    if entries.len() > u16::MAX as usize {
        return Err(FormatError::TooLarge);
    }
    let encoded_len = entries.iter().try_fold(8_usize, |length, entry| {
        length
            .checked_add(36)
            .and_then(|length| length.checked_add(entry.bytes.len()))
            .ok_or(FormatError::TooLarge)
    })?;
    if encoded_len as u64 > MAX_SECTION_BYTES {
        return Err(FormatError::TooLarge);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(encoded_len)
        .map_err(|_| FormatError::TooLarge)?;
    output.extend_from_slice(
        &u16::try_from(entries.len())
            .map_err(|_| FormatError::TooLarge)?
            .to_le_bytes(),
    );
    output.extend_from_slice(&[0; 6]);
    for entry in entries {
        if entry.bytes.len() > u32::MAX as usize || digest(&entry.bytes) != entry.digest {
            return Err(FormatError::Invalid("invalid dictionary"));
        }
        output.extend_from_slice(&entry.digest);
        output.extend_from_slice(
            &u32::try_from(entry.bytes.len())
                .map_err(|_| FormatError::TooLarge)?
                .to_le_bytes(),
        );
        output.extend_from_slice(&entry.bytes);
    }
    Ok(output)
}

pub fn decode_dictionary_table(input: &[u8]) -> Result<Vec<DictionaryEntry>, FormatError> {
    if input.len() < 8 {
        return Err(FormatError::Truncated);
    }
    let count = get_u16(input, 0) as usize;
    let mut cursor = 8_usize;
    let mut output = Vec::new();
    output
        .try_reserve_exact(count)
        .map_err(|_| FormatError::TooLarge)?;
    for _ in 0..count {
        let end = cursor.checked_add(36).ok_or(FormatError::TooLarge)?;
        if end > input.len() {
            return Err(FormatError::Truncated);
        }
        let expected: Digest = input[cursor..cursor + 32].try_into().expect("fixed digest");
        let length = get_u32(input, cursor + 32) as usize;
        cursor = end;
        let end = cursor.checked_add(length).ok_or(FormatError::TooLarge)?;
        if end > input.len() {
            return Err(FormatError::Truncated);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| FormatError::TooLarge)?;
        bytes.extend_from_slice(&input[cursor..end]);
        if digest(&bytes) != expected {
            return Err(FormatError::Digest);
        }
        output.push(DictionaryEntry {
            digest: expected,
            bytes,
        });
        cursor = end;
    }
    if cursor != input.len() {
        return Err(FormatError::Invalid("trailing dictionary bytes"));
    }
    Ok(output)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentHeader {
    pub database_id: DatabaseId,
    pub page_size: u32,
    pub start_txid: u64,
    pub base_history: Digest,
    /// Physical digest of the immediately preceding sealed segment. A zero
    /// digest marks a snapshot segment (including the empty genesis active).
    pub parent_physical_digest: Digest,
    pub base_logical_size: u64,
    pub generation: u64,
    pub last_dictionary_promotion_unix: u64,
    pub policy: StoragePolicyRecord,
    pub dictionary_offset: u64,
    pub dictionary_len: u64,
    pub base_map_offset: u64,
    pub base_map_len: u64,
    pub records_offset: u64,
}

impl SegmentHeader {
    #[must_use]
    pub fn encode(self) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(SEGMENT_MAGIC);
        put_u16(&mut output, 8, FORMAT_VERSION);
        output[16..48].copy_from_slice(&self.database_id);
        // Bytes 48..64 are reserved. Early pre-release V6 builds stored a
        // non-authoritative active ID here; decoders intentionally ignore it.
        put_u32(&mut output, 64, self.page_size);
        put_u64(&mut output, 72, self.start_txid);
        output[80..112].copy_from_slice(&self.base_history);
        output[112..144].copy_from_slice(&self.parent_physical_digest);
        put_u64(&mut output, 144, self.base_logical_size);
        put_u64(&mut output, 152, self.generation);
        put_u64(&mut output, 160, self.last_dictionary_promotion_unix);
        put_u64(&mut output, 168, self.dictionary_offset);
        put_u64(&mut output, 176, self.dictionary_len);
        put_u64(&mut output, 184, self.base_map_offset);
        put_u64(&mut output, 192, self.base_map_len);
        put_u64(&mut output, 200, self.records_offset);
        put_policy(&mut output[208..256], self.policy);
        put_sector_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, *SEGMENT_MAGIC)?;
        let output = Self {
            database_id: input[16..48].try_into().expect("fixed database ID"),
            page_size: get_u32(input, 64),
            start_txid: get_u64(input, 72),
            base_history: input[80..112].try_into().expect("fixed digest"),
            parent_physical_digest: input[112..144].try_into().expect("fixed digest"),
            base_logical_size: get_u64(input, 144),
            generation: get_u64(input, 152),
            last_dictionary_promotion_unix: get_u64(input, 160),
            dictionary_offset: get_u64(input, 168),
            dictionary_len: get_u64(input, 176),
            base_map_offset: get_u64(input, 184),
            base_map_len: get_u64(input, 192),
            records_offset: get_u64(input, 200),
            policy: get_policy(&input[208..256]),
        };
        let dictionary_end = output.dictionary_offset.checked_add(output.dictionary_len);
        let base_map_end = output.base_map_offset.checked_add(output.base_map_len);
        if output.start_txid == 0
            || output.generation == 0
            || !valid_policy(output.policy)
            || (output.page_size != 0 && !valid_page_size(output.page_size))
            || (output.page_size == 0 && output.base_logical_size != 0)
            || (output.page_size != 0
                && !output
                    .base_logical_size
                    .is_multiple_of(u64::from(output.page_size)))
            || (output.start_txid == 1) != (output.parent_physical_digest == [0; 32])
            || (output.start_txid == 1 && output.base_history != genesis_history())
            || output.dictionary_offset < SEGMENT_HEADER_SIZE as u64
            || output.dictionary_len > MAX_SECTION_BYTES
            || output.base_map_len > MAX_SECTION_BYTES
            || dictionary_end.is_none_or(|end| output.base_map_offset < end)
            || base_map_end.is_none_or(|end| output.records_offset < end)
            || !output.records_offset.is_multiple_of(SECTOR_SIZE as u64)
        {
            return Err(FormatError::Invalid("invalid segment header"));
        }
        Ok(output)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrameHeader {
    pub free: bool,
    pub page_no: u32,
    pub txid: u64,
    pub codec: Codec,
    pub dictionary_index: u16,
    pub stored_len: u32,
    pub raw_len: u32,
    pub capacity: u32,
    pub page_hash: Digest,
}

impl FrameHeader {
    #[must_use]
    pub fn record_len(self) -> u64 {
        FRAME_HEADER_SIZE as u64 + u64::from(self.capacity)
    }

    #[must_use]
    pub fn encode(self) -> [u8; FRAME_HEADER_SIZE] {
        let mut output = [0_u8; FRAME_HEADER_SIZE];
        output[..4].copy_from_slice(if self.free { FREE_MAGIC } else { FRAME_MAGIC });
        put_u32(&mut output, 4, self.capacity);
        put_u32(&mut output, 8, self.page_no);
        put_u64(&mut output, 16, self.txid);
        output[24] = self.codec as u8;
        put_u16(&mut output, 26, self.dictionary_index);
        put_u32(&mut output, 28, self.stored_len);
        put_u32(&mut output, 32, self.raw_len);
        output[40..72].copy_from_slice(&self.page_hash);
        put_record_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; FRAME_HEADER_SIZE]) -> Result<Self, FormatError> {
        if &input[..4] != FRAME_MAGIC && &input[..4] != FREE_MAGIC {
            return Err(FormatError::Invalid("invalid frame magic"));
        }
        check_record(input)?;
        let output = Self {
            free: &input[..4] == FREE_MAGIC,
            capacity: get_u32(input, 4),
            page_no: get_u32(input, 8),
            txid: get_u64(input, 16),
            codec: Codec::try_from(input[24])?,
            dictionary_index: get_u16(input, 26),
            stored_len: get_u32(input, 28),
            raw_len: get_u32(input, 32),
            page_hash: input[40..72].try_into().expect("fixed digest"),
        };
        if output.capacity < output.stored_len
            || output.capacity == 0
            || output.capacity > MAX_FRAME_CAPACITY
        {
            return Err(FormatError::Invalid("invalid frame capacity"));
        }
        if output.free
            && (output.page_no != 0
                || output.txid != 0
                || output.codec != Codec::Raw
                || output.dictionary_index != u16::MAX
                || output.stored_len != 0
                || output.raw_len != 0
                || output.page_hash != [0; 32])
        {
            return Err(FormatError::Invalid("invalid free frame"));
        }
        if !output.free
            && (output.page_no == 0
                || output.txid == 0
                || !valid_page_size(output.raw_len)
                || output.stored_len > output.raw_len
                || (output.codec == Codec::Raw
                    && (output.dictionary_index != u16::MAX
                        || output.stored_len != output.raw_len))
                || (output.codec == Codec::Zstd && output.dictionary_index == u16::MAX))
        {
            return Err(FormatError::Invalid("invalid live frame"));
        }
        Ok(output)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitEntry {
    pub page_no: u32,
    pub frame_offset: u64,
    pub frame_record_len: u32,
    pub page_hash: Digest,
}

impl CommitEntry {
    #[must_use]
    pub fn encode(self) -> [u8; COMMIT_ENTRY_SIZE] {
        let mut output = [0_u8; COMMIT_ENTRY_SIZE];
        put_u32(&mut output, 0, self.page_no);
        put_u64(&mut output, 8, self.frame_offset);
        put_u32(&mut output, 16, self.frame_record_len);
        output[24..56].copy_from_slice(&self.page_hash);
        output
    }

    pub fn decode(input: &[u8; COMMIT_ENTRY_SIZE]) -> Result<Self, FormatError> {
        let output = Self {
            page_no: get_u32(input, 0),
            frame_offset: get_u64(input, 8),
            frame_record_len: get_u32(input, 16),
            page_hash: input[24..56].try_into().expect("fixed digest"),
        };
        if output.page_no == 0
            || output.frame_record_len
                < u32::try_from(FRAME_HEADER_SIZE).expect("frame header size fits u32")
            || output.frame_record_len
                > u32::try_from(FRAME_HEADER_SIZE).expect("frame header size fits u32")
                    + MAX_FRAME_CAPACITY
        {
            return Err(FormatError::Invalid("invalid commit entry"));
        }
        Ok(output)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitHeader {
    pub record_len: u32,
    pub entry_count: u32,
    pub txid: u64,
    pub previous_commit: u64,
    pub logical_size: u64,
    pub page_size: u32,
    /// Lowest page-count reached during this transaction, if it shrank.
    /// Pages above this boundary are forgotten before applying the entries.
    pub truncate_pages: Option<u32>,
    pub previous_history: Digest,
    pub transaction_hash: Digest,
    pub resulting_history: Digest,
    pub entries_digest: Digest,
    pub commit_unix: u64,
}

impl CommitHeader {
    #[must_use]
    pub fn encode(self) -> [u8; COMMIT_HEADER_SIZE] {
        let mut output = [0_u8; COMMIT_HEADER_SIZE];
        output[..4].copy_from_slice(COMMIT_MAGIC);
        put_u32(&mut output, 4, self.record_len);
        put_u32(&mut output, 8, self.entry_count);
        put_u64(&mut output, 16, self.txid);
        put_u64(&mut output, 24, self.previous_commit);
        put_u64(&mut output, 32, self.logical_size);
        put_u32(&mut output, 40, self.page_size);
        if let Some(truncate_pages) = self.truncate_pages {
            output[44] = 1;
            put_u32(&mut output, 184, truncate_pages);
        }
        output[48..80].copy_from_slice(&self.previous_history);
        output[80..112].copy_from_slice(&self.transaction_hash);
        output[112..144].copy_from_slice(&self.resulting_history);
        output[144..176].copy_from_slice(&self.entries_digest);
        put_u64(&mut output, 176, self.commit_unix);
        put_record_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; COMMIT_HEADER_SIZE]) -> Result<Self, FormatError> {
        if &input[..4] != COMMIT_MAGIC {
            return Err(FormatError::Invalid("invalid commit magic"));
        }
        check_record(input)?;
        let output = Self {
            record_len: get_u32(input, 4),
            entry_count: get_u32(input, 8),
            txid: get_u64(input, 16),
            previous_commit: get_u64(input, 24),
            logical_size: get_u64(input, 32),
            page_size: get_u32(input, 40),
            truncate_pages: match input[44] {
                0 if get_u32(input, 184) == 0 => None,
                1 => Some(get_u32(input, 184)),
                _ => return Err(FormatError::Invalid("invalid commit truncate boundary")),
            },
            previous_history: input[48..80].try_into().expect("fixed digest"),
            transaction_hash: input[80..112].try_into().expect("fixed digest"),
            resulting_history: input[112..144].try_into().expect("fixed digest"),
            entries_digest: input[144..176].try_into().expect("fixed digest"),
            commit_unix: get_u64(input, 176),
        };
        let entries_len = usize::try_from(output.entry_count)
            .map_err(|_| FormatError::TooLarge)?
            .checked_mul(COMMIT_ENTRY_SIZE)
            .ok_or(FormatError::TooLarge)?;
        let unaligned = COMMIT_HEADER_SIZE
            .checked_add(entries_len)
            .ok_or(FormatError::TooLarge)?;
        let expected = unaligned
            .checked_add(SECTOR_SIZE - 1)
            .map(|value| value / SECTOR_SIZE * SECTOR_SIZE)
            .ok_or(FormatError::TooLarge)?;
        if output.record_len as usize != expected
            || u64::from(output.record_len) > MAX_SECTION_BYTES
            || output.txid == 0
            || !valid_page_size(output.page_size)
            || !output
                .logical_size
                .is_multiple_of(u64::from(output.page_size))
        {
            return Err(FormatError::Invalid("invalid commit record"));
        }
        let logical_pages = output.logical_size / u64::from(output.page_size);
        if logical_pages > u64::from(u32::MAX)
            || u64::from(output.entry_count) > logical_pages
            || output
                .truncate_pages
                .is_some_and(|pages| u64::from(pages) > logical_pages)
        {
            return Err(FormatError::Invalid("commit exceeds logical page count"));
        }
        Ok(output)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentIndexEntry {
    pub page_no: u32,
    pub last_txid: u64,
    pub frame_offset: u64,
    pub frame_record_len: u32,
    pub page_hash: Digest,
}

impl SegmentIndexEntry {
    #[must_use]
    pub fn encode(self) -> [u8; SEGMENT_INDEX_ENTRY_SIZE] {
        let mut output = [0_u8; SEGMENT_INDEX_ENTRY_SIZE];
        put_u32(&mut output, 0, self.page_no);
        put_u64(&mut output, 8, self.last_txid);
        put_u64(&mut output, 16, self.frame_offset);
        put_u32(&mut output, 24, self.frame_record_len);
        output[28..60].copy_from_slice(&self.page_hash);
        output
    }

    pub fn decode(input: &[u8; SEGMENT_INDEX_ENTRY_SIZE]) -> Result<Self, FormatError> {
        let output = Self {
            page_no: get_u32(input, 0),
            last_txid: get_u64(input, 8),
            frame_offset: get_u64(input, 16),
            frame_record_len: get_u32(input, 24),
            page_hash: input[28..60].try_into().expect("fixed digest"),
        };
        if output.page_no == 0
            || output.last_txid == 0
            || output.frame_record_len
                < u32::try_from(FRAME_HEADER_SIZE).expect("frame header size fits u32")
            || output.frame_record_len
                > u32::try_from(FRAME_HEADER_SIZE).expect("frame header size fits u32")
                    + MAX_FRAME_CAPACITY
        {
            return Err(FormatError::Invalid("invalid segment index entry"));
        }
        Ok(output)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentTrailer {
    pub database_id: DatabaseId,
    pub start_txid: u64,
    pub end_txid: u64,
    pub base_history: Digest,
    pub end_history: Digest,
    pub logical_size: u64,
    pub page_size: u32,
    pub index_offset: u64,
    pub index_len: u64,
    pub map_offset: u64,
    pub map_len: u64,
    pub content_root: Digest,
    pub physical_digest: Digest,
}

impl SegmentTrailer {
    pub const PHYSICAL_RANGE: std::ops::Range<usize> = 208..240;

    #[must_use]
    pub fn encode(self, checksum: bool) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(TRAILER_MAGIC);
        put_u16(&mut output, 8, FORMAT_VERSION);
        output[16..48].copy_from_slice(&self.database_id);
        put_u64(&mut output, 48, self.start_txid);
        put_u64(&mut output, 56, self.end_txid);
        output[64..96].copy_from_slice(&self.base_history);
        output[96..128].copy_from_slice(&self.end_history);
        put_u64(&mut output, 128, self.logical_size);
        put_u32(&mut output, 136, self.page_size);
        put_u64(&mut output, 144, self.index_offset);
        put_u64(&mut output, 152, self.index_len);
        put_u64(&mut output, 160, self.map_offset);
        put_u64(&mut output, 168, self.map_len);
        output[176..208].copy_from_slice(&self.content_root);
        output[Self::PHYSICAL_RANGE].copy_from_slice(&self.physical_digest);
        if checksum {
            put_sector_checksum(&mut output);
        }
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, *TRAILER_MAGIC)?;
        let output = Self {
            database_id: input[16..48].try_into().expect("fixed database ID"),
            start_txid: get_u64(input, 48),
            end_txid: get_u64(input, 56),
            base_history: input[64..96].try_into().expect("fixed digest"),
            end_history: input[96..128].try_into().expect("fixed digest"),
            logical_size: get_u64(input, 128),
            page_size: get_u32(input, 136),
            index_offset: get_u64(input, 144),
            index_len: get_u64(input, 152),
            map_offset: get_u64(input, 160),
            map_len: get_u64(input, 168),
            content_root: input[176..208].try_into().expect("fixed digest"),
            physical_digest: input[Self::PHYSICAL_RANGE]
                .try_into()
                .expect("fixed digest"),
        };
        if output.start_txid == 0
            || output.end_txid < output.start_txid
            || !valid_page_size(output.page_size)
            || !output
                .logical_size
                .is_multiple_of(u64::from(output.page_size))
            || output.index_len > MAX_SECTION_BYTES
            || output.map_len > MAX_SECTION_BYTES
        {
            return Err(FormatError::Invalid("invalid segment trailer"));
        }
        Ok(output)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FormatError {
    #[error("truncated record")]
    Truncated,
    #[error("unsupported zsqlite format version")]
    Version,
    #[error("invalid record: {0}")]
    Invalid(&'static str),
    #[error("record checksum mismatch")]
    Checksum,
    #[error("content digest mismatch")]
    Digest,
    #[error("record is too large")]
    TooLarge,
}

#[must_use]
pub fn valid_page_size(size: u32) -> bool {
    (512..=65_536).contains(&size) && size.is_power_of_two()
}

#[must_use]
pub fn digest(input: &[u8]) -> Digest {
    *blake3::hash(input).as_bytes()
}

#[must_use]
pub fn genesis_history() -> Digest {
    digest(b"zsqlite/history/v1/genesis")
}

#[must_use]
pub fn hex(value: &Digest) -> String {
    let mut output = String::with_capacity(64);
    for byte in value {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

pub fn parse_hex_digest(value: &str) -> Result<Digest, FormatError> {
    if value.len() != 64 {
        return Err(FormatError::Invalid("invalid digest length"));
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Ok(output)
}

fn nibble(value: u8) -> Result<u8, FormatError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(FormatError::Invalid("invalid lowercase hexadecimal")),
    }
}

fn valid_policy(value: StoragePolicyRecord) -> bool {
    value.settle_seconds > 0
        && value.max_stale_seconds >= value.settle_seconds
        && (8 * 1024..=112 * 1024).contains(&value.dictionary.dictionary_bytes)
        && value.dictionary.sample_bytes >= 1024 * 1024
        && value.dictionary.min_improvement_bps <= 10_000
        && value.dictionary.retrain_churn_bps <= 10_000
        && value.dictionary.promotion_cooldown_seconds > 0
}

fn put_policy(output: &mut [u8], value: StoragePolicyRecord) {
    put_u32(output, 0, value.settle_seconds);
    put_u32(output, 4, value.max_stale_seconds);
    // Bytes 8..16 are reserved for policy knobs removed before V6 stabilized.
    put_u32(output, 16, value.dictionary.dictionary_bytes);
    put_u64(output, 20, value.dictionary.sample_bytes);
    put_u16(output, 28, value.dictionary.min_improvement_bps);
    put_u16(output, 30, value.dictionary.retrain_churn_bps);
    put_u32(output, 32, value.dictionary.promotion_cooldown_seconds);
}

fn get_policy(input: &[u8]) -> StoragePolicyRecord {
    StoragePolicyRecord {
        settle_seconds: get_u32(input, 0),
        max_stale_seconds: get_u32(input, 4),
        dictionary: DictionaryPolicyRecord {
            dictionary_bytes: get_u32(input, 16),
            sample_bytes: get_u64(input, 20),
            min_improvement_bps: get_u16(input, 28),
            retrain_churn_bps: get_u16(input, 30),
            promotion_cooldown_seconds: get_u32(input, 32),
        },
    }
}

fn put_sector_checksum(output: &mut [u8; SECTOR_SIZE]) {
    let checksum = crc32fast::hash(&output[..SECTOR_SIZE - 4]);
    put_u32(output, SECTOR_SIZE - 4, checksum);
}

fn check_sector(input: &[u8; SECTOR_SIZE], magic: [u8; 8]) -> Result<(), FormatError> {
    if input[..8] != magic {
        return Err(FormatError::Invalid("invalid magic"));
    }
    if get_u16(input, 8) != FORMAT_VERSION {
        return Err(FormatError::Version);
    }
    if get_u32(input, SECTOR_SIZE - 4) != crc32fast::hash(&input[..SECTOR_SIZE - 4]) {
        return Err(FormatError::Checksum);
    }
    Ok(())
}

fn put_record_checksum<const N: usize>(output: &mut [u8; N]) {
    let checksum = crc32fast::hash(&output[..N - 4]);
    put_u32(output, N - 4, checksum);
}

fn check_record<const N: usize>(input: &[u8; N]) -> Result<(), FormatError> {
    if get_u32(input, N - 4) != crc32fast::hash(&input[..N - 4]) {
        return Err(FormatError::Checksum);
    }
    Ok(())
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}
fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
fn get_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(input[offset..offset + 2].try_into().expect("u16 range"))
}
fn get_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + 4].try_into().expect("u32 range"))
}
fn get_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().expect("u64 range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> StoragePolicyRecord {
        StoragePolicyRecord {
            settle_seconds: 300,
            max_stale_seconds: 3600,
            dictionary: DictionaryPolicyRecord {
                dictionary_bytes: 65_536,
                sample_bytes: 32 * 1024 * 1024,
                min_improvement_bps: 500,
                retrain_churn_bps: 2500,
                promotion_cooldown_seconds: 86_400,
            },
        }
    }

    #[test]
    fn commit_header_round_trips_truncate_boundary_and_rejects_invalid_encoding() {
        let header = CommitHeader {
            record_len: u32::try_from(SECTOR_SIZE).expect("sector size fits u32"),
            entry_count: 2,
            txid: 9,
            previous_commit: 128,
            logical_size: 8 * 4096,
            page_size: 4096,
            truncate_pages: Some(3),
            previous_history: [1; 32],
            transaction_hash: [2; 32],
            resulting_history: [3; 32],
            entries_digest: [4; 32],
            commit_unix: 5,
        };
        assert_eq!(
            CommitHeader::decode(&header.encode()).expect("commit"),
            header
        );

        let without_truncate = CommitHeader {
            truncate_pages: None,
            ..header
        };
        assert_eq!(
            CommitHeader::decode(&without_truncate.encode()).expect("commit without truncate"),
            without_truncate
        );

        let mut invalid_presence = header.encode();
        invalid_presence[44] = 2;
        put_record_checksum(&mut invalid_presence);
        assert!(matches!(
            CommitHeader::decode(&invalid_presence),
            Err(FormatError::Invalid("invalid commit truncate boundary"))
        ));

        let mut invalid_absence = without_truncate.encode();
        put_u32(&mut invalid_absence, 184, 3);
        put_record_checksum(&mut invalid_absence);
        assert!(matches!(
            CommitHeader::decode(&invalid_absence),
            Err(FormatError::Invalid("invalid commit truncate boundary"))
        ));
    }

    #[test]
    fn segment_names_sort_and_round_trip() {
        let a = SegmentId {
            start_txid: 1,
            end_txid: 2,
            end_history: [1; 32],
            physical_digest: [2; 32],
        };
        let b = SegmentId {
            start_txid: 3,
            end_txid: 10,
            end_history: [3; 32],
            physical_digest: [4; 32],
        };
        assert!(a.filename() < b.filename());
        assert_eq!(SegmentId::parse_filename(&a.filename()).expect("name"), a);
    }

    #[test]
    fn segment_header_rejects_wrapping_section_offsets() {
        let header = SegmentHeader {
            database_id: [1; 32],
            page_size: 4096,
            start_txid: 1,
            base_history: genesis_history(),
            parent_physical_digest: [0; 32],
            base_logical_size: 0,
            generation: 1,
            last_dictionary_promotion_unix: 0,
            policy: policy(),
            dictionary_offset: u64::MAX - 7,
            dictionary_len: 16,
            base_map_offset: u64::MAX,
            base_map_len: 1,
            records_offset: u64::MAX,
        };
        assert!(matches!(
            SegmentHeader::decode(&header.encode()),
            Err(FormatError::Invalid("invalid segment header"))
        ));
    }
}
