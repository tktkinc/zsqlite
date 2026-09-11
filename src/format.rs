//! Binary records for the LTX-style segment format.

use std::fmt::Write as _;

pub const FORMAT_VERSION: u16 = 6;
pub const SECTOR_SIZE: usize = 4096;
pub const SEGMENT_HEADER_SIZE: usize = SECTOR_SIZE;
pub const SEGMENT_TRAILER_SIZE: usize = SECTOR_SIZE;
pub const ACTIVE_STATE_SIZE: usize = SECTOR_SIZE;
pub const FRAME_HEADER_SIZE: usize = 24;
pub const SEGMENT_INDEX_ENTRY_SIZE: usize = 60;
pub const MAX_SECTION_BYTES: u64 = 512 * 1024 * 1024;
const MAX_FRAME_PAYLOAD: u32 = 65_536;

pub const SEGMENT_MAGIC: &[u8; 8] = b"ZSQLSE06";
pub const TRAILER_MAGIC: &[u8; 8] = b"ZSQLST06";
pub const FRAME_MAGIC: &[u8; 4] = b"PFR6";
pub const ACTIVE_STATE_MAGIC: &[u8; 8] = b"ZSQLAS06";

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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoragePolicyRecord {
    pub settle_seconds: u32,
    pub max_stale_seconds: u32,
    /// Seal an active segment at the next committed boundary once it reaches
    /// this many bytes. Zero disables the byte trigger.
    pub target_segment_bytes: u64,
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
    /// Active files contain mutable raw page records. Sealed segments never
    /// set this bit.
    pub mutable_snapshot: bool,
    pub database_id: DatabaseId,
    pub page_size: u32,
    pub start_txid: u64,
    pub base_history: Digest,
    /// Physical digest of the immediately preceding sealed segment. A zero
    /// digest marks a snapshot segment (including the empty genesis active).
    pub parent_physical_digest: Digest,
    pub base_logical_size: u64,
    pub generation: u64,
    pub policy: StoragePolicyRecord,
    /// Dictionary entries introduced by this segment. Frame selectors address
    /// the cumulative dictionary set inherited through the parent lineage.
    pub dictionary_offset: u64,
    pub dictionary_len: u64,
    pub base_map_offset: u64,
    pub base_map_len: u64,
    pub records_offset: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveState {
    pub database_id: DatabaseId,
    pub sequence: u64,
    pub txid: u64,
    pub logical_size: u64,
    pub page_size: u32,
    pub history: Digest,
    pub commit_unix: u64,
    pub record_count: u64,
    /// Lowest logical page count reached by this active generation.
    pub truncate_pages: Option<u32>,
}

impl ActiveState {
    #[must_use]
    pub fn encode(self) -> [u8; ACTIVE_STATE_SIZE] {
        let mut output = [0_u8; ACTIVE_STATE_SIZE];
        output[..8].copy_from_slice(ACTIVE_STATE_MAGIC);
        put_u16(&mut output, 8, FORMAT_VERSION);
        output[16..48].copy_from_slice(&self.database_id);
        put_u64(&mut output, 48, self.sequence);
        put_u64(&mut output, 56, self.txid);
        put_u64(&mut output, 64, self.logical_size);
        put_u32(&mut output, 72, self.page_size);
        output[80..112].copy_from_slice(&self.history);
        put_u64(&mut output, 112, self.commit_unix);
        put_u64(&mut output, 120, self.record_count);
        if let Some(truncate_pages) = self.truncate_pages {
            output[128] = 1;
            put_u32(&mut output, 132, truncate_pages);
        }
        put_sector_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; ACTIVE_STATE_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, *ACTIVE_STATE_MAGIC)?;
        let output = Self {
            database_id: input[16..48].try_into().expect("fixed database ID"),
            sequence: get_u64(input, 48),
            txid: get_u64(input, 56),
            logical_size: get_u64(input, 64),
            page_size: get_u32(input, 72),
            history: input[80..112].try_into().expect("fixed digest"),
            commit_unix: get_u64(input, 112),
            record_count: get_u64(input, 120),
            truncate_pages: match input[128] {
                0 if get_u32(input, 132) == 0 => None,
                1 => Some(get_u32(input, 132)),
                _ => return Err(FormatError::Invalid("invalid active truncate boundary")),
            },
        };
        if output.sequence == 0
            || (output.page_size == 0 && output.logical_size != 0)
            || (output.page_size != 0
                && (!valid_page_size(output.page_size)
                    || !output
                        .logical_size
                        .is_multiple_of(u64::from(output.page_size))))
            || input[10..16] != [0; 6]
            || input[76..80] != [0; 4]
            || output.truncate_pages.is_some_and(|pages| {
                output.page_size == 0
                    || u64::from(pages) > output.logical_size / u64::from(output.page_size)
            })
            || input[129..132] != [0; 3]
            || input[136..ACTIVE_STATE_SIZE - 4]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(FormatError::Invalid("invalid active state"));
        }
        Ok(output)
    }
}

impl SegmentHeader {
    #[must_use]
    pub fn encode(self) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(SEGMENT_MAGIC);
        put_u16(&mut output, 8, FORMAT_VERSION);
        output[10] = u8::from(self.mutable_snapshot);
        output[16..48].copy_from_slice(&self.database_id);
        // Bytes 48..64 are reserved. Early pre-release V6 builds stored a
        // non-authoritative active ID here; decoders intentionally ignore it.
        put_u32(&mut output, 64, self.page_size);
        put_u64(&mut output, 72, self.start_txid);
        output[80..112].copy_from_slice(&self.base_history);
        output[112..144].copy_from_slice(&self.parent_physical_digest);
        put_u64(&mut output, 144, self.base_logical_size);
        put_u64(&mut output, 152, self.generation);
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
            mutable_snapshot: match input[10] {
                0 => false,
                1 => true,
                _ => return Err(FormatError::Invalid("invalid active layout")),
            },
            database_id: input[16..48].try_into().expect("fixed database ID"),
            page_size: get_u32(input, 64),
            start_txid: get_u64(input, 72),
            base_history: input[80..112].try_into().expect("fixed digest"),
            parent_physical_digest: input[112..144].try_into().expect("fixed digest"),
            base_logical_size: get_u64(input, 144),
            generation: get_u64(input, 152),
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
            || input[11..16] != [0; 5]
            || input[160..168] != [0; 8]
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
    pub page_no: u32,
    pub codec: Codec,
    pub dictionary_index: u16,
    pub stored_len: u32,
    pub raw_len: u32,
}

impl FrameHeader {
    #[must_use]
    pub fn record_len(self) -> u64 {
        FRAME_HEADER_SIZE as u64 + u64::from(self.stored_len)
    }

    #[must_use]
    pub fn encode(self) -> [u8; FRAME_HEADER_SIZE] {
        let mut output = [0_u8; FRAME_HEADER_SIZE];
        output[..4].copy_from_slice(FRAME_MAGIC);
        put_u32(&mut output, 4, self.page_no);
        output[8] = self.codec as u8;
        put_u16(&mut output, 10, self.dictionary_index);
        put_u32(&mut output, 12, self.stored_len);
        put_u32(&mut output, 16, self.raw_len);
        put_record_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; FRAME_HEADER_SIZE]) -> Result<Self, FormatError> {
        if &input[..4] != FRAME_MAGIC {
            return Err(FormatError::Invalid("invalid frame magic"));
        }
        check_record(input)?;
        let output = Self {
            page_no: get_u32(input, 4),
            codec: Codec::try_from(input[8])?,
            dictionary_index: get_u16(input, 10),
            stored_len: get_u32(input, 12),
            raw_len: get_u32(input, 16),
        };
        if output.page_no == 0
            || input[9] != 0
            || !valid_page_size(output.raw_len)
            || output.stored_len == 0
            || output.stored_len > output.raw_len
            || output.stored_len > MAX_FRAME_PAYLOAD
            || (output.codec == Codec::Raw
                && (output.dictionary_index != u16::MAX || output.stored_len != output.raw_len))
            || (output.codec == Codec::Zstd && output.dictionary_index == u16::MAX)
        {
            return Err(FormatError::Invalid("invalid page frame"));
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
                    + MAX_FRAME_PAYLOAD
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
        && (value.target_segment_bytes == 0 || value.target_segment_bytes >= 1024 * 1024)
        && (8 * 1024..=112 * 1024).contains(&value.dictionary.dictionary_bytes)
        && value.dictionary.sample_bytes >= 1024 * 1024
}

fn put_policy(output: &mut [u8], value: StoragePolicyRecord) {
    put_u32(output, 0, value.settle_seconds);
    put_u32(output, 4, value.max_stale_seconds);
    put_u64(output, 8, value.target_segment_bytes);
    put_u32(output, 16, value.dictionary.dictionary_bytes);
    put_u64(output, 20, value.dictionary.sample_bytes);
}

fn get_policy(input: &[u8]) -> StoragePolicyRecord {
    StoragePolicyRecord {
        settle_seconds: get_u32(input, 0),
        max_stale_seconds: get_u32(input, 4),
        target_segment_bytes: get_u64(input, 8),
        dictionary: DictionaryPolicyRecord {
            dictionary_bytes: get_u32(input, 16),
            sample_bytes: get_u64(input, 20),
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
            target_segment_bytes: 64 * 1024 * 1024,
            dictionary: DictionaryPolicyRecord {
                dictionary_bytes: 65_536,
                sample_bytes: 32 * 1024 * 1024,
            },
        }
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
            mutable_snapshot: false,
            database_id: [1; 32],
            page_size: 4096,
            start_txid: 1,
            base_history: genesis_history(),
            parent_physical_digest: [0; 32],
            base_logical_size: 0,
            generation: 1,
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
