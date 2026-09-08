//! Binary records for the LTX-style segment format.

use std::fmt::Write as _;

pub const FORMAT_VERSION: u16 = 5;
pub const SECTOR_SIZE: usize = 4096;
pub const ANCHOR_HEADER_SIZE: usize = SECTOR_SIZE;
pub const ANCHOR_ROOT_A_OFFSET: u64 = SECTOR_SIZE as u64;
pub const ANCHOR_ROOT_B_OFFSET: u64 = (SECTOR_SIZE * 2) as u64;
pub const ANCHOR_SIZE: usize = SECTOR_SIZE * 3;
pub const SEGMENT_HEADER_SIZE: usize = SECTOR_SIZE;
pub const SEGMENT_TRAILER_SIZE: usize = SECTOR_SIZE;
pub const FRAME_HEADER_SIZE: usize = 80;
pub const COMMIT_HEADER_SIZE: usize = 184;
pub const COMMIT_ENTRY_SIZE: usize = 56;
pub const SEGMENT_INDEX_ENTRY_SIZE: usize = 60;
pub const MAX_SECTION_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_SEGMENTS: usize = 4_000_000;
const MAX_FRAME_CAPACITY: u32 = 65_536;

pub const ANCHOR_MAGIC: &[u8; 8] = b"ZSQLSG05";
pub const ROOT_MAGIC: &[u8; 8] = b"ZSQLRT05";
pub const CATALOG_MAGIC: &[u8; 8] = b"ZSQLRC05";
pub const SEGMENT_MAGIC: &[u8; 8] = b"ZSQLSE05";
pub const TRAILER_MAGIC: &[u8; 8] = b"ZSQLST05";
pub const FRAME_MAGIC: &[u8; 4] = b"PFR5";
pub const FREE_MAGIC: &[u8; 4] = b"FRE5";
pub const COMMIT_MAGIC: &[u8; 4] = b"CMT5";

pub type Digest = [u8; 32];
pub type DatabaseId = [u8; 32];
pub type ActiveId = [u8; 16];

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
    pub hot_horizon_seconds: u32,
    pub admission_reads: u16,
    pub gc_dead_percent: u16,
    pub dictionary: DictionaryPolicyRecord,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnchorHeader {
    pub database_id: DatabaseId,
}

impl AnchorHeader {
    #[must_use]
    pub fn encode(self) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(ANCHOR_MAGIC);
        put_u16(&mut output, 8, FORMAT_VERSION);
        output[16..48].copy_from_slice(&self.database_id);
        put_sector_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, *ANCHOR_MAGIC)?;
        Ok(Self {
            database_id: input[16..48].try_into().expect("fixed database ID"),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnchorRoot {
    pub sequence: u64,
    pub durable: bool,
    pub database_id: DatabaseId,
    pub page_size: u32,
    pub logical_size: u64,
    pub head_txid: u64,
    pub head_history: Digest,
    pub catalog_digest: Digest,
    pub active_id: ActiveId,
    pub active_commit_offset: u64,
    pub active_commit_end: u64,
    pub oldest_dirty_unix: u64,
    pub last_dirty_unix: u64,
    pub last_dictionary_promotion_unix: u64,
    pub policy: StoragePolicyRecord,
}

impl AnchorRoot {
    #[must_use]
    pub fn encode(self) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(ROOT_MAGIC);
        put_u16(&mut output, 8, FORMAT_VERSION);
        output[10] = u8::from(self.durable);
        put_u64(&mut output, 16, self.sequence);
        output[24..56].copy_from_slice(&self.database_id);
        put_u32(&mut output, 56, self.page_size);
        put_u64(&mut output, 64, self.logical_size);
        put_u64(&mut output, 72, self.head_txid);
        output[80..112].copy_from_slice(&self.head_history);
        output[112..144].copy_from_slice(&self.catalog_digest);
        output[144..160].copy_from_slice(&self.active_id);
        put_u64(&mut output, 160, self.active_commit_offset);
        put_u64(&mut output, 168, self.active_commit_end);
        put_u64(&mut output, 176, self.oldest_dirty_unix);
        put_u64(&mut output, 184, self.last_dirty_unix);
        put_u64(&mut output, 192, self.last_dictionary_promotion_unix);
        put_policy(&mut output[208..256], self.policy);
        put_sector_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, *ROOT_MAGIC)?;
        let root = Self {
            durable: input[10] == 1,
            sequence: get_u64(input, 16),
            database_id: input[24..56].try_into().expect("fixed database ID"),
            page_size: get_u32(input, 56),
            logical_size: get_u64(input, 64),
            head_txid: get_u64(input, 72),
            head_history: input[80..112].try_into().expect("fixed digest"),
            catalog_digest: input[112..144].try_into().expect("fixed digest"),
            active_id: input[144..160].try_into().expect("fixed active ID"),
            active_commit_offset: get_u64(input, 160),
            active_commit_end: get_u64(input, 168),
            oldest_dirty_unix: get_u64(input, 176),
            last_dirty_unix: get_u64(input, 184),
            last_dictionary_promotion_unix: get_u64(input, 192),
            policy: get_policy(&input[208..256]),
        };
        if input[10] > 1
            || !valid_policy(root.policy)
            || (root.page_size != 0 && !valid_page_size(root.page_size))
            || (root.page_size != 0 && !root.logical_size.is_multiple_of(u64::from(root.page_size)))
            || root.active_commit_end < root.active_commit_offset
            || (root.active_id == [0; 16]
                && (root.active_commit_offset != 0 || root.active_commit_end != 0))
            || (root.active_id != [0; 16]
                && (root.active_commit_offset == 0 || root.active_commit_end == 0))
            || (root.page_size == 0
                && (root.logical_size != 0 || root.head_txid != 0 || root.active_id != [0; 16]))
        {
            return Err(FormatError::Invalid("invalid anchor root"));
        }
        Ok(root)
    }
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
pub struct CatalogEntry {
    pub id: SegmentId,
    pub base_history: Digest,
    pub file_len: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootCatalog {
    pub generation: u64,
    pub database_id: DatabaseId,
    pub head_txid: u64,
    pub head_history: Digest,
    pub current_dictionary: Vec<u8>,
    pub segments: Vec<CatalogEntry>,
}

impl RootCatalog {
    pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
        self.validate()?;
        if self.segments.len() > MAX_SEGMENTS || self.current_dictionary.len() > u32::MAX as usize {
            return Err(FormatError::TooLarge);
        }
        let encoded_len = 136_usize
            .checked_add(self.current_dictionary.len())
            .and_then(|length| length.checked_add(self.segments.len().checked_mul(120)?))
            .ok_or(FormatError::TooLarge)?;
        if encoded_len as u64 > MAX_SECTION_BYTES {
            return Err(FormatError::TooLarge);
        }
        let mut output = Vec::with_capacity(encoded_len);
        output.extend_from_slice(CATALOG_MAGIC);
        output.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        output.extend_from_slice(&[0; 6]);
        output.extend_from_slice(&self.generation.to_le_bytes());
        output.extend_from_slice(&self.database_id);
        output.extend_from_slice(&self.head_txid.to_le_bytes());
        output.extend_from_slice(&self.head_history);
        let dictionary_digest = if self.current_dictionary.is_empty() {
            [0; 32]
        } else {
            digest(&self.current_dictionary)
        };
        output.extend_from_slice(&dictionary_digest);
        output.extend_from_slice(
            &u32::try_from(self.current_dictionary.len())
                .map_err(|_| FormatError::TooLarge)?
                .to_le_bytes(),
        );
        output.extend_from_slice(
            &u32::try_from(self.segments.len())
                .map_err(|_| FormatError::TooLarge)?
                .to_le_bytes(),
        );
        output.extend_from_slice(&self.current_dictionary);
        for entry in &self.segments {
            output.extend_from_slice(&entry.id.start_txid.to_le_bytes());
            output.extend_from_slice(&entry.id.end_txid.to_le_bytes());
            output.extend_from_slice(&entry.base_history);
            output.extend_from_slice(&entry.id.end_history);
            output.extend_from_slice(&entry.id.physical_digest);
            output.extend_from_slice(&entry.file_len.to_le_bytes());
        }
        Ok(output)
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        const HEADER: usize = 136;
        const ENTRY: usize = 120;
        if input.len() < HEADER
            || &input[..8] != CATALOG_MAGIC
            || get_u16(input, 8) != FORMAT_VERSION
        {
            return Err(FormatError::Invalid("invalid root catalog header"));
        }
        let dictionary_len = get_u32(input, 128) as usize;
        let segment_count = get_u32(input, 132) as usize;
        if segment_count > MAX_SEGMENTS {
            return Err(FormatError::TooLarge);
        }
        let expected = HEADER
            .checked_add(dictionary_len)
            .and_then(|value| value.checked_add(segment_count.checked_mul(ENTRY)?))
            .ok_or(FormatError::TooLarge)?;
        if expected as u64 > MAX_SECTION_BYTES || input.len() != expected {
            return Err(FormatError::Invalid("invalid root catalog length"));
        }
        let current_dictionary = input[HEADER..HEADER + dictionary_len].to_vec();
        let expected_dictionary: Digest = input[96..128].try_into().expect("fixed digest");
        if (current_dictionary.is_empty() && expected_dictionary != [0; 32])
            || (!current_dictionary.is_empty()
                && digest(&current_dictionary) != expected_dictionary)
        {
            return Err(FormatError::Digest);
        }
        let mut segments = Vec::with_capacity(segment_count);
        let mut cursor = HEADER + dictionary_len;
        for _ in 0..segment_count {
            let entry = CatalogEntry {
                id: SegmentId {
                    start_txid: get_u64(input, cursor),
                    end_txid: get_u64(input, cursor + 8),
                    end_history: input[cursor + 48..cursor + 80]
                        .try_into()
                        .expect("fixed digest"),
                    physical_digest: input[cursor + 80..cursor + 112]
                        .try_into()
                        .expect("fixed digest"),
                },
                base_history: input[cursor + 16..cursor + 48]
                    .try_into()
                    .expect("fixed digest"),
                file_len: get_u64(input, cursor + 112),
            };
            if entry.id.start_txid == 0 || entry.id.end_txid < entry.id.start_txid {
                return Err(FormatError::Invalid("invalid catalog range"));
            }
            segments.push(entry);
            cursor += ENTRY;
        }
        let catalog = Self {
            generation: get_u64(input, 16),
            database_id: input[24..56].try_into().expect("fixed database ID"),
            head_txid: get_u64(input, 56),
            head_history: input[64..96].try_into().expect("fixed digest"),
            current_dictionary,
            segments,
        };
        catalog.validate()?;
        Ok(catalog)
    }

    pub fn validate(&self) -> Result<(), FormatError> {
        let mut previous_end = 0_u64;
        let mut previous_history = genesis_history();
        for entry in &self.segments {
            if entry.id.start_txid != previous_end.saturating_add(1)
                || entry.base_history != previous_history
            {
                return Err(FormatError::Invalid("non-contiguous root catalog"));
            }
            previous_end = entry.id.end_txid;
            previous_history = entry.id.end_history;
        }
        if previous_end != self.head_txid || previous_history != self.head_history {
            return Err(FormatError::Invalid("catalog head mismatch"));
        }
        Ok(())
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
    let mut output = Vec::new();
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
    let mut output = Vec::with_capacity(count);
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
        let bytes = input[cursor..end].to_vec();
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
    pub active_id: ActiveId,
    pub page_size: u32,
    pub start_txid: u64,
    pub base_history: Digest,
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
        output[48..64].copy_from_slice(&self.active_id);
        put_u32(&mut output, 64, self.page_size);
        put_u64(&mut output, 72, self.start_txid);
        output[80..112].copy_from_slice(&self.base_history);
        put_u64(&mut output, 112, self.dictionary_offset);
        put_u64(&mut output, 120, self.dictionary_len);
        put_u64(&mut output, 128, self.base_map_offset);
        put_u64(&mut output, 136, self.base_map_len);
        put_u64(&mut output, 144, self.records_offset);
        put_sector_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, *SEGMENT_MAGIC)?;
        let output = Self {
            database_id: input[16..48].try_into().expect("fixed database ID"),
            active_id: input[48..64].try_into().expect("fixed active ID"),
            page_size: get_u32(input, 64),
            start_txid: get_u64(input, 72),
            base_history: input[80..112].try_into().expect("fixed digest"),
            dictionary_offset: get_u64(input, 112),
            dictionary_len: get_u64(input, 120),
            base_map_offset: get_u64(input, 128),
            base_map_len: get_u64(input, 136),
            records_offset: get_u64(input, 144),
        };
        if output.start_txid == 0
            || (output.page_size != 0 && !valid_page_size(output.page_size))
            || output.dictionary_offset < SEGMENT_HEADER_SIZE as u64
            || output.dictionary_len > MAX_SECTION_BYTES
            || output.base_map_len > MAX_SECTION_BYTES
            || output.base_map_offset
                < output
                    .dictionary_offset
                    .saturating_add(output.dictionary_len)
            || output.records_offset < output.base_map_offset.saturating_add(output.base_map_len)
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
    pub previous_history: Digest,
    pub transaction_hash: Digest,
    pub resulting_history: Digest,
    pub entries_digest: Digest,
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
        output[48..80].copy_from_slice(&self.previous_history);
        output[80..112].copy_from_slice(&self.transaction_hash);
        output[112..144].copy_from_slice(&self.resulting_history);
        output[144..176].copy_from_slice(&self.entries_digest);
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
            previous_history: input[48..80].try_into().expect("fixed digest"),
            transaction_hash: input[80..112].try_into().expect("fixed digest"),
            resulting_history: input[112..144].try_into().expect("fixed digest"),
            entries_digest: input[144..176].try_into().expect("fixed digest"),
        };
        let expected = COMMIT_HEADER_SIZE
            .checked_add(output.entry_count as usize * COMMIT_ENTRY_SIZE)
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
        && value.hot_horizon_seconds > 0
        && value.admission_reads > 0
        && value.gc_dead_percent <= 100
        && (8 * 1024..=112 * 1024).contains(&value.dictionary.dictionary_bytes)
        && value.dictionary.sample_bytes >= 1024 * 1024
        && value.dictionary.min_improvement_bps <= 10_000
        && value.dictionary.retrain_churn_bps <= 10_000
        && value.dictionary.promotion_cooldown_seconds > 0
}

fn put_policy(output: &mut [u8], value: StoragePolicyRecord) {
    put_u32(output, 0, value.settle_seconds);
    put_u32(output, 4, value.max_stale_seconds);
    put_u32(output, 8, value.hot_horizon_seconds);
    put_u16(output, 12, value.admission_reads);
    put_u16(output, 14, value.gc_dead_percent);
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
        hot_horizon_seconds: get_u32(input, 8),
        admission_reads: get_u16(input, 12),
        gc_dead_percent: get_u16(input, 14),
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
            hot_horizon_seconds: 86_400,
            admission_reads: 2,
            gc_dead_percent: 50,
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
    fn fixed_records_round_trip_and_reject_mutation() {
        let root = AnchorRoot {
            sequence: 9,
            durable: true,
            database_id: [7; 32],
            page_size: 4096,
            logical_size: 8192,
            head_txid: 4,
            head_history: [8; 32],
            catalog_digest: [9; 32],
            active_id: [3; 16],
            active_commit_offset: 100,
            active_commit_end: 200,
            oldest_dirty_unix: 1,
            last_dirty_unix: 2,
            last_dictionary_promotion_unix: 3,
            policy: policy(),
        };
        assert_eq!(AnchorRoot::decode(&root.encode()).expect("root"), root);
        let mut corrupt = root.encode();
        corrupt[80] ^= 1;
        assert!(matches!(
            AnchorRoot::decode(&corrupt),
            Err(FormatError::Checksum)
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
    fn catalog_round_trip() {
        let catalog = RootCatalog {
            generation: 1,
            database_id: [3; 32],
            head_txid: 2,
            head_history: [9; 32],
            current_dictionary: vec![4; 8192],
            segments: vec![CatalogEntry {
                id: SegmentId {
                    start_txid: 1,
                    end_txid: 2,
                    end_history: [9; 32],
                    physical_digest: [8; 32],
                },
                base_history: genesis_history(),
                file_len: 42,
            }],
        };
        let encoded = catalog.encode().expect("encode");
        assert_eq!(RootCatalog::decode(&encoded).expect("decode"), catalog);
    }
}
