//! V1 wire records for the active file and the common frame envelope.
//! These serialized fields are untrusted inputs, distinct from domain proofs.

use std::fmt::Write as _;

pub const FORMAT_VERSION: u16 = 1;
pub const SECTOR_SIZE: usize = 4096;
pub const ACTIVE_HEADER_SIZE: usize = SECTOR_SIZE;
pub const ACTIVE_STATE_SIZE: usize = SECTOR_SIZE;
pub const ACTIVE_METADATA_SIZE: usize = ACTIVE_HEADER_SIZE + 2 * ACTIVE_STATE_SIZE;
pub const FRAME_HEADER_SIZE: usize = 24;
const MAX_FRAME_PAYLOAD: u32 = 8 * 1024 * 1024;

pub const ACTIVE_MAGIC: &[u8; 8] = b"ZSQLAC01";
pub const FRAME_MAGIC: &[u8; 4] = b"PFR1";
pub const ACTIVE_STATE_MAGIC: &[u8; 8] = b"ZSQLAS01";

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
    /// Seal the active file at the next committed boundary once it reaches
    /// this many bytes. Zero disables the byte trigger.
    pub rollover_bytes: u64,
    pub dictionary: DictionaryPolicyRecord,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveHeader {
    /// Namespace attachment fencing token; changed only by local bootstrap.
    pub attachment_id: [u8; 32],
    pub database_id: DatabaseId,
    pub page_size: u32,
    pub start_txid: u64,
    pub base_history: Digest,
    /// Physical digest of the immediately preceding sealed manifest. A zero
    /// digest identifies an empty genesis active file.
    pub parent_physical_digest: Digest,
    pub base_logical_size: u64,
    pub policy: StoragePolicyRecord,
    pub layout: crate::layout::LayoutPolicy,
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
    /// Lowest logical page count reached by this active file.
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

impl ActiveHeader {
    #[must_use]
    pub fn encode(self) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(ACTIVE_MAGIC);
        put_u16(&mut output, 8, FORMAT_VERSION);
        output[16..48].copy_from_slice(&self.database_id);
        put_u32(&mut output, 64, self.page_size);
        put_u64(&mut output, 72, self.start_txid);
        output[80..112].copy_from_slice(&self.base_history);
        output[112..144].copy_from_slice(&self.parent_physical_digest);
        put_u64(&mut output, 144, self.base_logical_size);
        put_policy(&mut output[208..256], self.policy);
        output[256..384].copy_from_slice(&self.layout.encode());
        output[384..416].copy_from_slice(&self.attachment_id);
        put_sector_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, *ACTIVE_MAGIC)?;
        let output = Self {
            attachment_id: input[384..416].try_into().expect("fixed attachment ID"),
            database_id: input[16..48].try_into().expect("fixed database ID"),
            page_size: get_u32(input, 64),
            start_txid: get_u64(input, 72),
            base_history: input[80..112].try_into().expect("fixed digest"),
            parent_physical_digest: input[112..144].try_into().expect("fixed digest"),
            base_logical_size: get_u64(input, 144),
            policy: get_policy(&input[208..256]),
            layout: crate::layout::LayoutPolicy::decode(
                input[256..384].try_into().expect("fixed layout policy"),
            )
            .map_err(|_| FormatError::Invalid("invalid frame layout policy"))?,
        };
        if output.start_txid == 0
            || input[10..16] != [0; 6]
            || input[48..64] != [0; 16]
            || input[68..72] != [0; 4]
            || input[152..208].iter().any(|byte| *byte != 0)
            || !valid_policy(output.policy)
            || (output.page_size != 0 && !valid_page_size(output.page_size))
            || (output.page_size == 0 && output.base_logical_size != 0)
            || (output.page_size != 0
                && !output
                    .base_logical_size
                    .is_multiple_of(u64::from(output.page_size)))
            || (output.start_txid == 1) != (output.parent_physical_digest == [0; 32])
            || (output.start_txid == 1 && output.base_history != genesis_history())
            || input[236..256].iter().any(|byte| *byte != 0)
            || input[416..ACTIVE_HEADER_SIZE - 4]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(FormatError::Invalid("invalid active header"));
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
            || output.raw_len < 512
            || output.raw_len > MAX_FRAME_PAYLOAD
            || !output.raw_len.is_multiple_of(512)
            || output.stored_len == 0
            || output.stored_len > output.raw_len
            || output.stored_len > MAX_FRAME_PAYLOAD
            || (output.codec == Codec::Raw
                && (output.dictionary_index != u16::MAX || output.stored_len != output.raw_len))
        {
            return Err(FormatError::Invalid("invalid page frame"));
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
    digest(b"zsqlite/sealed-lineage/v1/genesis")
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
        && (value.rollover_bytes == 0 || value.rollover_bytes >= 1024 * 1024)
        && crate::DictionaryPolicy::new(
            value.dictionary.dictionary_bytes,
            value.dictionary.sample_bytes,
        )
        .is_ok()
}

fn put_policy(output: &mut [u8], value: StoragePolicyRecord) {
    put_u32(output, 0, value.settle_seconds);
    put_u32(output, 4, value.max_stale_seconds);
    put_u64(output, 8, value.rollover_bytes);
    put_u32(output, 16, value.dictionary.dictionary_bytes);
    put_u64(output, 20, value.dictionary.sample_bytes);
}

fn get_policy(input: &[u8]) -> StoragePolicyRecord {
    StoragePolicyRecord {
        settle_seconds: get_u32(input, 0),
        max_stale_seconds: get_u32(input, 4),
        rollover_bytes: get_u64(input, 8),
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
            rollover_bytes: 64 * 1024 * 1024,
            dictionary: DictionaryPolicyRecord {
                dictionary_bytes: 65_536,
                sample_bytes: 8 * 1024 * 1024,
            },
        }
    }

    #[test]
    fn active_header_rejects_nonzero_reserved_bytes() {
        let header = ActiveHeader {
            attachment_id: [1; 32],
            layout: crate::layout::LayoutPolicy::default(),
            database_id: [1; 32],
            page_size: 4096,
            start_txid: 1,
            base_history: genesis_history(),
            parent_physical_digest: [0; 32],
            base_logical_size: 0,
            policy: policy(),
        };
        for offset in [10, 48, 68, 152, 236, 416] {
            let mut encoded = header.encode();
            encoded[offset] = 1;
            put_sector_checksum(&mut encoded);
            assert!(matches!(
                ActiveHeader::decode(&encoded),
                Err(FormatError::Invalid("invalid active header"))
            ));
        }
    }
}
