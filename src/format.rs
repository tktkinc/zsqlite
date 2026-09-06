//! Stable, endian-independent V3 sidecar format primitives.

use crc32fast::Hasher;

pub const FORMAT_VERSION: u16 = 3;
pub const SECTOR_SIZE: usize = 4096;
const SECTOR_SIZE_U32: u32 = 4096;
pub const HEADER_SIZE: usize = SECTOR_SIZE * 3;
const HEADER_SIZE_U32: u32 = 12_288;
pub const SUPERBLOCK_A_OFFSET: u64 = SECTOR_SIZE as u64;
pub const SUPERBLOCK_B_OFFSET: u64 = (SECTOR_SIZE * 2) as u64;
pub const EXTENT_HEADER_SIZE: usize = 128;
pub const COMMIT_SIZE: usize = 128;
pub const INDEX_HEADER_SIZE: usize = 128;
pub const MAX_EXTENT_BYTES: u32 = 1_048_576;
pub const MAX_RECORD_BYTES: u64 = 16 * 1_048_576;
pub const MAX_INDEX_BYTES: u64 = 512 * 1_048_576;

pub const FILE_MAGIC: &[u8; 8] = b"ZSQLPG03";
pub const ANCHOR_MAGIC: &[u8; 8] = b"ZSQLAN03";
pub const EXTENT_MAGIC: &[u8; 4] = b"EXT3";
pub const INDEX_MAGIC: &[u8; 4] = b"IDX3";
pub const COMMIT_MAGIC: &[u8; 4] = b"CMT3";

pub type DatabaseId = [u8; 16];
pub type Digest = [u8; 32];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Codec {
    Raw = 0,
    Zstd = 1,
    ZstdSeekable = 2,
}

impl TryFrom<u8> for Codec {
    type Error = FormatError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Zstd),
            2 => Ok(Self::ZstdSeekable),
            _ => Err(FormatError::UnknownCodec(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Anchor {
    pub database_id: DatabaseId,
}

impl Anchor {
    #[must_use]
    pub fn encode(self) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(ANCHOR_MAGIC);
        output[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        output[16..32].copy_from_slice(&self.database_id);
        put_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, ANCHOR_MAGIC)?;
        if get_u16(input, 8) != FORMAT_VERSION || input[10..16].iter().any(|byte| *byte != 0) {
            return Err(FormatError::InvalidAnchor);
        }
        let database_id = input[16..32].try_into().expect("fixed database id");
        if database_id == [0; 16] || input[32..SECTOR_SIZE - 4].iter().any(|byte| *byte != 0) {
            return Err(FormatError::InvalidAnchor);
        }
        Ok(Self { database_id })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Header {
    pub database_id: DatabaseId,
}

impl Header {
    #[must_use]
    pub fn encode(self) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(FILE_MAGIC);
        output[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        output[12..16].copy_from_slice(&HEADER_SIZE_U32.to_le_bytes());
        output[16..32].copy_from_slice(&self.database_id);
        put_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Self, FormatError> {
        check_sector(input, FILE_MAGIC)?;
        if get_u16(input, 8) != FORMAT_VERSION
            || input[10..12].iter().any(|byte| *byte != 0)
            || get_u32(input, 12) as usize != HEADER_SIZE
        {
            return Err(FormatError::InvalidHeader);
        }
        let database_id = input[16..32].try_into().expect("fixed database id");
        if database_id == [0; 16] || input[32..SECTOR_SIZE - 4].iter().any(|byte| *byte != 0) {
            return Err(FormatError::InvalidHeader);
        }
        Ok(Self { database_id })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Superblock {
    pub sequence: u64,
    pub generation: u64,
    pub durable: bool,
    pub logical_size: u64,
    pub page_size: u32,
    pub page_count: u32,
    pub commit_offset: u64,
    pub commit_end: u64,
    pub index_offset: u64,
    pub index_end: u64,
    pub database_id: DatabaseId,
}

impl Superblock {
    #[must_use]
    pub fn encode(self) -> [u8; SECTOR_SIZE] {
        let mut output = [0_u8; SECTOR_SIZE];
        output[..8].copy_from_slice(FILE_MAGIC);
        output[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        output[10] = u8::from(self.durable);
        output[16..24].copy_from_slice(&self.sequence.to_le_bytes());
        output[24..32].copy_from_slice(&self.generation.to_le_bytes());
        output[32..40].copy_from_slice(&self.logical_size.to_le_bytes());
        output[40..44].copy_from_slice(&self.page_size.to_le_bytes());
        output[44..48].copy_from_slice(&self.page_count.to_le_bytes());
        output[48..56].copy_from_slice(&self.commit_offset.to_le_bytes());
        output[56..64].copy_from_slice(&self.commit_end.to_le_bytes());
        output[64..72].copy_from_slice(&self.index_offset.to_le_bytes());
        output[72..80].copy_from_slice(&self.index_end.to_le_bytes());
        output[80..96].copy_from_slice(&self.database_id);
        put_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; SECTOR_SIZE]) -> Result<Option<Self>, FormatError> {
        if input.iter().all(|byte| *byte == 0) {
            return Ok(None);
        }
        check_sector(input, FILE_MAGIC)?;
        if get_u16(input, 8) != FORMAT_VERSION
            || input[10] > 1
            || input[11..16].iter().any(|byte| *byte != 0)
            || input[96..SECTOR_SIZE - 4].iter().any(|byte| *byte != 0)
        {
            return Err(FormatError::InvalidSuperblock);
        }
        let value = Self {
            sequence: get_u64(input, 16),
            generation: get_u64(input, 24),
            durable: input[10] != 0,
            logical_size: get_u64(input, 32),
            page_size: get_u32(input, 40),
            page_count: get_u32(input, 44),
            commit_offset: get_u64(input, 48),
            commit_end: get_u64(input, 56),
            index_offset: get_u64(input, 64),
            index_end: get_u64(input, 72),
            database_id: input[80..96].try_into().expect("fixed database id"),
        };
        let empty = value.generation == 0
            && value.logical_size == 0
            && value.page_size == 0
            && value.page_count == 0
            && value.commit_offset == 0
            && value.commit_end == HEADER_SIZE as u64;
        let populated = value.generation != 0
            && valid_page_size(value.page_size)
            && value
                .logical_size
                .is_multiple_of(u64::from(value.page_size))
            && u64::from(value.page_count) == value.logical_size / u64::from(value.page_size)
            && value.commit_offset >= HEADER_SIZE as u64
            && value.commit_offset.checked_add(COMMIT_SIZE as u64) == Some(value.commit_end);
        let index_valid = (value.index_offset == 0 && value.index_end == 0)
            || (value.index_offset >= HEADER_SIZE as u64
                && value.index_end > value.index_offset
                && value.index_end - value.index_offset <= MAX_INDEX_BYTES);
        if value.sequence == 0
            || value.database_id == [0; 16]
            || (!empty && !populated)
            || !index_valid
        {
            return Err(FormatError::InvalidSuperblock);
        }
        Ok(Some(value))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExtentHeader {
    pub generation: u64,
    pub first_page: u32,
    pub page_count: u32,
    pub codec: Codec,
    pub raw_len: u32,
    pub stored_len: u32,
    pub allocation_len: u32,
    pub raw_digest: Digest,
}

impl ExtentHeader {
    #[must_use]
    pub fn encode(self) -> [u8; EXTENT_HEADER_SIZE] {
        let mut output = [0_u8; EXTENT_HEADER_SIZE];
        output[..4].copy_from_slice(EXTENT_MAGIC);
        output[4] = self.codec as u8;
        output[8..16].copy_from_slice(&self.generation.to_le_bytes());
        output[16..20].copy_from_slice(&self.first_page.to_le_bytes());
        output[20..24].copy_from_slice(&self.page_count.to_le_bytes());
        output[24..28].copy_from_slice(&self.raw_len.to_le_bytes());
        output[28..32].copy_from_slice(&self.stored_len.to_le_bytes());
        output[32..36].copy_from_slice(&self.allocation_len.to_le_bytes());
        output[40..72].copy_from_slice(&self.raw_digest);
        put_record_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; EXTENT_HEADER_SIZE]) -> Result<Self, FormatError> {
        check_record(input, EXTENT_MAGIC)?;
        let value = Self {
            codec: Codec::try_from(input[4])?,
            generation: get_u64(input, 8),
            first_page: get_u32(input, 16),
            page_count: get_u32(input, 20),
            raw_len: get_u32(input, 24),
            stored_len: get_u32(input, 28),
            allocation_len: get_u32(input, 32),
            raw_digest: input[40..72].try_into().expect("fixed digest"),
        };
        let minimum = (EXTENT_HEADER_SIZE as u64).checked_add(u64::from(value.stored_len));
        let allocation_valid = minimum.is_some_and(|minimum| {
            u64::from(value.allocation_len) >= minimum
                && u64::from(value.allocation_len) <= MAX_RECORD_BYTES
                && if value.codec == Codec::ZstdSeekable {
                    u64::from(value.allocation_len) == minimum
                } else {
                    value.allocation_len.is_multiple_of(SECTOR_SIZE_U32)
                }
        });
        if input[5..8].iter().any(|byte| *byte != 0)
            || input[36..40].iter().any(|byte| *byte != 0)
            || input[72..EXTENT_HEADER_SIZE - 4]
                .iter()
                .any(|byte| *byte != 0)
            || value.generation == 0
            || value.first_page == 0
            || value.page_count == 0
            || value.raw_len < 512
            || value.raw_len > MAX_EXTENT_BYTES
            || value.stored_len == 0
            || value.raw_digest == [0; 32]
            || (value.codec == Codec::Raw && value.raw_len != value.stored_len)
            || !allocation_valid
        {
            return Err(FormatError::InvalidExtent);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Commit {
    pub generation: u64,
    pub previous_commit: u64,
    pub generation_start: u64,
    pub logical_size: u64,
    pub page_size: u32,
    pub extent_count: u32,
    pub generation_digest: Digest,
}

impl Commit {
    #[must_use]
    pub fn encode(self) -> [u8; COMMIT_SIZE] {
        let mut output = [0_u8; COMMIT_SIZE];
        output[..4].copy_from_slice(COMMIT_MAGIC);
        output[8..16].copy_from_slice(&self.generation.to_le_bytes());
        output[16..24].copy_from_slice(&self.previous_commit.to_le_bytes());
        output[24..32].copy_from_slice(&self.generation_start.to_le_bytes());
        output[32..40].copy_from_slice(&self.logical_size.to_le_bytes());
        output[40..44].copy_from_slice(&self.page_size.to_le_bytes());
        output[44..48].copy_from_slice(&self.extent_count.to_le_bytes());
        output[48..80].copy_from_slice(&self.generation_digest);
        put_record_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; COMMIT_SIZE]) -> Result<Self, FormatError> {
        check_record(input, COMMIT_MAGIC)?;
        let value = Self {
            generation: get_u64(input, 8),
            previous_commit: get_u64(input, 16),
            generation_start: get_u64(input, 24),
            logical_size: get_u64(input, 32),
            page_size: get_u32(input, 40),
            extent_count: get_u32(input, 44),
            generation_digest: input[48..80].try_into().expect("fixed digest"),
        };
        if input[4..8].iter().any(|byte| *byte != 0)
            || input[80..COMMIT_SIZE - 4].iter().any(|byte| *byte != 0)
            || value.generation == 0
            || value.generation_start < HEADER_SIZE as u64
            || (value.previous_commit != 0 && value.previous_commit < HEADER_SIZE as u64)
            || !valid_page_size(value.page_size)
            || !value
                .logical_size
                .is_multiple_of(u64::from(value.page_size))
            || value.generation_digest == [0; 32]
        {
            return Err(FormatError::InvalidCommit);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexHeader {
    pub generation: u64,
    pub entry_count: u32,
    pub codec: Codec,
    pub raw_len: u64,
    pub stored_len: u64,
    pub raw_digest: Digest,
}

impl IndexHeader {
    #[must_use]
    pub fn encode(self) -> [u8; INDEX_HEADER_SIZE] {
        let mut output = [0_u8; INDEX_HEADER_SIZE];
        output[..4].copy_from_slice(INDEX_MAGIC);
        output[4] = self.codec as u8;
        output[8..16].copy_from_slice(&self.generation.to_le_bytes());
        output[16..20].copy_from_slice(&self.entry_count.to_le_bytes());
        output[24..32].copy_from_slice(&self.raw_len.to_le_bytes());
        output[32..40].copy_from_slice(&self.stored_len.to_le_bytes());
        output[40..72].copy_from_slice(&self.raw_digest);
        put_record_checksum(&mut output);
        output
    }

    pub fn decode(input: &[u8; INDEX_HEADER_SIZE]) -> Result<Self, FormatError> {
        check_record(input, INDEX_MAGIC)?;
        let value = Self {
            codec: Codec::try_from(input[4])?,
            generation: get_u64(input, 8),
            entry_count: get_u32(input, 16),
            raw_len: get_u64(input, 24),
            stored_len: get_u64(input, 32),
            raw_digest: input[40..72].try_into().expect("fixed digest"),
        };
        if input[5..8].iter().any(|byte| *byte != 0)
            || input[20..24].iter().any(|byte| *byte != 0)
            || input[72..INDEX_HEADER_SIZE - 4]
                .iter()
                .any(|byte| *byte != 0)
            || value.generation == 0
            || value.raw_len == 0
            || value.stored_len == 0
            || value.raw_len > MAX_INDEX_BYTES
            || value.stored_len > MAX_INDEX_BYTES
            || value.raw_digest == [0; 32]
            || !matches!(value.codec, Codec::Raw | Codec::Zstd)
            || (value.codec == Codec::Raw && value.raw_len != value.stored_len)
        {
            return Err(FormatError::InvalidIndex);
        }
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, thiserror::Error, Eq, PartialEq)]
pub enum FormatError {
    #[error("invalid zsqlite magic")]
    BadMagic,
    #[error("unsupported zsqlite format version {0}")]
    UnsupportedVersion(u16),
    #[error("metadata checksum mismatch")]
    Checksum,
    #[error("invalid zsqlite anchor")]
    InvalidAnchor,
    #[error("invalid zsqlite header")]
    InvalidHeader,
    #[error("invalid zsqlite superblock")]
    InvalidSuperblock,
    #[error("invalid zsqlite extent")]
    InvalidExtent,
    #[error("invalid zsqlite generation commit")]
    InvalidCommit,
    #[error("invalid zsqlite index")]
    InvalidIndex,
    #[error("unknown compression codec {0}")]
    UnknownCodec(u8),
}

#[must_use]
pub fn digest(data: &[u8]) -> Digest {
    *blake3::hash(data).as_bytes()
}

#[must_use]
pub fn valid_page_size(size: u32) -> bool {
    (512..=65_536).contains(&size) && size.is_power_of_two()
}

fn put_checksum<const N: usize>(output: &mut [u8; N]) {
    let checksum = crc32(&output[..N - 4]);
    output[N - 4..].copy_from_slice(&checksum.to_le_bytes());
}

fn put_record_checksum<const N: usize>(output: &mut [u8; N]) {
    put_checksum(output);
}

fn check_sector(input: &[u8; SECTOR_SIZE], magic: &[u8]) -> Result<(), FormatError> {
    if &input[..8] != magic {
        return Err(FormatError::BadMagic);
    }
    if get_u16(input, 8) != FORMAT_VERSION {
        return Err(FormatError::UnsupportedVersion(get_u16(input, 8)));
    }
    if crc32(&input[..SECTOR_SIZE - 4]) != get_u32(input, SECTOR_SIZE - 4) {
        return Err(FormatError::Checksum);
    }
    Ok(())
}

fn check_record<const N: usize>(input: &[u8; N], magic: &[u8]) -> Result<(), FormatError> {
    if &input[..4] != magic {
        return Err(FormatError::BadMagic);
    }
    if crc32(&input[..N - 4]) != get_u32(input, N - 4) {
        return Err(FormatError::Checksum);
    }
    Ok(())
}

#[must_use]
pub fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn get_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(input[offset..offset + 2].try_into().expect("u16 field"))
}

fn get_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + 4].try_into().expect("u32 field"))
}

fn get_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().expect("u64 field"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: DatabaseId = *b"0123456789abcdef";

    #[test]
    fn v3_metadata_round_trips() {
        let anchor = Anchor { database_id: ID };
        assert_eq!(Anchor::decode(&anchor.encode()), Ok(anchor));
        let header = Header { database_id: ID };
        assert_eq!(Header::decode(&header.encode()), Ok(header));
        let superblock = Superblock {
            sequence: 2,
            generation: 3,
            durable: true,
            logical_size: 8192,
            page_size: 4096,
            page_count: 2,
            commit_offset: 16384,
            commit_end: 16384 + COMMIT_SIZE as u64,
            index_offset: 0,
            index_end: 0,
            database_id: ID,
        };
        assert_eq!(
            Superblock::decode(&superblock.encode()),
            Ok(Some(superblock))
        );
        let extent = ExtentHeader {
            generation: 3,
            first_page: 1,
            page_count: 2,
            codec: Codec::Zstd,
            raw_len: 8192,
            stored_len: 100,
            allocation_len: 4096,
            raw_digest: digest(b"extent"),
        };
        assert_eq!(ExtentHeader::decode(&extent.encode()), Ok(extent));
        let seekable_extent = ExtentHeader {
            codec: Codec::ZstdSeekable,
            stored_len: 777,
            allocation_len: u32::try_from(EXTENT_HEADER_SIZE).expect("header fits") + 777,
            ..extent
        };
        assert_eq!(
            ExtentHeader::decode(&seekable_extent.encode()),
            Ok(seekable_extent)
        );
        let commit = Commit {
            generation: 3,
            previous_commit: 14000,
            generation_start: 12288,
            logical_size: 8192,
            page_size: 4096,
            extent_count: 1,
            generation_digest: digest(b"generation"),
        };
        assert_eq!(Commit::decode(&commit.encode()), Ok(commit));
        let index = IndexHeader {
            generation: 3,
            entry_count: 2,
            codec: Codec::Raw,
            raw_len: 48,
            stored_len: 48,
            raw_digest: digest(b"index"),
        };
        assert_eq!(IndexHeader::decode(&index.encode()), Ok(index));
    }

    #[test]
    fn checksum_rejects_every_single_bit_change() {
        let values = [
            Anchor { database_id: ID }.encode(),
            Header { database_id: ID }.encode(),
            Superblock {
                sequence: 1,
                generation: 0,
                durable: true,
                logical_size: 0,
                page_size: 0,
                page_count: 0,
                commit_offset: 0,
                commit_end: HEADER_SIZE as u64,
                index_offset: 0,
                index_end: 0,
                database_id: ID,
            }
            .encode(),
        ];
        for (kind, value) in values.into_iter().enumerate() {
            for byte in 0..SECTOR_SIZE {
                for bit in 0..8 {
                    let mut damaged = value;
                    damaged[byte] ^= 1 << bit;
                    let valid = match kind {
                        0 => Anchor::decode(&damaged).is_ok(),
                        1 => Header::decode(&damaged).is_ok(),
                        _ => Superblock::decode(&damaged).is_ok(),
                    };
                    assert!(!valid, "kind {kind}, byte {byte}, bit {bit}");
                }
            }
        }
    }

    #[test]
    fn all_sqlite_page_sizes_are_valid() {
        for size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
            assert!(valid_page_size(size));
        }
        assert!(!valid_page_size(0));
        assert!(!valid_page_size(768));
    }

    #[test]
    fn record_checksums_reject_every_single_bit_change() {
        let extent = ExtentHeader {
            generation: 7,
            first_page: 2,
            page_count: 3,
            codec: Codec::Zstd,
            raw_len: 12_288,
            stored_len: 700,
            allocation_len: 4096,
            raw_digest: digest(b"extent-record"),
        }
        .encode();
        for byte in 0..EXTENT_HEADER_SIZE {
            for bit in 0..8 {
                let mut damaged = extent;
                damaged[byte] ^= 1 << bit;
                assert!(ExtentHeader::decode(&damaged).is_err());
            }
        }

        let commit = Commit {
            generation: 7,
            previous_commit: 12_288,
            generation_start: 16_384,
            logical_size: 12_288,
            page_size: 4096,
            extent_count: 1,
            generation_digest: digest(b"commit-record"),
        }
        .encode();
        for byte in 0..COMMIT_SIZE {
            for bit in 0..8 {
                let mut damaged = commit;
                damaged[byte] ^= 1 << bit;
                assert!(Commit::decode(&damaged).is_err());
            }
        }

        let index = IndexHeader {
            generation: 7,
            entry_count: 3,
            codec: Codec::Zstd,
            raw_len: 72,
            stored_len: 40,
            raw_digest: digest(b"index-record"),
        }
        .encode();
        for byte in 0..INDEX_HEADER_SIZE {
            for bit in 0..8 {
                let mut damaged = index;
                damaged[byte] ^= 1 << bit;
                assert!(IndexHeader::decode(&damaged).is_err());
            }
        }
    }

    #[test]
    fn arbitrary_metadata_never_panics() {
        let mut state = 0x2d35_8dcc_aa6c_78a5_u64;
        for _ in 0..10_000 {
            let mut sector = [0_u8; SECTOR_SIZE];
            for chunk in sector.chunks_mut(8) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
            }
            let _ = Anchor::decode(&sector);
            let _ = Header::decode(&sector);
            let _ = Superblock::decode(&sector);
            let mut record = [0_u8; EXTENT_HEADER_SIZE];
            record.copy_from_slice(&sector[..EXTENT_HEADER_SIZE]);
            let _ = ExtentHeader::decode(&record);
            let _ = Commit::decode(&record);
            let _ = IndexHeader::decode(&record);
        }
    }
}
