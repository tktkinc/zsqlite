//! Encoding for compressed segment index and page-map sections.

use crate::format::{
    Codec, MAX_SECTION_BYTES, SEGMENT_INDEX_ENTRY_SIZE, SegmentIndexEntry, digest,
};
use crate::store::StoreError;

const MIN_FRAME_SAVINGS: usize = 64;
const ZSTD_LEVEL: i32 = 3;
const BLOB_HEADER_SIZE: usize = 80;
const BLOB_VERSION: u16 = 1;
const INDEX_BLOB_MAGIC: &[u8; 8] = b"ZIDX0001";
pub(crate) const MAP_BLOB_MAGIC: &[u8; 8] = b"ZMAP0001";

pub(crate) fn encode_index(entries: &[SegmentIndexEntry]) -> Result<Vec<u8>, StoreError> {
    let raw_len = entries
        .len()
        .checked_mul(SEGMENT_INDEX_ENTRY_SIZE)
        .ok_or(StoreError::Range)?;
    let mut raw = Vec::new();
    raw.try_reserve_exact(raw_len)
        .map_err(|_| StoreError::Range)?;
    for entry in entries {
        raw.extend_from_slice(&entry.encode());
    }
    encode_blob(*INDEX_BLOB_MAGIC, &raw)
}

pub(crate) fn decode_index(
    encoded: &[u8],
    max_page: u32,
) -> Result<Vec<SegmentIndexEntry>, StoreError> {
    let max_raw_len = u64::from(max_page)
        .checked_mul(SEGMENT_INDEX_ENTRY_SIZE as u64)
        .ok_or(StoreError::Range)?;
    let raw = decode_blob(*INDEX_BLOB_MAGIC, encoded, max_raw_len)?;
    if !raw.len().is_multiple_of(SEGMENT_INDEX_ENTRY_SIZE) {
        return Err(StoreError::Corrupt(0));
    }
    let count = raw.len() / SEGMENT_INDEX_ENTRY_SIZE;
    if count > max_page as usize {
        return Err(StoreError::Corrupt(0));
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(count)
        .map_err(|_| StoreError::Range)?;
    for chunk in raw.chunks_exact(SEGMENT_INDEX_ENTRY_SIZE) {
        let encoded: [u8; SEGMENT_INDEX_ENTRY_SIZE] = chunk.try_into().expect("exact index chunk");
        output.push(SegmentIndexEntry::decode(&encoded)?);
    }
    Ok(output)
}

pub(crate) fn encode_map(values: &[u64]) -> Result<Vec<u8>, StoreError> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&(values.len() as u64).to_le_bytes());
    let mut cursor = 0_usize;
    while cursor < values.len() {
        let value = values[cursor];
        let mut run = 1_usize;
        while cursor + run < values.len() && values[cursor + run] == value {
            run += 1;
        }
        put_varint(&mut raw, run as u64);
        put_varint(&mut raw, value);
        cursor += run;
    }
    encode_blob(*MAP_BLOB_MAGIC, &raw)
}

pub(crate) fn decode_map(encoded: &[u8], expected_count: u32) -> Result<Vec<u64>, StoreError> {
    let raw = decode_blob(*MAP_BLOB_MAGIC, encoded, max_map_raw_len(expected_count)?)?;
    if raw.len() < 8 {
        return Err(StoreError::Corrupt(0));
    }
    let count = u64::from_le_bytes(raw[..8].try_into().expect("map count"));
    if count != u64::from(expected_count) {
        return Err(StoreError::Corrupt(0));
    }
    let count = expected_count as usize;
    let mut output = Vec::new();
    output
        .try_reserve_exact(count)
        .map_err(|_| StoreError::Range)?;
    let mut cursor = 8_usize;
    while output.len() < count {
        let run = usize::try_from(get_varint(&raw, &mut cursor)?).map_err(|_| StoreError::Range)?;
        let value = get_varint(&raw, &mut cursor)?;
        let end = output.len().checked_add(run).ok_or(StoreError::Range)?;
        if run == 0 || end > count {
            return Err(StoreError::Corrupt(0));
        }
        output.resize(end, value);
    }
    if cursor != raw.len() {
        return Err(StoreError::Corrupt(0));
    }
    Ok(output)
}

pub(crate) fn encode_blob(magic: [u8; 8], raw: &[u8]) -> Result<Vec<u8>, StoreError> {
    let max_payload_len = MAX_SECTION_BYTES
        .checked_sub(BLOB_HEADER_SIZE as u64)
        .ok_or(StoreError::Range)?;
    if raw.len() as u64 > max_payload_len {
        return Err(StoreError::Range);
    }
    let compressed = zstd::bulk::compress(raw, ZSTD_LEVEL)
        .map_err(|error| StoreError::Zstd(error.to_string()))?;
    let (codec, payload): (Codec, &[u8]) =
        if compressed.len().saturating_add(MIN_FRAME_SAVINGS) < raw.len() {
            (Codec::Zstd, &compressed)
        } else {
            (Codec::Raw, raw)
        };
    let encoded_len = BLOB_HEADER_SIZE
        .checked_add(payload.len())
        .ok_or(StoreError::Range)?;
    if encoded_len as u64 > MAX_SECTION_BYTES {
        return Err(StoreError::Range);
    }
    let mut output = vec![0; BLOB_HEADER_SIZE];
    output
        .try_reserve_exact(payload.len())
        .map_err(|_| StoreError::Range)?;
    output[..8].copy_from_slice(&magic);
    output[8..10].copy_from_slice(&BLOB_VERSION.to_le_bytes());
    output[10] = codec as u8;
    output[16..24].copy_from_slice(&(raw.len() as u64).to_le_bytes());
    output[24..32].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    output[32..64].copy_from_slice(&digest(raw));
    let checksum = crc32fast::hash(&output[..BLOB_HEADER_SIZE - 4]);
    output[BLOB_HEADER_SIZE - 4..].copy_from_slice(&checksum.to_le_bytes());
    output.extend_from_slice(payload);
    Ok(output)
}

fn decode_blob(magic: [u8; 8], encoded: &[u8], max_raw_len: u64) -> Result<Vec<u8>, StoreError> {
    if encoded.len() < BLOB_HEADER_SIZE
        || encoded[..8] != magic
        || u16::from_le_bytes(encoded[8..10].try_into().expect("blob version")) != BLOB_VERSION
        || u32::from_le_bytes(
            encoded[BLOB_HEADER_SIZE - 4..BLOB_HEADER_SIZE]
                .try_into()
                .expect("blob checksum"),
        ) != crc32fast::hash(&encoded[..BLOB_HEADER_SIZE - 4])
    {
        return Err(StoreError::Corrupt(0));
    }
    let raw_len = usize::try_from(u64::from_le_bytes(
        encoded[16..24].try_into().expect("raw len"),
    ))
    .map_err(|_| StoreError::Range)?;
    let stored_len = usize::try_from(u64::from_le_bytes(
        encoded[24..32].try_into().expect("stored len"),
    ))
    .map_err(|_| StoreError::Range)?;
    if raw_len as u64 > MAX_SECTION_BYTES
        || raw_len as u64 > max_raw_len
        || stored_len as u64 > MAX_SECTION_BYTES
    {
        return Err(StoreError::Range);
    }
    if encoded.len()
        != BLOB_HEADER_SIZE
            .checked_add(stored_len)
            .ok_or(StoreError::Range)?
    {
        return Err(StoreError::Corrupt(0));
    }
    let payload = &encoded[BLOB_HEADER_SIZE..];
    let raw = match Codec::try_from(encoded[10])? {
        Codec::Raw if stored_len == raw_len => {
            let mut raw = Vec::new();
            raw.try_reserve_exact(raw_len)
                .map_err(|_| StoreError::Range)?;
            raw.extend_from_slice(payload);
            raw
        }
        Codec::Raw => return Err(StoreError::Corrupt(0)),
        Codec::Zstd => zstd::bulk::decompress(payload, raw_len)
            .map_err(|error| StoreError::Zstd(error.to_string()))?,
    };
    if raw.len() != raw_len || digest(&raw) != encoded[32..64] {
        return Err(StoreError::Corrupt(0));
    }
    Ok(raw)
}

pub(crate) fn max_map_raw_len(page_count: u32) -> Result<u64, StoreError> {
    // A canonical map starts with an eight-byte count. In the least
    // compressible case every page is its own run: five bytes for a u32 run
    // length and ten for a u64 TXID.
    u64::from(page_count)
        .checked_mul(15)
        .and_then(|length| length.checked_add(8))
        .ok_or(StoreError::Range)
}

pub(crate) fn validate_blob_section_len(
    encoded_len: u64,
    max_raw_len: u64,
) -> Result<(), StoreError> {
    let max_encoded_len = max_raw_len
        .checked_add(BLOB_HEADER_SIZE as u64)
        .ok_or(StoreError::Range)?
        .min(MAX_SECTION_BYTES);
    if encoded_len < BLOB_HEADER_SIZE as u64 || encoded_len > max_encoded_len {
        return Err(StoreError::Corrupt(0));
    }
    Ok(())
}

fn put_varint(output: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        output.push(u8::try_from(value & 0x7f).expect("varint chunk fits u8") | 0x80);
        value >>= 7;
    }
    output.push(u8::try_from(value).expect("terminal varint byte fits u8"));
}

fn get_varint(input: &[u8], cursor: &mut usize) -> Result<u64, StoreError> {
    let mut output = 0_u64;
    for (index, shift) in (0..=63).step_by(7).enumerate() {
        let byte = *input.get(*cursor).ok_or(StoreError::Corrupt(0))?;
        *cursor += 1;
        if index == 9 && byte > 1 {
            return Err(StoreError::Corrupt(0));
        }
        output |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            if index != 0 && byte == 0 {
                return Err(StoreError::Corrupt(0));
            }
            return Ok(output);
        }
    }
    Err(StoreError::Corrupt(0))
}
