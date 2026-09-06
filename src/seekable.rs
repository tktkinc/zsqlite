//! Standard Zstandard seekable streams with zsqlite integrity metadata.

use crate::format::{Digest, digest};

const SEEK_TABLE_SKIPPABLE_MAGIC: u32 = 0x184d_2a5e;
const DIGEST_TABLE_SKIPPABLE_MAGIC: u32 = 0x184d_2a5d;
const SEEKABLE_MAGIC: u32 = 0x8f92_eab1;
const DIGEST_TABLE_MAGIC: &[u8; 8] = b"ZSQLDG01";
const SEEK_ENTRY_SIZE: usize = 8;
pub(crate) const SEEK_FOOTER_SIZE: usize = 9;
const SKIPPABLE_HEADER_SIZE: usize = 8;
const DIGEST_TABLE_FIXED_SIZE: usize = 8 + 4 + blake3::OUT_LEN;

#[derive(Clone, Debug)]
pub(crate) struct Frame {
    pub compressed_offset: u32,
    pub compressed_size: u32,
    pub raw_offset: u32,
    pub raw_size: u32,
    pub raw_digest: Digest,
}

#[derive(Clone, Debug)]
pub(crate) struct Layout {
    pub frames: Vec<Frame>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SeekableError {
    #[error("invalid seekable Zstandard extent")]
    Invalid,
    #[error("numeric or allocation limit exceeded")]
    Range,
    #[error("zstd error: {0}")]
    Zstd(String),
}

pub(crate) struct Encoded {
    pub payload: Vec<u8>,
    pub metadata_digest: Digest,
    pub layout: Layout,
}

pub(crate) fn encode(raw: &[u8], chunk_size: usize, level: i32) -> Result<Encoded, SeekableError> {
    if raw.is_empty() || chunk_size == 0 {
        return Err(SeekableError::Invalid);
    }
    let frame_count = raw.len().div_ceil(chunk_size);
    let mut payload = Vec::new();
    let mut frames = Vec::new();
    frames
        .try_reserve_exact(frame_count)
        .map_err(|_| SeekableError::Range)?;
    for (index, chunk) in raw.chunks(chunk_size).enumerate() {
        let compressed = zstd::bulk::compress(chunk, level)
            .map_err(|error| SeekableError::Zstd(error.to_string()))?;
        let compressed_offset = u32::try_from(payload.len()).map_err(|_| SeekableError::Range)?;
        let compressed_size = u32::try_from(compressed.len()).map_err(|_| SeekableError::Range)?;
        let raw_offset = u32::try_from(index.checked_mul(chunk_size).ok_or(SeekableError::Range)?)
            .map_err(|_| SeekableError::Range)?;
        let raw_size = u32::try_from(chunk.len()).map_err(|_| SeekableError::Range)?;
        frames.push(Frame {
            compressed_offset,
            compressed_size,
            raw_offset,
            raw_size,
            raw_digest: digest(chunk),
        });
        payload.extend_from_slice(&compressed);
    }

    let metadata_size = metadata_frame_size(frame_count)?;
    let seek_table = encode_seek_table(&frames, metadata_size)?;
    let metadata = encode_digest_table(&frames, digest(&seek_table))?;
    debug_assert_eq!(metadata.len(), metadata_size);
    let metadata_digest = digest(&metadata);
    payload.extend_from_slice(&metadata);
    payload.extend_from_slice(&seek_table);
    Ok(Encoded {
        payload,
        metadata_digest,
        layout: Layout { frames },
    })
}

pub(crate) fn tail_size_from_footer(
    footer: &[u8; SEEK_FOOTER_SIZE],
) -> Result<usize, SeekableError> {
    let stored_frames = decode_footer(footer)?;
    let data_frames = stored_frames.checked_sub(1).ok_or(SeekableError::Invalid)?;
    metadata_frame_size(data_frames)?
        .checked_add(seek_table_size(stored_frames)?)
        .ok_or(SeekableError::Range)
}

pub(crate) fn decode_layout(
    tail: &[u8],
    stored_len: u32,
    raw_len: u32,
    page_size: u32,
    metadata_digest: Digest,
) -> Result<Layout, SeekableError> {
    let footer: &[u8; SEEK_FOOTER_SIZE] = tail
        .get(tail.len().saturating_sub(SEEK_FOOTER_SIZE)..)
        .and_then(|value| value.try_into().ok())
        .ok_or(SeekableError::Invalid)?;
    let stored_frames = decode_footer(footer)?;
    let data_frames = stored_frames.checked_sub(1).ok_or(SeekableError::Invalid)?;
    let metadata_size = metadata_frame_size(data_frames)?;
    let seek_size = seek_table_size(stored_frames)?;
    if tail.len()
        != metadata_size
            .checked_add(seek_size)
            .ok_or(SeekableError::Range)?
    {
        return Err(SeekableError::Invalid);
    }
    let (metadata, seek_table) = tail.split_at(metadata_size);
    if digest(metadata) != metadata_digest {
        return Err(SeekableError::Invalid);
    }
    let frame_digests = decode_digest_table(metadata, data_frames, digest(seek_table))?;
    let entries = decode_seek_table(seek_table, stored_frames)?;
    let metadata_entry = entries.last().ok_or(SeekableError::Invalid)?;
    if metadata_entry.0 as usize != metadata_size || metadata_entry.1 != 0 {
        return Err(SeekableError::Invalid);
    }

    let data_bytes = usize::try_from(stored_len)
        .map_err(|_| SeekableError::Range)?
        .checked_sub(tail.len())
        .ok_or(SeekableError::Invalid)?;
    let mut compressed_offset = 0_u32;
    let mut raw_offset = 0_u32;
    let mut frames = Vec::new();
    frames
        .try_reserve_exact(data_frames)
        .map_err(|_| SeekableError::Range)?;
    for ((compressed_size, raw_size), raw_digest) in
        entries.into_iter().take(data_frames).zip(frame_digests)
    {
        if compressed_size == 0
            || raw_size == 0
            || !raw_size.is_multiple_of(page_size)
            || raw_size > raw_len
        {
            return Err(SeekableError::Invalid);
        }
        frames.push(Frame {
            compressed_offset,
            compressed_size,
            raw_offset,
            raw_size,
            raw_digest,
        });
        compressed_offset = compressed_offset
            .checked_add(compressed_size)
            .ok_or(SeekableError::Range)?;
        raw_offset = raw_offset
            .checked_add(raw_size)
            .ok_or(SeekableError::Range)?;
    }
    if compressed_offset as usize != data_bytes || raw_offset != raw_len {
        return Err(SeekableError::Invalid);
    }
    Ok(Layout { frames })
}

fn encode_seek_table(frames: &[Frame], metadata_size: usize) -> Result<Vec<u8>, SeekableError> {
    let stored_frames = frames.len().checked_add(1).ok_or(SeekableError::Range)?;
    let frame_payload_size = stored_frames
        .checked_mul(SEEK_ENTRY_SIZE)
        .and_then(|size| size.checked_add(SEEK_FOOTER_SIZE))
        .ok_or(SeekableError::Range)?;
    let mut output = Vec::with_capacity(
        SKIPPABLE_HEADER_SIZE
            .checked_add(frame_payload_size)
            .ok_or(SeekableError::Range)?,
    );
    output.extend_from_slice(&SEEK_TABLE_SKIPPABLE_MAGIC.to_le_bytes());
    output.extend_from_slice(
        &u32::try_from(frame_payload_size)
            .map_err(|_| SeekableError::Range)?
            .to_le_bytes(),
    );
    for frame in frames {
        output.extend_from_slice(&frame.compressed_size.to_le_bytes());
        output.extend_from_slice(&frame.raw_size.to_le_bytes());
    }
    output.extend_from_slice(
        &u32::try_from(metadata_size)
            .map_err(|_| SeekableError::Range)?
            .to_le_bytes(),
    );
    output.extend_from_slice(&0_u32.to_le_bytes());
    output.extend_from_slice(
        &u32::try_from(stored_frames)
            .map_err(|_| SeekableError::Range)?
            .to_le_bytes(),
    );
    output.push(0);
    output.extend_from_slice(&SEEKABLE_MAGIC.to_le_bytes());
    Ok(output)
}

fn encode_digest_table(
    frames: &[Frame],
    seek_table_digest: Digest,
) -> Result<Vec<u8>, SeekableError> {
    let user_size = DIGEST_TABLE_FIXED_SIZE
        .checked_add(
            frames
                .len()
                .checked_mul(blake3::OUT_LEN)
                .ok_or(SeekableError::Range)?,
        )
        .ok_or(SeekableError::Range)?;
    let mut output = Vec::with_capacity(
        SKIPPABLE_HEADER_SIZE
            .checked_add(user_size)
            .ok_or(SeekableError::Range)?,
    );
    output.extend_from_slice(&DIGEST_TABLE_SKIPPABLE_MAGIC.to_le_bytes());
    output.extend_from_slice(
        &u32::try_from(user_size)
            .map_err(|_| SeekableError::Range)?
            .to_le_bytes(),
    );
    output.extend_from_slice(DIGEST_TABLE_MAGIC);
    output.extend_from_slice(
        &u32::try_from(frames.len())
            .map_err(|_| SeekableError::Range)?
            .to_le_bytes(),
    );
    output.extend_from_slice(&seek_table_digest);
    for frame in frames {
        output.extend_from_slice(&frame.raw_digest);
    }
    Ok(output)
}

fn decode_digest_table(
    input: &[u8],
    data_frames: usize,
    expected_seek_digest: Digest,
) -> Result<Vec<Digest>, SeekableError> {
    if get_u32(input, 0)? != DIGEST_TABLE_SKIPPABLE_MAGIC
        || usize::try_from(get_u32(input, 4)?).map_err(|_| SeekableError::Range)?
            != input
                .len()
                .checked_sub(SKIPPABLE_HEADER_SIZE)
                .ok_or(SeekableError::Invalid)?
        || input.get(8..16) != Some(DIGEST_TABLE_MAGIC.as_slice())
        || usize::try_from(get_u32(input, 16)?).map_err(|_| SeekableError::Range)? != data_frames
        || input.get(20..52) != Some(expected_seek_digest.as_slice())
    {
        return Err(SeekableError::Invalid);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(data_frames)
        .map_err(|_| SeekableError::Range)?;
    let digests = input.get(52..).ok_or(SeekableError::Invalid)?;
    let (chunks, remainder) = digests.as_chunks::<{ blake3::OUT_LEN }>();
    if chunks.len() != data_frames || !remainder.is_empty() {
        return Err(SeekableError::Invalid);
    }
    output.extend_from_slice(chunks);
    Ok(output)
}

fn decode_seek_table(input: &[u8], stored_frames: usize) -> Result<Vec<(u32, u32)>, SeekableError> {
    let expected_size = seek_table_size(stored_frames)?;
    if input.len() != expected_size
        || get_u32(input, 0)? != SEEK_TABLE_SKIPPABLE_MAGIC
        || usize::try_from(get_u32(input, 4)?).map_err(|_| SeekableError::Range)?
            != input
                .len()
                .checked_sub(SKIPPABLE_HEADER_SIZE)
                .ok_or(SeekableError::Invalid)?
    {
        return Err(SeekableError::Invalid);
    }
    let entries_end = SKIPPABLE_HEADER_SIZE
        .checked_add(
            stored_frames
                .checked_mul(SEEK_ENTRY_SIZE)
                .ok_or(SeekableError::Range)?,
        )
        .ok_or(SeekableError::Range)?;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(stored_frames)
        .map_err(|_| SeekableError::Range)?;
    for entry in input[SKIPPABLE_HEADER_SIZE..entries_end].chunks_exact(SEEK_ENTRY_SIZE) {
        entries.push((get_u32(entry, 0)?, get_u32(entry, 4)?));
    }
    Ok(entries)
}

fn decode_footer(footer: &[u8; SEEK_FOOTER_SIZE]) -> Result<usize, SeekableError> {
    if footer[4] != 0 || get_u32(footer, 5)? != SEEKABLE_MAGIC {
        return Err(SeekableError::Invalid);
    }
    usize::try_from(get_u32(footer, 0)?).map_err(|_| SeekableError::Range)
}

fn metadata_frame_size(data_frames: usize) -> Result<usize, SeekableError> {
    SKIPPABLE_HEADER_SIZE
        .checked_add(DIGEST_TABLE_FIXED_SIZE)
        .and_then(|size| size.checked_add(data_frames.checked_mul(blake3::OUT_LEN)?))
        .ok_or(SeekableError::Range)
}

fn seek_table_size(stored_frames: usize) -> Result<usize, SeekableError> {
    let payload_size = stored_frames
        .checked_mul(SEEK_ENTRY_SIZE)
        .and_then(|size| size.checked_add(SEEK_FOOTER_SIZE))
        .ok_or(SeekableError::Range)?;
    SKIPPABLE_HEADER_SIZE
        .checked_add(payload_size)
        .ok_or(SeekableError::Range)
}

fn get_u32(input: &[u8], offset: usize) -> Result<u32, SeekableError> {
    input
        .get(offset..offset.checked_add(4).ok_or(SeekableError::Range)?)
        .and_then(|value| value.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(SeekableError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn seekable_stream_is_standard_and_layout_round_trips() -> Result<(), Box<dyn std::error::Error>>
    {
        let raw = (0_u32..(300 * 4096))
            .map(|value| value.wrapping_mul(31).to_le_bytes()[0])
            .collect::<Vec<_>>();
        let encoded = encode(&raw, 64 * 1024, 3)?;
        let footer: &[u8; SEEK_FOOTER_SIZE] = encoded
            .payload
            .get(encoded.payload.len() - SEEK_FOOTER_SIZE..)
            .and_then(|value| value.try_into().ok())
            .ok_or(SeekableError::Invalid)?;
        let tail_size = tail_size_from_footer(footer)?;
        let tail = &encoded.payload[encoded.payload.len() - tail_size..];
        let decoded = decode_layout(
            tail,
            u32::try_from(encoded.payload.len())?,
            u32::try_from(raw.len())?,
            4096,
            encoded.metadata_digest,
        )?;
        assert_eq!(decoded.frames.len(), encoded.layout.frames.len());

        let mut stream = zstd::stream::Decoder::new(encoded.payload.as_slice())?;
        let mut standard = Vec::new();
        stream.read_to_end(&mut standard)?;
        assert_eq!(standard, raw);
        Ok(())
    }

    #[test]
    fn every_seek_metadata_bit_is_authenticated() -> Result<(), Box<dyn std::error::Error>> {
        let raw = vec![0x5a; 256 * 4096];
        let encoded = encode(&raw, 64 * 1024, 3)?;
        let footer: &[u8; SEEK_FOOTER_SIZE] = encoded
            .payload
            .get(encoded.payload.len() - SEEK_FOOTER_SIZE..)
            .and_then(|value| value.try_into().ok())
            .ok_or(SeekableError::Invalid)?;
        let tail_size = tail_size_from_footer(footer)?;
        let original = &encoded.payload[encoded.payload.len() - tail_size..];
        for byte in 0..original.len() {
            for bit in 0..8 {
                let mut damaged = original.to_vec();
                damaged[byte] ^= 1 << bit;
                assert!(
                    decode_layout(
                        &damaged,
                        u32::try_from(encoded.payload.len())?,
                        u32::try_from(raw.len())?,
                        4096,
                        encoded.metadata_digest,
                    )
                    .is_err(),
                    "byte {byte}, bit {bit}"
                );
            }
        }
        Ok(())
    }
}
