//! Pack identities commit to canonical frame identities and their positions.
use super::frame::FrameMetadata;
use crate::domain::{FrameId, PackId};
use crate::{StoreError, format};
use std::io::Write;

pub(super) const HEADER: [u8; super::segment::HEADER_SIZE] = {
    let mut header = [0; super::segment::HEADER_SIZE];
    let magic = *b"ZPACK001";
    let mut i = 0;
    while i < magic.len() {
        header[i] = magic[i];
        i += 1;
    }
    header
};
pub(super) struct Identity {
    hash: blake3::Hasher,
    offset: u64,
    frames: u64,
}
impl Identity {
    pub fn new() -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"zsqlite/pack/v1\0");
        hash.update(&HEADER);
        Self {
            hash,
            offset: HEADER.len() as u64,
            frames: 0,
        }
    }
    pub fn frame(&mut self, metadata: &FrameMetadata) -> Result<(), StoreError> {
        let length = 36_u64
            .checked_add(metadata.encoded_len() as u64)
            .and_then(|length| length.checked_add(format::FRAME_HEADER_SIZE as u64))
            .and_then(|length| length.checked_add(metadata.payload_bytes().get()))
            .ok_or(StoreError::Range)?;
        self.hash.update(metadata.id().as_bytes());
        self.hash.update(&self.offset.to_le_bytes());
        self.hash.update(&length.to_le_bytes());
        self.offset = self.offset.checked_add(length).ok_or(StoreError::Range)?;
        self.frames = self.frames.checked_add(1).ok_or(StoreError::Range)?;
        Ok(())
    }
    pub fn finish(mut self, length: u64) -> Result<PackId, StoreError> {
        if length != self.offset || self.frames == 0 {
            return Err(StoreError::Corrupt(length));
        }
        self.hash.update(&self.frames.to_le_bytes());
        self.hash.update(&length.to_le_bytes());
        Ok(PackId::from_bytes(*self.hash.finalize().as_bytes()))
    }
}

/// Copy and authenticate without retaining a pack or a complete frame payload.
/// Metadata-only identity derivation is used on core-owned staging files.
pub(super) fn copy(
    mut read: impl FnMut(u64, &mut [u8]) -> Result<(), StoreError>,
    length: u64,
    output: &mut dyn Write,
    payloads: bool,
) -> Result<PackId, StoreError> {
    if length < HEADER.len() as u64 {
        return Err(StoreError::Corrupt(0));
    }
    let mut header = [0; HEADER.len()];
    read(0, &mut header)?;
    if header != HEADER {
        return Err(StoreError::Corrupt(0));
    }
    output.write_all(&header)?;
    let mut identity = Identity::new();
    let mut offset = HEADER.len() as u64;
    let mut buffer = vec![0; 1024 * 1024];
    while offset < length {
        let mut prefix = [0; 36];
        if length - offset < prefix.len() as u64 {
            return Err(StoreError::Corrupt(offset));
        }
        read(offset, &mut prefix)?;
        let count = u32::from_le_bytes(prefix[..4].try_into().expect("fixed length")) as usize;
        if count > 1024 * 1024 {
            return Err(StoreError::Range);
        }
        let record_length = 36 + count as u64 + format::FRAME_HEADER_SIZE as u64;
        if record_length > length - offset {
            return Err(StoreError::Corrupt(offset));
        }
        let id = FrameId::from_bytes(prefix[4..].try_into().expect("fixed ID"));
        let mut encoded = vec![0; count];
        read(offset + 36, &mut encoded)?;
        let metadata = FrameMetadata::decode(&encoded, id)?;
        let mut record = [0; format::FRAME_HEADER_SIZE];
        read(offset + 36 + count as u64, &mut record)?;
        if record != metadata.record_header()?.encode() {
            return Err(StoreError::Corrupt(offset));
        }
        output.write_all(&prefix)?;
        output.write_all(&encoded)?;
        output.write_all(&record)?;
        offset += record_length;
        let payload_end = offset
            .checked_add(metadata.payload_bytes().get())
            .ok_or(StoreError::Range)?;
        if payload_end > length {
            return Err(StoreError::Corrupt(offset));
        }
        if payloads {
            let mut hash = blake3::Hasher::new();
            while offset < payload_end {
                let amount = usize::try_from((payload_end - offset).min(buffer.len() as u64))
                    .map_err(|_| StoreError::Range)?;
                read(offset, &mut buffer[..amount])?;
                hash.update(&buffer[..amount]);
                output.write_all(&buffer[..amount])?;
                offset += amount as u64;
            }
            if hash.finalize().as_bytes() != metadata.payload_hash() {
                return Err(StoreError::Corrupt(offset));
            }
        } else {
            offset = payload_end;
        }
        identity.frame(&metadata)?;
    }
    identity.finish(length)
}
