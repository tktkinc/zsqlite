//! Bounded wire decoding. No deserializer can construct a trusted view.
use crate::StoreError;
use std::io::Read;

/// Decode an authenticated envelope incrementally. Large view records must not
/// retain their entire uncompressed representation alongside the resolved maps.
pub(crate) struct ReadDecoder<R> {
    reader: R,
    length: usize,
    position: usize,
    hash: blake3::Hasher,
    expected: Option<[u8; 32]>,
}
impl<R: Read> ReadDecoder<R> {
    pub(crate) fn new(reader: R, length: usize) -> Self {
        Self {
            reader,
            length,
            position: 0,
            hash: blake3::Hasher::new(),
            expected: None,
        }
    }
    pub(crate) fn length(&self) -> usize {
        self.length
    }
    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), StoreError> {
        let end = self
            .position
            .checked_add(output.len())
            .ok_or(StoreError::Range)?;
        if end > self.length {
            return Err(StoreError::Corrupt(self.position as u64));
        }
        self.reader
            .read_exact(output)
            .map_err(|_| StoreError::Corrupt(self.position as u64))?;
        if self.expected.is_some() {
            self.hash.update(output);
        }
        self.position = end;
        Ok(())
    }
    pub(crate) fn take(&mut self, count: usize) -> Result<Vec<u8>, StoreError> {
        if count > self.length - self.position {
            return Err(StoreError::Corrupt(self.position as u64));
        }
        let mut bytes = vec![0; count];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }
    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], StoreError> {
        let mut bytes = [0; N];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }
    pub(crate) fn u32(&mut self) -> Result<u32, StoreError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, StoreError> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    pub(crate) fn finish(mut self) -> Result<(), StoreError> {
        if self.position != self.length
            || self
                .reader
                .read(&mut [0])
                .map_err(|_| StoreError::Corrupt(self.position as u64))?
                != 0
            || self
                .expected
                .is_some_and(|expected| *self.hash.finalize().as_bytes() != expected)
        {
            return Err(StoreError::Corrupt(self.position as u64));
        }
        Ok(())
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn open_envelope_reader<'a>(
    magic: &[u8; 8],
    encoded: &'a [u8],
    limit: usize,
) -> Result<ReadDecoder<impl Read + 'a>, StoreError> {
    let mut wire = Decoder::new(encoded);
    if wire.take(8)? != magic {
        return Err(StoreError::Corrupt(0));
    }
    let length = usize::try_from(wire.u64()?).map_err(|_| StoreError::Range)?;
    if length > limit {
        return Err(StoreError::Range);
    }
    let expected = wire.array()?;
    let compressed = wire.take(encoded.len().checked_sub(48).ok_or(StoreError::Range)?)?;
    let reader = zstd::stream::read::Decoder::with_buffer(compressed)
        .map_err(|error| StoreError::Zstd(error.to_string()))?;
    let mut decoder =
        ReadDecoder::new(std::io::BufReader::with_capacity(64 * 1024, reader), length);
    decoder.expected = Some(expected);
    Ok(decoder)
}

pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8], StoreError> {
        let end = self.position.checked_add(count).ok_or(StoreError::Range)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(StoreError::Corrupt(self.position as u64))?;
        self.position = end;
        Ok(bytes)
    }
    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], StoreError> {
        self.take(N)?.try_into().map_err(|_| StoreError::Range)
    }
    pub(crate) fn u8(&mut self) -> Result<u8, StoreError> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> Result<u32, StoreError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, StoreError> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    pub(crate) fn finish(self) -> Result<(), StoreError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(StoreError::Corrupt(self.position as u64))
        }
    }
}
pub(crate) fn u32_bytes(output: &mut Vec<u8>, value: u32) {
    output.extend(value.to_le_bytes());
}
pub(crate) fn u64_bytes(output: &mut Vec<u8>, value: u64) {
    output.extend(value.to_le_bytes());
}

#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn envelope(magic: &[u8; 8], raw: &[u8]) -> Result<Vec<u8>, StoreError> {
    let compressed =
        zstd::bulk::compress(raw, 3).map_err(|error| StoreError::Zstd(error.to_string()))?;
    let mut output = Vec::with_capacity(48 + compressed.len());
    output.extend(magic);
    u64_bytes(&mut output, raw.len() as u64);
    output.extend(blake3::hash(raw).as_bytes());
    output.extend(compressed);
    Ok(output)
}
#[allow(clippy::trivially_copy_pass_by_ref)]
pub(crate) fn open_envelope(
    magic: &[u8; 8],
    encoded: &[u8],
    limit: usize,
) -> Result<Vec<u8>, StoreError> {
    let mut wire = Decoder::new(encoded);
    if wire.take(8)? != magic {
        return Err(StoreError::Corrupt(0));
    }
    let length = usize::try_from(wire.u64()?).map_err(|_| StoreError::Range)?;
    if length > limit {
        return Err(StoreError::Range);
    }
    let hash: [u8; 32] = wire.array()?;
    let compressed = wire.take(encoded.len().checked_sub(48).ok_or(StoreError::Range)?)?;
    let raw = zstd::bulk::decompress(compressed, length)
        .map_err(|error| StoreError::Zstd(error.to_string()))?;
    if raw.len() != length || *blake3::hash(&raw).as_bytes() != hash {
        return Err(StoreError::Corrupt(0));
    }
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_envelopes_require_exact_length_hash_and_complete_zstd_stream() {
        let raw = vec![17; 128 * 1024];
        let encoded = envelope(b"ZTEST001", &raw).unwrap();
        let decode = |encoded: &[u8]| {
            let mut wire = open_envelope_reader(b"ZTEST001", encoded, raw.len())?;
            let bytes = wire.take(raw.len())?;
            wire.finish()?;
            Ok::<_, StoreError>(bytes)
        };
        assert_eq!(decode(&encoded).unwrap(), raw);
        assert!(open_envelope_reader(b"ZTEST001", &encoded, raw.len() - 1).is_err());
        let mut wrong_hash = encoded.clone();
        wrong_hash[16] ^= 1;
        assert!(decode(&wrong_hash).is_err());
        let mut short_length = encoded.clone();
        short_length[8..16].copy_from_slice(&((raw.len() - 1) as u64).to_le_bytes());
        assert!(decode(&short_length).is_err());
        assert!(decode(&encoded[..encoded.len() - 1]).is_err());
        let mut extra_frame = encoded.clone();
        extra_frame.extend(zstd::bulk::compress(&[0], 3).unwrap());
        assert!(decode(&extra_frame).is_err());
        let mut partial = open_envelope_reader(b"ZTEST001", &encoded, raw.len()).unwrap();
        partial.take(4).unwrap();
        assert!(partial.finish().is_err());
        let mut bounded = open_envelope_reader(b"ZTEST001", &encoded, raw.len()).unwrap();
        assert!(bounded.take(usize::MAX).is_err());
    }
}
