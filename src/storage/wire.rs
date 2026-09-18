//! Bounded wire decoding. No deserializer can construct a trusted view.
use crate::StoreError;

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
