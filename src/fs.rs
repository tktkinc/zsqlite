use crate::store::StoreError;
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

pub(crate) fn absolute_path(path: &Path) -> Result<PathBuf, StoreError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

pub(crate) fn sync_dir(path: &Path) -> Result<(), StoreError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<(), StoreError> {
    sync_dir(path.parent().ok_or(StoreError::Range)?)
}

#[cfg(unix)]
pub(crate) fn read_exact_at(
    file: &File,
    mut offset: u64,
    mut output: &mut [u8],
) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !output.is_empty() {
        let amount = match file.read_at(output, offset) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            result => result?,
        };
        if amount == 0 {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }
        offset = offset
            .checked_add(amount as u64)
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        output = &mut output[amount..];
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn write_all_at(file: &File, mut offset: u64, mut input: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !input.is_empty() {
        let amount = match file.write_at(input, offset) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            result => result?,
        };
        if amount == 0 {
            return Err(std::io::Error::from(ErrorKind::WriteZero));
        }
        offset = offset
            .checked_add(amount as u64)
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        input = &input[amount..];
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn read_exact_at(_file: &File, _offset: u64, _output: &mut [u8]) -> std::io::Result<()> {
    Err(std::io::Error::from(ErrorKind::Unsupported))
}

#[cfg(not(unix))]
pub(crate) fn write_all_at(_file: &File, _offset: u64, _input: &[u8]) -> std::io::Result<()> {
    Err(std::io::Error::from(ErrorKind::Unsupported))
}

#[cfg(unix)]
pub(crate) fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
pub(crate) fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}
