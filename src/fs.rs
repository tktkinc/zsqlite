use crate::store::StoreError;
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

/// Disposable, private, unlinked cache storage. This capability cannot be
/// constructed from an active database or immutable object file.
pub(crate) struct CacheFile {
    file: File,
    block: std::num::NonZeroU64,
    available: Option<u64>,
}
impl CacheFile {
    pub(crate) fn new() -> std::io::Result<Self> {
        let file = tempfile::tempfile()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // Unnamed temporary files can inherit a broader mode on Linux.
            // Restrict access before storing any decoded database pages.
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        }
        // Block geometry improves slot reclamation, and free space bounds
        // automatic sizing. If either is unavailable, cache I/O still fails
        // safely into normal verified reads.
        let (block, available) = cache_filesystem(&file).map_or_else(
            |_| {
                (
                    std::num::NonZeroU64::new(4096).expect("nonzero fallback"),
                    None,
                )
            },
            |(block, available)| (block, Some(available)),
        );
        Ok(Self {
            file,
            block,
            available,
        })
    }
    pub(crate) fn block_bytes(&self) -> u64 {
        self.block.get()
    }
    pub(crate) fn available_bytes(&self) -> Option<u64> {
        self.available
    }
    pub(crate) fn read(
        &self,
        offset: crate::domain::FileOffset,
        bytes: &mut [u8],
    ) -> std::io::Result<()> {
        read_exact_at(&self.file, offset.get(), bytes)
    }
    pub(crate) fn write(
        &self,
        offset: crate::domain::FileOffset,
        bytes: &[u8],
    ) -> std::io::Result<()> {
        write_all_at(&self.file, offset.get(), bytes)
    }
    pub(crate) fn set_len(&self, bytes: u64) -> std::io::Result<()> {
        self.file.set_len(bytes)
    }
    pub(crate) fn punch(&self, range: crate::domain::StoredRange) -> std::io::Result<()> {
        if range.length().get() == 0
            || !range.offset().get().is_multiple_of(self.block.get())
            || !range.length().get().is_multiple_of(self.block.get())
            || range.end().get() > self.file.metadata()?.len()
        {
            return Err(ErrorKind::InvalidInput.into());
        }
        punch_cache_hole(&self.file, range)
    }
    #[cfg(test)]
    pub(crate) fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        self.file.metadata()
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
// libc filesystem counter widths differ across targets.
#[allow(clippy::unnecessary_cast, clippy::useless_conversion)]
fn cache_filesystem(file: &File) -> std::io::Result<(std::num::NonZeroU64, u64)> {
    use std::os::fd::AsRawFd;
    let mut status = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: owned fd; fstatvfs initializes status on success and retains no pointer.
    if unsafe { libc::fstatvfs(file.as_raw_fd(), status.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fstatvfs succeeded above.
    let status = unsafe { status.assume_init() };
    let bytes = if status.f_frsize == 0 {
        status.f_bsize
    } else {
        status.f_frsize
    } as u64;
    let block = std::num::NonZeroU64::new(bytes).ok_or(ErrorKind::Unsupported)?;
    Ok((
        block,
        u64::from(status.f_bavail).saturating_mul(block.get()),
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
fn cache_filesystem(_file: &File) -> std::io::Result<(std::num::NonZeroU64, u64)> {
    Err(ErrorKind::Unsupported.into())
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
fn punch_cache_hole(file: &File, range: crate::domain::StoredRange) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let offset =
        libc::off_t::try_from(range.offset().get()).map_err(|_| ErrorKind::InvalidInput)?;
    let length =
        libc::off_t::try_from(range.length().get()).map_err(|_| ErrorKind::InvalidInput)?;
    loop {
        #[cfg(target_os = "macos")]
        let result = {
            let request = libc::fpunchhole_t {
                fp_flags: 0,
                reserved: 0,
                fp_offset: offset,
                fp_length: length,
            };
            // SAFETY: owned fd and initialized, aligned request; fcntl borrows synchronously.
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &raw const request) }
        };
        #[cfg(any(target_os = "linux", target_os = "android"))]
        // SAFETY: File keeps fd alive; checked range uses off_t bounds.
        let result = unsafe {
            libc::fallocate(
                file.as_raw_fd(),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                offset,
                length,
            )
        };
        if result == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
fn punch_cache_hole(_file: &File, _range: crate::domain::StoredRange) -> std::io::Result<()> {
    Err(ErrorKind::Unsupported.into())
}

/// Owned OS lock authority. Acquisition opens an independent descriptor unless
/// reserving Store's existing publication carrier. Explicit unlock on Drop is
/// essential in that case because flock ownership follows the open description.
#[derive(Debug)]
pub(crate) struct ExclusiveLock {
    file: File,
    unlock_on_drop: bool,
}

#[derive(Debug)]
pub(crate) struct SharedLock {
    _file: File,
}

impl ExclusiveLock {
    pub(crate) fn on_file(file: &File, nonblocking: bool) -> Result<Self, StoreError> {
        let file = file.try_clone()?;
        lock_exclusive(&file, nonblocking)?;
        Ok(Self {
            file,
            unlock_on_drop: true,
        })
    }
    pub(crate) fn acquire(path: &Path, nonblocking: bool) -> Result<Self, StoreError> {
        let file = lock_file(path)?;
        lock_exclusive(&file, nonblocking)?;
        Ok(Self {
            file,
            unlock_on_drop: true,
        })
    }
    /// Transfer the same open-file description to a lifetime read lease while
    /// publication exclusion is still held. No unlocked handoff is exposed.
    pub(crate) fn into_shared_file(mut self) -> Result<File, StoreError> {
        let file = self.file.try_clone()?;
        lock_shared(&file, false)?;
        self.unlock_on_drop = false;
        Ok(file)
    }
}

impl Drop for ExclusiveLock {
    fn drop(&mut self) {
        if self.unlock_on_drop {
            let _ = unlock_file(&self.file);
        }
    }
}

impl SharedLock {
    pub(crate) fn acquire(path: &Path) -> Result<Self, StoreError> {
        let file = lock_file(path)?;
        lock_shared(&file, false)?;
        Ok(Self { _file: file })
    }
}

fn lock_file(path: &Path) -> Result<File, StoreError> {
    // Local flock works on a read-only descriptor. Existing bundles remain
    // readable on read-only mounts; missing lease files require a writable
    // catalogue and are created only while holding catalogue exclusion.
    match File::open(path) {
        Ok(file) => return Ok(file),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?)
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
fn flock(file: &File, operation: libc::c_int) -> Result<(), StoreError> {
    use std::os::fd::AsRawFd;
    loop {
        // SAFETY: the owned/borrowed File keeps this descriptor valid throughout
        // the syscall. flock does not retain a pointer into Rust memory.
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() == ErrorKind::Interrupted {
            continue;
        }
        if error.kind() == ErrorKind::WouldBlock {
            return Err(StoreError::Busy);
        }
        return Err(error.into());
    }
}

pub(crate) fn lock_exclusive(file: &File, nonblocking: bool) -> Result<(), StoreError> {
    flock(
        file,
        libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 },
    )
}
pub(crate) fn lock_shared(file: &File, nonblocking: bool) -> Result<(), StoreError> {
    flock(
        file,
        libc::LOCK_SH | if nonblocking { libc::LOCK_NB } else { 0 },
    )
}
pub(crate) fn unlock_file(file: &File) -> Result<(), StoreError> {
    flock(file, libc::LOCK_UN)
}

pub(crate) fn sync_file(file: &File, full_sync: bool) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    if full_sync {
        use std::os::fd::AsRawFd;
        // SAFETY: File owns the live descriptor; fcntl retains no Rust pointer.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC, 0) } == 0 {
            return Ok(());
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = full_sync;
    file.sync_all()
}

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

/// Make every newly created directory entry durable, including intermediate
/// parents. Syncing only the object directory would not persist its own name.
pub(crate) fn create_dir_all_synced(path: &Path) -> Result<(), StoreError> {
    let path = absolute_path(path)?;
    let mut missing = Vec::new();
    let mut current = path.as_path();
    while !current.exists() {
        missing.push(current.to_path_buf());
        current = current.parent().ok_or(StoreError::Range)?;
    }
    std::fs::create_dir_all(&path)?;
    for directory in missing {
        sync_dir(&directory)?;
        sync_parent_dir(&directory)?;
    }
    Ok(())
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

#[cfg(any(target_os = "macos", target_os = "linux", target_os = "android"))]
#[allow(clippy::needless_pass_by_value)] // Transfer the staging owner on installation.
pub(crate) fn install_pagefile(
    staging: tempfile::NamedTempFile,
    destination: &Path,
) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(staging.path().as_os_str().as_bytes())?;
    let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())?;
    // SAFETY: valid NUL-terminated paths borrowed for the syscall. Both names
    // are in the same directory and exclusion forbids replacing a destination.
    #[cfg(target_os = "macos")]
    let result =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    #[cfg(any(target_os = "linux", target_os = "android"))]
    // SAFETY: Both CStrings own terminated path bytes through the syscall;
    // renameat2 borrows them synchronously and retains no Rust pointers.
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "android")))]
#[allow(clippy::needless_pass_by_value)] // Transfer the staging owner on installation.
pub(crate) fn install_pagefile(
    staging: tempfile::NamedTempFile,
    destination: &Path,
) -> std::io::Result<()> {
    staging
        .persist_noclobber(destination)
        .map(drop)
        .map_err(|error| error.error)
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    use crate::domain::{FileOffset, StoredBytes, StoredRange};

    #[test]
    fn cache_file_is_private_and_holes_do_not_touch_neighbors()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = CacheFile::new()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(cache.metadata()?.nlink(), 0);
            assert_eq!(cache.metadata()?.mode() & 0o777, 0o600);
        }
        let block = cache.block_bytes();
        let bytes = vec![19; usize::try_from(block * 3)?];
        cache.set_len(block * 3)?;
        cache.write(FileOffset::new(0), &bytes)?;
        cache.file.sync_all()?;
        let before = allocated_bytes(&cache.metadata()?);
        assert!(
            cache
                .punch(StoredRange::new(
                    FileOffset::new(1),
                    StoredBytes::new(block)
                )?)
                .is_err()
        );
        let punched = cache.punch(StoredRange::new(
            FileOffset::new(block),
            StoredBytes::new(block),
        )?);
        let mut actual = vec![0; bytes.len()];
        cache.read(FileOffset::new(0), &mut actual)?;
        let block = usize::try_from(block)?;
        assert_eq!(&actual[..block], &bytes[..block]);
        assert_eq!(&actual[block * 2..], &bytes[block * 2..]);
        if punched.is_ok() {
            assert!(actual[block..block * 2].iter().all(|byte| *byte == 0));
            let after = allocated_bytes(&cache.metadata()?);
            assert!(
                after < before,
                "successful punch must release an allocated block"
            );
            eprintln!("cache hole punch released {} bytes", before - after);
        }
        assert_eq!(cache.metadata()?.len(), bytes.len() as u64);
        Ok(())
    }
}

#[cfg(not(unix))]
pub(crate) fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}
