//! In-place native adoption and durable background-conversion controls.
use crate::format::{ActiveHeader, DatabaseId, Digest};
use crate::fs::{absolute_path, sync_parent_dir};
use crate::storage::{Catalog, CatalogGuard, PinnedView, RetentionName};
use crate::{Inspect, StoragePolicy, StoreError};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

pub(crate) const CHUNK_BYTES: u64 = 8 * 1024 * 1024;
const ROOT: &str = "__zsqlite_source_conversion";
const INFO: &str = "conversion.info";
const PAUSED: &str = "conversion.paused";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversionStatus {
    pub total_bytes: u64,
    pub converted_bytes: u64,
    pub paused: bool,
    pub complete: bool,
    pub running: bool,
    pub last_error: Option<String>,
}

pub(crate) struct SourceInfo {
    pub(crate) database: DatabaseId,
    pub(crate) bytes: u64,
    pub(crate) page_size: u32,
    pub(crate) header: Digest,
}
impl SourceInfo {
    pub(crate) fn write(&self, sidecar: &Path) -> Result<(), StoreError> {
        let mut bytes = Vec::from(&b"ZCONV001"[..]);
        bytes.extend(self.database);
        bytes.extend(self.bytes.to_le_bytes());
        bytes.extend(self.page_size.to_le_bytes());
        bytes.extend(self.header);
        bytes.extend(*blake3::hash(&bytes).as_bytes());
        atomic_write(&sidecar.join(INFO), &bytes)
    }
    pub(crate) fn read(sidecar: &Path) -> Result<Option<Self>, StoreError> {
        let file = match File::open(sidecar.join(INFO)) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if file.metadata()?.len() != 116 {
            return Err(StoreError::Corrupt(0));
        }
        let mut bytes = [0; 116];
        crate::fs::read_exact_at(&file, 0, &mut bytes)?;
        if bytes.len() != 116
            || &bytes[..8] != b"ZCONV001"
            || blake3::hash(&bytes[..84]).as_bytes() != &bytes[84..]
        {
            return Err(StoreError::Corrupt(0));
        }
        let result = Self {
            database: bytes[8..40].try_into().map_err(|_| StoreError::Range)?,
            bytes: u64::from_le_bytes(bytes[40..48].try_into().map_err(|_| StoreError::Range)?),
            page_size: u32::from_le_bytes(bytes[48..52].try_into().map_err(|_| StoreError::Range)?),
            header: bytes[52..84].try_into().map_err(|_| StoreError::Range)?,
        };
        if result.bytes == 0
            || !crate::format::valid_page_size(result.page_size)
            || !result.bytes.is_multiple_of(u64::from(result.page_size))
        {
            return Err(StoreError::Corrupt(0));
        }
        Ok(Some(result))
    }
}

pub(crate) fn source_path(sidecar: &Path) -> PathBuf {
    sidecar.join(".source")
}
pub(crate) fn paused(sidecar: &Path) -> bool {
    sidecar.join(PAUSED).exists()
}
pub(crate) fn root_name() -> Result<RetentionName, StoreError> {
    RetentionName::new(ROOT)
}
pub(crate) fn prefix(guard: &CatalogGuard) -> Result<Option<PinnedView>, StoreError> {
    match guard.read_root(&root_name()?) {
        Ok(pin) => guard.pin_retained(&pin).map(Some),
        Err(StoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}
pub(crate) fn status(
    guard: &CatalogGuard,
    header: &ActiveHeader,
) -> Result<Option<ConversionStatus>, StoreError> {
    let Some(info) = SourceInfo::read(guard.root())? else {
        if header.source_bytes != 0 {
            return Err(StoreError::Corrupt(0));
        }
        return Ok(None);
    };
    if info.database != header.database_id
        || (header.source_bytes != 0
            && (info.bytes != header.source_bytes
                || info.page_size != header.page_size
                || info.header != header.source_header_digest))
    {
        return Err(StoreError::IdentityMismatch);
    }
    let complete = header.source_bytes == 0 && header.parent_physical_digest != [0; 32];
    let converted_bytes = if complete {
        info.bytes
    } else {
        prefix(guard)?.map_or(0, |view| view.logical_size().get())
    };
    if converted_bytes > info.bytes {
        return Err(StoreError::Corrupt(0));
    }
    let error_path = guard.root().join("conversion.error");
    let last_error = match if error_path
        .metadata()
        .is_ok_and(|meta| meta.len() > 16 * 1024)
    {
        Ok("conversion error message exceeds its size limit".to_owned())
    } else {
        std::fs::read_to_string(error_path)
    } {
        Ok(error) => Some(error),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let running = if complete {
        false
    } else {
        match File::open(guard.root().join("locks/conversion-worker.lock")) {
            Ok(file) => match crate::fs::ExclusiveLock::on_file(&file, true) {
                Ok(_lease) => false,
                Err(StoreError::Busy) => true,
                Err(error) => return Err(error),
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error.into()),
        }
    };
    Ok(Some(ConversionStatus {
        total_bytes: info.bytes,
        converted_bytes,
        running,
        paused: !complete && paused(guard.root()),
        complete,
        last_error,
    }))
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let parent = path.parent().ok_or(StoreError::Range)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(path)
        .map_err(|error| StoreError::Io(error.error))?;
    sync_parent_dir(path)
}
pub(crate) fn record_error(sidecar: &Path, error: &str) -> Result<(), StoreError> {
    let error: String = error.chars().take(4096).collect();
    atomic_write(&sidecar.join("conversion.error"), error.as_bytes())
}
pub(crate) fn control_lock(sidecar: &Path) -> Result<crate::fs::ExclusiveLock, StoreError> {
    crate::fs::ExclusiveLock::acquire(&sidecar.join("locks/conversion-control.lock"), false)
}

/// Adopt a native `.db` without copying its pages. Native clients must be closed
/// and prevented from opening until this call returns. `SQLite` checkpoints the
/// WAL and obtains exclusive access; this cannot redirect an existing handle.
/// Conversion runs on writable VFS opens or explicit `conversion_step()` calls.
/// Requires the `static` feature for the native `SQLite` checkpoint API.
pub fn adopt_to_zsqlite(path: impl AsRef<Path>) -> Result<Inspect, StoreError> {
    adopt_to_zsqlite_with_policy(path, StoragePolicy::default())
}
pub fn adopt_to_zsqlite_with_policy(
    path: impl AsRef<Path>,
    policy: StoragePolicy,
) -> Result<Inspect, StoreError> {
    let logical = absolute_path(path.as_ref())?;
    let physical = crate::facade::storage_path(&logical);
    if physical == logical {
        return Err(StoreError::InvalidConfiguration(
            "in-place adoption requires a logical .db path",
        ));
    }
    let sidecar = crate::backend::sidecar_dir(&physical);
    // A durable source and its descriptor survive any interruption before
    // stub installation. Repeating adoption finishes that same handoff.
    if let Some(info) = SourceInfo::read(&sidecar)? {
        let _handoff = crate::fs::ExclusiveLock::acquire(
            &sidecar.join("locks/conversion-handoff.lock"),
            false,
        )?;
        let mut store = crate::store::Store::open_existing(&physical)?;
        if store.source_needs_initialization() {
            if !source_path(&sidecar).exists() {
                let native = checkpoint_native(&logical)?;
                std::fs::rename(&logical, source_path(&sidecar))?;
                sync_parent_dir(&logical)?;
                sync_parent_dir(&source_path(&sidecar))?;
                drop(native);
            }
            store.initialize_source(&info)?;
        }
        crate::facade::ensure_notice(&physical)?;
        return store.inspect();
    }
    crate::ensure_bundle_absent(&physical)?;
    if std::fs::symlink_metadata(&logical)?
        .file_type()
        .is_symlink()
    {
        return Err(StoreError::InvalidConfiguration(
            "adopt the native database's real path",
        ));
    }
    let native = checkpoint_native(&logical)?;
    crate::store::reject_auxiliary_files(&logical)?;
    let file = File::open(&logical)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if file.metadata()?.nlink() != 1 {
            return Err(StoreError::Busy);
        }
    }
    let mut header = [0; 100];
    crate::fs::read_exact_at(&file, 0, &mut header)?;
    let page_size = crate::sqlite_page_size(&header).ok_or(StoreError::InvalidStandardDatabase)?;
    let bytes = file.metadata()?.len();
    if bytes == 0 || !bytes.is_multiple_of(u64::from(page_size)) {
        return Err(StoreError::InvalidStandardDatabase);
    }
    crate::domain::LogicalBytes::new(bytes, crate::domain::PageSize::new(page_size)?)?;
    file.sync_all()?;
    let mut cleanup = crate::CleanupPaths::new(crate::bundle_paths(&physical).to_vec());
    let mut store = crate::store::Store::open(&physical, true)?;
    if policy != StoragePolicy::default() {
        store.set_storage_policy(policy)?;
    }
    let info = SourceInfo {
        database: store.database_id(),
        bytes,
        page_size,
        header: *blake3::hash(&header).as_bytes(),
    };
    let _handoff =
        crate::fs::ExclusiveLock::acquire(&sidecar.join("locks/conversion-handoff.lock"), false)?;
    info.write(&sidecar)?;
    // A durable descriptor lets a repeated call finish this handoff after a
    // source rename or active-file installation is interrupted.
    cleanup.disarm();
    std::fs::rename(&logical, source_path(&sidecar))?;
    sync_parent_dir(&logical)?;
    sync_parent_dir(&source_path(&sidecar))?;
    // Keep the native connection and every other descriptor for its inode
    // alive through the move. Closing any fd can release POSIX SQLite locks.
    drop(file);
    drop(native);
    #[cfg(test)]
    crate::storage::faults::check(crate::storage::faults::Point::SourceRenamed)?;
    store.initialize_source(&info)?;
    crate::facade::ensure_notice(&physical)?;
    store.inspect()
}

pub fn conversion_status(path: impl AsRef<Path>) -> Result<Option<ConversionStatus>, StoreError> {
    let physical = crate::database_storage_path(path.as_ref())?;
    let store = crate::store::Store::open_existing_read_only(&physical)?;
    let catalog = Catalog::open(&crate::backend::sidecar_dir(&physical), false)?;
    status(&catalog.lock()?, &store.header())
}

/// Pause durably at a chunk publication boundary. Reads and writes continue.
pub fn pause_conversion(path: impl AsRef<Path>) -> Result<ConversionStatus, StoreError> {
    set_paused(path.as_ref(), true)
}
pub fn resume_conversion(path: impl AsRef<Path>) -> Result<ConversionStatus, StoreError> {
    set_paused(path.as_ref(), false)
}
fn set_paused(path: &Path, pause: bool) -> Result<ConversionStatus, StoreError> {
    let physical = crate::database_storage_path(path)?;
    let sidecar = crate::backend::sidecar_dir(&physical);
    let _control = control_lock(&sidecar)?;
    let state = conversion_status(path)?.ok_or(StoreError::InvalidConfiguration(
        "database has no background conversion",
    ))?;
    if !state.complete {
        if pause {
            atomic_write(&sidecar.join(PAUSED), b"")?;
        } else {
            match std::fs::remove_file(sidecar.join(PAUSED)) {
                Ok(()) => sync_parent_dir(&sidecar.join(PAUSED))?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let _ = std::fs::remove_file(sidecar.join("conversion.error"));
        }
    }
    conversion_status(path)?.ok_or(StoreError::Corrupt(0))
}

/// Compress and durably publish at most one bounded source chunk. Multiple
/// workers serialize only with each other. Paused conversion does no work.
pub fn conversion_step(path: impl AsRef<Path>) -> Result<ConversionStatus, StoreError> {
    let physical = crate::database_storage_path(path.as_ref())?;
    let mut store = crate::store::Store::open_existing(&physical)?;
    let result = (|| {
        if let Some(work) = store.prepare_source_work()? {
            store.finish_work(work.prepare()?)?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        if !matches!(error, StoreError::Busy) {
            let _ = record_error(&crate::backend::sidecar_dir(&physical), &error.to_string());
        }
        return Err(error);
    }
    conversion_status(path)?.ok_or(StoreError::InvalidConfiguration(
        "database has no background conversion",
    ))
}

struct NativeCheckpoint {
    #[cfg(feature = "static")]
    database: *mut libsqlite3_sys::sqlite3,
}
impl Drop for NativeCheckpoint {
    fn drop(&mut self) {
        #[cfg(feature = "static")]
        // SAFETY: sole live owner, no statements/callbacks survive this scope.
        unsafe {
            libsqlite3_sys::sqlite3_close(self.database);
        }
    }
}
#[cfg(not(feature = "static"))]
fn checkpoint_native(_path: &Path) -> Result<NativeCheckpoint, StoreError> {
    Err(StoreError::InvalidConfiguration(
        "native adoption requires a build with --no-default-features --features static",
    ))
}

#[cfg(feature = "static")]
fn checkpoint_native(path: &Path) -> Result<NativeCheckpoint, StoreError> {
    use libsqlite3_sys as ffi;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let filename = CString::new(path.as_os_str().as_bytes()).map_err(|_| StoreError::Range)?;
    let mut raw = std::ptr::null_mut();
    // SAFETY: valid C string and exclusive output slot. The default native VFS
    // is selected; no Rust callback or input pointer is retained.
    let code = unsafe {
        ffi::sqlite3_open_v2(
            filename.as_ptr(),
            &raw mut raw,
            ffi::SQLITE_OPEN_READWRITE,
            std::ptr::null(),
        )
    };
    if raw.is_null() {
        return Err(StoreError::InvalidStandardDatabase);
    }
    let native = NativeCheckpoint { database: raw };
    let check = |code| {
        if code == ffi::SQLITE_OK {
            Ok(())
        } else if code == ffi::SQLITE_BUSY || code == ffi::SQLITE_LOCKED {
            Err(StoreError::Busy)
        } else {
            Err(StoreError::InvalidStandardDatabase)
        }
    };
    check(code)?;
    let exec = |sql: &std::ffi::CStr| {
        // SAFETY: exclusively owned connection; static SQL; no callbacks or
        // error allocation requested. SQLite does not retain the SQL pointer.
        check(unsafe {
            ffi::sqlite3_exec(
                native.database,
                sql.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        })
    };
    exec(c"PRAGMA synchronous=FULL; PRAGMA locking_mode=EXCLUSIVE;")?;
    // SAFETY: the native connection is exclusive to this thread. Null output
    // pointers are supported. Success in TRUNCATE mode backfills and syncs WAL.
    check(unsafe {
        ffi::sqlite3_wal_checkpoint_v2(
            native.database,
            c"main".as_ptr(),
            ffi::SQLITE_CHECKPOINT_TRUNCATE,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    })?;
    exec(c"PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE; COMMIT;")?;
    Ok(native)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::read_exact_at;
    use crate::storage::faults::{self, Mode, Point};
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn pending(path: &Path, pages: usize) -> Result<(crate::store::Store, Vec<u8>), StoreError> {
        let physical = crate::facade::storage_path(path);
        let mut store = crate::store::Store::open(&physical, true)?;
        store.set_storage_policy(
            StoragePolicy::default().with_dictionary(crate::DictionaryPolicy::new(0, 1024 * 1024)?),
        )?;
        let sidecar = crate::backend::sidecar_dir(&physical);
        let mut bytes = vec![3; pages * 4096];
        bytes[..16].copy_from_slice(b"SQLite format 3\0");
        bytes[16..18].copy_from_slice(&4096_u16.to_be_bytes());
        bytes[18..20].fill(1);
        std::fs::write(source_path(&sidecar), &bytes)?;
        let info = SourceInfo {
            database: store.database_id(),
            bytes: bytes.len() as u64,
            page_size: 4096,
            header: *blake3::hash(&bytes[..100]).as_bytes(),
        };
        info.write(&sidecar)?;
        store.initialize_source(&info)?;
        crate::facade::ensure_notice(&physical)?;
        Ok((store, bytes))
    }

    #[test]
    fn pause_after_chunk_publication_prevents_source_attachment() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("pause.db");
        let (mut store, original) = pending(&path, 16)?;
        let work = store.prepare_source_work()?.unwrap();
        assert!(conversion_status(&path)?.unwrap().running);
        let publication = crate::fs::ExclusiveLock::acquire(
            &crate::backend::sidecar_dir(&crate::facade::storage_path(&path))
                .join("locks/publication.lock"),
            true,
        )?;
        // Preparing payloads succeeds while a different owner reserves SQL
        // publication; the compressor needs neither that lock nor a Store borrow.
        let prepared = work.prepare()?;
        drop(publication);
        pause_conversion(&path)?;
        assert!(matches!(store.finish_work(prepared), Err(StoreError::Busy)));
        let status = conversion_status(&path)?.unwrap();
        assert_eq!(status.converted_bytes, original.len() as u64);
        assert!(!status.complete);
        let mut bytes = vec![0; original.len()];
        store.read_at(0, &mut bytes)?;
        assert_eq!(bytes, original);
        resume_conversion(&path)?;
        assert!(conversion_step(&path)?.complete);
        Ok(())
    }

    #[test]
    fn pause_before_preparing_a_chunk_prevents_publication() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("pause-before.db");
        let (mut store, _) = pending(&path, 16)?;
        let work = store.prepare_source_work()?.unwrap();
        pause_conversion(&path)?;
        assert!(matches!(work.prepare(), Err(StoreError::Busy)));
        assert_eq!(conversion_status(&path)?.unwrap().converted_bytes, 0);
        assert!(store.prepare_source_work()?.is_none());
        resume_conversion(&path)?;
        assert!(conversion_step(&path)?.complete);
        Ok(())
    }

    #[test]
    fn source_attachment_preserves_truncation_regrowth_and_zero_overrides() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("truncate.db");
        let (mut store, mut expected) = pending(&path, 8)?;
        let work = store.prepare_source_work()?.unwrap();
        store.truncate(2 * 4096)?;
        store.publish(true)?;
        store.truncate(8 * 4096)?;
        store.write_at(4096, &vec![0; 4096])?;
        store.write_at(3 * 4096, &vec![7; 4096])?;
        store.publish(true)?;
        expected[4096..].fill(0);
        expected[3 * 4096..4 * 4096].fill(7);
        store.finish_work(work.prepare()?)?;
        drop(store);
        let mut store = crate::store::Store::open_existing(crate::facade::storage_path(&path))?;
        let mut actual = vec![0; expected.len()];
        store.read_at(0, &mut actual)?;
        assert_eq!(actual, expected);
        store.flush_sidecars()?;
        store.verify()?;
        store.read_at(0, &mut actual)?;
        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn interrupted_source_attachment_recovers_without_losing_overrides() -> TestResult {
        for point in [
            Point::ActiveDataSynced,
            Point::ActiveRenamed,
            Point::ActiveDirectorySynced,
        ] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("recover.db");
            let (mut store, mut expected) = pending(&path, 16)?;
            store.write_at(4096, &vec![9; 4096])?;
            store.publish(true)?;
            expected[4096..8192].fill(9);
            let prepared = store.prepare_source_work()?.unwrap().prepare()?;
            let injection = faults::inject(point, Mode::Error);
            assert!(
                store.finish_work(prepared).is_err(),
                "{point:?} was not reached"
            );
            drop(injection);
            drop(store);
            let mut store = crate::store::Store::open_existing(crate::facade::storage_path(&path))?;
            if let Some(work) = store.prepare_source_work()? {
                store.finish_work(work.prepare()?)?;
            }
            let mut actual = vec![0; expected.len()];
            store.read_at(0, &mut actual)?;
            assert_eq!(actual, expected, "{point:?}");
            store.verify()?;
            assert!(conversion_status(&path)?.unwrap().complete);
            assert!(
                !source_path(&crate::backend::sidecar_dir(&crate::facade::storage_path(
                    &path
                )))
                .exists()
            );
        }
        Ok(())
    }

    #[test]
    fn interrupted_chunk_publication_can_be_collected_and_retried() -> TestResult {
        for point in [
            Point::ObjectDataSynced,
            Point::ObjectLinked,
            Point::ObjectDirectorySynced,
            Point::ManifestReady,
            Point::RootRenamed,
            Point::RootDirectorySynced,
        ] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("chunk-recovery.db");
            let (mut store, mut expected) = pending(&path, 16)?;
            store.write_at(4096, &vec![9; 4096])?;
            store.publish(true)?;
            expected[4096..8192].fill(9);
            let work = store.prepare_source_work()?.unwrap();
            let injection = faults::inject(point, Mode::Error);
            assert!(work.prepare().is_err(), "{point:?} was not reached");
            drop(injection);
            drop(store);
            crate::collect(&path, 100)?;
            assert!(conversion_step(&path)?.complete);
            let mut store = crate::store::Store::open_existing(crate::facade::storage_path(&path))?;
            let mut actual = vec![0; expected.len()];
            store.read_at(0, &mut actual)?;
            assert_eq!(actual, expected, "{point:?}");
            store.verify()?;
        }
        Ok(())
    }

    #[test]
    fn user_retention_cannot_release_or_replace_conversion_progress() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("reserved-root.db");
        let (mut store, _) = pending(&path, 16)?;
        let work = store.prepare_source_work()?.unwrap().prepare()?;
        pause_conversion(&path)?;
        drop(work);
        assert!(matches!(
            store.retain_view(root_name()?, true),
            Err(StoreError::InvalidConfiguration(_))
        ));
        let pin = crate::retention(&path, &root_name()?)?;
        assert!(matches!(
            store.release_view(pin),
            Err(StoreError::InvalidConfiguration(_))
        ));
        assert_eq!(
            conversion_status(&path)?.unwrap().converted_bytes,
            16 * 4096
        );
        Ok(())
    }

    #[test]
    #[cfg(feature = "static")]
    fn native_handoff_can_be_retried_at_every_rename_boundary() -> TestResult {
        for point in [
            Point::SourceRenamed,
            Point::ActiveDataSynced,
            Point::ActiveRenamed,
            Point::ActiveDirectorySynced,
        ] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("handoff.db");
            let physical = crate::facade::storage_path(&path);
            crate::facade::ensure_notice(&physical)?;
            let file = File::open(&path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            }
            drop(file);
            let injection = faults::inject(point, Mode::Error);
            assert!(
                adopt_to_zsqlite(&path).is_err(),
                "{point:?} was not reached"
            );
            drop(injection);
            adopt_to_zsqlite(&path)?;
            let state = conversion_status(&path)?.unwrap();
            assert!(!state.complete);
            assert_eq!(state.total_bytes, 4096);
            assert!(conversion_step(&path)?.complete);
        }
        Ok(())
    }

    #[test]
    fn stale_active_snapshot_never_overwrites_a_new_commit() -> TestResult {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("snapshot.zsqlite");
        let mut store = crate::store::Store::open(&path, true)?;
        let mut original = vec![3; 8192];
        original[..16].copy_from_slice(b"SQLite format 3\0");
        original[16..18].copy_from_slice(&4096_u16.to_be_bytes());
        store.write_at(0, &original)?;
        store.publish(true)?;
        let work = store.prepare_flush_work()?.unwrap();
        let mut other = crate::store::Store::open_existing(&path)?;
        other.write_at(4096, &vec![8; 4096])?;
        other.publish(true)?;
        assert!(matches!(
            store.finish_work(work.prepare()?),
            Err(StoreError::Busy)
        ));
        let mut actual = vec![0; 4096];
        other.read_at(4096, &mut actual)?;
        assert_eq!(actual, vec![8; 4096]);
        // There is no sealed publication from the stale candidate.
        let file = File::open(path)?;
        let mut header = [0; crate::format::ACTIVE_HEADER_SIZE];
        read_exact_at(&file, 0, &mut header)?;
        assert_eq!(
            ActiveHeader::decode(&header)?.parent_physical_digest,
            [0; 32]
        );
        other.verify()?;
        Ok(())
    }
}
