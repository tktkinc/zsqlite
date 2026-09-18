//! Safe database behavior behind the `SQLite` callback boundary.
#![forbid(unsafe_code)]

use super::parent::ParentFile;
use crate::store::Store;
use libsqlite3_sys as ffi;
use std::collections::HashMap;
use std::ffi::{CStr, CString, OsStr, c_int};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, TryLockError, Weak};

const WAL_CHECKPOINT_LOCK: c_int = 1;
fn write_result(error: &crate::StoreError, operation: c_int) -> c_int {
    // SQLITE_BUSY is not a valid failure result from xWrite/xTruncate. In
    // particular, WAL checkpoint code normalizes BUSY to success because it
    // assumes it came from a reader lock. Returning it for storage contention
    // can therefore let last-close checkpointing delete an unbackfilled WAL.
    if matches!(error, crate::StoreError::Busy) {
        operation
    } else {
        sqlite_result(error)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CheckpointState {
    Idle,
    /// Publication was reserved by the real WAL checkpoint SHM lock.
    ShmWriting,
    /// `CKPT_DONE` was observed; truncate/sync and the SHM unlock may follow.
    ShmFinishing,
    /// `locking_mode=EXCLUSIVE` skipped SHM callbacks, so `CKPT_START` reserved
    /// publication as a fallback.
    FallbackWriting,
    /// The fallback lease was released at `CKPT_DONE`. `SQLite` may immediately
    /// issue the checkpoint's xTruncate without another identifying callback.
    FallbackFinishing,
}

impl CheckpointState {
    fn is_writing(self) -> bool {
        matches!(self, Self::ShmWriting | Self::FallbackWriting)
    }

    fn uses_shm(self) -> bool {
        matches!(self, Self::ShmWriting | Self::ShmFinishing)
    }

    fn holds_publication(self) -> bool {
        self.uses_shm() || self == Self::FallbackWriting
    }
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum PendingState {
    #[default]
    Clean,
    Owned,
    NeedsSync,
    OwnedNeedsSync,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum SyncStrength {
    #[default]
    None,
    Normal,
    Full,
}

#[derive(Default)]
struct PublicationState {
    pending: PendingState,
    main_sync: SyncStrength,
}

impl PublicationState {
    fn owns_pending(&self) -> bool {
        matches!(
            self.pending,
            PendingState::Owned | PendingState::OwnedNeedsSync
        )
    }

    fn needs_sync(&self) -> bool {
        matches!(
            self.pending,
            PendingState::NeedsSync | PendingState::OwnedNeedsSync
        )
    }

    fn begin_write(&mut self) {
        self.pending = if self.needs_sync() {
            PendingState::OwnedNeedsSync
        } else {
            PendingState::Owned
        };
        self.main_sync = SyncStrength::None;
    }

    fn clear_owned(&mut self) {
        self.pending = if self.needs_sync() {
            PendingState::NeedsSync
        } else {
            PendingState::Clean
        };
    }

    fn require_sync(&mut self) {
        self.pending = if self.owns_pending() {
            PendingState::OwnedNeedsSync
        } else {
            PendingState::NeedsSync
        };
    }

    fn clear_pending(&mut self) {
        self.pending = PendingState::Clean;
    }

    fn reset(&mut self) {
        *self = Self::default();
    }

    fn record_parent_sync(&mut self, full: bool) {
        self.main_sync = if full {
            SyncStrength::Full
        } else {
            SyncStrength::Normal
        };
    }
}

pub(super) struct FileState {
    store: Option<Arc<Mutex<Store>>>,
    read_only: bool,
    lock_level: c_int,
    /// Tracks staged `Store` writes, commits awaiting a later xSync, and the
    /// strength of the last successful parent main-file sync.
    publication: PublicationState,
    checkpoint: CheckpointState,
    io_statistics: crate::statistics::HandleIoStats,
}

fn discard_owned_pending(file_state: &mut FileState, lock_error: c_int) -> c_int {
    if !file_state.publication.owns_pending() {
        return ffi::SQLITE_OK;
    }
    let Some(store) = file_state.store.as_ref().map(Arc::clone) else {
        file_state.publication.clear_pending();
        return ffi::SQLITE_OK;
    };
    match store
        .lock()
        .map_err(|_| lock_error)
        .map(|mut store| store.discard_pending())
    {
        Ok(()) => {
            file_state.publication.clear_pending();
            ffi::SQLITE_OK
        }
        Err(error) => error,
    }
}

#[derive(Default)]
pub(super) struct RegistryEntry {
    pub(super) store: Mutex<Weak<Mutex<Store>>>,
}

type Registry = HashMap<PathBuf, Arc<RegistryEntry>>;
static STORES: OnceLock<Mutex<Registry>> = OnceLock::new();

fn registry() -> &'static Mutex<Registry> {
    STORES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn sqlite_result(error: &crate::StoreError) -> c_int {
    match error {
        crate::StoreError::PageChecksum(_)
        | crate::StoreError::Format(_)
        | crate::StoreError::Corrupt(_)
        | crate::StoreError::Zstd(_)
        | crate::StoreError::IdentityMismatch => ffi::SQLITE_IOERR_DATA,
        crate::StoreError::NotZsqlite | crate::StoreError::MissingSidecar => ffi::SQLITE_NOTADB,
        crate::StoreError::Busy => ffi::SQLITE_BUSY,
        crate::StoreError::ReadOnly => ffi::SQLITE_READONLY,
        crate::StoreError::UnknownPageSize | crate::StoreError::InvalidPageSize(_) => {
            ffi::SQLITE_NOTADB
        }
        crate::StoreError::InvalidConfiguration(_) => ffi::SQLITE_CANTOPEN,
        crate::StoreError::Io(error) if error.raw_os_error() == Some(libc::ENOSPC) => {
            ffi::SQLITE_FULL
        }
        _ => ffi::SQLITE_IOERR,
    }
}

pub(super) fn canonical_key(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    if let Ok(canonical) = absolute.canonicalize() {
        return canonical;
    }
    let Some(name) = absolute.file_name() else {
        return absolute;
    };
    absolute
        .parent()
        .and_then(|parent| parent.canonicalize().ok())
        .map_or(absolute.clone(), |parent| parent.join(name))
}

pub(super) fn get_store(
    path: &Path,
    create: bool,
    writable: bool,
) -> Result<(Arc<Mutex<Store>>, bool), crate::StoreError> {
    let key = canonical_key(path);
    get_store_with(&key, writable, |key| {
        if writable {
            Store::open(key, create)
        } else {
            Store::open_existing_read_only(key)
        }
    })
}

pub(super) fn registry_entry(key: &Path) -> Result<Arc<RegistryEntry>, crate::StoreError> {
    let mut stores = registry().lock().map_err(|_| crate::StoreError::Range)?;
    Ok(Arc::clone(
        stores
            .entry(key.to_path_buf())
            .or_insert_with(|| Arc::new(RegistryEntry::default())),
    ))
}

pub(super) fn get_store_with(
    key: &Path,
    writable: bool,
    open: impl FnOnce(&Path) -> Result<Store, crate::StoreError>,
) -> Result<(Arc<Mutex<Store>>, bool), crate::StoreError> {
    let entry = registry_entry(key)?;
    // Serialize only opens of the same database. In particular, do not hold
    // the process-wide registry mutex over Store::open(), filesystem I/O, or
    // another Store's mutex.
    let mut registered = entry.store.lock().map_err(|_| crate::StoreError::Range)?;
    if let Some(store) = registered.upgrade() {
        let mut opened = store.lock().map_err(|_| crate::StoreError::Range)?;
        let start_worker = writable && opened.upgrade_writable()?;
        drop(opened);
        drop(registered);
        if start_worker {
            spawn_maintenance_worker(Arc::downgrade(&store));
        }
        return Ok((store, false));
    }
    let opened = open(key)?;
    let store = Arc::new(Mutex::new(opened));
    *registered = Arc::downgrade(&store);
    drop(registered);
    if writable {
        spawn_maintenance_worker(Arc::downgrade(&store));
    }
    Ok((store, true))
}

pub(super) fn delete_registered_store_with<T>(
    path: &Path,
    delete: impl FnOnce(&Path) -> Result<(T, bool), crate::StoreError>,
) -> Result<T, crate::StoreError> {
    let key = canonical_key(path);
    let entry = registry_entry(&key)?;
    // xDelete has no useful reason to wait behind an in-progress xOpen for
    // this path. Returning BUSY also avoids an unbounded callback wait if that
    // open is stuck in filesystem I/O.
    let mut registered = match entry.store.try_lock() {
        Ok(registered) => registered,
        Err(TryLockError::WouldBlock) => return Err(crate::StoreError::Busy),
        Err(TryLockError::Poisoned(_)) => return Err(crate::StoreError::Range),
    };
    let (output, clear_registration) = delete(path)?;
    // Keep the per-path entry itself. Removing it after deletion permits a
    // racing creator to install a replacement entry which this deleter then
    // accidentally removes from the process registry.
    if clear_registration {
        *registered = Weak::new();
    }
    Ok(output)
}

fn spawn_maintenance_worker(store: Weak<Mutex<Store>>) {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let Some(store) = store.upgrade() else {
                break;
            };
            if let Ok(mut opened) = store.try_lock() {
                let _ = opened.try_background_maintenance();
            }
        }
    });
}

fn path_to_c_string(path: &Path) -> Result<CString, ()> {
    #[cfg(unix)]
    {
        CString::new(path.as_os_str().as_bytes()).map_err(|_| ())
    }
    #[cfg(not(unix))]
    {
        CString::new(path.to_string_lossy().as_bytes()).map_err(|_| ())
    }
}

fn path_from_name(name: &CStr) -> PathBuf {
    let bytes = name.to_bytes();
    PathBuf::from(OsStr::from_bytes(bytes))
}

pub(super) fn mapped_storage_name(name: Option<&CStr>) -> Result<Option<CString>, ()> {
    let Some(name) = name else {
        return Ok(None);
    };
    let input = path_from_name(name);
    let storage = crate::facade::vfs_storage_path(&input);
    if storage == input {
        Ok(None)
    } else {
        path_to_c_string(&storage).map(Some)
    }
}

pub(super) fn prepare_open(
    storage: Option<&crate::Storage>,
    name: Option<&CStr>,
    flags: c_int,
) -> Result<(FileState, Option<CString>), c_int> {
    let requested_read_only = flags & ffi::SQLITE_OPEN_READONLY != 0;
    let mut store = None;
    let mut parent_name = None;
    if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
        let Some(name) = name else {
            return Err(ffi::SQLITE_CANTOPEN);
        };
        let path = crate::facade::vfs_storage_path(&path_from_name(name));
        let managed = storage.is_some()
            || path.extension().and_then(|value| value.to_str()) == Some("zsqlite");
        if managed {
            if let Some(storage) = storage
                && let Err(error) = storage.bind(&path)
            {
                return Err(sqlite_result(&error));
            }
            let create = flags & ffi::SQLITE_OPEN_CREATE != 0;
            let storage_existed = path.exists();
            if !storage_existed
                && crate::facade::notice_path(&path).is_some_and(|notice| notice.exists())
            {
                return Err(ffi::SQLITE_CANTOPEN);
            }
            if let Err(error) = crate::facade::validate_notice(&path) {
                return Err(sqlite_result(&error));
            }
            let opened = match get_store(&path, create, !requested_read_only) {
                Ok((opened, _newly_opened)) => opened,
                Err(error) => return Err(sqlite_result(&error)),
            };
            if !requested_read_only && let Err(error) = crate::facade::ensure_notice(&path) {
                return Err(sqlite_result(&error));
            }
            let lock_path = match opened.lock() {
                Ok(store) => store.sqlite_lock_path(),
                Err(_) => return Err(ffi::SQLITE_IOERR),
            };
            let Ok(encoded) = path_to_c_string(&lock_path) else {
                return Err(ffi::SQLITE_CANTOPEN);
            };
            parent_name = Some(encoded);
            store = Some(opened);
        }
    }

    Ok((
        FileState {
            store,
            read_only: requested_read_only,
            lock_level: ffi::SQLITE_LOCK_NONE,
            publication: PublicationState::default(),
            checkpoint: CheckpointState::Idle,
            io_statistics: crate::statistics::HandleIoStats::default(),
        },
        parent_name,
    ))
}

pub(super) fn delete(name: Option<&CStr>, mut parent_delete: impl FnMut() -> c_int) -> c_int {
    if let Some(name) = name {
        let path = crate::facade::vfs_storage_path(&path_from_name(name));
        if path.extension().and_then(|value| value.to_str()) == Some("zsqlite")
            || crate::Storage::is_bound(&crate::backend::sidecar_dir(&path)).unwrap_or(false)
        {
            if let Err(error) = crate::facade::validate_notice(&path) {
                return sqlite_result(&error);
            }
            // Keep exclusion through the parent fallback: a racing open must not
            // create a replacement between bundle deletion and parent deletion.
            return match delete_registered_store_with(&path, |key| {
                match Store::delete_bundle(key) {
                    Ok(()) => {
                        crate::facade::delete_notice(key)?;
                        Ok((ffi::SQLITE_OK, true))
                    }
                    Err(crate::StoreError::NotZsqlite) => {
                        let rc = parent_delete();
                        Ok((rc, rc == ffi::SQLITE_OK))
                    }
                    Err(error) => Err(error),
                }
            }) {
                Ok(rc) => rc,
                Err(error) => sqlite_result(&error),
            };
        }
    }
    parent_delete()
}

impl FileState {
    pub(super) fn is_managed(&self) -> bool {
        self.store.is_some()
    }
    pub(super) fn set_read_only(&mut self, read_only: bool) {
        self.read_only = read_only;
    }
    pub(super) fn close(&mut self) -> c_int {
        let checkpoint_open = self.checkpoint.holds_publication();
        let mut rc = discard_owned_pending(self, ffi::SQLITE_IOERR);
        if checkpoint_open && let Some(store) = self.store.as_ref().map(Arc::clone) {
            match store.lock() {
                Ok(mut store) => store.finish_checkpoint_publication(),
                Err(_) if rc == ffi::SQLITE_OK => rc = ffi::SQLITE_IOERR,
                Err(_) => {}
            }
        }
        self.checkpoint = CheckpointState::Idle;
        rc
    }
    pub(super) fn read(&mut self, parent: &mut ParentFile, data: &mut [u8], offset: i64) -> c_int {
        let file_state = self;
        if file_state.checkpoint == CheckpointState::FallbackFinishing {
            file_state.checkpoint = CheckpointState::Idle;
        }
        if let Some(store) = &file_state.store {
            let result = store
                .lock()
                .map_err(|_| ffi::SQLITE_IOERR)
                .and_then(|mut store| {
                    store
                        .read_with_statistics(
                            offset.cast_unsigned(),
                            data,
                            &mut file_state.io_statistics,
                        )
                        .map_err(|error| sqlite_result(&error))
                });
            match result {
                Ok(read) if read == data.len() => ffi::SQLITE_OK,
                Ok(_) => ffi::SQLITE_IOERR_SHORT_READ,
                Err(error) => error,
            }
        } else {
            parent.read(data, offset)
        }
    }
    pub(super) fn write(&mut self, parent: &mut ParentFile, data: &[u8], offset: i64) -> c_int {
        let file_state = self;
        if let Some(store) = &file_state.store {
            if file_state.read_only {
                return ffi::SQLITE_READONLY;
            }
            let store = Arc::clone(store);
            if file_state.checkpoint == CheckpointState::FallbackFinishing {
                file_state.checkpoint = CheckpointState::Idle;
            }
            let checkpoint_active = file_state.checkpoint.is_writing();
            let result = match store.lock() {
                Ok(mut store) => (|| -> Result<usize, c_int> {
                    // Store::write_at() may acquire the publication lock and
                    // stage one or more pages before a later write fails. Mark
                    // ownership before entering it so every failure path can
                    // roll that partial operation back.
                    file_state.publication.begin_write();
                    let written = store
                        .write_at(offset.cast_unsigned(), data)
                        .map_err(|error| write_result(&error, ffi::SQLITE_IOERR_WRITE))?;
                    if written == data.len() && checkpoint_active {
                        // CKPT_DONE and the checkpoint-lock xShmLock(UNLOCK)
                        // are notifications whose return codes SQLite ignores.
                        // Publish each completed backfill write here so any
                        // failure is returned through xWrite before SQLite can
                        // advance nBackfill.
                        store
                            .publish_checkpoint(false)
                            .map_err(|error| write_result(&error, ffi::SQLITE_IOERR_WRITE))?;
                    }
                    Ok(written)
                })(),
                Err(_) => Err(ffi::SQLITE_IOERR),
            };
            match result {
                Ok(written) if written == data.len() => {
                    if checkpoint_active {
                        file_state.publication.clear_owned();
                    }
                    ffi::SQLITE_OK
                }
                Ok(_) => {
                    let _ = discard_owned_pending(file_state, ffi::SQLITE_IOERR_WRITE);
                    ffi::SQLITE_IOERR_WRITE
                }
                Err(error) => {
                    let _ = discard_owned_pending(file_state, ffi::SQLITE_IOERR_WRITE);
                    error
                }
            }
        } else {
            parent.write(data, offset)
        }
    }
    pub(super) fn truncate(&mut self, parent: &mut ParentFile, size: i64) -> c_int {
        if size < 0 {
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        let file_state = self;
        if let Some(store) = &file_state.store {
            if file_state.read_only {
                return ffi::SQLITE_READONLY;
            }
            let store = Arc::clone(store);
            let retain_checkpoint = file_state.checkpoint.uses_shm();
            let fallback_checkpoint = file_state.checkpoint == CheckpointState::FallbackFinishing;
            let checkpoint_truncate = retain_checkpoint || fallback_checkpoint;
            let result = match store.lock() {
                Ok(mut store) => (|| -> Result<(), c_int> {
                    // truncate() begins a Store transaction before all size
                    // validation and filesystem work has completed.
                    file_state.publication.begin_write();
                    store
                        .truncate(size.cast_unsigned())
                        .map_err(|error| write_result(&error, ffi::SQLITE_IOERR_TRUNCATE))?;
                    if retain_checkpoint {
                        // A complete ordinary WAL checkpoint truncates after
                        // CKPT_DONE. Keep its SHM-owned publication lease until
                        // SQLite releases the checkpoint lock.
                        store
                            .publish_checkpoint(false)
                            .map_err(|error| write_result(&error, ffi::SQLITE_IOERR_TRUNCATE))?;
                    } else if fallback_checkpoint {
                        // locking_mode=EXCLUSIVE has no SHM unlock. CKPT_DONE
                        // released its lease, so publish this immediately and
                        // independently. A following xSync can promote it.
                        store
                            .publish(false)
                            .map_err(|error| write_result(&error, ffi::SQLITE_IOERR_TRUNCATE))?;
                    }
                    Ok(())
                })(),
                Err(_) => Err(ffi::SQLITE_IOERR),
            };
            match result {
                Ok(()) => {
                    if checkpoint_truncate {
                        file_state.publication.clear_owned();
                        // If SQLite calls xSync, make the newly visible commit
                        // and all of its active data durable.
                        file_state.publication.require_sync();
                    }
                    if fallback_checkpoint {
                        file_state.checkpoint = CheckpointState::Idle;
                    }
                    ffi::SQLITE_OK
                }
                Err(error) => {
                    let _ = discard_owned_pending(file_state, ffi::SQLITE_IOERR_TRUNCATE);
                    if fallback_checkpoint {
                        file_state.checkpoint = CheckpointState::Idle;
                    }
                    error
                }
            }
        } else {
            parent.truncate(size)
        }
    }
    pub(super) fn sync(&mut self, parent: &mut ParentFile, flags: c_int) -> c_int {
        let file_state = self;
        if let Some(store) = &file_state.store
            && (file_state.publication.owns_pending() || file_state.publication.needs_sync())
        {
            let in_checkpoint = file_state.checkpoint.uses_shm();
            let full_sync = flags & 0x0f == ffi::SQLITE_SYNC_FULL;
            if let Err(error) =
                store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR_FSYNC)
                    .and_then(|mut store| {
                        if in_checkpoint {
                            store.publish_checkpoint_synced(full_sync)
                        } else {
                            store.publish_synced(full_sync)
                        }
                        .map_err(|error| write_result(&error, ffi::SQLITE_IOERR_FSYNC))
                    })
            {
                return error;
            }
            file_state.publication.clear_pending();
        }
        let rc = parent.sync(flags);
        if rc == ffi::SQLITE_OK && file_state.store.is_some() {
            file_state
                .publication
                .record_parent_sync(flags & 0x0f == ffi::SQLITE_SYNC_FULL);
        }
        rc
    }
    pub(super) fn file_size(&mut self, parent: &mut ParentFile, output: &mut i64) -> c_int {
        let file_state = self;
        if file_state.checkpoint == CheckpointState::FallbackFinishing {
            file_state.checkpoint = CheckpointState::Idle;
        }
        if let Some(store) = &file_state.store {
            match store.lock() {
                Ok(store) => match i64::try_from(store.logical_size()) {
                    Ok(size) => {
                        *output = size;
                        ffi::SQLITE_OK
                    }
                    Err(_) => ffi::SQLITE_IOERR_FSTAT,
                },
                Err(_) => ffi::SQLITE_IOERR_FSTAT,
            }
        } else {
            parent.file_size(output)
        }
    }

    pub(super) fn lock(&mut self, parent: &mut ParentFile, level: c_int) -> c_int {
        if self.checkpoint == CheckpointState::FallbackFinishing {
            self.checkpoint = CheckpointState::Idle;
        }
        let previous = self.lock_level;
        let rc = parent.lock(level);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        if previous == ffi::SQLITE_LOCK_NONE
            && level == ffi::SQLITE_LOCK_SHARED
            && let Some(store) = &self.store
            && let Err(error) = store
                .lock()
                .map_err(|_| ffi::SQLITE_IOERR_LOCK)
                .and_then(|mut store| store.refresh().map_err(|error| sqlite_result(&error)))
        {
            let _ = parent.unlock(ffi::SQLITE_LOCK_NONE);
            return error;
        }
        self.lock_level = level;
        ffi::SQLITE_OK
    }
    pub(super) fn unlock(&mut self, parent: &mut ParentFile, level: c_int) -> c_int {
        if self.checkpoint == CheckpointState::FallbackFinishing {
            self.checkpoint = CheckpointState::Idle;
        }
        let store_rc = if level < ffi::SQLITE_LOCK_RESERVED {
            discard_owned_pending(self, ffi::SQLITE_IOERR_UNLOCK)
        } else {
            ffi::SQLITE_OK
        };
        // SQLite may ignore cleanup errors; always release the parent lock.
        let rc = parent.unlock(level);
        if rc == ffi::SQLITE_OK {
            self.lock_level = level;
        }
        if level < ffi::SQLITE_LOCK_RESERVED && store_rc == ffi::SQLITE_OK {
            self.publication.reset();
        }
        if store_rc == ffi::SQLITE_OK {
            rc
        } else {
            store_rc
        }
    }
    pub(super) fn statistics(&self) -> Result<crate::statistics::FileControlStatsV1, c_int> {
        let store = self.store.as_ref().ok_or(ffi::SQLITE_NOTFOUND)?;
        let store = store.lock().map_err(|_| ffi::SQLITE_IOERR)?;
        Ok(crate::statistics::FileControlStatsV1::snapshot(
            self.io_statistics,
            store.cache_stats(),
        ))
    }
    pub(super) fn has_moved(&self, parent: &mut ParentFile) -> Result<bool, c_int> {
        let parent_moved = parent.has_moved()?;
        let store = self.store.as_ref().ok_or(ffi::SQLITE_IOERR)?;
        let store = store.lock().map_err(|_| ffi::SQLITE_IOERR)?;
        let moved = store
            .database_has_moved()
            .map_err(|error| sqlite_result(&error))?;
        Ok(parent_moved || moved)
    }
    #[allow(clippy::too_many_lines)]
    pub(super) fn file_control(&mut self, operation: c_int) -> Option<c_int> {
        let is_main = self.is_managed();
        if is_main && operation == ffi::SQLITE_FCNTL_CKPT_START {
            let checkpoint_store = (|| {
                if self.checkpoint == CheckpointState::FallbackFinishing {
                    self.checkpoint = CheckpointState::Idle;
                }
                if self.checkpoint != CheckpointState::Idle {
                    return None;
                }
                // SQLite ignores CKPT_START's return value. Record checkpoint
                // intent before trying Store publication so a later xWrite is
                // still classified as checkpoint I/O if initial contention
                // disappears between these callbacks.
                self.checkpoint = CheckpointState::FallbackWriting;
                self.store.as_ref().map(Arc::clone)
            })();
            if let Some(store) = checkpoint_store {
                // Normal WAL mode reserved publication while acquiring the
                // exclusive checkpoint SHM lock. This fallback covers hosts
                // that signal CKPT_START without exposing that lock callback.
                // SQLite treats CKPT_START as advisory and may ignore this
                // result, so xWrite still maps any residual BUSY to IOERR.
                if let Err(error) =
                    store
                        .lock()
                        .map_err(|_| ffi::SQLITE_IOERR)
                        .and_then(|mut store| {
                            store
                                .begin_checkpoint_publication()
                                .map_err(|error| sqlite_result(&error))
                        })
                {
                    return Some(error);
                }
            }
        }
        if is_main
            && !matches!(
                operation,
                ffi::SQLITE_FCNTL_CKPT_START | ffi::SQLITE_FCNTL_CKPT_DONE
            )
            && self.checkpoint == CheckpointState::FallbackFinishing
        {
            self.checkpoint = CheckpointState::Idle;
        }
        if is_main && operation == ffi::SQLITE_FCNTL_SYNC {
            self.publication.require_sync();
            let pending_store = self
                .publication
                .owns_pending()
                .then(|| self.store.as_ref().map(Arc::clone))
                .flatten();
            let had_pending = pending_store.is_some();
            let result = pending_store.map_or(Ok(()), |store| {
                store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR)
                    .and_then(|mut store| {
                        store.publish(false).map_err(|error| sqlite_result(&error))
                    })
            });
            if let Err(error) = result {
                return Some(error);
            }
            if had_pending {
                // SQLITE_FCNTL_SYNC is observed even when Pager.noSync skips
                // xSync(), including during hot-journal recovery. Publish a
                // visible nondurable commit before SQLite finalizes the
                // journal; a following xSync() makes that prefix durable.
                self.publication.clear_owned();
            }
        }
        if is_main && operation == ffi::SQLITE_FCNTL_CKPT_DONE {
            let fallback = self.checkpoint == CheckpointState::FallbackWriting;
            if self.checkpoint == CheckpointState::ShmWriting {
                self.checkpoint = CheckpointState::ShmFinishing;
            }
            // Every successful checkpoint xWrite has already published. If
            // anything remains pending, its xWrite already returned an error.
            // CKPT_DONE's result is ignored, so only best-effort rollback is
            // safe at this boundary.
            let _ = discard_owned_pending(self, ffi::SQLITE_IOERR);
            if fallback {
                // locking_mode=EXCLUSIVE makes SQLite's WAL SHM lock/unlock
                // routines no-ops. There will be no later unlock callback,
                // and partial checkpoints have no truncate or sync callback
                // either, so release the fallback lease here. Any following
                // xTruncate publishes independently before returning.
                if let Some(store) = self.store.as_ref().map(Arc::clone)
                    && let Ok(mut store) = store.lock()
                {
                    store.finish_checkpoint_publication();
                }
                self.checkpoint = CheckpointState::FallbackFinishing;
            }
        }
        if is_main && operation == ffi::SQLITE_FCNTL_COMMIT_PHASETWO {
            let pending_store = self
                .publication
                .owns_pending()
                .then(|| {
                    self.store.as_ref().map(|store| {
                        (
                            Arc::clone(store),
                            self.publication.main_sync != SyncStrength::None,
                            self.publication.main_sync == SyncStrength::Full,
                        )
                    })
                })
                .flatten();
            let result = pending_store.map_or(Ok(()), |(store, durable, full_sync)| {
                store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR)
                    .and_then(|mut store| {
                        if durable {
                            store.publish_synced(full_sync)
                        } else {
                            store.publish(false)
                        }
                        .map_err(|error| sqlite_result(&error))
                    })
            });
            if let Err(error) = result {
                return Some(error);
            }
            self.publication.reset();
        }
        if is_main && operation == ffi::SQLITE_FCNTL_SIZE_HINT {
            return Some(ffi::SQLITE_OK);
        }
        if is_main && operation == ffi::SQLITE_FCNTL_CHUNK_SIZE {
            return Some(ffi::SQLITE_OK);
        }

        None
    }
    pub(super) fn shm_lock(
        &mut self,
        parent: &mut ParentFile,
        offset: c_int,
        count: c_int,
        flags: c_int,
    ) -> c_int {
        if !parent.supports_shm_lock() {
            return ffi::SQLITE_IOERR_SHMLOCK;
        }
        let mut cleanup_rc = ffi::SQLITE_OK;
        if flags & ffi::SQLITE_SHM_UNLOCK != 0
            && flags & ffi::SQLITE_SHM_EXCLUSIVE != 0
            && offset == WAL_CHECKPOINT_LOCK
            && count == 1
            && self.checkpoint.uses_shm()
        {
            self.checkpoint = CheckpointState::Idle;
            cleanup_rc = discard_owned_pending(self, ffi::SQLITE_IOERR_SHMLOCK);
            if let Some(store) = self.store.as_ref().map(Arc::clone) {
                match store.lock() {
                    Ok(mut store) => store.finish_checkpoint_publication(),
                    Err(_) if cleanup_rc == ffi::SQLITE_OK => {
                        cleanup_rc = ffi::SQLITE_IOERR_SHMLOCK;
                    }
                    Err(_) => {}
                }
            }
            if cleanup_rc == ffi::SQLITE_OK {
                self.publication.reset();
            }
        }
        // walUnlockExclusive() ignores xShmLock()'s return value. Always pass
        // the unlock through to the parent so a Store cleanup error does
        // not leak a WAL lock that SQLite believes it has released.
        let rc = parent.shm_lock(offset, count, flags);
        let checkpoint_lock = rc == ffi::SQLITE_OK
            && flags & ffi::SQLITE_SHM_LOCK != 0
            && flags & ffi::SQLITE_SHM_EXCLUSIVE != 0
            && offset == WAL_CHECKPOINT_LOCK
            && count == 1;
        if rc == ffi::SQLITE_OK && flags & ffi::SQLITE_SHM_LOCK != 0 {
            let refresh = self.store.as_ref().map(Arc::clone).map_or(Ok(()), |store| {
                store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR_SHMLOCK)
                    .and_then(|mut store| {
                        if checkpoint_lock {
                            // Reserve publication at the checkpoint-lock
                            // boundary. Returning BUSY here is understood
                            // by SQLite and prevents any backfill xWrite
                            // from racing sidecar maintenance.
                            store.begin_checkpoint_publication()
                        } else {
                            store.refresh()
                        }
                        .map_err(|error| sqlite_result(&error))
                    })
            });
            if let Err(error) = refresh {
                let unlock_flags = (flags & !ffi::SQLITE_SHM_LOCK) | ffi::SQLITE_SHM_UNLOCK;
                let _ = parent.shm_lock(offset, count, unlock_flags);
                return error;
            }
            if checkpoint_lock && self.store.is_some() {
                // CKPT_START is advisory and its return value is ignored. The
                // exclusive checkpoint lock is the reliable acquisition point.
                self.checkpoint = CheckpointState::ShmWriting;
            }
        }
        if cleanup_rc == ffi::SQLITE_OK {
            rc
        } else {
            cleanup_rc
        }
    }
}
