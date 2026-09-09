//! Thin `SQLite` ABI shim. Main-database I/O is redirected to [`Store`]; every
//! other operation is forwarded to the host VFS selected during registration.

use crate::store::Store;
use libsqlite3_sys as ffi;
use std::collections::HashMap;
use std::ffi::{CStr, CString, OsStr, c_char, c_int, c_void};
use std::mem::{MaybeUninit, size_of};
use std::path::{Path, PathBuf};
use std::ptr::{self, null, null_mut};
use std::slice;
use std::sync::{Arc, Mutex, OnceLock, TryLockError, Weak};

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

const VFS_NAME: &[u8] = b"zsqlite\0";
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

#[repr(C)]
struct ZFile {
    base: ffi::sqlite3_file,
    parent: *mut ffi::sqlite3_file,
    state: *mut FileState,
    io: MaybeUninit<ffi::sqlite3_io_methods>,
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

struct FileState {
    store: Option<Arc<Mutex<Store>>>,
    /// Owns the alternate main-file pathname for as long as the parent VFS
    /// may retain the pointer passed to xOpen.
    _parent_name: Option<CString>,
    read_only: bool,
    lock_level: c_int,
    vfs: *mut ffi::sqlite3_vfs,
    /// Tracks staged Store writes, commits awaiting a later xSync, and the
    /// strength of the last successful parent main-file sync.
    publication: PublicationState,
    checkpoint: CheckpointState,
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

struct AppData {
    parent: *mut ffi::sqlite3_vfs,
}

// Access is serialized by SQLite's global VFS registration mutex after setup.
unsafe impl Send for AppData {}
unsafe impl Sync for AppData {}

#[derive(Default)]
struct RegistryEntry {
    store: Mutex<Weak<Mutex<Store>>>,
}

type Registry = HashMap<PathBuf, Arc<RegistryEntry>>;
static STORES: OnceLock<Mutex<Registry>> = OnceLock::new();
static REGISTRATION_LOCK: Mutex<()> = Mutex::new(());

fn registry() -> &'static Mutex<Registry> {
    STORES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn sqlite_result(error: &crate::StoreError) -> c_int {
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

fn canonical_key(path: &Path) -> PathBuf {
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

fn get_store(
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

fn registry_entry(key: &Path) -> Result<Arc<RegistryEntry>, crate::StoreError> {
    let mut stores = registry().lock().map_err(|_| crate::StoreError::Range)?;
    Ok(Arc::clone(
        stores
            .entry(key.to_path_buf())
            .or_insert_with(|| Arc::new(RegistryEntry::default())),
    ))
}

fn get_store_with(
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

fn delete_registered_store_with<T>(
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
            let separate_flush = store
                .try_lock()
                .ok()
                .and_then(|opened| opened.background_flush_due().then(|| opened.path()));
            if let Some(path) = separate_flush {
                drop(store);
                if let Ok(mut maintenance) = Store::open_existing(&path) {
                    let _ = maintenance.try_background_maintenance();
                }
            } else if let Ok(mut opened) = store.try_lock() {
                let _ = opened.try_background_maintenance();
            }
        }
    });
}

unsafe fn app_data(vfs: *mut ffi::sqlite3_vfs) -> Option<&'static AppData> {
    let raw = unsafe { vfs.as_ref()?.pAppData.cast::<AppData>() };
    unsafe { raw.as_ref() }
}

unsafe fn parent_vfs_for(
    vfs: *mut ffi::sqlite3_vfs,
) -> Option<(*mut ffi::sqlite3_vfs, &'static ffi::sqlite3_vfs)> {
    let parent = unsafe { app_data(vfs) }?.parent;
    Some((parent, unsafe { parent.as_ref()? }))
}

unsafe fn zfile(file: *mut ffi::sqlite3_file) -> Option<&'static mut ZFile> {
    unsafe { file.cast::<ZFile>().as_mut() }
}

unsafe fn state(file: *mut ffi::sqlite3_file) -> Option<&'static mut FileState> {
    let state = unsafe { zfile(file)?.state };
    unsafe { state.as_mut() }
}

fn ffi_guard(callback: impl FnOnce() -> c_int) -> c_int {
    ffi_guard_or(ffi::SQLITE_IOERR, callback)
}

fn ffi_guard_or<T>(fallback: T, callback: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).unwrap_or(fallback)
}

#[allow(clippy::too_many_lines)]
unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    output: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    ffi_guard(|| {
        if output.is_null() {
            return ffi::SQLITE_MISUSE;
        }
        let output_pointer = output.cast::<ZFile>();
        unsafe {
            ptr::write(
                output_pointer,
                ZFile {
                    base: ffi::sqlite3_file { pMethods: null() },
                    parent: null_mut(),
                    state: null_mut(),
                    io: MaybeUninit::uninit(),
                },
            );
        }
        let output = unsafe { &mut *output_pointer };
        let Some(app) = (unsafe { app_data(vfs) }) else {
            return ffi::SQLITE_INTERNAL;
        };
        let Some(parent_vfs) = (unsafe { app.parent.as_ref() }) else {
            return ffi::SQLITE_INTERNAL;
        };
        let requested_read_only = flags & ffi::SQLITE_OPEN_READONLY != 0;
        let mut store = None;
        let mut parent_name = None;
        if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
            if name.is_null() {
                return ffi::SQLITE_CANTOPEN;
            }
            let path = crate::facade::vfs_storage_path(&path_from_name(name));
            let managed = path.extension().and_then(|value| value.to_str()) == Some("zsqlite");
            if managed {
                let create = flags & ffi::SQLITE_OPEN_CREATE != 0;
                let storage_existed = path.exists();
                if !storage_existed
                    && crate::facade::notice_path(&path).is_some_and(|notice| notice.exists())
                {
                    return ffi::SQLITE_CANTOPEN;
                }
                if let Err(error) = crate::facade::validate_notice(&path) {
                    return sqlite_result(&error);
                }
                let opened = match get_store(&path, create, !requested_read_only) {
                    Ok((opened, _newly_opened)) => opened,
                    Err(error) => return sqlite_result(&error),
                };
                if !requested_read_only && let Err(error) = crate::facade::ensure_notice(&path) {
                    return sqlite_result(&error);
                }
                let lock_path = match opened.lock() {
                    Ok(store) => store.sqlite_lock_path(),
                    Err(_) => return ffi::SQLITE_IOERR,
                };
                let Ok(encoded) = path_to_c_string(&lock_path) else {
                    return ffi::SQLITE_CANTOPEN;
                };
                parent_name = Some(encoded);
                store = Some(opened);
            }
        }
        let parent_size = match usize::try_from(parent_vfs.szOsFile) {
            Ok(size) if size >= size_of::<ffi::sqlite3_file>() => size,
            _ => return ffi::SQLITE_INTERNAL,
        };
        let parent_file =
            unsafe { ffi::sqlite3_malloc64(parent_size as u64) }.cast::<ffi::sqlite3_file>();
        if parent_file.is_null() {
            return ffi::SQLITE_NOMEM;
        }
        unsafe { ptr::write_bytes(parent_file.cast::<u8>(), 0, parent_size) };
        let Some(parent_open) = parent_vfs.xOpen else {
            unsafe { ffi::sqlite3_free(parent_file.cast()) };
            return ffi::SQLITE_INTERNAL;
        };
        let parent_name_pointer = parent_name.as_ref().map_or(name, |value| value.as_ptr());
        let rc = unsafe {
            parent_open(
                app.parent,
                parent_name_pointer,
                parent_file,
                flags,
                out_flags,
            )
        };
        if rc != ffi::SQLITE_OK {
            // SQLite permits a failed parent xOpen to leave pMethods set, in
            // which case xClose must release partially acquired resources.
            unsafe { close_parent(parent_file) };
            return rc;
        }
        let Some(parent_methods) = (unsafe { parent_methods_for(parent_file) }) else {
            unsafe { close_parent(parent_file) };
            return ffi::SQLITE_IOERR;
        };
        let Some(io) = io_methods(parent_methods) else {
            unsafe { close_parent(parent_file) };
            return ffi::SQLITE_IOERR;
        };
        output.io.write(io);
        let io = output.io.as_ptr();

        let out_read_only = if out_flags.is_null() {
            flags & ffi::SQLITE_OPEN_READONLY != 0
        } else {
            (unsafe { *out_flags }) & ffi::SQLITE_OPEN_READONLY != 0
        };
        output.parent = parent_file;
        output.state = Box::into_raw(Box::new(FileState {
            store,
            _parent_name: parent_name,
            read_only: out_read_only,
            lock_level: ffi::SQLITE_LOCK_NONE,
            vfs,
            publication: PublicationState::default(),
            checkpoint: CheckpointState::Idle,
        }));
        output.base.pMethods = io;
        ffi::SQLITE_OK
    })
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

unsafe fn close_parent(parent: *mut ffi::sqlite3_file) -> c_int {
    let rc = unsafe {
        parent
            .as_ref()
            .and_then(|file| file.pMethods.as_ref())
            .and_then(|methods| methods.xClose)
            .map_or(ffi::SQLITE_OK, |close| close(parent))
    };
    unsafe { ffi::sqlite3_free(parent.cast()) };
    rc
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    ffi_guard(|| {
        let Some(file) = (unsafe { zfile(file) }) else {
            return ffi::SQLITE_MISUSE;
        };
        let mut index_rc = ffi::SQLITE_OK;
        if !file.state.is_null() {
            let file_state = unsafe { &mut *file.state };
            let checkpoint_open = file_state.checkpoint.holds_publication();
            index_rc = discard_owned_pending(file_state, ffi::SQLITE_IOERR);
            if checkpoint_open && let Some(store) = file_state.store.as_ref().map(Arc::clone) {
                match store.lock() {
                    Ok(mut store) => store.finish_checkpoint_publication(),
                    Err(_) if index_rc == ffi::SQLITE_OK => index_rc = ffi::SQLITE_IOERR,
                    Err(_) => {}
                }
            }
            file_state.checkpoint = CheckpointState::Idle;
        }
        let rc = unsafe { close_parent(file.parent) };
        file.parent = null_mut();
        if !file.state.is_null() {
            unsafe { drop(Box::from_raw(file.state)) };
            file.state = null_mut();
        }
        file.base.pMethods = null();
        if index_rc == ffi::SQLITE_OK {
            rc
        } else {
            index_rc
        }
    })
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    output: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    ffi_guard(|| {
        if amount < 0 || offset < 0 || (output.is_null() && amount != 0) {
            return ffi::SQLITE_IOERR_READ;
        }
        if amount == 0 {
            return ffi::SQLITE_OK;
        }
        let Some(file_state) = (unsafe { state(file) }) else {
            return ffi::SQLITE_IOERR_READ;
        };
        if file_state.checkpoint == CheckpointState::FallbackFinishing {
            file_state.checkpoint = CheckpointState::Idle;
        }
        if let Some(store) = &file_state.store {
            let Ok(amount) = usize::try_from(amount) else {
                return ffi::SQLITE_IOERR_READ;
            };
            let data = unsafe { slice::from_raw_parts_mut(output.cast::<u8>(), amount) };
            let result = store
                .lock()
                .map_err(|_| ffi::SQLITE_IOERR)
                .and_then(|mut store| {
                    store
                        .read_at(offset.cast_unsigned(), data)
                        .map_err(|error| sqlite_result(&error))
                });
            match result {
                Ok(read) if read == data.len() => ffi::SQLITE_OK,
                Ok(_) => ffi::SQLITE_IOERR_SHORT_READ,
                Err(error) => error,
            }
        } else {
            unsafe {
                call_read(
                    zfile(file).expect("validated file").parent,
                    output,
                    amount,
                    offset,
                )
            }
        }
    })
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    input: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    ffi_guard(|| {
        if amount < 0 || offset < 0 || (input.is_null() && amount != 0) {
            return ffi::SQLITE_IOERR_WRITE;
        }
        if amount == 0 {
            return ffi::SQLITE_OK;
        }
        let Some(file_state) = (unsafe { state(file) }) else {
            return ffi::SQLITE_IOERR_WRITE;
        };
        if let Some(store) = &file_state.store {
            if file_state.read_only {
                return ffi::SQLITE_READONLY;
            }
            let Ok(amount) = usize::try_from(amount) else {
                return ffi::SQLITE_IOERR_WRITE;
            };
            let data = unsafe { slice::from_raw_parts(input.cast::<u8>(), amount) };
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
            unsafe {
                call_write(
                    zfile(file).expect("validated file").parent,
                    input,
                    amount,
                    offset,
                )
            }
        }
    })
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    ffi_guard(|| {
        if size < 0 {
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        let Some(file_state) = (unsafe { state(file) }) else {
            return ffi::SQLITE_IOERR_TRUNCATE;
        };
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
            let parent = unsafe { zfile(file).expect("validated file").parent };
            unsafe {
                parent_methods_for(parent)
                    .and_then(|methods| methods.xTruncate)
                    .map_or(ffi::SQLITE_IOERR_TRUNCATE, |truncate| {
                        truncate(parent, size)
                    })
            }
        }
    })
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    ffi_guard(|| {
        let Some(file_state) = (unsafe { state(file) }) else {
            return ffi::SQLITE_IOERR_FSYNC;
        };
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
        let parent = unsafe { zfile(file).expect("validated file").parent };
        let rc = unsafe {
            parent_methods_for(parent)
                .and_then(|methods| methods.xSync)
                .map_or(ffi::SQLITE_OK, |sync| sync(parent, flags))
        };
        if rc == ffi::SQLITE_OK && file_state.store.is_some() {
            file_state
                .publication
                .record_parent_sync(flags & 0x0f == ffi::SQLITE_SYNC_FULL);
        }
        rc
    })
}

unsafe extern "C" fn x_file_size(
    file: *mut ffi::sqlite3_file,
    output: *mut ffi::sqlite3_int64,
) -> c_int {
    ffi_guard(|| {
        let Some(output) = (unsafe { output.as_mut() }) else {
            return ffi::SQLITE_IOERR_FSTAT;
        };
        let Some(file_state) = (unsafe { state(file) }) else {
            return ffi::SQLITE_IOERR_FSTAT;
        };
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
            let parent = unsafe { zfile(file).expect("validated file").parent };
            unsafe {
                parent_methods_for(parent)
                    .and_then(|methods| methods.xFileSize)
                    .map_or(ffi::SQLITE_IOERR_FSTAT, |file_size| {
                        file_size(parent, output)
                    })
            }
        }
    })
}

unsafe fn parent_methods_for(
    parent: *mut ffi::sqlite3_file,
) -> Option<&'static ffi::sqlite3_io_methods> {
    let parent = unsafe { parent.as_ref()? };
    unsafe { parent.pMethods.as_ref() }
}

unsafe fn call_read(
    parent: *mut ffi::sqlite3_file,
    output: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        parent_methods_for(parent)
            .and_then(|methods| methods.xRead)
            .map_or(ffi::SQLITE_IOERR_READ, |read| {
                read(parent, output, amount, offset)
            })
    }
}

unsafe fn call_write(
    parent: *mut ffi::sqlite3_file,
    input: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        parent_methods_for(parent)
            .and_then(|methods| methods.xWrite)
            .map_or(ffi::SQLITE_IOERR_WRITE, |write| {
                write(parent, input, amount, offset)
            })
    }
}

unsafe fn call_lock(parent: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        parent_methods_for(parent)
            .and_then(|methods| methods.xLock)
            .map_or(ffi::SQLITE_IOERR_LOCK, |lock| lock(parent, level))
    }
}

unsafe fn call_unlock(parent: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        parent_methods_for(parent)
            .and_then(|methods| methods.xUnlock)
            .map_or(ffi::SQLITE_IOERR_UNLOCK, |unlock| unlock(parent, level))
    }
}

unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    ffi_guard(|| unsafe {
        let Some(zfile) = file.cast::<ZFile>().as_mut() else {
            return ffi::SQLITE_MISUSE;
        };
        let parent = zfile.parent;
        let file_state = zfile.state;
        if let Some(state) = file_state.as_mut()
            && state.checkpoint == CheckpointState::FallbackFinishing
        {
            state.checkpoint = CheckpointState::Idle;
        }
        let previous = file_state
            .as_ref()
            .map_or(ffi::SQLITE_LOCK_NONE, |state| state.lock_level);
        let rc = call_lock(parent, level);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        if let Some(state) = file_state.as_mut() {
            if previous == ffi::SQLITE_LOCK_NONE
                && level == ffi::SQLITE_LOCK_SHARED
                && let Some(store) = &state.store
                && let Err(error) = store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR_LOCK)
                    .and_then(|mut store| store.refresh().map_err(|error| sqlite_result(&error)))
            {
                let _ = call_unlock(parent, ffi::SQLITE_LOCK_NONE);
                return error;
            }
            state.lock_level = level;
        }
        ffi::SQLITE_OK
    })
}

unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    ffi_guard(|| unsafe {
        let Some(zfile) = file.cast::<ZFile>().as_mut() else {
            return ffi::SQLITE_MISUSE;
        };
        let parent = zfile.parent;
        let file_state = zfile.state;
        if let Some(state) = file_state.as_mut()
            && state.checkpoint == CheckpointState::FallbackFinishing
        {
            state.checkpoint = CheckpointState::Idle;
        }
        let store_rc = if level < ffi::SQLITE_LOCK_RESERVED {
            file_state.as_mut().map_or(ffi::SQLITE_OK, |state| {
                discard_owned_pending(state, ffi::SQLITE_IOERR_UNLOCK)
            })
        } else {
            ffi::SQLITE_OK
        };
        // Parent locks must be released even when Store cleanup fails. SQLite
        // may discard an xUnlock() error, and retaining the parent lock would
        // otherwise wedge unrelated connections indefinitely.
        let rc = call_unlock(parent, level);
        if rc == ffi::SQLITE_OK
            && let Some(state) = file_state.as_mut()
        {
            state.lock_level = level;
        }
        if level < ffi::SQLITE_LOCK_RESERVED
            && store_rc == ffi::SQLITE_OK
            && let Some(state) = file_state.as_mut()
        {
            state.publication.reset();
        }
        if store_rc == ffi::SQLITE_OK {
            rc
        } else {
            store_rc
        }
    })
}

unsafe extern "C" fn x_check_reserved_lock(
    file: *mut ffi::sqlite3_file,
    output: *mut c_int,
) -> c_int {
    ffi_guard(|| unsafe {
        let parent = zfile(file).map_or(null_mut(), |file| file.parent);
        parent_methods_for(parent)
            .and_then(|methods| methods.xCheckReservedLock)
            .map_or(ffi::SQLITE_IOERR_CHECKRESERVEDLOCK, |check| {
                check(parent, output)
            })
    })
}

#[allow(clippy::too_many_lines)]
unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    operation: c_int,
    argument: *mut c_void,
) -> c_int {
    ffi_guard(|| unsafe {
        let is_main = state(file).is_some_and(|state| state.store.is_some());
        if is_main && operation == ffi::SQLITE_FCNTL_CKPT_START {
            let checkpoint_store = state(file).and_then(|file_state| {
                if file_state.checkpoint == CheckpointState::FallbackFinishing {
                    file_state.checkpoint = CheckpointState::Idle;
                }
                if file_state.checkpoint != CheckpointState::Idle {
                    return None;
                }
                // SQLite ignores CKPT_START's return value. Record checkpoint
                // intent before trying Store publication so a later xWrite is
                // still classified as checkpoint I/O if initial contention
                // disappears between these callbacks.
                file_state.checkpoint = CheckpointState::FallbackWriting;
                file_state.store.as_ref().map(Arc::clone)
            });
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
                    return error;
                }
            }
        }
        if is_main
            && !matches!(
                operation,
                ffi::SQLITE_FCNTL_CKPT_START | ffi::SQLITE_FCNTL_CKPT_DONE
            )
            && let Some(file_state) = state(file)
            && file_state.checkpoint == CheckpointState::FallbackFinishing
        {
            file_state.checkpoint = CheckpointState::Idle;
        }
        if is_main && operation == ffi::SQLITE_FCNTL_SYNC {
            let pending_store = state(file).and_then(|state| {
                state.publication.require_sync();
                state
                    .publication
                    .owns_pending()
                    .then(|| state.store.as_ref().map(Arc::clone))
                    .flatten()
            });
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
                return error;
            }
            if had_pending && let Some(file_state) = state(file) {
                // SQLITE_FCNTL_SYNC is observed even when Pager.noSync skips
                // xSync(), including during hot-journal recovery. Publish a
                // visible nondurable commit before SQLite finalizes the
                // journal; a following xSync() makes that prefix durable.
                file_state.publication.clear_owned();
            }
        }
        if is_main
            && operation == ffi::SQLITE_FCNTL_CKPT_DONE
            && let Some(file_state) = state(file)
        {
            let fallback = file_state.checkpoint == CheckpointState::FallbackWriting;
            if file_state.checkpoint == CheckpointState::ShmWriting {
                file_state.checkpoint = CheckpointState::ShmFinishing;
            }
            // Every successful checkpoint xWrite has already published. If
            // anything remains pending, its xWrite already returned an error.
            // CKPT_DONE's result is ignored, so only best-effort rollback is
            // safe at this boundary.
            let _ = discard_owned_pending(file_state, ffi::SQLITE_IOERR);
            if fallback {
                // locking_mode=EXCLUSIVE makes SQLite's WAL SHM lock/unlock
                // routines no-ops. There will be no later unlock callback,
                // and partial checkpoints have no truncate or sync callback
                // either, so release the fallback lease here. Any following
                // xTruncate publishes independently before returning.
                if let Some(store) = file_state.store.as_ref().map(Arc::clone)
                    && let Ok(mut store) = store.lock()
                {
                    store.finish_checkpoint_publication();
                }
                file_state.checkpoint = CheckpointState::FallbackFinishing;
            }
        }
        if is_main && operation == ffi::SQLITE_FCNTL_COMMIT_PHASETWO {
            let pending_store = state(file).and_then(|state| {
                state
                    .publication
                    .owns_pending()
                    .then(|| {
                        state.store.as_ref().map(|store| {
                            (
                                Arc::clone(store),
                                state.publication.main_sync != SyncStrength::None,
                                state.publication.main_sync == SyncStrength::Full,
                            )
                        })
                    })
                    .flatten()
            });
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
                return error;
            }
            if let Some(file_state) = state(file) {
                file_state.publication.reset();
            }
        }
        if is_main && operation == ffi::SQLITE_FCNTL_SIZE_HINT {
            return ffi::SQLITE_OK;
        }
        if is_main && operation == ffi::SQLITE_FCNTL_CHUNK_SIZE {
            return ffi::SQLITE_OK;
        }
        if is_main && operation == ffi::SQLITE_FCNTL_MMAP_SIZE {
            if let Some(size) = argument.cast::<i64>().as_mut() {
                *size = 0;
            }
            return ffi::SQLITE_OK;
        }
        if is_main && operation == ffi::SQLITE_FCNTL_HAS_MOVED {
            let Some(moved) = argument.cast::<c_int>().as_mut() else {
                return ffi::SQLITE_MISUSE;
            };
            let parent = zfile(file).map_or(null_mut(), |file| file.parent);
            let mut parent_moved = 0;
            let parent_result = parent_methods_for(parent)
                .and_then(|methods| methods.xFileControl)
                .map_or(ffi::SQLITE_NOTFOUND, |control| {
                    control(parent, operation, (&raw mut parent_moved).cast::<c_void>())
                });
            if !matches!(parent_result, ffi::SQLITE_OK | ffi::SQLITE_NOTFOUND) {
                return parent_result;
            }
            let database_moved = state(file)
                .and_then(|state| state.store.as_ref().map(Arc::clone))
                .ok_or(ffi::SQLITE_IOERR)
                .and_then(|store| {
                    store
                        .lock()
                        .map_err(|_| ffi::SQLITE_IOERR)
                        .and_then(|store| {
                            store
                                .database_has_moved()
                                .map_err(|error| sqlite_result(&error))
                        })
                });
            match database_moved {
                Ok(database_moved) => {
                    *moved = c_int::from(parent_moved != 0 || database_moved);
                    return ffi::SQLITE_OK;
                }
                Err(error) => return error,
            }
        }
        if is_main && operation == ffi::SQLITE_FCNTL_FILE_POINTER {
            if let Some(output) = argument.cast::<*mut ffi::sqlite3_file>().as_mut() {
                *output = file;
                return ffi::SQLITE_OK;
            }
            return ffi::SQLITE_MISUSE;
        }
        if is_main && operation == ffi::SQLITE_FCNTL_VFS_POINTER {
            if let Some(output) = argument.cast::<*mut ffi::sqlite3_vfs>().as_mut()
                && let Some(file_state) = state(file)
            {
                *output = file_state.vfs;
                return ffi::SQLITE_OK;
            }
            return ffi::SQLITE_MISUSE;
        }
        let parent = zfile(file).map_or(null_mut(), |file| file.parent);
        parent_methods_for(parent)
            .and_then(|methods| methods.xFileControl)
            .map_or(ffi::SQLITE_NOTFOUND, |control| {
                control(parent, operation, argument)
            })
    })
}

unsafe extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    ffi_guard(|| unsafe {
        let parent = zfile(file).map_or(null_mut(), |file| file.parent);
        parent_methods_for(parent)
            .and_then(|methods| methods.xSectorSize)
            .map_or(4096, |sector_size| sector_size(parent))
    })
}

unsafe extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    ffi_guard(|| unsafe {
        if state(file).is_some_and(|state| state.store.is_some()) {
            0
        } else {
            let parent = zfile(file).map_or(null_mut(), |file| file.parent);
            parent_methods_for(parent)
                .and_then(|methods| methods.xDeviceCharacteristics)
                .map_or(0, |characteristics| characteristics(parent))
        }
    })
}

unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    page: c_int,
    page_size: c_int,
    extend: c_int,
    output: *mut *mut c_void,
) -> c_int {
    ffi_guard(|| unsafe {
        let parent = zfile(file).map_or(null_mut(), |file| file.parent);
        parent_methods_for(parent)
            .filter(|methods| methods.iVersion >= 2)
            .and_then(|methods| methods.xShmMap)
            .map_or(ffi::SQLITE_IOERR_SHMMAP, |map| {
                map(parent, page, page_size, extend, output)
            })
    })
}

unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    count: c_int,
    flags: c_int,
) -> c_int {
    ffi_guard(|| unsafe {
        let parent = zfile(file).map_or(null_mut(), |file| file.parent);
        let Some(lock) = parent_methods_for(parent)
            .filter(|methods| methods.iVersion >= 2)
            .and_then(|methods| methods.xShmLock)
        else {
            return ffi::SQLITE_IOERR_SHMLOCK;
        };
        let mut cleanup_rc = ffi::SQLITE_OK;
        if flags & ffi::SQLITE_SHM_UNLOCK != 0
            && flags & ffi::SQLITE_SHM_EXCLUSIVE != 0
            && offset == WAL_CHECKPOINT_LOCK
            && count == 1
            && let Some(file_state) = state(file)
            && file_state.checkpoint.uses_shm()
        {
            file_state.checkpoint = CheckpointState::Idle;
            cleanup_rc = discard_owned_pending(file_state, ffi::SQLITE_IOERR_SHMLOCK);
            if let Some(store) = file_state.store.as_ref().map(Arc::clone) {
                match store.lock() {
                    Ok(mut store) => store.finish_checkpoint_publication(),
                    Err(_) if cleanup_rc == ffi::SQLITE_OK => {
                        cleanup_rc = ffi::SQLITE_IOERR_SHMLOCK;
                    }
                    Err(_) => {}
                }
            }
            if cleanup_rc == ffi::SQLITE_OK {
                file_state.publication.reset();
            }
        }
        // walUnlockExclusive() ignores xShmLock()'s return value. Always pass
        // the unlock through to the parent so a Store cleanup error does
        // not leak a WAL lock that SQLite believes it has released.
        let rc = lock(parent, offset, count, flags);
        let checkpoint_lock = rc == ffi::SQLITE_OK
            && flags & ffi::SQLITE_SHM_LOCK != 0
            && flags & ffi::SQLITE_SHM_EXCLUSIVE != 0
            && offset == WAL_CHECKPOINT_LOCK
            && count == 1;
        if rc == ffi::SQLITE_OK && flags & ffi::SQLITE_SHM_LOCK != 0 {
            let refresh = state(file)
                .and_then(|state| state.store.as_ref().map(Arc::clone))
                .map_or(Ok(()), |store| {
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
                let _ = lock(parent, offset, count, unlock_flags);
                return error;
            }
            if checkpoint_lock
                && let Some(file_state) = state(file)
                && file_state.store.is_some()
            {
                // CKPT_START is advisory and its return value is ignored. The
                // exclusive checkpoint lock is the reliable acquisition point.
                file_state.checkpoint = CheckpointState::ShmWriting;
            }
        }
        if cleanup_rc == ffi::SQLITE_OK {
            rc
        } else {
            cleanup_rc
        }
    })
}

unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    let _ = std::panic::catch_unwind(|| unsafe {
        let parent = zfile(file).map_or(null_mut(), |file| file.parent);
        if let Some(barrier) = parent_methods_for(parent)
            .filter(|methods| methods.iVersion >= 2)
            .and_then(|methods| methods.xShmBarrier)
        {
            barrier(parent);
        }
    });
}

unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    ffi_guard(|| unsafe {
        let parent = zfile(file).map_or(null_mut(), |file| file.parent);
        parent_methods_for(parent)
            .filter(|methods| methods.iVersion >= 2)
            .and_then(|methods| methods.xShmUnmap)
            .map_or(ffi::SQLITE_OK, |unmap| unmap(parent, delete))
    })
}

unsafe extern "C" fn x_fetch(
    file: *mut ffi::sqlite3_file,
    offset: ffi::sqlite3_int64,
    amount: c_int,
    output: *mut *mut c_void,
) -> c_int {
    ffi_guard(|| unsafe {
        if state(file).is_some_and(|state| state.store.is_some()) {
            if let Some(output) = output.as_mut() {
                *output = null_mut();
            }
            ffi::SQLITE_OK
        } else {
            let parent = zfile(file).map_or(null_mut(), |file| file.parent);
            parent_methods_for(parent)
                .filter(|methods| methods.iVersion >= 3)
                .and_then(|methods| methods.xFetch)
                .map_or(ffi::SQLITE_OK, |fetch| {
                    fetch(parent, offset, amount, output)
                })
        }
    })
}

unsafe extern "C" fn x_unfetch(
    file: *mut ffi::sqlite3_file,
    offset: ffi::sqlite3_int64,
    pointer: *mut c_void,
) -> c_int {
    ffi_guard(|| unsafe {
        if state(file).is_some_and(|state| state.store.is_some()) {
            ffi::SQLITE_OK
        } else {
            let parent = zfile(file).map_or(null_mut(), |file| file.parent);
            parent_methods_for(parent)
                .filter(|methods| methods.iVersion >= 3)
                .and_then(|methods| methods.xUnfetch)
                .map_or(ffi::SQLITE_OK, |unfetch| unfetch(parent, offset, pointer))
        }
    })
}

unsafe extern "C" fn x_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    ffi_guard(|| unsafe {
        let Some(app) = app_data(vfs) else {
            return ffi::SQLITE_INTERNAL;
        };
        let Ok(mapped) = mapped_storage_name(name) else {
            return ffi::SQLITE_CANTOPEN;
        };
        let parent_name = mapped.as_ref().map_or(name, |path| path.as_ptr());
        if !name.is_null() {
            let path = crate::facade::vfs_storage_path(&path_from_name(name));
            if path.extension().and_then(|value| value.to_str()) == Some("zsqlite") {
                if let Err(error) = crate::facade::validate_notice(&path) {
                    return sqlite_result(&error);
                }
                // Keep the same-path registry gate through both managed
                // bundle deletion and the parent-VFS fallback. Releasing it
                // between the two would let a racing xOpen create a new
                // database file which this xDelete then removes.
                match delete_registered_store_with(&path, |key| match Store::delete_bundle(key) {
                    Ok(()) => {
                        crate::facade::delete_notice(key)?;
                        Ok((ffi::SQLITE_OK, true))
                    }
                    Err(crate::StoreError::NotZsqlite) => {
                        let rc = app
                            .parent
                            .as_ref()
                            .and_then(|parent| parent.xDelete)
                            .map_or(ffi::SQLITE_IOERR_DELETE, |delete| {
                                delete(app.parent, parent_name, sync_dir)
                            });
                        Ok((rc, rc == ffi::SQLITE_OK))
                    }
                    Err(error) => Err(error),
                }) {
                    Ok(rc) => return rc,
                    Err(error) => return sqlite_result(&error),
                }
            }
        }
        app.parent
            .as_ref()
            .and_then(|parent| parent.xDelete)
            .map_or(ffi::SQLITE_IOERR_DELETE, |delete| {
                delete(app.parent, parent_name, sync_dir)
            })
    })
}

unsafe extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    output: *mut c_int,
) -> c_int {
    ffi_guard(|| unsafe {
        let Some(app) = app_data(vfs) else {
            return ffi::SQLITE_INTERNAL;
        };
        let Ok(mapped) = mapped_storage_name(name) else {
            return ffi::SQLITE_CANTOPEN;
        };
        let name = mapped.as_ref().map_or(name, |path| path.as_ptr());
        app.parent
            .as_ref()
            .and_then(|parent| parent.xAccess)
            .map_or(ffi::SQLITE_IOERR_ACCESS, |access| {
                access(app.parent, name, flags, output)
            })
    })
}

unsafe extern "C" fn x_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    output_size: c_int,
    output: *mut c_char,
) -> c_int {
    ffi_guard(|| unsafe {
        let Some(app) = app_data(vfs) else {
            return ffi::SQLITE_INTERNAL;
        };
        let Ok(mapped) = mapped_storage_name(name) else {
            return ffi::SQLITE_CANTOPEN;
        };
        let name = mapped.as_ref().map_or(name, |path| path.as_ptr());
        app.parent
            .as_ref()
            .and_then(|parent| parent.xFullPathname)
            .map_or(ffi::SQLITE_IOERR, |full_pathname| {
                full_pathname(app.parent, name, output_size, output)
            })
    })
}

unsafe extern "C" fn x_dl_open(vfs: *mut ffi::sqlite3_vfs, filename: *const c_char) -> *mut c_void {
    ffi_guard_or(null_mut(), || unsafe {
        let Some((parent, methods)) = parent_vfs_for(vfs) else {
            return null_mut();
        };
        methods
            .xDlOpen
            .map_or(null_mut(), |open| open(parent, filename))
    })
}

unsafe extern "C" fn x_dl_error(
    vfs: *mut ffi::sqlite3_vfs,
    output_size: c_int,
    output: *mut c_char,
) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if let Some((parent, methods)) = parent_vfs_for(vfs)
            && let Some(error) = methods.xDlError
        {
            error(parent, output_size, output);
        }
    }));
}

unsafe extern "C" fn x_dl_sym(
    vfs: *mut ffi::sqlite3_vfs,
    handle: *mut c_void,
    symbol: *const c_char,
) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
    ffi_guard_or(None, || unsafe {
        let (parent, methods) = parent_vfs_for(vfs)?;
        methods
            .xDlSym
            .and_then(|lookup| lookup(parent, handle, symbol))
    })
}

unsafe extern "C" fn x_dl_close(vfs: *mut ffi::sqlite3_vfs, handle: *mut c_void) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if let Some((parent, methods)) = parent_vfs_for(vfs)
            && let Some(close) = methods.xDlClose
        {
            close(parent, handle);
        }
    }));
}

unsafe extern "C" fn x_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    amount: c_int,
    output: *mut c_char,
) -> c_int {
    ffi_guard_or(0, || unsafe {
        let Some((parent, methods)) = parent_vfs_for(vfs) else {
            return 0;
        };
        methods
            .xRandomness
            .map_or(0, |randomness| randomness(parent, amount, output))
    })
}

unsafe extern "C" fn x_sleep(vfs: *mut ffi::sqlite3_vfs, microseconds: c_int) -> c_int {
    ffi_guard_or(0, || unsafe {
        let Some((parent, methods)) = parent_vfs_for(vfs) else {
            return 0;
        };
        methods
            .xSleep
            .map_or(0, |sleep| sleep(parent, microseconds))
    })
}

unsafe extern "C" fn x_current_time(vfs: *mut ffi::sqlite3_vfs, output: *mut f64) -> c_int {
    ffi_guard(|| unsafe {
        let Some((parent, methods)) = parent_vfs_for(vfs) else {
            return ffi::SQLITE_IOERR;
        };
        methods
            .xCurrentTime
            .map_or(ffi::SQLITE_IOERR, |current_time| {
                current_time(parent, output)
            })
    })
}

unsafe extern "C" fn x_get_last_error(
    vfs: *mut ffi::sqlite3_vfs,
    output_size: c_int,
    output: *mut c_char,
) -> c_int {
    ffi_guard_or(0, || unsafe {
        let Some((parent, methods)) = parent_vfs_for(vfs) else {
            return 0;
        };
        methods
            .xGetLastError
            .map_or(0, |last_error| last_error(parent, output_size, output))
    })
}

unsafe extern "C" fn x_current_time_int64(
    vfs: *mut ffi::sqlite3_vfs,
    output: *mut ffi::sqlite3_int64,
) -> c_int {
    ffi_guard(|| unsafe {
        let Some((parent, methods)) = parent_vfs_for(vfs) else {
            return ffi::SQLITE_IOERR;
        };
        methods
            .xCurrentTimeInt64
            .map_or(ffi::SQLITE_IOERR, |current_time| {
                current_time(parent, output)
            })
    })
}

unsafe extern "C" fn x_set_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    call: ffi::sqlite3_syscall_ptr,
) -> c_int {
    ffi_guard(|| unsafe {
        let Some((parent, methods)) = parent_vfs_for(vfs) else {
            return ffi::SQLITE_NOTFOUND;
        };
        methods
            .xSetSystemCall
            .map_or(ffi::SQLITE_NOTFOUND, |set| set(parent, name, call))
    })
}

unsafe extern "C" fn x_get_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> ffi::sqlite3_syscall_ptr {
    ffi_guard_or(None, || unsafe {
        let (parent, methods) = parent_vfs_for(vfs)?;
        methods.xGetSystemCall.and_then(|get| get(parent, name))
    })
}

unsafe extern "C" fn x_next_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> *const c_char {
    ffi_guard_or(null(), || unsafe {
        let Some((parent, methods)) = parent_vfs_for(vfs) else {
            return null();
        };
        methods
            .xNextSystemCall
            .map_or(null(), |next| next(parent, name))
    })
}

fn path_from_name(name: *const c_char) -> PathBuf {
    let bytes = unsafe { CStr::from_ptr(name) }.to_bytes();
    PathBuf::from(OsStr::from_bytes(bytes))
}

fn mapped_storage_name(name: *const c_char) -> Result<Option<CString>, ()> {
    if name.is_null() {
        return Ok(None);
    }
    let input = path_from_name(name);
    let storage = crate::facade::vfs_storage_path(&input);
    if storage == input {
        Ok(None)
    } else {
        path_to_c_string(&storage).map(Some)
    }
}

fn io_methods(parent: &ffi::sqlite3_io_methods) -> Option<ffi::sqlite3_io_methods> {
    if parent.iVersion < 1
        || parent.xClose.is_none()
        || parent.xRead.is_none()
        || parent.xWrite.is_none()
        || parent.xTruncate.is_none()
        || parent.xSync.is_none()
        || parent.xFileSize.is_none()
        || parent.xLock.is_none()
        || parent.xUnlock.is_none()
        || parent.xCheckReservedLock.is_none()
        || parent.xFileControl.is_none()
        || parent.xDeviceCharacteristics.is_none()
    {
        return None;
    }
    let supports_shm = parent.iVersion >= 2
        && parent.xShmMap.is_some()
        && parent.xShmLock.is_some()
        && parent.xShmBarrier.is_some()
        && parent.xShmUnmap.is_some();
    let supports_fetch = supports_shm
        && parent.iVersion >= 3
        && parent.xFetch.is_some()
        && parent.xUnfetch.is_some();
    let version = if supports_fetch {
        3
    } else if supports_shm {
        2
    } else {
        1
    };
    Some(ffi::sqlite3_io_methods {
        iVersion: version,
        xClose: Some(x_close),
        xRead: Some(x_read),
        xWrite: Some(x_write),
        xTruncate: Some(x_truncate),
        xSync: Some(x_sync),
        xFileSize: Some(x_file_size),
        xLock: Some(x_lock),
        xUnlock: Some(x_unlock),
        xCheckReservedLock: Some(x_check_reserved_lock),
        xFileControl: Some(x_file_control),
        xSectorSize: Some(x_sector_size),
        xDeviceCharacteristics: Some(x_device_characteristics),
        xShmMap: supports_shm.then_some(x_shm_map),
        xShmLock: supports_shm.then_some(x_shm_lock),
        xShmBarrier: supports_shm.then_some(x_shm_barrier),
        xShmUnmap: supports_shm.then_some(x_shm_unmap),
        xFetch: supports_fetch.then_some(x_fetch),
        xUnfetch: supports_fetch.then_some(x_unfetch),
    })
}

fn parent_vfs_is_usable(parent: &ffi::sqlite3_vfs) -> bool {
    parent.iVersion >= 1
        && usize::try_from(parent.szOsFile).is_ok_and(|size| size >= size_of::<ffi::sqlite3_file>())
        && parent.xOpen.is_some()
        && parent.xDelete.is_some()
        && parent.xAccess.is_some()
        && parent.xFullPathname.is_some()
        && parent.xRandomness.is_some()
        && parent.xSleep.is_some()
        && parent.xCurrentTime.is_some()
}

fn install_parent_vfs_wrappers(shim: &mut ffi::sqlite3_vfs, parent: &ffi::sqlite3_vfs) {
    // The Rust binding describes VFS versions through 3. Never claim a future
    // tail that this allocation does not contain, and leave every optional
    // callback null when the parent does not implement it.
    shim.iVersion = parent.iVersion.min(3);
    shim.xOpen = Some(x_open);
    shim.xDelete = Some(x_delete);
    shim.xAccess = Some(x_access);
    shim.xFullPathname = Some(x_full_pathname);
    shim.xDlOpen = if parent.xDlOpen.is_some() {
        Some(x_dl_open)
    } else {
        None
    };
    shim.xDlError = if parent.xDlError.is_some() {
        Some(x_dl_error)
    } else {
        None
    };
    shim.xDlSym = if parent.xDlSym.is_some() {
        Some(x_dl_sym)
    } else {
        None
    };
    shim.xDlClose = if parent.xDlClose.is_some() {
        Some(x_dl_close)
    } else {
        None
    };
    shim.xRandomness = Some(x_randomness);
    shim.xSleep = Some(x_sleep);
    shim.xCurrentTime = Some(x_current_time);
    shim.xGetLastError = if parent.xGetLastError.is_some() {
        Some(x_get_last_error)
    } else {
        None
    };
    shim.xCurrentTimeInt64 = if shim.iVersion >= 2 && parent.xCurrentTimeInt64.is_some() {
        Some(x_current_time_int64)
    } else {
        None
    };
    shim.xSetSystemCall = if shim.iVersion >= 3 && parent.xSetSystemCall.is_some() {
        Some(x_set_system_call)
    } else {
        None
    };
    shim.xGetSystemCall = if shim.iVersion >= 3 && parent.xGetSystemCall.is_some() {
        Some(x_get_system_call)
    } else {
        None
    };
    shim.xNextSystemCall = if shim.iVersion >= 3 && parent.xNextSystemCall.is_some() {
        Some(x_next_system_call)
    } else {
        None
    };
}

unsafe fn register_vfs() -> c_int {
    let Ok(_registration) = REGISTRATION_LOCK.lock() else {
        return ffi::SQLITE_ERROR;
    };
    let name = VFS_NAME.as_ptr().cast::<c_char>();
    let existing = unsafe { ffi::sqlite3_vfs_find(name) };
    if let Some(existing) = unsafe { existing.as_ref() } {
        return if existing
            .xOpen
            .map(|function| function as *const () as usize)
            == Some(x_open as *const () as usize)
        {
            ffi::SQLITE_OK
        } else {
            ffi::SQLITE_MISUSE
        };
    }
    let parent = unsafe { ffi::sqlite3_vfs_find(null()) };
    let Some(parent_ref) = (unsafe { parent.as_ref() }) else {
        return ffi::SQLITE_ERROR;
    };
    if !parent_vfs_is_usable(parent_ref) {
        return ffi::SQLITE_ERROR;
    }
    let mut shim = *parent_ref;
    shim.szOsFile = match c_int::try_from(size_of::<ZFile>()) {
        Ok(size) => size,
        Err(_) => return ffi::SQLITE_ERROR,
    };
    shim.mxPathname = parent_ref.mxPathname.saturating_add(8);
    let app = Box::new(AppData { parent });
    let app_ptr = Box::into_raw(app);
    shim.pNext = null_mut();
    shim.zName = name;
    shim.pAppData = app_ptr.cast();
    install_parent_vfs_wrappers(&mut shim, parent_ref);
    let shim_ptr = Box::into_raw(Box::new(shim));
    let rc = unsafe { ffi::sqlite3_vfs_register(shim_ptr, 0) };
    if rc != ffi::SQLITE_OK {
        unsafe {
            drop(Box::from_raw(shim_ptr));
            drop(Box::from_raw(app_ptr));
        }
    }
    rc
}

/// Registers `zsqlite` as a named, permanently loaded process-wide VFS.
///
/// The connection used to load this extension is already open and therefore
/// continues using its previous VFS. Later connections must explicitly select
/// `zsqlite`; registration never changes `SQLite`'s default VFS.
#[cfg(feature = "loadable")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_zsqlite_init(
    _database: *mut ffi::sqlite3,
    _error: *mut *mut c_char,
    api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    ffi_guard(|| {
        if api.is_null() {
            return ffi::SQLITE_ERROR;
        }
        if unsafe { ffi::rusqlite_extension_init2(api) }.is_err() {
            return ffi::SQLITE_ERROR;
        }
        let rc = unsafe { register_vfs() };
        if rc == ffi::SQLITE_OK {
            ffi::SQLITE_OK_LOAD_PERMANENTLY
        } else {
            rc
        }
    })
}

/// Generic entry point for hosts that do not derive extension names.
#[cfg(feature = "loadable")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_extension_init(
    database: *mut ffi::sqlite3,
    error: *mut *mut c_char,
    api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    unsafe { sqlite3_zsqlite_init(database, error, api) }
}

/// Registers zsqlite against a statically linked `SQLite` library.
#[cfg(feature = "static")]
pub fn register_static_vfs() -> Result<(), c_int> {
    let rc = unsafe { register_vfs() };
    if rc == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(rc)
    }
}

#[cfg(all(test, feature = "static", unix))]
#[test]
fn managed_extension_with_non_zsqlite_header_fails_closed() -> Result<(), Box<dyn std::error::Error>>
{
    use std::ffi::CString;

    register_static_vfs().map_err(|code| format!("VFS registration failed: {code}"))?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("corrupt.zsqlite");
    std::fs::write(&path, b"this is not a zsqlite database")?;
    let filename = CString::new(path.as_os_str().as_bytes())?;
    let vfs = CString::new("zsqlite")?;
    let mut database = null_mut();
    // SAFETY: Both C strings outlive the call and `database` is a valid output
    // pointer. SQLite owns any non-null handle until sqlite3_close().
    let rc = unsafe {
        ffi::sqlite3_open_v2(
            filename.as_ptr(),
            &raw mut database,
            ffi::SQLITE_OPEN_READWRITE,
            vfs.as_ptr(),
        )
    };
    if !database.is_null() {
        // SAFETY: sqlite3_open_v2 initialized this handle, even on failure.
        let _ = unsafe { ffi::sqlite3_close(database) };
    }
    assert_eq!(rc, ffi::SQLITE_NOTADB);
    Ok(())
}

#[cfg(all(test, feature = "static"))]
#[path = "vfs_tests.rs"]
mod tests;
