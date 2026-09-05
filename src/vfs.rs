//! Thin `SQLite` ABI shim. Main-database I/O is redirected to [`Store`]; every
//! other operation is forwarded to the host VFS selected during registration.

use crate::store::Store;
use libsqlite3_sys as ffi;
use std::collections::HashMap;
use std::ffi::{CStr, OsStr, c_char, c_int, c_void};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::ptr::{self, null, null_mut};
use std::slice;
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

const VFS_NAME: &[u8] = b"zsqlite\0";
const WAL_CHECKPOINT_LOCK: c_int = 1;

#[repr(C)]
struct ZFile {
    base: ffi::sqlite3_file,
    parent: *mut ffi::sqlite3_file,
    state: *mut FileState,
}

struct FileState {
    store: Option<Arc<Mutex<Store>>>,
    read_only: bool,
    lock_level: c_int,
    vfs: *mut ffi::sqlite3_vfs,
    sync_pending: bool,
    owns_store_pending: bool,
}

struct AppData {
    parent: *mut ffi::sqlite3_vfs,
    io: ffi::sqlite3_io_methods,
}

// Access is serialized by SQLite's global VFS registration mutex after setup.
unsafe impl Send for AppData {}
unsafe impl Sync for AppData {}

type Registry = HashMap<PathBuf, Weak<Mutex<Store>>>;
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
        crate::StoreError::Io(error) if error.raw_os_error() == Some(libc::ENOSPC) => {
            ffi::SQLITE_FULL
        }
        _ => ffi::SQLITE_IOERR,
    }
}

fn canonical_key(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(path)
        }
    })
}

fn get_store(
    path: &Path,
    create: bool,
    writable: bool,
) -> Result<(Arc<Mutex<Store>>, bool), crate::StoreError> {
    let key = canonical_key(path);
    let mut stores = registry().lock().map_err(|_| crate::StoreError::Range)?;
    if let Some(store) = stores.get(&key).and_then(Weak::upgrade) {
        if writable {
            store
                .lock()
                .map_err(|_| crate::StoreError::Range)?
                .upgrade_writable()?;
        }
        return Ok((store, false));
    }
    let opened = if writable {
        Store::open(&key, create)?
    } else {
        Store::open_existing_read_only(&key)?
    };
    let store = Arc::new(Mutex::new(opened));
    stores.insert(key, Arc::downgrade(&store));
    Ok((store, true))
}

unsafe fn app_data(vfs: *mut ffi::sqlite3_vfs) -> Option<&'static AppData> {
    let raw = unsafe { vfs.as_ref()?.pAppData.cast::<AppData>() };
    unsafe { raw.as_ref() }
}

unsafe fn zfile(file: *mut ffi::sqlite3_file) -> Option<&'static mut ZFile> {
    unsafe { file.cast::<ZFile>().as_mut() }
}

unsafe fn state(file: *mut ffi::sqlite3_file) -> Option<&'static mut FileState> {
    let state = unsafe { zfile(file)?.state };
    unsafe { state.as_mut() }
}

fn ffi_guard(callback: impl FnOnce() -> c_int) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).unwrap_or(ffi::SQLITE_IOERR)
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
        let rc = unsafe { parent_open(app.parent, name, parent_file, flags, out_flags) };
        if rc != ffi::SQLITE_OK {
            unsafe { ffi::sqlite3_free(parent_file.cast()) };
            return rc;
        }

        let out_read_only = if out_flags.is_null() {
            flags & ffi::SQLITE_OPEN_READONLY != 0
        } else {
            (unsafe { *out_flags }) & ffi::SQLITE_OPEN_READONLY != 0
        };
        let mut store = None;
        if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
            if name.is_null() {
                unsafe { close_parent(parent_file) };
                return ffi::SQLITE_CANTOPEN;
            }
            let path = path_from_name(name);
            let create = flags & ffi::SQLITE_OPEN_CREATE != 0;
            match get_store(&path, create, !out_read_only) {
                Ok((opened, _newly_opened)) => store = Some(opened),
                Err(crate::StoreError::NotZsqlite) => {}
                Err(error) => {
                    unsafe { close_parent(parent_file) };
                    return sqlite_result(&error);
                }
            }
        }

        output.parent = parent_file;
        output.state = Box::into_raw(Box::new(FileState {
            store,
            read_only: out_read_only,
            lock_level: ffi::SQLITE_LOCK_NONE,
            vfs,
            sync_pending: false,
            owns_store_pending: false,
        }));
        output.base.pMethods = &raw const app.io;
        ffi::SQLITE_OK
    })
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
            if file_state.owns_store_pending
                && let Some(store) = &file_state.store
                && let Err(error) =
                    store
                        .lock()
                        .map_err(|_| ffi::SQLITE_IOERR)
                        .and_then(|mut store| {
                            store
                                .discard_pending()
                                .map_err(|error| sqlite_result(&error))
                        })
            {
                index_rc = error;
            }
            file_state.owns_store_pending = false;
            if !file_state.read_only
                && let Some(store) = &file_state.store
                && Arc::strong_count(store) == 1
                && index_rc == ffi::SQLITE_OK
            {
                index_rc = store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR)
                    .and_then(|mut store| {
                        store
                            .checkpoint_index()
                            .map_err(|error| sqlite_result(&error))
                    })
                    .map_or_else(|error| error, |()| ffi::SQLITE_OK);
            }
            unsafe { drop(Box::from_raw(file.state)) };
            file.state = null_mut();
        }
        let rc = unsafe { close_parent(file.parent) };
        file.parent = null_mut();
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
            let result = store
                .lock()
                .map_err(|_| ffi::SQLITE_IOERR)
                .and_then(|mut store| {
                    store
                        .write_at(offset.cast_unsigned(), data)
                        .map_err(|error| sqlite_result(&error))
                });
            match result {
                Ok(written) if written == data.len() => {
                    file_state.owns_store_pending = true;
                    ffi::SQLITE_OK
                }
                Ok(_) => ffi::SQLITE_IOERR_WRITE,
                Err(error) => error,
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
            let result = store
                .lock()
                .map_err(|_| ffi::SQLITE_IOERR)
                .and_then(|mut store| {
                    store
                        .truncate(size.cast_unsigned())
                        .map_err(|error| sqlite_result(&error))
                });
            match result {
                Ok(()) => {
                    file_state.owns_store_pending = true;
                    ffi::SQLITE_OK
                }
                Err(error) => error,
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
            && (file_state.owns_store_pending || file_state.sync_pending)
        {
            if let Err(error) = store
                .lock()
                .map_err(|_| ffi::SQLITE_IOERR_FSYNC)
                .and_then(|mut store| store.publish(true).map_err(|error| sqlite_result(&error)))
            {
                return error;
            }
            file_state.sync_pending = false;
            file_state.owns_store_pending = false;
        }
        let parent = unsafe { zfile(file).expect("validated file").parent };
        unsafe {
            parent_methods_for(parent)
                .and_then(|methods| methods.xSync)
                .map_or(ffi::SQLITE_OK, |sync| sync(parent, flags))
        }
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
        let previous = file_state
            .as_ref()
            .map_or(ffi::SQLITE_LOCK_NONE, |state| state.lock_level);
        let rc = call_lock(parent, level);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        if let Some(state) = file_state.as_mut() {
            if let Some(store) = &state.store
                && let Err(error) = store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR_LOCK)
                    .and_then(|mut store| store.refresh().map_err(|error| sqlite_result(&error)))
            {
                let _ = call_unlock(parent, previous);
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
        if level == ffi::SQLITE_LOCK_NONE
            && let Some(state) = file_state.as_mut()
            && state.owns_store_pending
            && let Some(store) = &state.store
            && let Err(error) =
                store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR_UNLOCK)
                    .and_then(|mut store| {
                        store
                            .discard_pending()
                            .map_err(|error| sqlite_result(&error))
                    })
        {
            return error;
        }
        if level == ffi::SQLITE_LOCK_NONE
            && let Some(state) = file_state.as_mut()
        {
            state.sync_pending = false;
            state.owns_store_pending = false;
        }
        let rc = call_unlock(parent, level);
        if rc == ffi::SQLITE_OK
            && let Some(state) = file_state.as_mut()
        {
            state.lock_level = level;
        }
        rc
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

unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    operation: c_int,
    argument: *mut c_void,
) -> c_int {
    ffi_guard(|| unsafe {
        let is_main = state(file).is_some_and(|state| state.store.is_some());
        if is_main
            && operation == ffi::SQLITE_FCNTL_SYNC
            && let Some(file_state) = state(file)
        {
            file_state.sync_pending = true;
        }
        if is_main && operation == ffi::SQLITE_FCNTL_CKPT_DONE {
            let pending_store = state(file).and_then(|state| {
                state
                    .owns_store_pending
                    .then(|| state.store.as_ref().map(Arc::clone))
                    .flatten()
            });
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
            if let Some(file_state) = state(file) {
                file_state.owns_store_pending = false;
            }
        }
        if is_main && operation == ffi::SQLITE_FCNTL_COMMIT_PHASETWO {
            let pending_store = state(file).and_then(|state| {
                (state.sync_pending && state.owns_store_pending)
                    .then(|| state.store.as_ref().map(Arc::clone))
                    .flatten()
            });
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
            if let Some(file_state) = state(file) {
                file_state.sync_pending = false;
                file_state.owns_store_pending = false;
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
        if flags & ffi::SQLITE_SHM_UNLOCK != 0 && offset == WAL_CHECKPOINT_LOCK && count == 1 {
            let pending_store = state(file).and_then(|state| {
                state
                    .owns_store_pending
                    .then(|| state.store.as_ref().map(Arc::clone))
                    .flatten()
            });
            let result = pending_store.map_or(Ok(()), |store| {
                store
                    .lock()
                    .map_err(|_| ffi::SQLITE_IOERR_SHMLOCK)
                    .and_then(|mut store| {
                        store.publish(false).map_err(|error| sqlite_result(&error))
                    })
            });
            if let Err(error) = result {
                return error;
            }
            if let Some(file_state) = state(file) {
                file_state.owns_store_pending = false;
            }
        }
        let rc = lock(parent, offset, count, flags);
        if rc == ffi::SQLITE_OK
            && flags & ffi::SQLITE_SHM_LOCK != 0
            && let Some(store) = state(file).and_then(|state| state.store.as_ref())
            && let Err(error) = store
                .lock()
                .map_err(|_| ffi::SQLITE_IOERR_SHMLOCK)
                .and_then(|mut store| store.refresh().map_err(|error| sqlite_result(&error)))
        {
            let unlock_flags = (flags & !ffi::SQLITE_SHM_LOCK) | ffi::SQLITE_SHM_UNLOCK;
            let _ = lock(parent, offset, count, unlock_flags);
            return error;
        }
        rc
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
        if !name.is_null() {
            let path = path_from_name(name);
            match Store::delete_bundle(&path) {
                Ok(()) => {
                    if let Ok(mut stores) = registry().lock() {
                        stores.remove(&canonical_key(&path));
                    }
                    return ffi::SQLITE_OK;
                }
                Err(crate::StoreError::NotZsqlite) => {}
                Err(error) => return sqlite_result(&error),
            }
        }
        app.parent
            .as_ref()
            .and_then(|parent| parent.xDelete)
            .map_or(ffi::SQLITE_IOERR_DELETE, |delete| {
                delete(app.parent, name, sync_dir)
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
        app.parent
            .as_ref()
            .and_then(|parent| parent.xFullPathname)
            .map_or(ffi::SQLITE_IOERR, |full_pathname| {
                full_pathname(app.parent, name, output_size, output)
            })
    })
}

fn path_from_name(name: *const c_char) -> PathBuf {
    let bytes = unsafe { CStr::from_ptr(name) }.to_bytes();
    PathBuf::from(OsStr::from_bytes(bytes))
}

fn io_methods() -> ffi::sqlite3_io_methods {
    ffi::sqlite3_io_methods {
        iVersion: 3,
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
        xShmMap: Some(x_shm_map),
        xShmLock: Some(x_shm_lock),
        xShmBarrier: Some(x_shm_barrier),
        xShmUnmap: Some(x_shm_unmap),
        xFetch: Some(x_fetch),
        xUnfetch: Some(x_unfetch),
    }
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
    let mut shim = *parent_ref;
    shim.szOsFile = match c_int::try_from(size_of::<ZFile>()) {
        Ok(size) => size,
        Err(_) => return ffi::SQLITE_ERROR,
    };
    let app = Box::new(AppData {
        parent,
        io: io_methods(),
    });
    let app_ptr = Box::into_raw(app);
    shim.pNext = null_mut();
    shim.zName = name;
    shim.pAppData = app_ptr.cast();
    shim.xOpen = Some(x_open);
    shim.xDelete = Some(x_delete);
    shim.xAccess = Some(x_access);
    shim.xFullPathname = Some(x_full_pathname);
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

#[cfg(all(test, feature = "static"))]
#[path = "vfs_tests.rs"]
mod tests;
