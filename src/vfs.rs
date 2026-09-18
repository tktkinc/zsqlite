//! `SQLite` ABI boundary. Database behavior lives in `runtime`, which forbids
//! unsafe code; parent allocation and filename ownership live in `parent`.
//!
//! `SQLite` serializes callbacks on each file (or requires its caller to do so).
//! Different files may be called concurrently: shared `Store` data uses `Mutex`,
//! while registration data is immutable. The registration mutex only protects
//! registration, and the host must keep the selected parent VFS registered and
//! alive for the lifetime of this permanently registered shim.
mod parent;
mod runtime;

use libsqlite3_sys as ffi;
use parent::ParentFile;
use runtime::FileState;
#[cfg(all(test, feature = "static"))]
use runtime::{delete_registered_store_with, get_store_with};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::mem::{MaybeUninit, size_of};
#[cfg(all(test, feature = "static", unix))]
use std::os::unix::ffi::OsStrExt;
use std::ptr::{self, null, null_mut};
use std::slice;
use std::sync::Mutex;

const VFS_NAME: &[u8] = b"zsqlite\0";
static REGISTRATION_LOCK: Mutex<()> = Mutex::new(());

#[repr(C)]
struct ZFile {
    base: ffi::sqlite3_file,
    state: *mut OpenFile,
    io: MaybeUninit<ffi::sqlite3_io_methods>,
}
struct OpenFile {
    state: FileState,
    parent: ParentFile,
    vfs: *mut ffi::sqlite3_vfs,
}
struct AppData {
    parent: *mut ffi::sqlite3_vfs,
    storage: Option<crate::Storage>,
}

/// # Safety
/// vfs belongs to this shim; its immutable, boxed `AppData` remains live for
/// registration's lifetime. Unrelated VFS registrations have different layouts.
unsafe fn app_data(vfs: &ffi::sqlite3_vfs) -> Option<&AppData> {
    // SAFETY: The caller establishes the allocation's type and lifetime;
    // pAppData is never replaced after our registration is published.
    unsafe { vfs.pAppData.cast::<AppData>().as_ref() }
}
/// # Safety
/// vfs is our registered shim and its parent remains registered and live.
unsafe fn parent_vfs_for(
    vfs: &ffi::sqlite3_vfs,
) -> Option<(*mut ffi::sqlite3_vfs, &ffi::sqlite3_vfs)> {
    // SAFETY: Our registration owns AppData and retains the parent's identity;
    // the caller guarantees both registrations' lifetimes during this borrow.
    unsafe {
        let parent = app_data(vfs)?.parent;
        Some((parent, parent.as_ref()?))
    }
}
/// # Safety
/// file is a live `ZFile` installed by our successful `xOpen`. `SQLite` guarantees
/// exclusive, non-reentrant access to this file until the callback returns.
/// callback must not reenter `SQLite` on this file. No reference can escape it.
unsafe fn with_file<T>(
    file: *mut ffi::sqlite3_file,
    callback: impl FnOnce(&mut OpenFile) -> T,
) -> Option<T> {
    // SAFETY: repr(C) puts sqlite3_file first; SQLite allocated szOsFile bytes.
    // xOpen installed a Box<OpenFile> and xClose alone consumes it. The caller
    // supplies exclusive access; the closure confines the borrow to this call.
    unsafe { Some(callback(file.cast::<ZFile>().as_mut()?.state.as_mut()?)) }
}
/// # Safety
/// name is null or a NUL-terminated `SQLite` filename valid for the returned borrow.
unsafe fn optional_name<'a>(name: *const c_char) -> Option<&'a CStr> {
    if name.is_null() {
        None
    } else {
        // SAFETY: The caller supplies the terminated string's validity/lifetime;
        // the null check only handles SQLite's unnamed-temporary-file case.
        Some(unsafe { CStr::from_ptr(name) })
    }
}
fn ffi_guard(callback: impl FnOnce() -> c_int) -> c_int {
    ffi_guard_or(ffi::SQLITE_IOERR, callback)
}

fn ffi_guard_or<T>(fallback: T, callback: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).unwrap_or(fallback)
}

/// # Safety
/// `SQLite` supplies its registered VFS, a framed optional filename, writable
/// `szOsFile` output allocation, and an optional writable flags output.
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
        // SAFETY: SQLite allocated aligned szOsFile == size_of::<ZFile>() storage.
        // It is not yet initialized; write installs a valid closed state so an
        // error leaves pMethods null and SQLite will not call xClose on it.
        unsafe {
            ptr::write(
                output.cast::<ZFile>(),
                ZFile {
                    base: ffi::sqlite3_file { pMethods: null() },
                    state: null_mut(),
                    io: MaybeUninit::uninit(),
                },
            );
        }
        // SAFETY: SQLite passed our live VFS and the filename remains valid
        // throughout xOpen (and until xClose for the parent to retain it).
        let (app, name_ref) = unsafe { (app_data(&*vfs), optional_name(name)) };
        let Some(app) = app else {
            return ffi::SQLITE_INTERNAL;
        };
        let (mut state, mapped) = match runtime::prepare_open(app.storage.as_ref(), name_ref, flags)
        {
            Ok(prepared) => prepared,
            Err(rc) => return rc,
        };
        let mut actual_flags = flags;
        // SAFETY: Registration retains the parent VFS; SQLite supplied name with
        // its xOpen lifetime/framing. ParentFile owns any remapped copy and all
        // cleanup; actual_flags is an exclusive initialized output for this call.
        let parent = match unsafe {
            ParentFile::open(
                app.parent,
                name,
                mapped.as_deref(),
                flags,
                (!out_flags.is_null()).then_some(&mut actual_flags),
            )
        } {
            Ok(parent) => parent,
            Err(rc) => return rc,
        };
        if !out_flags.is_null() {
            // SAFETY: SQLite supplies a writable c_int slot. write initializes
            // it without borrowing or reading potentially uninitialized output.
            unsafe { out_flags.write(actual_flags) };
        }
        let Some(io) = parent.methods().and_then(io_methods) else {
            return ffi::SQLITE_IOERR;
        };
        state.set_read_only(actual_flags & ffi::SQLITE_OPEN_READONLY != 0);
        let opened = Box::new(OpenFile { state, parent, vfs });
        // SAFETY: output was initialized above and remains exclusively owned by
        // xOpen. The boxed owner and in-place method table survive until xClose;
        // pMethods is published last, after every fallible initialization step.
        unsafe {
            let output = &mut *output.cast::<ZFile>();
            output.io.write(io);
            output.state = Box::into_raw(opened);
            output.base.pMethods = output.io.as_ptr();
        }
        ffi::SQLITE_OK
    })
}
/// # Safety
/// file is this shim's live, exclusively accessible file and is closed once.
unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    ffi_guard(|| {
        // SAFETY: SQLite supplies our initialized ZFile exclusively. Taking the
        // state and clearing pMethods before cleanup transfers its Box exactly
        // once; an unwind still drops the owned ParentFile and retained filename.
        let opened = unsafe {
            let Some(file) = file.cast::<ZFile>().as_mut() else {
                return ffi::SQLITE_MISUSE;
            };
            let state = std::mem::replace(&mut file.state, null_mut());
            file.base.pMethods = null();
            if state.is_null() {
                return ffi::SQLITE_MISUSE;
            }
            Box::from_raw(state)
        };
        let OpenFile {
            mut state, parent, ..
        } = *opened;
        let state_rc = state.close();
        let rc = parent.close();
        if state_rc == ffi::SQLITE_OK {
            rc
        } else {
            state_rc
        }
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live file and a writable buffer of amount bytes.
unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    output: *mut c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    ffi_guard(|| {
        if amount < 0 || offset < 0 || (output.is_null() && amount != 0) {
            return ffi::SQLITE_IOERR_READ;
        }
        if amount == 0 {
            return ffi::SQLITE_OK;
        }
        let Ok(amount) = usize::try_from(amount) else {
            return ffi::SQLITE_IOERR_READ;
        };
        // SAFETY: SQLite provides amount writable bytes exclusive to this call.
        // Initialize them before exposing a Rust byte slice; SQLite may supply
        // uninitialized read storage. No buffer pointer is retained by runtime.
        let data = unsafe {
            ptr::write_bytes(output.cast::<u8>(), 0, amount);
            slice::from_raw_parts_mut(output.cast::<u8>(), amount)
        };
        // SAFETY: SQLite guarantees exclusive access to our initialized file;
        // runtime borrows the file and buffer only for this callback.
        unsafe { with_file(file, |f| f.state.read(&mut f.parent, data, offset)) }
            .unwrap_or(ffi::SQLITE_IOERR_READ)
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live file and a readable initialized buffer of amount bytes.
unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    input: *const c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    ffi_guard(|| {
        if amount < 0 || offset < 0 || (input.is_null() && amount != 0) {
            return ffi::SQLITE_IOERR_WRITE;
        }
        if amount == 0 {
            return ffi::SQLITE_OK;
        }
        let Ok(amount) = usize::try_from(amount) else {
            return ffi::SQLITE_IOERR_WRITE;
        };
        // SAFETY: SQLite guarantees amount initialized readable bytes for
        // xWrite. This shared slice lasts only through the synchronous write.
        let data = unsafe { slice::from_raw_parts(input.cast::<u8>(), amount) };
        // SAFETY: SQLite guarantees exclusive access to our initialized file;
        // runtime borrows the file and buffer only for this callback.
        unsafe { with_file(file, |f| f.state.write(&mut f.parent, data, offset)) }
            .unwrap_or(ffi::SQLITE_IOERR_WRITE)
    })
}
/// # Safety
/// file is this shim's live file, exclusively borrowed by `SQLite` for this callback.
unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    ffi_guard(|| {
        // SAFETY: SQLite supplies the initialized file and prevents concurrent
        // or reentrant callbacks on it; the borrow ends with this dispatch.
        unsafe { with_file(file, |f| f.state.truncate(&mut f.parent, size)) }
            .unwrap_or(ffi::SQLITE_IOERR_TRUNCATE)
    })
}
/// # Safety
/// file is this shim's live file, exclusively borrowed by `SQLite` for this callback.
unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    ffi_guard(|| {
        // SAFETY: SQLite supplies the initialized file and prevents concurrent
        // or reentrant callbacks on it; the borrow ends with this dispatch.
        unsafe { with_file(file, |f| f.state.sync(&mut f.parent, flags)) }
            .unwrap_or(ffi::SQLITE_IOERR_FSYNC)
    })
}
/// # Safety
/// file is this shim's live file, exclusively borrowed by `SQLite` for this callback.
unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    ffi_guard(|| {
        // SAFETY: SQLite supplies the initialized file and prevents concurrent
        // or reentrant callbacks on it; the borrow ends with this dispatch.
        unsafe { with_file(file, |f| f.state.lock(&mut f.parent, level)) }
            .unwrap_or(ffi::SQLITE_IOERR_LOCK)
    })
}
/// # Safety
/// file is this shim's live file, exclusively borrowed by `SQLite` for this callback.
unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    ffi_guard(|| {
        // SAFETY: SQLite supplies the initialized file and prevents concurrent
        // or reentrant callbacks on it; the borrow ends with this dispatch.
        unsafe { with_file(file, |f| f.state.unlock(&mut f.parent, level)) }
            .unwrap_or(ffi::SQLITE_IOERR_UNLOCK)
    })
}
/// # Safety
/// file is this shim's live file, exclusively borrowed by `SQLite` for this callback.
unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    count: c_int,
    flags: c_int,
) -> c_int {
    ffi_guard(|| {
        // SAFETY: SQLite supplies the initialized file and prevents concurrent
        // or reentrant callbacks on it; the borrow ends with this dispatch.
        unsafe {
            with_file(file, |f| {
                f.state.shm_lock(&mut f.parent, offset, count, flags)
            })
        }
        .unwrap_or(ffi::SQLITE_IOERR_SHMLOCK)
    })
}
/// # Safety
/// file is an exclusive live shim file; output points to a writable i64 slot.
unsafe extern "C" fn x_file_size(file: *mut ffi::sqlite3_file, output: *mut i64) -> c_int {
    ffi_guard(|| {
        if output.is_null() {
            return ffi::SQLITE_IOERR_FSTAT;
        }
        let mut size = 0;
        // SAFETY: SQLite provides exclusive access to the initialized file;
        // runtime and parent write into our local initialized output.
        let rc = unsafe { with_file(file, |f| f.state.file_size(&mut f.parent, &mut size)) }
            .unwrap_or(ffi::SQLITE_IOERR_FSTAT);
        if rc == ffi::SQLITE_OK {
            // SAFETY: SQLite provides a writable, aligned i64 output slot.
            unsafe { output.write(size) };
        }
        rc
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live shim file and the arguments required by
/// `xCheckReservedLock`, including valid output slots or previously returned mapping pointers.
unsafe extern "C" fn x_check_reserved_lock(
    file: *mut ffi::sqlite3_file,
    output: *mut c_int,
) -> c_int {
    ffi_guard(|| {
        // SAFETY: The file borrow is confined to this callback. Pointer arguments
        // retain SQLite's xCheckReservedLock contract and are forwarded unchanged to the
        // owning parent, whose method table remains live until close.
        unsafe {
            with_file(file, |f| {
                let parent = f.parent.raw();
                f.parent
                    .methods()
                    .and_then(|m| m.xCheckReservedLock)
                    .map_or(ffi::SQLITE_IOERR_CHECKRESERVEDLOCK, |call| {
                        call(parent, output)
                    })
            })
        }
        .unwrap_or(ffi::SQLITE_IOERR_CHECKRESERVEDLOCK)
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live shim file and the arguments required by
/// `xSectorSize`, including valid output slots or previously returned mapping pointers.
unsafe extern "C" fn x_sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    ffi_guard(|| {
        // SAFETY: The file borrow is confined to this callback. Pointer arguments
        // retain SQLite's xSectorSize contract and are forwarded unchanged to the
        // owning parent, whose method table remains live until close.
        unsafe {
            with_file(file, |f| {
                let parent = f.parent.raw();
                f.parent
                    .methods()
                    .and_then(|m| m.xSectorSize)
                    .map_or(4096, |call| call(parent))
            })
        }
        .unwrap_or(4096)
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live shim file and the arguments required by
/// `xDeviceCharacteristics`, including valid output slots or previously returned mapping pointers.
unsafe extern "C" fn x_device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    ffi_guard(|| {
        // SAFETY: The file borrow is confined to this callback. Pointer arguments
        // retain SQLite's xDeviceCharacteristics contract and are forwarded unchanged to the
        // owning parent, whose method table remains live until close.
        unsafe {
            with_file(file, |f| {
                if f.state.is_managed() {
                    return 0;
                }
                let parent = f.parent.raw();
                f.parent
                    .methods()
                    .and_then(|m| m.xDeviceCharacteristics)
                    .map_or(0, |call| call(parent))
            })
        }
        .unwrap_or(0)
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live shim file and the arguments required by
/// `xShmMap`, including valid output slots or previously returned mapping pointers.
unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    page: c_int,
    page_size: c_int,
    extend: c_int,
    output: *mut *mut c_void,
) -> c_int {
    ffi_guard(|| {
        // SAFETY: The file borrow is confined to this callback. Pointer arguments
        // retain SQLite's xShmMap contract and are forwarded unchanged to the
        // owning parent, whose method table remains live until close.
        unsafe {
            with_file(file, |f| {
                let parent = f.parent.raw();
                f.parent
                    .methods()
                    .filter(|m| m.iVersion >= 2)
                    .and_then(|m| m.xShmMap)
                    .map_or(ffi::SQLITE_IOERR_SHMMAP, |call| {
                        call(parent, page, page_size, extend, output)
                    })
            })
        }
        .unwrap_or(ffi::SQLITE_IOERR_SHMMAP)
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live shim file and the arguments required by
/// `xShmUnmap`, including valid output slots or previously returned mapping pointers.
unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    ffi_guard(|| {
        // SAFETY: The file borrow is confined to this callback. Pointer arguments
        // retain SQLite's xShmUnmap contract and are forwarded unchanged to the
        // owning parent, whose method table remains live until close.
        unsafe {
            with_file(file, |f| {
                let parent = f.parent.raw();
                f.parent
                    .methods()
                    .filter(|m| m.iVersion >= 2)
                    .and_then(|m| m.xShmUnmap)
                    .map_or(ffi::SQLITE_OK, |call| call(parent, delete))
            })
        }
        .unwrap_or(ffi::SQLITE_OK)
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live shim file and the arguments required by
/// `xFetch`, including valid output slots or previously returned mapping pointers.
unsafe extern "C" fn x_fetch(
    file: *mut ffi::sqlite3_file,
    offset: i64,
    amount: c_int,
    output: *mut *mut c_void,
) -> c_int {
    ffi_guard(|| {
        // SAFETY: The file borrow is confined to this callback. Pointer arguments
        // retain SQLite's xFetch contract and are forwarded unchanged to the
        // owning parent, whose method table remains live until close.
        unsafe {
            with_file(file, |f| {
                if f.state.is_managed() {
                    if !output.is_null() {
                        output.write(null_mut());
                    }
                    return ffi::SQLITE_OK;
                }
                let parent = f.parent.raw();
                f.parent
                    .methods()
                    .filter(|m| m.iVersion >= 3)
                    .and_then(|m| m.xFetch)
                    .map_or(ffi::SQLITE_OK, |call| call(parent, offset, amount, output))
            })
        }
        .unwrap_or(ffi::SQLITE_OK)
    })
}
/// # Safety
/// `SQLite` supplies an exclusive live shim file and the arguments required by
/// `xUnfetch`, including valid output slots or previously returned mapping pointers.
unsafe extern "C" fn x_unfetch(
    file: *mut ffi::sqlite3_file,
    offset: i64,
    pointer: *mut c_void,
) -> c_int {
    ffi_guard(|| {
        // SAFETY: The file borrow is confined to this callback. Pointer arguments
        // retain SQLite's xUnfetch contract and are forwarded unchanged to the
        // owning parent, whose method table remains live until close.
        unsafe {
            with_file(file, |f| {
                if f.state.is_managed() {
                    return ffi::SQLITE_OK;
                }
                let parent = f.parent.raw();
                f.parent
                    .methods()
                    .filter(|m| m.iVersion >= 3)
                    .and_then(|m| m.xUnfetch)
                    .map_or(ffi::SQLITE_OK, |call| call(parent, offset, pointer))
            })
        }
        .unwrap_or(ffi::SQLITE_OK)
    })
}
/// # Safety
/// file is a live shim file exclusively accessible for this callback.
unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    let _ = std::panic::catch_unwind(|| {
        // SAFETY: SQLite supplies an exclusive live file. Only a version-2+
        // parent barrier is invoked, without passing any borrowed Rust buffer.
        unsafe {
            with_file(file, |f| {
                let parent = f.parent.raw();
                if let Some(call) = f
                    .parent
                    .methods()
                    .filter(|m| m.iVersion >= 2)
                    .and_then(|m| m.xShmBarrier)
                {
                    call(parent);
                }
            });
        }
    });
}
/// # Safety
/// file is exclusive and live; argument satisfies the selected `SQLite` opcode's
/// type, alignment, size and lifetime contract (including custom stats version).
unsafe extern "C" fn x_file_control(
    file: *mut ffi::sqlite3_file,
    operation: c_int,
    argument: *mut c_void,
) -> c_int {
    ffi_guard(|| {
        // SAFETY: SQLite supplies exclusive access to our initialized file;
        // dispatch keeps the borrow within the callback.
        unsafe { with_file(file, |f| file_control(f, file, operation, argument)) }
            .unwrap_or(ffi::SQLITE_MISUSE)
    })
}
/// # Safety
/// argument obeys operation's file-control ABI; file is f's live outer `ZFile`.
unsafe fn file_control(
    f: &mut OpenFile,
    file: *mut ffi::sqlite3_file,
    operation: c_int,
    argument: *mut c_void,
) -> c_int {
    if operation == crate::statistics::FILE_CONTROL_STATS_V1 {
        // SAFETY: The custom opcode requires at least an initialized StatsHeader.
        // Checking version/size selects the full struct; it does not establish
        // allocation validity, which is the file-control caller's obligation.
        let Some(header) = (unsafe { argument.cast::<crate::statistics::StatsHeader>().as_ref() })
        else {
            return ffi::SQLITE_MISUSE;
        };
        if header.version != 1
            || header.size as usize != size_of::<crate::statistics::FileControlStatsV1>()
        {
            return ffi::SQLITE_MISUSE;
        }
        let snapshot = match f.state.statistics() {
            Ok(snapshot) => snapshot,
            Err(rc) => return rc,
        };
        // SAFETY: A matching header promises a writable full V1 allocation.
        // write initializes the output without reading its counter fields.
        unsafe {
            argument
                .cast::<crate::statistics::FileControlStatsV1>()
                .write(snapshot);
        };
        return ffi::SQLITE_OK;
    }
    if let Some(rc) = f.state.file_control(operation) {
        return rc;
    }
    if f.state.is_managed() {
        match operation {
            ffi::SQLITE_FCNTL_MMAP_SIZE => {
                if !argument.is_null() {
                    // SAFETY: MMAP_SIZE takes a writable i64; managed files
                    // decline mapping and report zero to the caller.
                    unsafe { argument.cast::<i64>().write(0) };
                }
                return ffi::SQLITE_OK;
            }
            ffi::SQLITE_FCNTL_HAS_MOVED => {
                if argument.is_null() {
                    return ffi::SQLITE_MISUSE;
                }
                let moved = match f.state.has_moved(&mut f.parent) {
                    Ok(moved) => moved,
                    Err(rc) => return rc,
                };
                // SAFETY: HAS_MOVED's ABI supplies a writable c_int output.
                unsafe { argument.cast::<c_int>().write(c_int::from(moved)) };
                return ffi::SQLITE_OK;
            }
            ffi::SQLITE_FCNTL_FILE_POINTER => {
                if argument.is_null() {
                    return ffi::SQLITE_MISUSE;
                }
                // SAFETY: FILE_POINTER takes a writable sqlite3_file* slot;
                // file remains owned by SQLite until its later xClose callback.
                unsafe { argument.cast::<*mut ffi::sqlite3_file>().write(file) };
                return ffi::SQLITE_OK;
            }
            ffi::SQLITE_FCNTL_VFS_POINTER => {
                if argument.is_null() {
                    return ffi::SQLITE_MISUSE;
                }
                // SAFETY: VFS_POINTER takes a writable sqlite3_vfs* slot; our
                // registration and allocation are permanent after successful init.
                unsafe { argument.cast::<*mut ffi::sqlite3_vfs>().write(f.vfs) };
                return ffi::SQLITE_OK;
            }
            _ => {}
        }
    }
    let parent = f.parent.raw();
    // SAFETY: Unknown arguments are opaque here. The file-control caller supplies
    // the opcode-specific validity required by the parent; we forward unchanged
    // and do not expose the pointer to the safe database implementation.
    unsafe {
        f.parent
            .methods()
            .and_then(|m| m.xFileControl)
            .map_or(ffi::SQLITE_NOTFOUND, |control| {
                control(parent, operation, argument)
            })
    }
}
/// # Safety
/// `SQLite` supplies our registered VFS and a valid optional pathname.
unsafe extern "C" fn x_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_dir: c_int,
) -> c_int {
    ffi_guard(|| {
        // SAFETY: SQLite guarantees the VFS registration and filename remain
        // live through the callback; the optional borrow ends on return.
        let (app, name_ref) = unsafe { (app_data(&*vfs), optional_name(name)) };
        let Some(app) = app else {
            return ffi::SQLITE_INTERNAL;
        };
        let Ok(mapped) = runtime::mapped_storage_name(name_ref) else {
            return ffi::SQLITE_CANTOPEN;
        };
        let parent_name = mapped.as_ref().map_or(name, |name| name.as_ptr());
        runtime::delete(name_ref, || {
            // SAFETY: Registration retains the live parent. The original SQLite
            // name or our CString remains valid throughout each synchronous call;
            // runtime keeps its registry gate across this fallback.
            unsafe {
                app.parent
                    .as_ref()
                    .and_then(|p| p.xDelete)
                    .map_or(ffi::SQLITE_IOERR_DELETE, |delete| {
                        delete(app.parent, parent_name, sync_dir)
                    })
            }
        })
    })
}
/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    output: *mut c_int,
) -> c_int {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // name is a live optional C string and output is a writable c_int slot.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard(|| unsafe {
        let Some(app) = app_data(&*vfs) else {
            return ffi::SQLITE_INTERNAL;
        };
        let Ok(mapped) = runtime::mapped_storage_name(optional_name(name)) else {
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

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    output_size: c_int,
    output: *mut c_char,
) -> c_int {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // name is a live optional C string; output has output_size writable bytes.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard(|| unsafe {
        let Some(app) = app_data(&*vfs) else {
            return ffi::SQLITE_INTERNAL;
        };
        let Ok(mapped) = runtime::mapped_storage_name(optional_name(name)) else {
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

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_dl_open(vfs: *mut ffi::sqlite3_vfs, filename: *const c_char) -> *mut c_void {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // filename is a live C string; the parent owns the returned library handle.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard_or(null_mut(), || unsafe {
        let Some((parent, methods)) = parent_vfs_for(&*vfs) else {
            return null_mut();
        };
        methods
            .xDlOpen
            .map_or(null_mut(), |open| open(parent, filename))
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_dl_error(
    vfs: *mut ffi::sqlite3_vfs,
    output_size: c_int,
    output: *mut c_char,
) {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // output has output_size writable bytes for the parent error message.
    // Forwarding preserves the parent context and borrows only for this call.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if let Some((parent, methods)) = parent_vfs_for(&*vfs)
            && let Some(error) = methods.xDlError
        {
            error(parent, output_size, output);
        }
    }));
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_dl_sym(
    vfs: *mut ffi::sqlite3_vfs,
    handle: *mut c_void,
    symbol: *const c_char,
) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // handle belongs to the parent loader and symbol is a live C string.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard_or(None, || unsafe {
        let (parent, methods) = parent_vfs_for(&*vfs)?;
        methods
            .xDlSym
            .and_then(|lookup| lookup(parent, handle, symbol))
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_dl_close(vfs: *mut ffi::sqlite3_vfs, handle: *mut c_void) {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // handle belongs to the parent loader and SQLite closes it exactly once.
    // Forwarding preserves the parent context and borrows only for this call.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        if let Some((parent, methods)) = parent_vfs_for(&*vfs)
            && let Some(close) = methods.xDlClose
        {
            close(parent, handle);
        }
    }));
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    amount: c_int,
    output: *mut c_char,
) -> c_int {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // output has amount writable bytes and is not retained by the parent.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard_or(0, || unsafe {
        let Some((parent, methods)) = parent_vfs_for(&*vfs) else {
            return 0;
        };
        methods
            .xRandomness
            .map_or(0, |randomness| randomness(parent, amount, output))
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_sleep(vfs: *mut ffi::sqlite3_vfs, microseconds: c_int) -> c_int {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // the live parent receives only a numeric duration and retains no Rust data.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard_or(0, || unsafe {
        let Some((parent, methods)) = parent_vfs_for(&*vfs) else {
            return 0;
        };
        methods
            .xSleep
            .map_or(0, |sleep| sleep(parent, microseconds))
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_current_time(vfs: *mut ffi::sqlite3_vfs, output: *mut f64) -> c_int {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // output is an aligned writable f64 slot valid for the callback.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard(|| unsafe {
        let Some((parent, methods)) = parent_vfs_for(&*vfs) else {
            return ffi::SQLITE_IOERR;
        };
        methods
            .xCurrentTime
            .map_or(ffi::SQLITE_IOERR, |current_time| {
                current_time(parent, output)
            })
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_get_last_error(
    vfs: *mut ffi::sqlite3_vfs,
    output_size: c_int,
    output: *mut c_char,
) -> c_int {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // output has output_size writable bytes for the synchronous error query.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard_or(0, || unsafe {
        let Some((parent, methods)) = parent_vfs_for(&*vfs) else {
            return 0;
        };
        methods
            .xGetLastError
            .map_or(0, |last_error| last_error(parent, output_size, output))
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_current_time_int64(
    vfs: *mut ffi::sqlite3_vfs,
    output: *mut ffi::sqlite3_int64,
) -> c_int {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // output is an aligned writable i64 slot valid for the callback.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard(|| unsafe {
        let Some((parent, methods)) = parent_vfs_for(&*vfs) else {
            return ffi::SQLITE_IOERR;
        };
        methods
            .xCurrentTimeInt64
            .map_or(ffi::SQLITE_IOERR, |current_time| {
                current_time(parent, output)
            })
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_set_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    call: ffi::sqlite3_syscall_ptr,
) -> c_int {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // name and call satisfy SQLite's system-call override ABI and lifetime.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard(|| unsafe {
        let Some((parent, methods)) = parent_vfs_for(&*vfs) else {
            return ffi::SQLITE_NOTFOUND;
        };
        methods
            .xSetSystemCall
            .map_or(ffi::SQLITE_NOTFOUND, |set| set(parent, name, call))
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_get_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> ffi::sqlite3_syscall_ptr {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // name is a valid C string identifying a parent system call.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard_or(None, || unsafe {
        let (parent, methods) = parent_vfs_for(&*vfs)?;
        methods.xGetSystemCall.and_then(|get| get(parent, name))
    })
}

/// # Safety
/// `SQLite` supplies this shim's live VFS and the selected callback's valid
/// buffers, strings, handles and function pointers for their required lifetimes.
unsafe extern "C" fn x_next_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> *const c_char {
    // SAFETY: SQLite supplies our live VFS; registration retains the parent.
    // name is null or a valid C string; the returned name is parent-owned.
    // Forwarding preserves the parent context and borrows only for this call.
    ffi_guard_or(null(), || unsafe {
        let Some((parent, methods)) = parent_vfs_for(&*vfs) else {
            return null();
        };
        methods
            .xNextSystemCall
            .map_or(null(), |next| next(parent, name))
    })
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

/// # Safety
/// `SQLite`'s API is initialized and the default parent VFS outlives the shim.
unsafe fn register_vfs() -> c_int {
    // SAFETY: The caller establishes SQLite initialization and parent lifetime;
    // the static terminated name is valid and register_named_vfs copies it.
    unsafe {
        register_named_vfs(
            CStr::from_bytes_with_nul(VFS_NAME).expect("static VFS name"),
            None,
        )
    }
}
/// # Safety
/// `SQLite`'s API is initialized. The host keeps the selected parent VFS alive
/// and does not unregister/free it while this permanent shim exists.
unsafe fn register_named_vfs(requested_name: &CStr, storage: Option<crate::Storage>) -> c_int {
    let Ok(_registration) = REGISTRATION_LOCK.lock() else {
        return ffi::SQLITE_ERROR;
    };
    let name = requested_name.as_ptr();
    // SAFETY: SQLite is initialized; requested_name keeps this C string live.
    // vfs_find manages its own registry lock and returns a registered VFS.
    let existing = unsafe { ffi::sqlite3_vfs_find(name) };
    // SAFETY: The host retains registered VFS allocations during registration;
    // no reference is retained beyond this check.
    if let Some(existing) = unsafe { existing.as_ref() } {
        // Only our own callback establishes the type of pAppData.
        if existing
            .xOpen
            .map(|function| function as *const () as usize)
            != Some(x_open as *const () as usize)
        {
            return ffi::SQLITE_MISUSE;
        }
        // SAFETY: The xOpen identity check above establishes our AppData layout;
        // our successful registrations permanently own their immutable AppData.
        let same_storage =
            unsafe { app_data(existing) }.is_some_and(|app| match (&app.storage, &storage) {
                (None, None) => true,
                (Some(old), Some(new)) => {
                    old.backend().identity() == new.backend().identity()
                        && old.coordination_directory() == new.coordination_directory()
                }
                _ => false,
            });
        return if same_storage
            && existing
                .xOpen
                .map(|function| function as *const () as usize)
                == Some(x_open as *const () as usize)
        {
            ffi::SQLITE_OK
        } else {
            ffi::SQLITE_MISUSE
        };
    }
    // SAFETY: SQLite is initialized; null requests its default registered VFS.
    let parent = unsafe { ffi::sqlite3_vfs_find(null()) };
    // SAFETY: The caller guarantees the default parent's registration lifetime.
    // Null means no default VFS and is handled without dereferencing it.
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
    let app = Box::new(AppData { parent, storage });
    let app_ptr = Box::into_raw(app);
    shim.pNext = null_mut();
    shim.zName = requested_name.to_owned().into_raw();
    shim.pAppData = app_ptr.cast();
    install_parent_vfs_wrappers(&mut shim, parent_ref);
    let shim_ptr = Box::into_raw(Box::new(shim));
    // SAFETY: The shim, name and AppData are fully initialized heap allocations.
    // On success they remain allocated permanently, as SQLite requires. This
    // mutex serializes our registration attempts, not runtime file callbacks.
    let rc = unsafe { ffi::sqlite3_vfs_register(shim_ptr, 0) };
    if rc != ffi::SQLITE_OK {
        // SAFETY: Failed registration did not transfer the allocations to the
        // registry. Reclaim exactly the CString and Boxes allocated above.
        unsafe {
            drop(CString::from_raw((*shim_ptr).zName.cast_mut()));
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
///
/// # Safety
/// The `SQLite` loader supplies a compatible API table valid for the extension's
/// lifetime. The host must retain the parent VFS after registration.
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
        // SAFETY: The SQLite extension loader supplies a live compatible API
        // table; initializing the bindings is required before any SQLite call.
        if unsafe { ffi::rusqlite_extension_init2(api) }.is_err() {
            return ffi::SQLITE_ERROR;
        }
        // SAFETY: The extension API was initialized above. The host owns the
        // default parent VFS for the lifetime of this permanent registration.
        let rc = unsafe { register_vfs() };
        if rc == ffi::SQLITE_OK {
            ffi::SQLITE_OK_LOAD_PERMANENTLY
        } else {
            rc
        }
    })
}

/// Generic entry point for hosts that do not derive extension names.
///
/// # Safety
/// Arguments obey `sqlite3_zsqlite_init`'s loader and parent lifetime contract.
#[cfg(feature = "loadable")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_extension_init(
    database: *mut ffi::sqlite3,
    error: *mut *mut c_char,
    api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    // SAFETY: The generic entry point receives the identical loader contract
    // and passes its connection, error slot and live API table unchanged.
    unsafe { sqlite3_zsqlite_init(database, error, api) }
}

/// Registers zsqlite against a statically linked `SQLite` library.
#[cfg(feature = "static")]
pub fn register_static_vfs() -> Result<(), c_int> {
    // SAFETY: The static build links SQLite directly; vfs_find initializes its
    // built-in VFS. The host retains any replacement parent for this shim.
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

#[cfg(feature = "static")]
pub(crate) fn register_storage_static_vfs(
    name: &str,
    storage: crate::Storage,
) -> Result<(), c_int> {
    let name = CString::new(name).map_err(|_| ffi::SQLITE_MISUSE)?;
    if name.as_bytes().is_empty() {
        return Err(ffi::SQLITE_MISUSE);
    }
    // SAFETY: The static SQLite API is linked and initializes its default VFS.
    // name is live and copied by registration; the host retains the parent.
    let result = unsafe { register_named_vfs(&name, Some(storage)) };
    if result == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(result)
    }
}
