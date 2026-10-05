//! An immutable `SQLite` image backed only by a finalized sealed view.
use crate::domain::PageNumber;
use crate::{PinnedView, StoreError};
use libsqlite3_sys as ffi;
use std::collections::{BTreeMap, HashMap};
use std::ffi::{CString, c_char, c_int, c_void};
use std::ptr;
use std::sync::Mutex;

#[cfg(all(feature = "browser", target_os = "emscripten"))]
mod browser_cache {
    unsafe extern "C" {
        pub fn zsqlite_page_cache_get(
            manifest: *const u8,
            page: u32,
            size: u32,
            output: *mut u8,
        ) -> i32;
        pub fn zsqlite_page_cache_put(manifest: *const u8, page: u32, size: u32, input: *const u8);
        pub fn zsqlite_page_cache_remove(manifest: *const u8, page: u32);
        pub fn zsqlite_page_cache_status(bytes: u32, hit: u32, page_size: u32);
        pub fn zsqlite_snapshot_info(logical: f64, sealed: f64, page_size: u32, pages: u32);
    }
}

struct Snapshot {
    view: PinnedView,
    pages: HashMap<u32, (Vec<u8>, u64)>,
    lru: BTreeMap<u64, u32>,
    clock: u64,
    capacity: usize,
}
impl Snapshot {
    fn load_page(&self, number: u32) -> Result<Vec<u8>, StoreError> {
        let resolved = self.view.resolve(PageNumber::new(number)?)?;
        #[cfg(all(feature = "browser", target_os = "emscripten"))]
        {
            let manifest = self.view.id();
            let size = self.view.logical_size().page_size().get();
            let mut bytes = vec![0; size as usize];
            // SAFETY: All pointers borrow live manifest/page buffers only for
            // the synchronous worker callback, which writes at most size bytes.
            let count = unsafe {
                browser_cache::zsqlite_page_cache_get(
                    manifest.as_bytes().as_ptr(),
                    number,
                    size,
                    bytes.as_mut_ptr(),
                )
            };
            if count == i32::try_from(size).map_err(|_| StoreError::Range)?
                && resolved.accepts_cached(&bytes)
            {
                return Ok(bytes);
            }
            if count >= 0 {
                // SAFETY: Borrowed digest is live for this synchronous call.
                unsafe {
                    browser_cache::zsqlite_page_cache_remove(manifest.as_bytes().as_ptr(), number);
                }
            }
            resolved.read_and_cache(|page, bytes| {
                // SAFETY: Only authenticated live page slices are admitted;
                // JavaScript copies them synchronously and retains no pointers.
                unsafe {
                    browser_cache::zsqlite_page_cache_put(
                        manifest.as_bytes().as_ptr(),
                        page.get(),
                        size,
                        bytes.as_ptr(),
                    );
                }
            })
        }
        #[cfg(not(all(feature = "browser", target_os = "emscripten")))]
        resolved.read()
    }
    fn cache_status(&self, hit: bool) {
        let _ = (self, hit);
        #[cfg(all(feature = "browser", target_os = "emscripten"))]
        // SAFETY: Scalar cache statistics; callback retains no Rust references.
        unsafe {
            browser_cache::zsqlite_page_cache_status(
                u32::try_from(self.pages.len() * self.view.logical_size().page_size().as_usize())
                    .unwrap_or(u32::MAX),
                u32::from(hit),
                self.view.logical_size().page_size().get(),
            );
        }
    }
    fn page(&mut self, number: u32) -> Result<&[u8], StoreError> {
        // Renumber before overflow, preserving the eviction order.
        if self.clock == u64::MAX {
            let numbers: Vec<_> = self.lru.values().copied().collect();
            self.lru.clear();
            for (stamp, number) in numbers.into_iter().enumerate() {
                self.pages.get_mut(&number).expect("cached page").1 = stamp as u64;
                self.lru.insert(stamp as u64, number);
            }
            self.clock = self.pages.len() as u64;
        }
        let stamp = self.clock;
        self.clock += 1;
        if let Some((_, old)) = self.pages.get(&number) {
            self.lru.remove(old);
            self.cache_status(true);
        } else {
            let bytes = self.load_page(number)?;
            if self.pages.len() >= self.capacity
                && let Some((_, victim)) = self.lru.pop_first()
            {
                self.pages.remove(&victim);
            }
            self.pages.insert(number, (bytes, stamp));
            self.cache_status(false);
        }
        let page = self.pages.get_mut(&number).expect("inserted page");
        page.1 = stamp;
        self.lru.insert(stamp, number);
        Ok(&page.0)
    }
    fn read(&mut self, offset: u64, output: &mut [u8]) -> Result<usize, StoreError> {
        output.fill(0);
        let size = self.view.logical_size();
        let available = usize::try_from(size.get().saturating_sub(offset))
            .unwrap_or(usize::MAX)
            .min(output.len());
        let page_size = size.page_size().as_usize();
        let mut copied = 0;
        while copied < available {
            let position = offset.checked_add(copied as u64).ok_or(StoreError::Range)?;
            let number =
                u32::try_from(position / page_size as u64 + 1).map_err(|_| StoreError::Range)?;
            let within =
                usize::try_from(position % page_size as u64).expect("page offset fits usize");
            let count = (page_size - within).min(available - copied);
            if self.capacity == 0 {
                let page = self.load_page(number)?;
                output[copied..copied + count].copy_from_slice(&page[within..within + count]);
            } else {
                let page = self.page(number)?;
                output[copied..copied + count].copy_from_slice(&page[within..within + count]);
            }
            copied += count;
        }
        Ok(copied)
    }
}

#[repr(C)]
struct File {
    base: ffi::sqlite3_file,
    snapshot: *const Mutex<Snapshot>,
}
struct AppData {
    snapshot: Mutex<Snapshot>,
    parent: *mut ffi::sqlite3_vfs,
}

/// Register once and retain the snapshot, name and VFS for `SQLite`'s lifetime.
pub(crate) fn register(name: &str, view: PinnedView, cache_bytes: usize) -> Result<(), StoreError> {
    let name = CString::new(name)
        .map_err(|_| StoreError::InvalidConfiguration("invalid read-only VFS name"))?;
    let _registration = super::REGISTRATION_LOCK
        .lock()
        .map_err(|_| StoreError::Busy)?;
    // SAFETY: SQLite owns the built-in VFS for the process lifetime. Registration
    // is serialized and the CString remains live while finding the name.
    let parent = unsafe { ffi::sqlite3_vfs_find(ptr::null()) };
    // SAFETY: The name is terminated and registration is serialized.
    let existing = unsafe { ffi::sqlite3_vfs_find(name.as_ptr()) };
    if parent.is_null() || !existing.is_null() {
        return Err(StoreError::InvalidConfiguration(
            "VFS name already registered or no parent VFS",
        ));
    }
    let capacity = cache_bytes / view.logical_size().page_size().as_usize();
    #[cfg(all(feature = "browser", target_os = "emscripten"))]
    {
        let size = view.logical_size();
        let sealed = view.object_bytes()?.0;
        // The browser transport already restricts object sizes to JS safe
        // integers. Practical SQLite and sealed-store sizes are also below it.
        #[allow(clippy::cast_precision_loss)]
        // SAFETY: Scalar immutable snapshot statistics, no borrowed pointers.
        unsafe {
            browser_cache::zsqlite_snapshot_info(
                size.get() as f64,
                sealed as f64,
                size.page_size().get(),
                size.pages(),
            );
        }
    }
    let app = Box::new(AppData {
        snapshot: Mutex::new(Snapshot {
            view,
            capacity,
            pages: HashMap::new(),
            lru: BTreeMap::new(),
            clock: 0,
        }),
        parent,
    });
    // SAFETY: The parent VFS is initialized and remains registered. Forward its
    // platform callbacks with its own VFS pointer and pAppData below.
    let mut vfs = Box::new(unsafe { ptr::read(parent) });
    vfs.iVersion = 1;
    vfs.pNext = ptr::null_mut();
    vfs.szOsFile = c_int::try_from(std::mem::size_of::<File>()).expect("small file handle");
    vfs.zName = name.as_ptr();
    vfs.pAppData = ptr::from_ref(app.as_ref()).cast_mut().cast();
    vfs.xOpen = Some(open);
    vfs.xDelete = Some(delete);
    vfs.xAccess = Some(access);
    vfs.xFullPathname = Some(full_path);
    vfs.xRandomness = Some(randomness);
    vfs.xSleep = Some(sleep);
    vfs.xCurrentTime = Some(current_time);
    // Disable extension loading through this VFS.
    vfs.xDlOpen = None;
    vfs.xDlError = None;
    vfs.xDlSym = None;
    vfs.xDlClose = None;
    // SAFETY: All callbacks and owned allocations live until process teardown.
    // SQLite retains vfs only on successful registration, after which we leak it.
    let rc = unsafe { ffi::sqlite3_vfs_register(vfs.as_mut(), 0) };
    if rc != ffi::SQLITE_OK {
        return Err(StoreError::InvalidConfiguration(
            "cannot register read-only VFS",
        ));
    }
    Box::leak(app);
    Box::leak(vfs);
    let _ = name.into_raw();
    Ok(())
}

unsafe extern "C" fn open(
    vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    super::ffi_guard(|| {
        // SAFETY: SQLite supplies szOsFile bytes and our live registered VFS.
        unsafe {
            file.cast::<File>().write(File {
                base: ffi::sqlite3_file {
                    pMethods: ptr::null(),
                },
                snapshot: ptr::null(),
            });
            if flags & ffi::SQLITE_OPEN_MAIN_DB == 0
                || flags & (ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE) != 0
                || flags & ffi::SQLITE_OPEN_READONLY == 0
            {
                return ffi::SQLITE_READONLY;
            }
            let app = &*(*vfs).pAppData.cast::<AppData>();
            (*file.cast::<File>()).snapshot = &raw const app.snapshot;
            (*file).pMethods = &raw const METHODS;
            if !out_flags.is_null() {
                out_flags.write(ffi::SQLITE_OPEN_READONLY);
            }
        }
        ffi::SQLITE_OK
    })
}
unsafe extern "C" fn close(file: *mut ffi::sqlite3_file) -> c_int {
    // SAFETY: SQLite exclusively closes a successful open once; snapshot is VFS-owned.
    unsafe {
        (*file).pMethods = ptr::null();
    }
    ffi::SQLITE_OK
}
unsafe extern "C" fn read(
    file: *mut ffi::sqlite3_file,
    output: *mut c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    super::ffi_guard(|| {
        let (Ok(amount), Ok(offset)) = (usize::try_from(amount), u64::try_from(offset)) else {
            return ffi::SQLITE_IOERR_READ;
        };
        if amount == 0 {
            return ffi::SQLITE_OK;
        }
        // SAFETY: SQLite supplies an exclusive writable amount-byte buffer and
        // our initialized file. Registration retains the snapshot allocation.
        let (snapshot, output) = unsafe {
            (
                &*(*file.cast::<File>()).snapshot,
                std::slice::from_raw_parts_mut(output.cast::<u8>(), amount),
            )
        };
        match snapshot
            .lock()
            .map_err(|_| StoreError::Busy)
            .and_then(|mut snapshot| snapshot.read(offset, output))
        {
            Ok(read) if read == amount => ffi::SQLITE_OK,
            Ok(_) => ffi::SQLITE_IOERR_SHORT_READ,
            Err(_) => {
                output.fill(0);
                ffi::SQLITE_IOERR_READ
            }
        }
    })
}
unsafe extern "C" fn size(file: *mut ffi::sqlite3_file, output: *mut i64) -> c_int {
    super::ffi_guard(|| {
        // SAFETY: SQLite supplies our live initialized file and an exclusive output slot.
        let snapshot = unsafe { &*(*file.cast::<File>()).snapshot };
        let Ok(snapshot) = snapshot.lock() else {
            return ffi::SQLITE_IOERR_FSTAT;
        };
        let Ok(size) = i64::try_from(snapshot.view.logical_size().get()) else {
            return ffi::SQLITE_IOERR_FSTAT;
        };
        // SAFETY: SQLite supplied a writable i64 output slot.
        unsafe {
            output.write(size);
        }
        ffi::SQLITE_OK
    })
}
unsafe extern "C" fn write(
    _file: *mut ffi::sqlite3_file,
    _input: *const c_void,
    _amount: c_int,
    _offset: i64,
) -> c_int {
    ffi::SQLITE_READONLY
}
unsafe extern "C" fn truncate(_file: *mut ffi::sqlite3_file, _size: i64) -> c_int {
    ffi::SQLITE_READONLY
}
unsafe extern "C" fn sync(_file: *mut ffi::sqlite3_file, _flags: c_int) -> c_int {
    ffi::SQLITE_OK
}
unsafe extern "C" fn lock(_file: *mut ffi::sqlite3_file, _level: c_int) -> c_int {
    ffi::SQLITE_OK
}
unsafe extern "C" fn reserved(_file: *mut ffi::sqlite3_file, output: *mut c_int) -> c_int {
    // SAFETY: SQLite supplied a writable integer output slot.
    unsafe {
        output.write(0);
    }
    ffi::SQLITE_OK
}
unsafe extern "C" fn control(
    _file: *mut ffi::sqlite3_file,
    operation: c_int,
    output: *mut c_void,
) -> c_int {
    if operation == ffi::SQLITE_FCNTL_LOCKSTATE {
        // SAFETY: LOCKSTATE requires SQLite to supply an integer output slot.
        unsafe {
            output.cast::<c_int>().write(ffi::SQLITE_LOCK_NONE);
        }
        ffi::SQLITE_OK
    } else {
        ffi::SQLITE_NOTFOUND
    }
}
unsafe extern "C" fn sector(_file: *mut ffi::sqlite3_file) -> c_int {
    4096
}
unsafe extern "C" fn characteristics(_file: *mut ffi::sqlite3_file) -> c_int {
    ffi::SQLITE_IOCAP_IMMUTABLE
}
unsafe extern "C" fn delete(
    _vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
    _sync: c_int,
) -> c_int {
    ffi::SQLITE_READONLY
}
unsafe extern "C" fn access(
    _vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
    _flags: c_int,
    output: *mut c_int,
) -> c_int {
    // SAFETY: SQLite supplies an integer output. Immutable snapshots have no auxiliary files.
    unsafe {
        output.write(0);
    }
    ffi::SQLITE_OK
}
static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(close),
    xRead: Some(read),
    xWrite: Some(write),
    xTruncate: Some(truncate),
    xSync: Some(sync),
    xFileSize: Some(size),
    xLock: Some(lock),
    xUnlock: Some(lock),
    xCheckReservedLock: Some(reserved),
    xFileControl: Some(control),
    xSectorSize: Some(sector),
    xDeviceCharacteristics: Some(characteristics),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

unsafe extern "C" fn full_path(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    amount: c_int,
    output: *mut c_char,
) -> c_int {
    // SAFETY: Our registration owns AppData and retains its parent. SQLite
    // supplies the filename and output buffer under the parent's callback contract.
    unsafe {
        let parent = (*(*vfs).pAppData.cast::<AppData>()).parent;
        (*parent)
            .xFullPathname
            .map_or(ffi::SQLITE_CANTOPEN, |callback| {
                callback(parent, name, amount, output)
            })
    }
}
unsafe extern "C" fn randomness(
    vfs: *mut ffi::sqlite3_vfs,
    amount: c_int,
    output: *mut c_char,
) -> c_int {
    // SAFETY: Retained parent and SQLite-provided writable amount-byte buffer.
    unsafe {
        let parent = (*(*vfs).pAppData.cast::<AppData>()).parent;
        (*parent)
            .xRandomness
            .map_or(0, |callback| callback(parent, amount, output))
    }
}
unsafe extern "C" fn sleep(vfs: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    // SAFETY: Retained parent; its sleep callback takes no borrowed buffers.
    unsafe {
        let parent = (*(*vfs).pAppData.cast::<AppData>()).parent;
        (*parent)
            .xSleep
            .map_or(0, |callback| callback(parent, micros))
    }
}
unsafe extern "C" fn current_time(vfs: *mut ffi::sqlite3_vfs, output: *mut f64) -> c_int {
    // SAFETY: Retained parent and SQLite-provided writable double output slot.
    unsafe {
        let parent = (*(*vfs).pAppData.cast::<AppData>()).parent;
        (*parent)
            .xCurrentTime
            .map_or(ffi::SQLITE_ERROR, |callback| callback(parent, output))
    }
}
