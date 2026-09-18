//! Ownership and calls into a parent `SQLite` VFS.
//!
//! Parent files are neither `Send` nor `Sync`. `SQLite` may hand a file to another
//! thread between callbacks, but never calls it concurrently or reentrantly.
//! Registration does not serialize callbacks on different files.
use libsqlite3_sys as ffi;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::ptr::{self, null_mut};

/// Owns `SQLite`'s framed filename, including URI fields retained by the parent.
struct SqliteFilename(*const c_char);
impl SqliteFilename {
    /// # Safety
    /// `original` is `SQLite`'s live main-database filename, including its private
    /// framing, not an arbitrary C string. It remains valid throughout the call.
    unsafe fn remap(path: &CStr, original: *const c_char) -> Result<Self, c_int> {
        let mut parameters = Vec::new();
        let mut count: c_int = 0;
        loop {
            // SAFETY: The caller supplies a framed SQLite filename. SQLite's
            // enumeration borrows it and returns null when count is exhausted.
            let key = unsafe { ffi::sqlite3_uri_key(original, count) };
            if key.is_null() {
                break;
            }
            parameters.push(key);
            // SAFETY: key was returned for this live filename; both remain valid
            // until create_filename below copies the parameter strings.
            parameters.push(unsafe { ffi::sqlite3_uri_parameter(original, key) });
            count = count.checked_add(1).ok_or(ffi::SQLITE_TOOBIG)?;
        }
        // SAFETY: SQLite requires its framed filename for journal/WAL lookup.
        // path and all 2*count parameter pointers are live; create_filename
        // copies them and returns a separately owned SQLite allocation.
        let filename = unsafe {
            ffi::sqlite3_create_filename(
                path.as_ptr(),
                ffi::sqlite3_filename_journal(original),
                ffi::sqlite3_filename_wal(original),
                count,
                parameters.as_mut_ptr(),
            )
        };
        if filename.is_null() {
            Err(ffi::SQLITE_NOMEM)
        } else {
            Ok(Self(filename))
        }
    }
}
impl Drop for SqliteFilename {
    fn drop(&mut self) {
        // SAFETY: This is the sole owner of create_filename's allocation.
        // ParentFile closes its handle before dropping this retained name.
        unsafe { ffi::sqlite3_free_filename(self.0.cast_mut()) };
    }
}

pub(super) struct ParentFile {
    raw: *mut ffi::sqlite3_file,
    name: Option<SqliteFilename>,
}
impl ParentFile {
    /// # Safety
    /// vfs is a live parent VFS whose callbacks and method tables outlive this
    /// file. name satisfies `SQLite` `xOpen`'s filename contract and remains alive
    /// through close; a mapped main filename is copied into this owner instead.
    /// Calls on this file must remain exclusive, including across threads.
    pub(super) unsafe fn open(
        vfs: *mut ffi::sqlite3_vfs,
        name: *const c_char,
        mapped: Option<&CStr>,
        flags: c_int,
        out_flags: Option<&mut c_int>,
    ) -> Result<Self, c_int> {
        // SAFETY: The caller guarantees the parent registration's lifetime.
        let parent = unsafe { vfs.as_ref() }.ok_or(ffi::SQLITE_INTERNAL)?;
        let size = usize::try_from(parent.szOsFile).map_err(|_| ffi::SQLITE_INTERNAL)?;
        if size < size_of::<ffi::sqlite3_file>() {
            return Err(ffi::SQLITE_INTERNAL);
        }
        let open = parent.xOpen.ok_or(ffi::SQLITE_INTERNAL)?;
        let name_owner = if let Some(mapped) = mapped {
            // SAFETY: Only a main-database filename is remapped; the caller
            // supplies SQLite's framing and mapped is a live C string.
            Some(unsafe { SqliteFilename::remap(mapped, name) }?)
        } else {
            None
        };
        // SAFETY: SQLite allocation provides the size/alignment its VFS requests.
        // The result is checked before initialization or access.
        let raw = unsafe { ffi::sqlite3_malloc64(size as u64) }.cast::<ffi::sqlite3_file>();
        if raw.is_null() {
            return Err(ffi::SQLITE_NOMEM);
        }
        // SAFETY: raw uniquely owns size writable bytes. A null pMethods makes
        // cleanup safe even when xOpen fails before installing a method table.
        unsafe { ptr::write_bytes(raw.cast::<u8>(), 0, size) };
        let file = Self {
            raw,
            name: name_owner,
        };
        let filename = file.name.as_ref().map_or(name, |name| name.0);
        // SAFETY: The allocation matches szOsFile; filename obeys SQLite's
        // framing/lifetime contract. The optional output reference is exclusive
        // and live for this synchronous callback. file owns failure cleanup.
        let rc = unsafe {
            open(
                vfs,
                filename,
                raw,
                flags,
                out_flags.map_or(null_mut(), ptr::from_mut),
            )
        };
        if rc == ffi::SQLITE_OK {
            Ok(file)
        } else {
            Err(rc)
        }
    }
    pub(super) fn methods(&self) -> Option<&ffi::sqlite3_io_methods> {
        // SAFETY: raw is either our live allocation or null after close.
        // The parent's method table remains live until close by open's contract.
        unsafe { self.raw.as_ref()?.pMethods.as_ref() }
    }
    pub(super) fn raw(&mut self) -> *mut ffi::sqlite3_file {
        self.raw
    }
    #[cfg(all(test, feature = "static"))]
    pub(super) fn filename(&self) -> Option<*const c_char> {
        self.name.as_ref().map(|name| name.0)
    }
    pub(super) fn close(mut self) -> c_int {
        self.close_inner()
    }
    fn close_inner(&mut self) -> c_int {
        let raw = std::mem::replace(&mut self.raw, null_mut());
        if raw.is_null() {
            return ffi::SQLITE_OK;
        }
        // SAFETY: We have taken sole ownership of the allocation, preventing a
        // second close even after an error. A failed xOpen may leave pMethods
        // set; SQLite requires xClose in that case. The filename is still owned.
        let rc = unsafe {
            (*raw)
                .pMethods
                .as_ref()
                .and_then(|methods| methods.xClose)
                .map_or(ffi::SQLITE_OK, |close| close(raw))
        };
        // SAFETY: raw came from sqlite3_malloc64; xClose releases file resources
        // but SQLite's contract leaves the file allocation to its caller.
        unsafe { ffi::sqlite3_free(raw.cast()) };
        rc
    }
    pub(super) fn read(&mut self, data: &mut [u8], offset: i64) -> c_int {
        let Ok(amount) = c_int::try_from(data.len()) else {
            return ffi::SQLITE_IOERR_READ;
        };
        if offset < 0 {
            return ffi::SQLITE_IOERR_READ;
        }
        // SAFETY: The live file is exclusively borrowed, and data exposes exactly
        // amount writable bytes for the synchronous parent read, without retention.
        unsafe {
            self.methods()
                .and_then(|m| m.xRead)
                .map_or(ffi::SQLITE_IOERR_READ, |read| {
                    read(self.raw, data.as_mut_ptr().cast(), amount, offset)
                })
        }
    }
    pub(super) fn write(&mut self, data: &[u8], offset: i64) -> c_int {
        let Ok(amount) = c_int::try_from(data.len()) else {
            return ffi::SQLITE_IOERR_WRITE;
        };
        if offset < 0 {
            return ffi::SQLITE_IOERR_WRITE;
        }
        // SAFETY: data contains amount initialized bytes and outlives the
        // synchronous write. The live file is exclusively borrowed.
        unsafe {
            self.methods()
                .and_then(|m| m.xWrite)
                .map_or(ffi::SQLITE_IOERR_WRITE, |write| {
                    write(self.raw, data.as_ptr().cast(), amount, offset)
                })
        }
    }
    pub(super) fn truncate(&mut self, size: i64) -> c_int {
        if size < 0 {
            return ffi::SQLITE_IOERR_TRUNCATE;
        }
        // SAFETY: The owner exclusively borrows its live file; size is nonnegative.
        unsafe {
            self.methods()
                .and_then(|m| m.xTruncate)
                .map_or(ffi::SQLITE_IOERR_TRUNCATE, |f| f(self.raw, size))
        }
    }
    pub(super) fn sync(&mut self, flags: c_int) -> c_int {
        // SAFETY: The owner exclusively borrows its live file. Flags are forwarded
        // unchanged and this callback receives no borrowed data pointers.
        unsafe {
            self.methods()
                .and_then(|m| m.xSync)
                .map_or(ffi::SQLITE_OK, |f| f(self.raw, flags))
        }
    }
    pub(super) fn file_size(&mut self, output: &mut i64) -> c_int {
        // SAFETY: output is an aligned, exclusive i64 reference live for the
        // synchronous callback; the parent file remains exclusively owned.
        unsafe {
            self.methods()
                .and_then(|m| m.xFileSize)
                .map_or(ffi::SQLITE_IOERR_FSTAT, |f| f(self.raw, output))
        }
    }
    pub(super) fn lock(&mut self, level: c_int) -> c_int {
        // SAFETY: The owned file is live and exclusively borrowed; no Rust
        // pointers other than its allocation are passed or retained.
        unsafe {
            self.methods()
                .and_then(|m| m.xLock)
                .map_or(ffi::SQLITE_IOERR_LOCK, |f| f(self.raw, level))
        }
    }
    pub(super) fn unlock(&mut self, level: c_int) -> c_int {
        // SAFETY: The owned file is live and exclusively borrowed; this only
        // invokes the parent's lock transition on that file.
        unsafe {
            self.methods()
                .and_then(|m| m.xUnlock)
                .map_or(ffi::SQLITE_IOERR_UNLOCK, |f| f(self.raw, level))
        }
    }
    pub(super) fn has_moved(&mut self) -> Result<bool, c_int> {
        let mut moved: c_int = 0;
        // SAFETY: HAS_MOVED specifically takes an int output, so moved supplies
        // the required type, alignment and lifetime. The file is exclusive.
        let rc = unsafe {
            self.methods()
                .and_then(|m| m.xFileControl)
                .map_or(ffi::SQLITE_NOTFOUND, |f| {
                    f(
                        self.raw,
                        ffi::SQLITE_FCNTL_HAS_MOVED,
                        (&raw mut moved).cast::<c_void>(),
                    )
                })
        };
        if matches!(rc, ffi::SQLITE_OK | ffi::SQLITE_NOTFOUND) {
            Ok(moved != 0)
        } else {
            Err(rc)
        }
    }
    pub(super) fn supports_shm_lock(&self) -> bool {
        self.methods()
            .is_some_and(|m| m.iVersion >= 2 && m.xShmLock.is_some())
    }
    pub(super) fn shm_lock(&mut self, offset: c_int, count: c_int, flags: c_int) -> c_int {
        let valid_flags = [
            ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_SHARED,
            ffi::SQLITE_SHM_LOCK | ffi::SQLITE_SHM_EXCLUSIVE,
            ffi::SQLITE_SHM_UNLOCK | ffi::SQLITE_SHM_SHARED,
            ffi::SQLITE_SHM_UNLOCK | ffi::SQLITE_SHM_EXCLUSIVE,
        ];
        if offset < 0
            || count <= 0
            || offset
                .checked_add(count)
                .is_none_or(|end| end > ffi::SQLITE_SHM_NLOCK)
            || !valid_flags.contains(&flags)
            || (count != 1 && flags & ffi::SQLITE_SHM_SHARED != 0)
        {
            return ffi::SQLITE_IOERR_SHMLOCK;
        }
        // SAFETY: Only a version-2+ method is called on the exclusively borrowed
        // parent file. Numeric lock parameters carry no borrowed Rust memory.
        unsafe {
            self.methods()
                .filter(|m| m.iVersion >= 2)
                .and_then(|m| m.xShmLock)
                .map_or(ffi::SQLITE_IOERR_SHMLOCK, |f| {
                    f(self.raw, offset, count, flags)
                })
        }
    }
}
impl Drop for ParentFile {
    fn drop(&mut self) {
        let _ = self.close_inner();
    }
}

#[cfg(all(test, feature = "static"))]
mod tests {
    use super::*;

    struct Context {
        closes: usize,
        install_methods: bool,
        open_result: c_int,
        close_result: c_int,
        saw_uri_at_close: bool,
    }
    #[repr(C)]
    struct TestFile {
        base: ffi::sqlite3_file,
        context: *mut Context,
        filename: *const c_char,
    }
    // SAFETY: sqlite3_io_methods is a C struct of integer flags and nullable
    // function pointers, all valid when zeroed. Only xClose is exercised here.
    static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
        iVersion: 1,
        xClose: Some(close),
        ..unsafe { std::mem::zeroed() }
    };
    /// # Safety
    /// vfs points to the local test VFS with `Context` as `pAppData`, file has space
    /// for `TestFile`, and name remains alive through the matching close.
    unsafe extern "C" fn open(
        vfs: *mut ffi::sqlite3_vfs,
        name: *const c_char,
        file: *mut ffi::sqlite3_file,
        _flags: c_int,
        _out: *mut c_int,
    ) -> c_int {
        // SAFETY: The test supplies its live local context and ParentFile's
        // allocation is sized for TestFile. write initializes all fields before
        // any close callback can observe them; name is retained by the owner.
        unsafe {
            let context = (*vfs).pAppData.cast::<Context>();
            file.cast::<TestFile>().write(TestFile {
                base: ffi::sqlite3_file {
                    pMethods: if (*context).install_methods {
                        &raw const METHODS
                    } else {
                        ptr::null()
                    },
                },
                context,
                filename: name,
            });
            (*context).open_result
        }
    }
    /// # Safety
    /// file is the exclusive `TestFile` initialized by open; its context and
    /// optional framed filename remain alive until this callback returns.
    unsafe extern "C" fn close(file: *mut ffi::sqlite3_file) -> c_int {
        // SAFETY: ParentFile calls close before freeing either the TestFile
        // allocation or its copied filename. The test retains Context until
        // the owner is dropped. URI lookup only borrows the framed filename.
        unsafe {
            let file = &*file.cast::<TestFile>();
            let context = &mut *file.context;
            context.closes += 1;
            if !file.filename.is_null() {
                let value = ffi::sqlite3_uri_parameter(file.filename, c"training".as_ptr());
                context.saw_uri_at_close = !value.is_null() && CStr::from_ptr(value) == c"example";
            }
            context.close_result
        }
    }
    fn context() -> Context {
        Context {
            closes: 0,
            install_methods: true,
            open_result: ffi::SQLITE_OK,
            close_result: ffi::SQLITE_OK,
            saw_uri_at_close: false,
        }
    }
    fn vfs(context: &mut Context) -> ffi::sqlite3_vfs {
        // SAFETY: This C struct contains integers and nullable pointers, whose
        // zero values are valid. ParentFile needs only the explicitly set fields.
        ffi::sqlite3_vfs {
            iVersion: 1,
            szOsFile: c_int::try_from(size_of::<TestFile>()).unwrap(),
            pAppData: ptr::from_mut(context).cast(),
            xOpen: Some(open),
            ..unsafe { std::mem::zeroed() }
        }
    }
    #[test]
    fn failed_open_closes_only_when_parent_installed_methods() {
        for installed in [false, true] {
            let mut context = context();
            context.install_methods = installed;
            context.open_result = ffi::SQLITE_CANTOPEN;
            let mut vfs = vfs(&mut context);
            // SAFETY: The local VFS/context outlive the temporary owner and its
            // cleanup; our fake parent accepts an unnamed file with these flags.
            let result = unsafe { ParentFile::open(&raw mut vfs, ptr::null(), None, 0, None) };
            assert!(matches!(result, Err(ffi::SQLITE_CANTOPEN)));
            assert_eq!(context.closes, usize::from(installed));
        }
    }
    #[test]
    fn close_error_consumes_owner_and_preserves_filename_until_close() {
        let mut context = context();
        context.close_result = ffi::SQLITE_IOERR_CLOSE;
        let mut vfs = vfs(&mut context);
        let mut parameters = [c"training".as_ptr(), c"example".as_ptr()];
        // SAFETY: All four C strings and both parameter pointers are live;
        // create_filename copies them into an allocation owned by SqliteFilename.
        let original = SqliteFilename(unsafe {
            ffi::sqlite3_create_filename(
                c"/db".as_ptr(),
                c"/db-journal".as_ptr(),
                c"/db-wal".as_ptr(),
                1,
                parameters.as_mut_ptr(),
            )
        });
        assert!(!original.0.is_null());
        // SAFETY: original carries genuine SQLite framing and lives through
        // remapping; VFS/context remain live until explicit owner consumption.
        let file = unsafe {
            ParentFile::open(
                &raw mut vfs,
                original.0,
                Some(c"/coord"),
                ffi::SQLITE_OPEN_MAIN_DB,
                None,
            )
        }
        .unwrap();
        drop(original);
        assert_eq!(file.close(), ffi::SQLITE_IOERR_CLOSE);
        assert_eq!(context.closes, 1);
        assert!(context.saw_uri_at_close);
    }
    #[test]
    fn callback_panic_drops_parent_before_returning_error() {
        let mut context = context();
        let mut vfs = vfs(&mut context);
        let rc = super::super::ffi_guard(|| {
            // SAFETY: This local VFS/context outlive the owner through unwinding;
            // the fake parent accepts an unnamed file and retains no other data.
            let _file =
                unsafe { ParentFile::open(&raw mut vfs, ptr::null(), None, 0, None) }.unwrap();
            panic!("injected callback panic");
        });
        assert_eq!(rc, ffi::SQLITE_IOERR);
        assert_eq!(context.closes, 1);
    }
    #[test]
    fn rejected_parent_methods_close_before_failed_shim_open_returns() {
        use super::super::{AppData, ZFile, x_open};
        let mut context = context();
        let mut parent = vfs(&mut context);
        let app = AppData {
            parent: &raw mut parent,
            storage: None,
        };
        let mut shim = parent;
        shim.szOsFile = c_int::try_from(size_of::<ZFile>()).unwrap();
        shim.pAppData = (&raw const app).cast_mut().cast();
        let mut output = std::mem::MaybeUninit::<ZFile>::uninit();
        // SAFETY: The local shim has our AppData and an aligned ZFile allocation.
        // Context and parent outlive the complete open/failure cleanup. Our fake
        // parent accepts null names and exposes only xClose, so validation fails.
        let rc = unsafe {
            x_open(
                &raw mut shim,
                ptr::null(),
                output.as_mut_ptr().cast(),
                0,
                null_mut(),
            )
        };
        assert_eq!(rc, ffi::SQLITE_IOERR);
        assert_eq!(context.closes, 1);
        // SAFETY: xOpen initializes ZFile before performing any fallible work.
        // Its method table may remain uninitialized inside MaybeUninit, but the
        // outer struct and its closed-state pointer fields are initialized.
        let output = unsafe { output.assume_init() };
        assert!(output.base.pMethods.is_null());
        assert!(output.state.is_null());
    }
}
