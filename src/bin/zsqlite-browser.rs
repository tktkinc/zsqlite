//! Browser-only bridge. The worker supplies immutable storage reads.
#[cfg(target_os = "emscripten")]
mod browser {
    use libsqlite3_sys as ffi;
    use std::ffi::{CStr, CString, c_char, c_int};
    use std::sync::{Arc, Mutex};
    use zsqlite::domain::{BackendId, StoredBytes};
    use zsqlite::storage::adapter::{
        BackendError, DeletePermit, MAX_BATCH_BYTES, MAX_ROOT_BYTES, ObjectKey, ObjectRange,
        ObjectWriter, Publication, Revision, RootRecord, StorageBackend,
    };

    unsafe extern "C" {
        fn zsqlite_http_read(
            path: *const c_char,
            offset: f64,
            length: u32,
            output: *mut u8,
        ) -> c_int;
        fn zsqlite_http_stat(path: *const c_char) -> f64;
        fn zsqlite_http_read_many(requests: *const c_char, length: u32, output: *mut u8) -> c_int;
    }
    static ERROR: Mutex<Option<CString>> = Mutex::new(None);
    struct HttpBackend;

    unsafe extern "C" fn authorize(
        _context: *mut std::ffi::c_void,
        action: c_int,
        first: *const c_char,
        second: *const c_char,
        _database: *const c_char,
        _trigger: *const c_char,
    ) -> c_int {
        match action {
            ffi::SQLITE_SELECT
            | ffi::SQLITE_READ
            | ffi::SQLITE_FUNCTION
            | ffi::SQLITE_RECURSIVE => ffi::SQLITE_OK,
            ffi::SQLITE_PRAGMA if !first.is_null() => {
                // Some setters run during prepare, before stmt_readonly can
                // reject them. Authorize only read-only introspection pragmas.
                // SAFETY: SQLite borrows terminated authorizer arguments during this call.
                let name = unsafe { CStr::from_ptr(first) }.to_bytes();
                let argument_queries: &[&[u8]] = &[
                    b"table_info",
                    b"table_xinfo",
                    b"table_list",
                    b"index_info",
                    b"index_xinfo",
                    b"index_list",
                    b"foreign_key_list",
                    b"foreign_key_check",
                    b"integrity_check",
                    b"quick_check",
                ];
                let scalar_queries: &[&[u8]] = &[
                    b"compile_options",
                    b"database_list",
                    b"collation_list",
                    b"function_list",
                    b"module_list",
                    b"pragma_list",
                    b"page_size",
                    b"page_count",
                    b"freelist_count",
                    b"schema_version",
                    b"user_version",
                    b"application_id",
                    b"encoding",
                    b"query_only",
                    b"cache_size",
                    b"temp_store",
                    b"journal_mode",
                    b"data_version",
                    b"auto_vacuum",
                    b"secure_delete",
                    b"synchronous",
                    b"mmap_size",
                    b"locking_mode",
                    b"busy_timeout",
                ];
                if argument_queries
                    .iter()
                    .any(|allowed| name.eq_ignore_ascii_case(allowed))
                    || (second.is_null()
                        && scalar_queries
                            .iter()
                            .any(|allowed| name.eq_ignore_ascii_case(allowed)))
                {
                    ffi::SQLITE_OK
                } else {
                    ffi::SQLITE_DENY
                }
            }
            _ => ffi::SQLITE_DENY,
        }
    }
    fn transport() -> BackendError {
        BackendError::Transport {
            message: "bucket read failed (see worker transport error)".into(),
            retryable: false,
        }
    }
    fn readonly() -> BackendError {
        BackendError::Transport {
            message: "browser bucket is read-only".into(),
            retryable: false,
        }
    }
    // The explicit JavaScript safe-integer bound below makes this conversion exact.
    #[allow(clippy::cast_precision_loss)]
    fn read(path: &str, offset: u64, length: usize) -> Result<Vec<u8>, BackendError> {
        if offset > 9_007_199_254_740_991 || length as u64 > MAX_BATCH_BYTES {
            return Err(BackendError::Range);
        }
        let path = CString::new(path).map_err(|_| BackendError::InvalidData)?;
        let mut bytes = vec![0; length];
        // SAFETY: The worker callback borrows the terminated path and output
        // while Asyncify suspends this call, writes at most length bytes, and
        // retains neither pointer once the native call resumes.
        let count = unsafe {
            zsqlite_http_read(
                path.as_ptr(),
                offset as f64,
                u32::try_from(length).map_err(|_| BackendError::Range)?,
                bytes.as_mut_ptr(),
            )
        };
        let count = usize::try_from(count).map_err(|_| transport())?;
        if count > length {
            return Err(BackendError::InvalidData);
        }
        bytes.truncate(count);
        Ok(bytes)
    }
    impl StorageBackend for HttpBackend {
        fn identity(&self) -> BackendId {
            BackendId::from_bytes(*blake3::hash(b"zsqlite/browser/private-worker").as_bytes())
        }
        fn begin_write(&self) -> Result<Box<dyn ObjectWriter + '_>, BackendError> {
            Err(readonly())
        }
        fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError> {
            let total = requests.iter().try_fold(0_u64, |total, r| {
                total
                    .checked_add(r.range().length().get())
                    .ok_or(BackendError::Range)
            })?;
            if requests.is_empty() || requests.len() > 4096 || total > MAX_BATCH_BYTES {
                return Err(BackendError::Range);
            }
            let descriptions = requests
                .iter()
                .map(|request| {
                    let offset = request.range().offset().get();
                    let length = request.range().length().get();
                    if offset
                        .checked_add(length)
                        .is_none_or(|end| end > 9_007_199_254_740_991)
                    {
                        return Err(BackendError::Range);
                    }
                    // Object filenames contain only hex digits and a fixed extension.
                    Ok(format!(
                        "{{\"path\":\"objects/{}\",\"offset\":{offset},\"length\":{length}}}",
                        request.key().file_name()
                    ))
                })
                .collect::<Result<Vec<_>, BackendError>>()?;
            let descriptions = CString::new(format!("[{}]", descriptions.join(",")))
                .map_err(|_| BackendError::InvalidData)?;
            let mut bytes = vec![0; usize::try_from(total).map_err(|_| BackendError::Range)?];
            // SAFETY: The request string and output stay live while Asyncify
            // suspends. JavaScript validates all results before copying exactly
            // total bytes into the output; it retains neither pointer afterward.
            let count = unsafe {
                zsqlite_http_read_many(
                    descriptions.as_ptr(),
                    u32::try_from(total).map_err(|_| BackendError::Range)?,
                    bytes.as_mut_ptr(),
                )
            };
            if usize::try_from(count).map_err(|_| transport())? != bytes.len() {
                return Err(BackendError::InvalidData);
            }
            if requests.len() == 1 {
                return Ok(vec![bytes]);
            }
            let mut start = 0;
            requests
                .iter()
                .map(|request| {
                    let end = start
                        + request
                            .range()
                            .length()
                            .as_usize()
                            .map_err(|_| BackendError::Range)?;
                    let result = bytes[start..end].to_vec();
                    start = end;
                    Ok(result)
                })
                .collect()
        }
        // The bridge returns exact integer sentinels; all nonnegative values
        // are checked for sign, finiteness, integrality and the JS integer bound.
        #[allow(
            clippy::float_cmp,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        fn stat(&self, key: ObjectKey) -> Result<Option<StoredBytes>, BackendError> {
            let path = CString::new(format!("objects/{}", key.file_name())).expect("object name");
            // SAFETY: The terminated path remains live while Asyncify suspends
            // this call; JavaScript retains no pointer once it resumes.
            let length = unsafe { zsqlite_http_stat(path.as_ptr()) };
            if length == -1.0 {
                return Ok(None);
            }
            if !length.is_finite()
                || !(0.0..=9_007_199_254_740_991.0).contains(&length)
                || length.fract() != 0.0
            {
                return Err(transport());
            }
            Ok(Some(StoredBytes::new(length as u64)))
        }
        fn read_root(&self) -> Result<Option<RootRecord>, BackendError> {
            let bytes = read("catalog-head", 0, MAX_ROOT_BYTES + 72)?;
            if bytes.is_empty() {
                return Ok(None);
            }
            RootRecord::from_file_bytes(&bytes).map(Some)
        }
        fn compare_exchange_root(
            &self,
            _expected: Option<&Revision>,
            _bytes: &[u8],
        ) -> Result<Publication, BackendError> {
            Err(readonly())
        }
        fn inventory(
            &self,
            _after: Option<ObjectKey>,
            _limit: usize,
        ) -> Result<Vec<ObjectKey>, BackendError> {
            Err(readonly())
        }
        fn delete(&self, _permit: DeletePermit<'_>) -> Result<(), BackendError> {
            Err(readonly())
        }
    }

    /// The worker calls this once per instance, then closes `SQLite` and terminates
    /// the worker to release its VFS registration and private filesystem.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn zsqlite_browser_open(
        head: *const c_char,
        cache_bytes: u32,
    ) -> *mut ffi::sqlite3 {
        let result = (|| {
            // SAFETY: The JS bridge provides a terminated UTF-8 string for this call.
            let head = unsafe { CStr::from_ptr(head) }
                .to_str()
                .map_err(|e| e.to_string())?;
            let storage = zsqlite::Storage::new(Arc::new(HttpBackend), "/zsqlite-coordination")
                .and_then(|storage| storage.head(head))
                .map_err(|e| e.to_string())?;
            storage
                .register_read_only_vfs("bucket", cache_bytes as usize)
                .map_err(|e| e.to_string())?;
            let mut database = std::ptr::null_mut();
            // SAFETY: Constant C strings and the exclusive output slot remain
            // live. SQLite initializes the owned connection even on open failure.
            let rc = unsafe {
                ffi::sqlite3_open_v2(
                    c"file:/snapshot?immutable=1".as_ptr(),
                    &raw mut database,
                    ffi::SQLITE_OPEN_READONLY | ffi::SQLITE_OPEN_URI,
                    c"bucket".as_ptr(),
                )
            };
            if rc != ffi::SQLITE_OK {
                // SAFETY: SQLite initialized database; copy its message before closing.
                let error = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(database)) }
                    .to_string_lossy()
                    .into_owned();
                // SAFETY: No statements exist and this owns the live handle.
                unsafe {
                    ffi::sqlite3_close(database);
                }
                return Err(error);
            }
            // SAFETY: Live exclusively owned connection and constant SQL; no callbacks retained.
            let rc = unsafe {
                ffi::sqlite3_exec(
                    database,
                    c"PRAGMA query_only=ON; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-2048;"
                        .as_ptr(),
                    None,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            };
            if rc != ffi::SQLITE_OK {
                // SAFETY: Live owned connection, no statements remain after exec.
                unsafe {
                    ffi::sqlite3_close(database);
                }
                return Err("cannot configure read-only SQLite".into());
            }
            // SAFETY: The callback is a static function, retains no borrowed
            // pointers, and is installed on this exclusively owned connection.
            let rc = unsafe {
                ffi::sqlite3_set_authorizer(database, Some(authorize), std::ptr::null_mut())
            };
            if rc != ffi::SQLITE_OK {
                // SAFETY: Live owned connection without outstanding statements.
                unsafe {
                    ffi::sqlite3_close(database);
                }
                return Err("cannot authorize read-only queries".into());
            }
            Ok(database)
        })();
        match result {
            Ok(database) => database,
            Err(error) => {
                *ERROR.lock().expect("error lock") =
                    Some(CString::new(error.replace('\0', "")).expect("sanitized error"));
                std::ptr::null_mut()
            }
        }
    }
    #[unsafe(no_mangle)]
    pub extern "C" fn zsqlite_browser_error() -> *const c_char {
        ERROR
            .lock()
            .expect("error lock")
            .as_ref()
            .map_or(c"unknown browser error".as_ptr(), |error| error.as_ptr())
    }
}

fn main() {}
