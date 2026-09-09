use super::*;
use std::ffi::{CStr, CString};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::time::{Duration, Instant};

const ZSQLITE_VFS: &CStr = c"zsqlite";

static EXPECTED_PARENT_VFS: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(null_mut());
static EXPECTED_PARENT_APP_DATA: AtomicPtr<c_void> = AtomicPtr::new(null_mut());
static PARENT_CONTEXT_CALLS: AtomicUsize = AtomicUsize::new(0);

const PARENT_OPEN: usize = 1 << 0;
const PARENT_DELETE: usize = 1 << 1;
const PARENT_ACCESS: usize = 1 << 2;
const PARENT_FULL_PATHNAME: usize = 1 << 3;
const PARENT_DL_OPEN: usize = 1 << 4;
const PARENT_DL_ERROR: usize = 1 << 5;
const PARENT_DL_SYM: usize = 1 << 6;
const PARENT_DL_CLOSE: usize = 1 << 7;
const PARENT_RANDOMNESS: usize = 1 << 8;
const PARENT_SLEEP: usize = 1 << 9;
const PARENT_CURRENT_TIME: usize = 1 << 10;
const PARENT_LAST_ERROR: usize = 1 << 11;
const PARENT_CURRENT_TIME_I64: usize = 1 << 12;
const PARENT_SET_SYSTEM_CALL: usize = 1 << 13;
const PARENT_GET_SYSTEM_CALL: usize = 1 << 14;
const PARENT_NEXT_SYSTEM_CALL: usize = 1 << 15;
const ALL_PARENT_CONTEXT_CALLS: usize = (1 << 16) - 1;

fn record_parent_context(vfs: *mut ffi::sqlite3_vfs, callback: usize) -> bool {
    if vfs != EXPECTED_PARENT_VFS.load(Ordering::Acquire) {
        return false;
    }
    // SAFETY: Equality with EXPECTED_PARENT_VFS establishes that `vfs` is the
    // live parent object installed by the test for the duration of each call.
    if unsafe { vfs.as_ref() }.map(|vfs| vfs.pAppData)
        != Some(EXPECTED_PARENT_APP_DATA.load(Ordering::Acquire))
    {
        return false;
    }
    PARENT_CONTEXT_CALLS.fetch_or(callback, Ordering::AcqRel);
    true
}

unsafe extern "C" fn context_checking_open(
    vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
    _output: *mut ffi::sqlite3_file,
    _flags: c_int,
    _out_flags: *mut c_int,
) -> c_int {
    if record_parent_context(vfs, PARENT_OPEN) {
        ffi::SQLITE_CANTOPEN
    } else {
        ffi::SQLITE_MISUSE
    }
}

unsafe extern "C" fn context_checking_delete(
    vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
    _sync_dir: c_int,
) -> c_int {
    if record_parent_context(vfs, PARENT_DELETE) {
        ffi::SQLITE_OK
    } else {
        ffi::SQLITE_MISUSE
    }
}

unsafe extern "C" fn context_checking_access(
    vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
    _flags: c_int,
    _output: *mut c_int,
) -> c_int {
    if record_parent_context(vfs, PARENT_ACCESS) {
        ffi::SQLITE_OK
    } else {
        ffi::SQLITE_MISUSE
    }
}

unsafe extern "C" fn context_checking_full_pathname(
    vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
    _output_size: c_int,
    _output: *mut c_char,
) -> c_int {
    if record_parent_context(vfs, PARENT_FULL_PATHNAME) {
        ffi::SQLITE_OK
    } else {
        ffi::SQLITE_MISUSE
    }
}

unsafe extern "C" fn context_checking_dl_open(
    vfs: *mut ffi::sqlite3_vfs,
    _filename: *const c_char,
) -> *mut c_void {
    if record_parent_context(vfs, PARENT_DL_OPEN) {
        std::ptr::dangling_mut()
    } else {
        null_mut()
    }
}

unsafe extern "C" fn context_checking_dl_error(
    vfs: *mut ffi::sqlite3_vfs,
    _output_size: c_int,
    _output: *mut c_char,
) {
    record_parent_context(vfs, PARENT_DL_ERROR);
}

unsafe extern "C" fn context_checking_dl_entry(
    _vfs: *mut ffi::sqlite3_vfs,
    _handle: *mut c_void,
    _symbol: *const c_char,
) {
}

unsafe extern "C" fn context_checking_dl_sym(
    vfs: *mut ffi::sqlite3_vfs,
    _handle: *mut c_void,
    _symbol: *const c_char,
) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
    record_parent_context(vfs, PARENT_DL_SYM).then_some(context_checking_dl_entry)
}

unsafe extern "C" fn context_checking_dl_close(vfs: *mut ffi::sqlite3_vfs, _handle: *mut c_void) {
    record_parent_context(vfs, PARENT_DL_CLOSE);
}

unsafe extern "C" fn context_checking_randomness(
    vfs: *mut ffi::sqlite3_vfs,
    _amount: c_int,
    _output: *mut c_char,
) -> c_int {
    if record_parent_context(vfs, PARENT_RANDOMNESS) {
        7
    } else {
        -1
    }
}

unsafe extern "C" fn context_checking_sleep(
    vfs: *mut ffi::sqlite3_vfs,
    microseconds: c_int,
) -> c_int {
    if record_parent_context(vfs, PARENT_SLEEP) {
        microseconds
    } else {
        -1
    }
}

unsafe extern "C" fn context_checking_current_time(
    vfs: *mut ffi::sqlite3_vfs,
    output: *mut f64,
) -> c_int {
    if !record_parent_context(vfs, PARENT_CURRENT_TIME) {
        return ffi::SQLITE_MISUSE;
    }
    if let Some(output) = unsafe { output.as_mut() } {
        *output = 17.0;
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn context_checking_last_error(
    vfs: *mut ffi::sqlite3_vfs,
    _output_size: c_int,
    _output: *mut c_char,
) -> c_int {
    if record_parent_context(vfs, PARENT_LAST_ERROR) {
        19
    } else {
        -1
    }
}

unsafe extern "C" fn context_checking_current_time_i64(
    vfs: *mut ffi::sqlite3_vfs,
    output: *mut ffi::sqlite3_int64,
) -> c_int {
    if !record_parent_context(vfs, PARENT_CURRENT_TIME_I64) {
        return ffi::SQLITE_MISUSE;
    }
    if let Some(output) = unsafe { output.as_mut() } {
        *output = 23;
    }
    ffi::SQLITE_OK
}

unsafe extern "C" fn context_checking_system_call() {}

unsafe extern "C" fn context_checking_set_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
    _call: ffi::sqlite3_syscall_ptr,
) -> c_int {
    if record_parent_context(vfs, PARENT_SET_SYSTEM_CALL) {
        29
    } else {
        -1
    }
}

unsafe extern "C" fn context_checking_get_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
) -> ffi::sqlite3_syscall_ptr {
    record_parent_context(vfs, PARENT_GET_SYSTEM_CALL).then_some(context_checking_system_call)
}

unsafe extern "C" fn context_checking_next_system_call(
    vfs: *mut ffi::sqlite3_vfs,
    _name: *const c_char,
) -> *const c_char {
    if record_parent_context(vfs, PARENT_NEXT_SYSTEM_CALL) {
        c"next".as_ptr()
    } else {
        null()
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Value {
    Null,
    Integer(i64),
    Float(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

struct Connection(*mut ffi::sqlite3);

impl Connection {
    fn open(path: &Path) -> Result<Self, String> {
        Self::open_with_vfs_and_flags(
            path,
            ZSQLITE_VFS,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
        )
    }

    fn open_native(path: &Path) -> Result<Self, String> {
        let vfs = native_vfs_name()?;
        Self::open_with_vfs_and_flags(
            path,
            &vfs,
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
        )
    }

    fn open_read_only(path: &Path) -> Result<Self, String> {
        Self::open_with_vfs_and_flags(path, ZSQLITE_VFS, ffi::SQLITE_OPEN_READONLY)
    }

    fn open_with_vfs_and_flags(path: &Path, vfs: &CStr, flags: c_int) -> Result<Self, String> {
        #[cfg(unix)]
        let path = CString::new(path.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
        Self::open_filename_with_vfs_and_flags(&path, vfs, flags)
    }

    fn open_filename_with_vfs_and_flags(
        path: &CStr,
        vfs: &CStr,
        flags: c_int,
    ) -> Result<Self, String> {
        let mut database = null_mut();
        let rc =
            unsafe { ffi::sqlite3_open_v2(path.as_ptr(), &raw mut database, flags, vfs.as_ptr()) };
        if rc == ffi::SQLITE_OK {
            let connection = Self(database);
            let timeout_rc = unsafe { ffi::sqlite3_busy_timeout(connection.0, 10_000) };
            if timeout_rc != ffi::SQLITE_OK {
                return Err(connection.error("setting busy timeout", timeout_rc));
            }
            Ok(connection)
        } else {
            let message = if database.is_null() {
                "open failed".into()
            } else {
                unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(database)) }
                    .to_string_lossy()
                    .into_owned()
            };
            if !database.is_null() {
                unsafe { ffi::sqlite3_close(database) };
            }
            Err(format!("SQLite error {rc}: {message}"))
        }
    }

    fn execute(&self, sql: &str) -> Result<(), String> {
        self.execute_with_code(sql)
            .map_err(|(rc, message)| format!("SQLite error {rc}: {message}"))
    }

    fn execute_with_code(&self, sql: &str) -> Result<(), (c_int, String)> {
        let sql = CString::new(sql).map_err(|error| (-1, error.to_string()))?;
        let mut error = null_mut();
        let rc =
            unsafe { ffi::sqlite3_exec(self.0, sql.as_ptr(), None, null_mut(), &raw mut error) };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            let message = if error.is_null() {
                unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) }
                    .to_string_lossy()
                    .into_owned()
            } else {
                let message = unsafe { CStr::from_ptr(error) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { ffi::sqlite3_free(error.cast()) };
                message
            };
            Err((rc, message))
        }
    }

    fn busy_timeout(&self, milliseconds: c_int) -> Result<(), String> {
        let rc = unsafe { ffi::sqlite3_busy_timeout(self.0, milliseconds) };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(self.error("setting busy timeout", rc))
        }
    }

    fn text(&self, sql: &str) -> Result<String, String> {
        match self.rows(sql)?.as_slice() {
            [row] => match row.as_slice() {
                [Value::Text(value)] => String::from_utf8(value.clone()).map_err(|e| e.to_string()),
                [Value::Integer(value)] => Ok(value.to_string()),
                _ => Err(format!("query returned an unexpected value: {row:?}")),
            },
            rows => Err(format!("query returned {} rows, expected one", rows.len())),
        }
    }

    fn integer(&self, sql: &str) -> Result<i64, String> {
        match self.rows(sql)?.as_slice() {
            [row] if matches!(row.as_slice(), [Value::Integer(_)]) => {
                let [Value::Integer(value)] = row.as_slice() else {
                    unreachable!();
                };
                Ok(*value)
            }
            rows => Err(format!("query returned unexpected rows: {rows:?}")),
        }
    }

    fn rows(&self, sql: &str) -> Result<Vec<Vec<Value>>, String> {
        let sql = CString::new(sql).map_err(|e| e.to_string())?;
        let mut statement = null_mut();
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(self.0, sql.as_ptr(), -1, &raw mut statement, null_mut())
        };
        if rc != ffi::SQLITE_OK {
            return Err(self.error("prepare", rc));
        }
        let result = (|| {
            let mut rows = Vec::new();
            loop {
                let rc = unsafe { ffi::sqlite3_step(statement) };
                if rc == ffi::SQLITE_DONE {
                    return Ok(rows);
                }
                if rc != ffi::SQLITE_ROW {
                    return Err(self.error("step", rc));
                }
                let columns = unsafe { ffi::sqlite3_column_count(statement) };
                let capacity = usize::try_from(columns)
                    .map_err(|_| "SQLite returned a negative column count")?;
                let mut row = Vec::with_capacity(capacity);
                for column in 0..columns {
                    row.push(unsafe { column_value(statement, column) }?);
                }
                rows.push(row);
            }
        })();
        let finalize_rc = unsafe { ffi::sqlite3_finalize(statement) };
        if finalize_rc != ffi::SQLITE_OK {
            return Err(self.error("finalize", finalize_rc));
        }
        result
    }

    fn error(&self, operation: &str, rc: c_int) -> String {
        let message = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) }.to_string_lossy();
        format!("{operation} failed with SQLite error {rc}: {message}")
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let rc = unsafe { ffi::sqlite3_close(self.0) };
        assert_eq!(rc, ffi::SQLITE_OK);
    }
}

unsafe fn column_value(statement: *mut ffi::sqlite3_stmt, column: c_int) -> Result<Value, String> {
    match unsafe { ffi::sqlite3_column_type(statement, column) } {
        ffi::SQLITE_NULL => Ok(Value::Null),
        ffi::SQLITE_INTEGER => Ok(Value::Integer(unsafe {
            ffi::sqlite3_column_int64(statement, column)
        })),
        ffi::SQLITE_FLOAT => Ok(Value::Float(
            unsafe { ffi::sqlite3_column_double(statement, column) }.to_bits(),
        )),
        ffi::SQLITE_TEXT | ffi::SQLITE_BLOB => {
            let kind = unsafe { ffi::sqlite3_column_type(statement, column) };
            let length = unsafe { ffi::sqlite3_column_bytes(statement, column) };
            if length < 0 {
                return Err("SQLite returned a negative column length".into());
            }
            let data = if length == 0 {
                Vec::new()
            } else {
                let pointer = if kind == ffi::SQLITE_TEXT {
                    unsafe { ffi::sqlite3_column_text(statement, column) }.cast::<u8>()
                } else {
                    unsafe { ffi::sqlite3_column_blob(statement, column) }.cast::<u8>()
                };
                if pointer.is_null() {
                    return Err("SQLite returned a null pointer for non-null data".into());
                }
                let length = usize::try_from(length)
                    .map_err(|_| "SQLite returned a negative column length")?;
                unsafe { slice::from_raw_parts(pointer, length) }.to_vec()
            };
            if kind == ffi::SQLITE_TEXT {
                Ok(Value::Text(data))
            } else {
                Ok(Value::Blob(data))
            }
        }
        kind => Err(format!("unknown SQLite column type {kind}")),
    }
}

fn register() -> Result<(), String> {
    register_static_vfs().map_err(|rc| format!("VFS registration failed: {rc}"))
}

fn native_vfs_name() -> Result<CString, String> {
    let zsqlite = unsafe { ffi::sqlite3_vfs_find(VFS_NAME.as_ptr().cast()) };
    if let Some(app) = unsafe { app_data(zsqlite) } {
        let parent = unsafe { app.parent.as_ref() }.ok_or("zsqlite parent VFS is null")?;
        return Ok(unsafe { CStr::from_ptr(parent.zName) }.to_owned());
    }
    let default = unsafe { ffi::sqlite3_vfs_find(null()) };
    let default = unsafe { default.as_ref() }.ok_or("no default SQLite VFS")?;
    Ok(unsafe { CStr::from_ptr(default.zName) }.to_owned())
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn wal_path(path: &Path) -> PathBuf {
    append_suffix(path, "-wal")
}

fn assert_integrity(connection: &Connection) -> Result<(), String> {
    let rows = connection.rows("PRAGMA integrity_check")?;
    if rows == vec![vec![Value::Text(b"ok".to_vec())]] {
        Ok(())
    } else {
        Err(format!("integrity_check returned {rows:?}"))
    }
}

fn assert_busy(result: Result<(), (c_int, String)>) {
    let (rc, message) = result.expect_err("operation unexpectedly acquired a database lock");
    let primary = rc & 0xff;
    assert!(
        matches!(primary, ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED),
        "expected SQLITE_BUSY or SQLITE_LOCKED, got {rc}: {message}"
    );
}

#[test]
fn all_authenticated_storage_corruption_maps_to_sqlite_ioerr_data() {
    for error in [
        crate::StoreError::PageChecksum(7),
        crate::StoreError::Corrupt(8192),
        crate::StoreError::Format(crate::format::FormatError::Checksum),
        crate::StoreError::IdentityMismatch,
        crate::StoreError::Zstd("damaged compressed frame".into()),
    ] {
        assert_eq!(
            sqlite_result(&error),
            ffi::SQLITE_IOERR_DATA,
            "wrong SQLite result for {error}"
        );
    }
}

#[test]
fn db_facade_is_a_read_only_native_notice() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    register()?;
    let logical = directory.path().join("application.db");
    let storage = append_suffix(&logical, ".zsqlite");
    let storage_wal = append_suffix(&storage, "-wal");
    let native_wal = append_suffix(&logical, "-wal");

    let writer = Connection::open(&logical)?;
    assert_eq!(writer.text("PRAGMA journal_mode=WAL")?, "wal");
    writer.execute(
        "CREATE TABLE actual_data(value TEXT NOT NULL);
         INSERT INTO actual_data VALUES('stored by zsqlite');",
    )?;

    assert_eq!(std::fs::read(&storage)?[..8], *b"ZSQLSE06");
    assert_eq!(std::fs::metadata(&logical)?.len(), 4096);
    assert!(storage_wal.exists());
    assert!(!native_wal.exists());

    let native = Connection::open_native(&logical)?;
    assert_eq!(
        native.text("SELECT message FROM zsqlite_extension_required")?,
        "This database uses zsqlite storage. Load the zsqlite extension and reopen with vfs=zsqlite."
    );
    assert_integrity(&native)?;
    let Err((write_rc, write_error)) =
        native.execute_with_code("CREATE TABLE accidental_write(value)")
    else {
        return Err("the native notice database accepted a write".into());
    };
    assert_eq!(write_rc & 0xff, ffi::SQLITE_READONLY, "{write_error}");
    assert!(
        native
            .execute_with_code("SELECT * FROM actual_data")
            .is_err(),
        "the native notice exposed the real schema"
    );
    assert_eq!(
        writer.text("SELECT value FROM actual_data")?,
        "stored by zsqlite"
    );
    Ok(())
}

#[test]
fn db_facade_never_overwrites_an_existing_native_database() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    register()?;
    let logical = directory.path().join("existing.db");
    let native = Connection::open_native(&logical)?;
    native.execute("CREATE TABLE native_data(value)")?;
    drop(native);

    let Err(error) = Connection::open(&logical) else {
        return Err("zsqlite replaced a native database".into());
    };
    assert!(
        error.contains("SQLite error 14"),
        "unexpected error: {error}"
    );
    assert!(!append_suffix(&logical, ".zsqlite").exists());
    let native = Connection::open_native(&logical)?;
    assert_eq!(native.integer("SELECT count(*) FROM native_data")?, 0);
    Ok(())
}

#[test]
fn deleting_a_logical_db_removes_its_notice_and_bundle() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    register()?;
    let logical = directory.path().join("delete.db");
    let storage = append_suffix(&logical, ".zsqlite");
    let sidecar = append_suffix(&storage, ".d");
    Connection::open(&logical)?.execute("CREATE TABLE payload(value)")?;

    let vfs = unsafe { ffi::sqlite3_vfs_find(VFS_NAME.as_ptr().cast()) };
    let vfs = unsafe { vfs.as_mut() }.ok_or("zsqlite VFS is not registered")?;
    let encoded = CString::new(logical.as_os_str().as_bytes())?;
    let rc = unsafe { vfs.xDelete.expect("xDelete")(&raw mut *vfs, encoded.as_ptr(), 1) };
    assert_eq!(rc, ffi::SQLITE_OK);
    assert!(!logical.exists());
    assert!(!storage.exists());
    assert!(!sidecar.exists());
    Ok(())
}

#[test]
fn wal_database_round_trip_through_vfs() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    register()?;
    let path = directory.path().join("transcripts.zsqlite");
    {
        let writer = Connection::open(&path)?;
        assert_eq!(writer.text("PRAGMA journal_mode=WAL")?, "wal");
        writer.execute(
            "CREATE TABLE transcript(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
             WITH RECURSIVE n(x) AS (
               VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<300
             )
             INSERT INTO transcript(body)
             SELECT printf('{\"id\":%d,\"text\":\"%.*c\"}', x, 2000, 'x') FROM n;",
        )?;
        let reader = Connection::open(&path)?;
        assert_eq!(reader.text("SELECT count(*) FROM transcript")?, "300");
        assert_integrity(&reader)?;
        let wal = std::fs::read(wal_path(&path))?;
        assert!(wal.len() >= 32);
        let magic = u32::from_be_bytes(wal[..4].try_into()?);
        assert!(matches!(magic, 0x377f_0682 | 0x377f_0683));
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        assert_integrity(&writer)?;
    }

    let mut store = Store::open_existing(&path)?;
    store.verify()?;
    let info = store.inspect()?;
    assert_eq!(info.indexed_pages, info.page_count as usize);
    drop(store);

    let reopened = Connection::open(&path)?;
    assert_eq!(reopened.text("SELECT count(*) FROM transcript")?, "300");
    assert_eq!(reopened.text("PRAGMA journal_mode")?, "wal");
    assert_integrity(&reopened)?;
    drop(reopened);
    let read_only = Connection::open_read_only(&path)?;
    assert_eq!(read_only.text("SELECT count(*) FROM transcript")?, "300");
    assert!(read_only.execute("DELETE FROM transcript").is_err());
    drop(read_only);

    Ok(())
}

#[test]
fn exclusive_locking_wal_repeated_checkpoints_preserve_every_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    register()?;
    let path = directory.path().join("exclusive-wal-checkpoints.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA page_size=1024;
             PRAGMA locking_mode=EXCLUSIVE;
             PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE events(id INTEGER PRIMARY KEY, body BLOB);
             INSERT INTO events VALUES(1, randomblob(900));",
        )?;
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        connection.execute(
            "WITH RECURSIVE sequence(id) AS (
               VALUES(2) UNION ALL SELECT id+1 FROM sequence WHERE id<100
             )
             INSERT INTO events SELECT id, randomblob(900) FROM sequence;",
        )?;
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        assert_eq!(connection.integer("SELECT count(*) FROM events")?, 100);
    }

    let reopened = Connection::open(&path)?;
    assert_eq!(reopened.integer("SELECT count(*) FROM events")?, 100);
    assert_integrity(&reopened)?;
    Ok(())
}

#[test]
fn rollback_journal_post_commit_truncate_updates_the_store_size()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    register()?;
    let path = directory
        .path()
        .join("rollback-post-commit-truncate.zsqlite");
    let shrunk_pages;
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA page_size=4096;
             PRAGMA auto_vacuum=FULL;
             PRAGMA journal_mode=DELETE;
             PRAGMA synchronous=FULL;
             CREATE TABLE events(id INTEGER PRIMARY KEY, body BLOB);
             WITH RECURSIVE sequence(id) AS (
               VALUES(1) UNION ALL SELECT id+1 FROM sequence WHERE id<300
             )
             INSERT INTO events SELECT id, randomblob(3000) FROM sequence;",
        )?;
        let grown_pages = connection.integer("PRAGMA page_count")?;
        connection.execute("DELETE FROM events WHERE id > 10")?;
        shrunk_pages = connection.integer("PRAGMA page_count")?;
        assert!(shrunk_pages < grown_pages);
    }

    let store = Store::open_existing(&path)?;
    let info = store.inspect()?;
    assert_eq!(info.page_count, u32::try_from(shrunk_pages)?);
    drop(store);
    let reopened = Connection::open(&path)?;
    assert_eq!(reopened.integer("PRAGMA page_count")?, shrunk_pages);
    assert_eq!(reopened.integer("SELECT count(*) FROM events")?, 10);
    assert_integrity(&reopened)?;
    Ok(())
}

#[test]
fn moving_the_active_path_rejects_a_write() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    register()?;
    let path = directory.path().join("moved-active.zsqlite");
    let moved = directory.path().join("moved-active-away.zsqlite");
    let connection = Connection::open(&path)?;
    connection.execute(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE events(id INTEGER PRIMARY KEY);",
    )?;

    std::fs::rename(&path, &moved)?;
    let result = connection.execute_with_code("INSERT INTO events VALUES(1)");
    let extended = unsafe { ffi::sqlite3_extended_errcode(connection.0) };
    std::fs::rename(&moved, &path)?;
    assert!(result.is_err());
    assert_ne!(extended, ffi::SQLITE_OK);
    drop(connection);

    let reopened = Connection::open(&path)?;
    assert_eq!(reopened.integer("SELECT count(*) FROM events")?, 0);
    Ok(())
}

#[test]
fn sidecar_flush_does_not_checkpoint_sqlite_wal() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    register()?;
    let path = directory.path().join("no-implicit-checkpoint.zsqlite");
    let connection = Connection::open(&path)?;
    connection.execute(
        "PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO messages VALUES(1, printf('%.*c', 20000, 'w'));",
    )?;
    let wal = wal_path(&path);
    let before = std::fs::metadata(&wal)?.len();
    assert!(before > 32);
    crate::flush(&path)?;
    assert_eq!(std::fs::metadata(&wal)?.len(), before);
    assert_eq!(connection.integer("SELECT count(*) FROM messages")?, 1);
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    let flushed = crate::flush(&path)?;
    assert_eq!(flushed.indexed_pages, flushed.page_count as usize);
    Ok(())
}

#[test]
fn registration_is_idempotent_under_concurrency() -> Result<(), Box<dyn std::error::Error>> {
    let barrier = Arc::new(Barrier::new(17));
    let mut threads = Vec::new();
    for _ in 0..16 {
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            register()
        }));
    }
    barrier.wait();
    for thread in threads {
        thread
            .join()
            .map_err(|_| "registration thread panicked")??;
    }
    let registered = unsafe { ffi::sqlite3_vfs_find(VFS_NAME.as_ptr().cast()) };
    assert!(!registered.is_null());
    let default = unsafe { ffi::sqlite3_vfs_find(null()) };
    assert_ne!(registered, default);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn inherited_parent_vfs_callbacks_receive_the_parent_context() {
    // SAFETY: An all-zero sqlite3_vfs contains only integers, raw pointers,
    // and nullable function pointers. The fields used below are initialized
    // before the callback is invoked.
    let mut parent: ffi::sqlite3_vfs = unsafe { std::mem::zeroed() };
    parent.iVersion = 3;
    parent.szOsFile =
        c_int::try_from(std::mem::size_of::<ffi::sqlite3_file>()).expect("sqlite3_file size fits");
    parent.xOpen = Some(context_checking_open);
    parent.xDelete = Some(context_checking_delete);
    parent.xAccess = Some(context_checking_access);
    parent.xFullPathname = Some(context_checking_full_pathname);
    parent.xDlOpen = Some(context_checking_dl_open);
    parent.xDlError = Some(context_checking_dl_error);
    parent.xDlSym = Some(context_checking_dl_sym);
    parent.xDlClose = Some(context_checking_dl_close);
    parent.xRandomness = Some(context_checking_randomness);
    parent.xSleep = Some(context_checking_sleep);
    parent.xCurrentTime = Some(context_checking_current_time);
    parent.xGetLastError = Some(context_checking_last_error);
    parent.xCurrentTimeInt64 = Some(context_checking_current_time_i64);
    parent.xSetSystemCall = Some(context_checking_set_system_call);
    parent.xGetSystemCall = Some(context_checking_get_system_call);
    parent.xNextSystemCall = Some(context_checking_next_system_call);
    let mut parent_context = 0_u8;
    parent.pAppData = (&raw mut parent_context).cast();
    let parent_pointer = &raw mut parent;
    EXPECTED_PARENT_VFS.store(parent_pointer, Ordering::Release);
    EXPECTED_PARENT_APP_DATA.store(parent.pAppData, Ordering::Release);
    PARENT_CONTEXT_CALLS.store(0, Ordering::Release);

    let app = AppData {
        parent: parent_pointer,
    };
    let mut shim = parent;
    shim.pAppData = std::ptr::from_ref(&app).cast_mut().cast();
    install_parent_vfs_wrappers(&mut shim, &parent);

    let mut file = std::mem::MaybeUninit::<ZFile>::uninit();
    assert_eq!(
        unsafe {
            shim.xOpen.expect("open")(
                &raw mut shim,
                null(),
                file.as_mut_ptr().cast(),
                0,
                null_mut(),
            )
        },
        ffi::SQLITE_CANTOPEN
    );
    assert_eq!(
        unsafe { shim.xDelete.expect("delete")(&raw mut shim, null(), 0) },
        ffi::SQLITE_OK
    );
    assert_eq!(
        unsafe { shim.xAccess.expect("access")(&raw mut shim, null(), 0, null_mut()) },
        ffi::SQLITE_OK
    );
    assert_eq!(
        unsafe { shim.xFullPathname.expect("full pathname")(&raw mut shim, null(), 0, null_mut()) },
        ffi::SQLITE_OK
    );
    let handle = unsafe { shim.xDlOpen.expect("dl open")(&raw mut shim, null()) };
    assert!(!handle.is_null());
    unsafe { shim.xDlError.expect("dl error")(&raw mut shim, 0, null_mut()) };
    assert!(unsafe { shim.xDlSym.expect("dl sym")(&raw mut shim, handle, null()) }.is_some());
    unsafe { shim.xDlClose.expect("dl close")(&raw mut shim, handle) };
    assert_eq!(
        unsafe { shim.xRandomness.expect("randomness")(&raw mut shim, 0, null_mut()) },
        7,
    );
    assert_eq!(
        unsafe { shim.xSleep.expect("sleep")(&raw mut shim, 31) },
        31
    );
    let mut current_time = 0.0;
    assert_eq!(
        unsafe { shim.xCurrentTime.expect("current time")(&raw mut shim, &raw mut current_time) },
        ffi::SQLITE_OK
    );
    assert_eq!(current_time.to_bits(), 17.0_f64.to_bits());
    assert_eq!(
        unsafe { shim.xGetLastError.expect("last error")(&raw mut shim, 0, null_mut()) },
        19
    );
    let mut current_time_i64 = 0;
    assert_eq!(
        unsafe {
            shim.xCurrentTimeInt64.expect("current time i64")(
                &raw mut shim,
                &raw mut current_time_i64,
            )
        },
        ffi::SQLITE_OK
    );
    assert_eq!(current_time_i64, 23);
    assert_eq!(
        unsafe { shim.xSetSystemCall.expect("set system call")(&raw mut shim, null(), None) },
        29
    );
    assert!(
        unsafe { shim.xGetSystemCall.expect("get system call")(&raw mut shim, null()) }.is_some()
    );
    assert_eq!(
        unsafe { shim.xNextSystemCall.expect("next system call")(&raw mut shim, null()) },
        c"next".as_ptr()
    );
    assert_eq!(
        PARENT_CONTEXT_CALLS.load(Ordering::Acquire),
        ALL_PARENT_CONTEXT_CALLS,
        "at least one callback did not receive the parent VFS"
    );
}

#[test]
fn advertised_io_version_tracks_each_parent_files_capabilities() {
    let mut parent = ffi::sqlite3_io_methods {
        iVersion: 1,
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
        xSectorSize: None,
        xDeviceCharacteristics: Some(x_device_characteristics),
        xShmMap: None,
        xShmLock: None,
        xShmBarrier: None,
        xShmUnmap: None,
        xFetch: None,
        xUnfetch: None,
    };

    let methods = io_methods(&parent).expect("valid v1 parent");
    assert_eq!(methods.iVersion, 1);
    assert!(methods.xShmMap.is_none());
    assert!(methods.xFetch.is_none());

    parent.iVersion = 2;
    parent.xShmMap = Some(x_shm_map);
    assert_eq!(io_methods(&parent).expect("partial v2 parent").iVersion, 1);
    parent.xShmLock = Some(x_shm_lock);
    parent.xShmBarrier = Some(x_shm_barrier);
    parent.xShmUnmap = Some(x_shm_unmap);
    let methods = io_methods(&parent).expect("complete v2 parent");
    assert_eq!(methods.iVersion, 2);
    assert!(methods.xShmMap.is_some());
    assert!(methods.xFetch.is_none());

    parent.iVersion = 3;
    parent.xFetch = Some(x_fetch);
    assert_eq!(io_methods(&parent).expect("partial v3 parent").iVersion, 2);
    parent.xUnfetch = Some(x_unfetch);
    let methods = io_methods(&parent).expect("complete v3 parent");
    assert_eq!(methods.iVersion, 3);
    assert!(methods.xFetch.is_some());
    assert!(methods.xUnfetch.is_some());
}

#[test]
fn advertised_vfs_version_and_optional_callbacks_do_not_exceed_the_parent() {
    // SAFETY: Every field in sqlite3_vfs is an integer, raw pointer, or
    // nullable function pointer. No callback is invoked in this test.
    let mut parent: ffi::sqlite3_vfs = unsafe { std::mem::zeroed() };
    parent.iVersion = 1;
    parent.xCurrentTimeInt64 = Some(context_checking_current_time_i64);
    parent.xSetSystemCall = Some(context_checking_set_system_call);
    parent.xGetSystemCall = Some(context_checking_get_system_call);
    parent.xNextSystemCall = Some(context_checking_next_system_call);
    let mut shim = parent;
    install_parent_vfs_wrappers(&mut shim, &parent);
    assert_eq!(shim.iVersion, 1);
    assert!(shim.xCurrentTimeInt64.is_none());
    assert!(shim.xSetSystemCall.is_none());
    assert!(shim.xGetSystemCall.is_none());
    assert!(shim.xNextSystemCall.is_none());

    parent.iVersion = 7;
    parent.xDlOpen = Some(context_checking_dl_open);
    let mut shim = parent;
    install_parent_vfs_wrappers(&mut shim, &parent);
    assert_eq!(shim.iVersion, 3);
    assert!(shim.xDlOpen.is_some());
    assert!(shim.xDlError.is_none());
    assert!(shim.xCurrentTimeInt64.is_some());
    assert!(shim.xSetSystemCall.is_some());
    assert!(shim.xGetSystemCall.is_some());
    assert!(shim.xNextSystemCall.is_some());
}

#[test]
fn registry_open_of_one_database_does_not_block_another() -> Result<(), Box<dyn std::error::Error>>
{
    let directory = tempfile::tempdir()?;
    let slow_path = directory.path().join("slow-open.zsqlite");
    let fast_path = directory.path().join("fast-open.zsqlite");
    let (slow_started_tx, slow_started_rx) = mpsc::channel();
    let (release_slow_tx, release_slow_rx) = mpsc::channel();

    let slow = std::thread::spawn(move || -> Result<(), String> {
        let (store, _) = get_store_with(&slow_path, true, |key| {
            slow_started_tx
                .send(())
                .map_err(|_| crate::StoreError::Range)?;
            release_slow_rx
                .recv()
                .map_err(|_| crate::StoreError::Range)?;
            Store::open(key, true)
        })
        .map_err(|error| error.to_string())?;
        drop(store);
        Ok(())
    });
    slow_started_rx.recv_timeout(Duration::from_secs(5))?;

    let (fast_done_tx, fast_done_rx) = mpsc::channel();
    let fast = std::thread::spawn(move || {
        let result = get_store(&fast_path, true, true)
            .map(|_| ())
            .map_err(|error| error.to_string());
        let _ = fast_done_tx.send(result);
    });
    fast_done_rx
        .recv_timeout(Duration::from_secs(5))?
        .map_err(|error| format!("independent open failed: {error}"))?;

    release_slow_tx.send(())?;
    slow.join().map_err(|_| "slow open thread panicked")??;
    fast.join().map_err(|_| "fast open thread panicked")?;
    Ok(())
}

#[test]
fn registry_delete_returns_busy_while_same_path_is_opening()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("opening.zsqlite");
    let opening_path = canonical_key(&path);
    let (open_started_tx, open_started_rx) = mpsc::channel();
    let (release_open_tx, release_open_rx) = mpsc::channel();
    let opener = std::thread::spawn(move || -> Result<(), String> {
        get_store_with(&opening_path, true, |key| {
            open_started_tx
                .send(())
                .map_err(|_| crate::StoreError::Range)?;
            release_open_rx
                .recv()
                .map_err(|_| crate::StoreError::Range)?;
            Store::open(key, true)
        })
        .map(|_| ())
        .map_err(|error| error.to_string())
    });
    open_started_rx.recv_timeout(Duration::from_secs(5))?;

    let delete_called = AtomicBool::new(false);
    let result = delete_registered_store_with(&path, |_| {
        delete_called.store(true, Ordering::Release);
        Ok(((), true))
    });
    assert!(matches!(result, Err(crate::StoreError::Busy)));
    assert!(!delete_called.load(Ordering::Acquire));

    release_open_tx.send(())?;
    opener.join().map_err(|_| "open thread panicked")??;
    Ok(())
}

#[test]
fn registry_delete_cannot_remove_a_racing_replacement() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("delete-recreate.zsqlite");
    let key = canonical_key(&path);
    let original = Arc::new(Mutex::new(Store::open(&key, true)?));
    let entry = registry_entry(&key)?;
    *entry.store.lock().map_err(|_| "registry entry poisoned")? = Arc::downgrade(&original);
    drop(original);

    let delete_path = path.clone();
    let (deleted_tx, deleted_rx) = mpsc::channel();
    let (finish_delete_tx, finish_delete_rx) = mpsc::channel();
    let deleter = std::thread::spawn(move || -> Result<(), String> {
        delete_registered_store_with(&delete_path, |key| {
            Store::delete_bundle(key)?;
            deleted_tx.send(()).map_err(|_| crate::StoreError::Range)?;
            finish_delete_rx
                .recv()
                .map_err(|_| crate::StoreError::Range)?;
            Ok(((), true))
        })
        .map_err(|error| error.to_string())
    });
    deleted_rx.recv_timeout(Duration::from_secs(5))?;
    assert!(!path.exists());
    assert!(!append_suffix(&path, ".d").exists());

    let recreate_path = path.clone();
    let (recreate_started_tx, recreate_started_rx) = mpsc::channel();
    let (recreated_tx, recreated_rx) = mpsc::channel();
    let recreator = std::thread::spawn(move || {
        let _ = recreate_started_tx.send(());
        let result = get_store(&recreate_path, true, true).map_err(|error| error.to_string());
        let _ = recreated_tx.send(result);
    });
    recreate_started_rx.recv_timeout(Duration::from_secs(5))?;
    assert!(
        recreated_rx
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );

    finish_delete_tx.send(())?;
    deleter.join().map_err(|_| "delete thread panicked")??;
    let (replacement, newly_opened) = recreated_rx.recv_timeout(Duration::from_secs(5))??;
    assert!(newly_opened);
    recreator.join().map_err(|_| "recreate thread panicked")?;

    let (same_replacement, newly_opened) = get_store(&path, true, true)?;
    assert!(!newly_opened);
    assert!(Arc::ptr_eq(&replacement, &same_replacement));
    Ok(())
}

#[test]
fn vfs_delete_removes_database_and_sidecar_for_unusual_path()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("spaces-and-ünicode.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO messages VALUES(1, 'delete me');",
        )?;
    }
    let sidecar = append_suffix(&path, ".d");
    assert!(path.exists());
    assert!(sidecar.exists());

    let path_string = CString::new(path.to_string_lossy().as_bytes())?;
    let vfs = unsafe { ffi::sqlite3_vfs_find(VFS_NAME.as_ptr().cast()) };
    let delete = unsafe { vfs.as_ref() }
        .and_then(|registered| registered.xDelete)
        .ok_or("registered VFS has no xDelete")?;
    let active = Connection::open(&path)?;
    assert_eq!(
        unsafe { delete(vfs, path_string.as_ptr(), 1) },
        ffi::SQLITE_BUSY
    );
    assert_eq!(active.integer("SELECT count(*) FROM messages")?, 1);
    drop(active);
    assert_eq!(
        unsafe { delete(vfs, path_string.as_ptr(), 1) },
        ffi::SQLITE_OK
    );
    assert!(!path.exists());
    assert!(!sidecar.exists());
    let recreated = Connection::open(&path)?;
    recreated.execute("CREATE TABLE replacement(id INTEGER PRIMARY KEY)")?;
    assert_integrity(&recreated)?;
    Ok(())
}

#[test]
fn hard_linked_working_file_is_rejected_without_modification()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("hard-link.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO messages VALUES(1, 'preserve me');",
        )?;
    }
    let active_alias = directory.path().join("active-alias");
    std::fs::hard_link(&path, &active_alias)?;
    assert!(matches!(
        Store::open_existing(&path),
        Err(crate::StoreError::Busy)
    ));
    std::fs::remove_file(active_alias)?;

    let reopened = Connection::open(&path)?;
    assert_eq!(
        reopened.text("SELECT body FROM messages WHERE id=1")?,
        "preserve me"
    );
    assert_integrity(&reopened)?;
    Ok(())
}

#[test]
fn every_sqlite_page_size_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    for page_size in [512_u32, 1024, 2048, 4096, 8192, 16_384, 32_768, 65_536] {
        let path = directory.path().join(format!("pages-{page_size}.zsqlite"));
        {
            let connection = Connection::open(&path)?;
            connection.execute(&format!(
                "PRAGMA page_size={page_size};
                 PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 CREATE TABLE transcript(
                   id INTEGER PRIMARY KEY,
                   speaker TEXT NOT NULL,
                   body TEXT NOT NULL,
                   embedding BLOB
                 );
                 WITH RECURSIVE n(x) AS (
                   VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<80
                 )
                 INSERT INTO transcript
                 SELECT x, printf('speaker-%d', x%5),
                        printf('{{\"id\":%d,\"body\":\"%.*c\"}}', x, 3000+x, 't'),
                        randomblob((x%7)*31)
                 FROM n;"
            ))?;
            assert_eq!(
                connection.integer("PRAGMA page_size")?,
                i64::from(page_size)
            );
            assert_integrity(&connection)?;
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        }
        let mut store = Store::open_existing(&path)?;
        store.verify()?;
        let info = store.inspect()?;
        assert_eq!(info.page_size, page_size);
        assert_eq!(info.logical_size % u64::from(page_size), 0);
        drop(store);

        let reopened = Connection::open(&path)?;
        assert_eq!(reopened.integer("SELECT count(*) FROM transcript")?, 80);
        assert_eq!(
            reopened.integer("SELECT sum(length(body)) FROM transcript")?,
            244_751
        );
        assert_integrity(&reopened)?;
    }
    Ok(())
}

#[test]
fn rollback_journal_savepoints_vacuum_and_incremental_vacuum()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("rollback.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection
            .execute(
                "PRAGMA page_size=8192;
             PRAGMA auto_vacuum=INCREMENTAL;
             PRAGMA journal_mode=DELETE;
             CREATE TABLE transcript(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
             CREATE INDEX transcript_body ON transcript(substr(body, 1, 24));
             BEGIN;
             WITH RECURSIVE n(x) AS (
               VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<250
             )
             INSERT INTO transcript
             SELECT x, printf('{\"id\":%d,\"text\":\"%.*c\"}', x, 1800, 'a') FROM n;
             SAVEPOINT discard_me;
             UPDATE transcript SET body=body||'discarded' WHERE id%3=0;
             ROLLBACK TO discard_me;
             RELEASE discard_me;
             COMMIT;
             BEGIN;
             DELETE FROM transcript WHERE id<=50;
             ROLLBACK;",
            )
            .map_err(|error| format!("initial rollback/savepoint workload: {error}"))?;
        assert_eq!(connection.integer("SELECT count(*) FROM transcript")?, 250);
        assert_eq!(
            connection.integer("SELECT count(*) FROM transcript WHERE body LIKE '%discarded%'")?,
            0
        );
        connection
            .execute("DELETE FROM transcript WHERE id%2=0")
            .map_err(|error| format!("delete before vacuum: {error}"))?;
        connection
            .execute("PRAGMA incremental_vacuum")
            .map_err(|error| format!("incremental vacuum: {error}"))?;
        connection
            .execute("VACUUM")
            .map_err(|error| format!("full vacuum: {error}"))?;
        assert_eq!(connection.integer("PRAGMA page_size")?, 8192);
        assert_integrity(&connection)?;
    }
    assert!(!append_suffix(&path, "-journal").exists());
    let reopened = Connection::open(&path)?;
    assert_eq!(reopened.integer("SELECT count(*) FROM transcript")?, 125);
    assert_integrity(&reopened)?;
    drop(reopened);
    Store::open_existing(&path)?.verify()?;
    Ok(())
}

#[test]
fn attach_and_backup_cross_the_vfs_boundary() -> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("source.zsqlite");
    let attached_path = directory.path().join("attached.zsqlite");
    let native_backup_path = directory.path().join("native-backup.db");
    let restored_path = directory.path().join("restored.zsqlite");

    {
        let source = Connection::open(&source_path)?;
        source.execute(&format!(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE main.messages(id INTEGER PRIMARY KEY, body TEXT);
             ATTACH DATABASE '{}' AS archive;
             CREATE TABLE archive.messages(id INTEGER PRIMARY KEY, body TEXT);
             BEGIN;
             INSERT INTO main.messages VALUES(1, printf('%.*c', 12000, 'm'));
             INSERT INTO archive.messages VALUES(2, printf('%.*c', 15000, 'a'));
             COMMIT;
             DETACH DATABASE archive;
             PRAGMA wal_checkpoint(TRUNCATE);",
            sql_path(&attached_path)
        ))?;
        assert_integrity(&source)?;
    }
    Store::open_existing(&source_path)?.verify()?;
    Store::open_existing(&attached_path)?.verify()?;

    {
        let source = Connection::open(&source_path)?;
        let native_backup = Connection::open_native(&native_backup_path)?;
        backup(&source, &native_backup)?;
        assert_eq!(
            native_backup.integer("SELECT length(body) FROM messages")?,
            12_000
        );
        assert_integrity(&native_backup)?;

        let restored = Connection::open(&restored_path)?;
        backup(&native_backup, &restored)?;
        assert_eq!(
            restored.rows("SELECT id, body FROM messages ORDER BY id")?,
            source.rows("SELECT id, body FROM messages ORDER BY id")?
        );
        assert_integrity(&restored)?;
    }
    Store::open_existing(&restored_path)?.verify()?;
    Ok(())
}

#[test]
fn online_backup_into_empty_zsqlite_accepts_every_page_size()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    for page_size in [512, 1024, 2048, 4096, 8192, 16_384, 32_768, 65_536] {
        let source_path = directory.path().join(format!("source-{page_size}.db"));
        let destination_path = directory
            .path()
            .join(format!("destination-{page_size}.zsqlite"));
        let source = Connection::open_native(&source_path)?;
        source.execute(&format!(
            "PRAGMA page_size={page_size};
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
             WITH RECURSIVE n(x) AS (
               VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<750
             )
             INSERT INTO messages(body)
             SELECT printf('{{\"id\":%d,\"text\":\"%.*c\"}}', x, 1500+x%300, 'x') FROM n;"
        ))?;
        let destination = Connection::open(&destination_path)?;
        backup_incremental(&source, &destination, 17)?;
        assert_eq!(destination.integer("PRAGMA page_size")?, page_size);
        assert_eq!(destination.integer("SELECT count(*) FROM messages")?, 750);
        assert_eq!(
            destination.integer("SELECT sum(length(body)) FROM messages")?,
            source.integer("SELECT sum(length(body)) FROM messages")?
        );
        assert_integrity(&destination)?;
        drop(destination);

        let reopened = Connection::open(&destination_path)?;
        assert_eq!(reopened.integer("PRAGMA page_size")?, page_size);
        assert_eq!(reopened.integer("SELECT count(*) FROM messages")?, 750);
        assert_integrity(&reopened)?;
    }
    Ok(())
}

#[test]
fn online_backup_tracks_a_wal_commit_between_batches() -> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let source_path = directory.path().join("live-source.db");
    let destination_path = directory.path().join("online-destination.zsqlite");
    let source = Connection::open_native(&source_path)?;
    source.execute(
        "PRAGMA page_size=1024;
         PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
         WITH RECURSIVE n(x) AS (
           VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<3000
         )
         INSERT INTO messages
         SELECT x, printf('{\"id\":%d,\"text\":\"%.*c\"}', x, 1800+x%200, 'x') FROM n;",
    )?;
    let destination = Connection::open(&destination_path)?;
    let backup = unsafe {
        ffi::sqlite3_backup_init(destination.0, c"main".as_ptr(), source.0, c"main".as_ptr())
    };
    if backup.is_null() {
        return Err(destination
            .error("backup init", unsafe {
                ffi::sqlite3_errcode(destination.0)
            })
            .into());
    }
    let first_rc = unsafe { ffi::sqlite3_backup_step(backup, 17) };
    assert_eq!(
        first_rc,
        ffi::SQLITE_OK,
        "backup unexpectedly completed in one batch"
    );

    let writer_path = source_path.clone();
    std::thread::spawn(move || -> Result<(), String> {
        let writer = Connection::open_native(&writer_path)?;
        writer.execute(
            "INSERT INTO messages VALUES(
               4000,
               printf('{\"id\":4000,\"text\":\"%.*c\"}', 2400, 'w')
             );",
        )
    })
    .join()
    .map_err(|_| "source writer panicked")??;

    let step_rc = loop {
        let rc = unsafe { ffi::sqlite3_backup_step(backup, 17) };
        match rc {
            ffi::SQLITE_OK => {}
            ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => std::thread::sleep(Duration::from_millis(1)),
            _ => break rc,
        }
    };
    let finish_rc = unsafe { ffi::sqlite3_backup_finish(backup) };
    assert_eq!(step_rc, ffi::SQLITE_DONE);
    assert_eq!(finish_rc, ffi::SQLITE_OK);
    assert_eq!(destination.integer("PRAGMA page_size")?, 1024);
    assert_eq!(destination.integer("SELECT count(*) FROM messages")?, 3001);
    assert_eq!(
        destination.integer("SELECT length(body) FROM messages WHERE id=4000")?,
        source.integer("SELECT length(body) FROM messages WHERE id=4000")?
    );
    assert_integrity(&destination)?;
    drop(destination);

    let reopened = Connection::open(&destination_path)?;
    assert_eq!(reopened.integer("SELECT count(*) FROM messages")?, 3001);
    assert_integrity(&reopened)?;
    Ok(())
}

fn backup(source: &Connection, destination: &Connection) -> Result<(), String> {
    let backup = unsafe {
        ffi::sqlite3_backup_init(destination.0, c"main".as_ptr(), source.0, c"main".as_ptr())
    };
    if backup.is_null() {
        return Err(destination.error("backup init", unsafe {
            ffi::sqlite3_errcode(destination.0)
        }));
    }
    let step_rc = unsafe { ffi::sqlite3_backup_step(backup, -1) };
    let finish_rc = unsafe { ffi::sqlite3_backup_finish(backup) };
    if step_rc != ffi::SQLITE_DONE {
        return Err(destination.error("backup step", step_rc));
    }
    if finish_rc != ffi::SQLITE_OK {
        return Err(destination.error("backup finish", finish_rc));
    }
    Ok(())
}

fn backup_incremental(
    source: &Connection,
    destination: &Connection,
    pages_per_step: c_int,
) -> Result<(), String> {
    let backup = unsafe {
        ffi::sqlite3_backup_init(destination.0, c"main".as_ptr(), source.0, c"main".as_ptr())
    };
    if backup.is_null() {
        return Err(destination.error("backup init", unsafe {
            ffi::sqlite3_errcode(destination.0)
        }));
    }
    let step_rc = loop {
        let rc = unsafe { ffi::sqlite3_backup_step(backup, pages_per_step) };
        match rc {
            ffi::SQLITE_OK => {}
            ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED => std::thread::sleep(Duration::from_millis(1)),
            _ => break rc,
        }
    };
    let finish_rc = unsafe { ffi::sqlite3_backup_finish(backup) };
    if step_rc != ffi::SQLITE_DONE {
        return Err(destination.error("incremental backup step", step_rc));
    }
    if finish_rc != ffi::SQLITE_OK {
        return Err(destination.error("incremental backup finish", finish_rc));
    }
    Ok(())
}

fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

#[test]
fn deterministic_random_workload_matches_native_sqlite() -> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let compressed_path = directory.path().join("random-zsqlite.zsqlite");
    let native_path = directory.path().join("random-native.db");
    let mut compressed = Connection::open(&compressed_path)?;
    let mut native = Connection::open_native(&native_path)?;
    let setup = "PRAGMA page_size=8192;
                 PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 CREATE TABLE messages(
                   id INTEGER PRIMARY KEY,
                   revision INTEGER NOT NULL,
                   body TEXT,
                   data BLOB
                 );
                 CREATE INDEX messages_revision ON messages(revision);";
    compressed.execute(setup)?;
    native.execute(setup)?;

    let mut random = XorShift64(0x4d59_5df4_d0f3_3173);
    for batch in 0..20 {
        compressed.execute("BEGIN IMMEDIATE")?;
        native.execute("BEGIN IMMEDIATE")?;
        let mut statements = Vec::new();
        for _ in 0..40 {
            let id = random.next() % 180;
            let revision = random.next() % 10_000;
            let body_length = random.next() % 9000;
            let blob_length = random.next() % 600;
            let operation = random.next() % 5;
            let statement = match operation {
                0 | 1 => format!(
                    "INSERT INTO messages(id, revision, body, data)
                     VALUES({id}, {revision},
                            printf('{{\"id\":{id},\"text\":\"%.*c\"}}', {body_length}, '{}'),
                            CAST(printf('%.*c', {blob_length}, '{}') AS BLOB))
                     ON CONFLICT(id) DO UPDATE SET revision=excluded.revision, body=excluded.body, data=excluded.data",
                    char::from(b'a' + (id % 26) as u8),
                    char::from(b'A' + (id % 26) as u8)
                ),
                2 => format!(
                    "UPDATE messages SET revision={revision}, body=coalesce(body,'')||'-u{revision}' WHERE id={id}"
                ),
                3 => format!("DELETE FROM messages WHERE id={id}"),
                _ => format!(
                    "INSERT OR IGNORE INTO messages(id,revision,body,data) VALUES({id},{revision},NULL,X'')"
                ),
            };
            statements.push(statement);
        }
        for statement in &statements {
            compressed.execute(statement)?;
            native.execute(statement)?;
        }
        if batch % 4 == 3 {
            compressed.execute("ROLLBACK")?;
            native.execute("ROLLBACK")?;
        } else {
            compressed.execute("COMMIT")?;
            native.execute("COMMIT")?;
        }
        assert_eq!(logical_rows(&compressed)?, logical_rows(&native)?);
        assert_integrity(&compressed)?;
        if batch % 5 == 4 {
            compressed.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            native.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            drop(compressed);
            drop(native);
            compressed = Connection::open(&compressed_path)?;
            native = Connection::open_native(&native_path)?;
        }
    }
    compressed.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert_integrity(&compressed)?;
    drop(compressed);
    let mut store = Store::open_existing(&compressed_path)?;
    store.verify()?;
    store.compact()?;
    store.verify()?;
    drop(store);
    let reopened = Connection::open(&compressed_path)?;
    assert_eq!(logical_rows(&reopened)?, logical_rows(&native)?);
    assert_integrity(&reopened)?;
    Ok(())
}

fn logical_rows(connection: &Connection) -> Result<Vec<Vec<Value>>, String> {
    connection.rows(
        "SELECT id, revision, body, data, typeof(body), typeof(data)
         FROM messages ORDER BY id",
    )
}

struct XorShift64(u64);

impl XorShift64 {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[test]
fn concurrent_wal_readers_and_writers_preserve_all_commits()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("concurrent.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA page_size=8192;
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=7;
             CREATE TABLE events(writer INTEGER, sequence INTEGER, body TEXT,
                                 PRIMARY KEY(writer, sequence));",
        )?;
    }

    let workers = 4;
    let rows_per_worker = 80;
    let expected_rows = i64::try_from(workers * rows_per_worker)?;
    let expected_workers = i64::try_from(workers)?;
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut threads = Vec::new();
    for worker in 0..workers {
        let path = path.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || -> Result<(), String> {
            let connection = Connection::open(&path)?;
            let worker_byte = u8::try_from(worker).map_err(|error| error.to_string())?;
            barrier.wait();
            for sequence in 0..rows_per_worker {
                connection.execute(&format!(
                    "INSERT INTO events VALUES({worker}, {sequence},
                     printf('{{\"writer\":{worker},\"sequence\":{sequence},\"body\":\"%.*c\"}}', {}, '{}'))",
                    500 + (sequence % 31),
                    char::from(b'a' + worker_byte)
                ))?;
                if sequence % 13 == 0 {
                    let _ = connection.integer("SELECT count(*) FROM events")?;
                }
            }
            Ok(())
        }));
    }
    barrier.wait();
    let observer = Connection::open(&path)?;
    while threads.iter().any(|thread| !thread.is_finished()) {
        let count = observer.integer("SELECT count(*) FROM events")?;
        assert!((0..=expected_rows).contains(&count));
        assert_integrity(&observer)?;
        std::thread::yield_now();
    }
    for thread in threads {
        thread.join().map_err(|_| "writer thread panicked")??;
    }
    assert_eq!(
        observer.integer("SELECT count(*) FROM events")?,
        expected_rows
    );
    assert_eq!(
        observer.integer(
            "SELECT count(*) FROM (
               SELECT writer, count(*) AS n, min(sequence) AS lo, max(sequence) AS hi
               FROM events GROUP BY writer HAVING n=80 AND lo=0 AND hi=79
             )"
        )?,
        expected_workers
    );
    assert_integrity(&observer)?;
    observer.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(observer);
    Store::open_existing(&path)?.verify()?;
    Ok(())
}

#[test]
fn wal_reader_keeps_a_stable_snapshot_during_commit() -> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("snapshot.zsqlite");
    let writer = Connection::open(&path)?;
    writer.execute(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO messages VALUES(1, 'visible');",
    )?;
    let reader = Connection::open(&path)?;
    reader.execute("BEGIN")?;
    assert_eq!(reader.integer("SELECT count(*) FROM messages")?, 1);
    writer.execute(
        "BEGIN IMMEDIATE;
         INSERT INTO messages VALUES(2, printf('%.*c', 20000, 'x'));
         COMMIT;",
    )?;
    assert_eq!(reader.integer("SELECT count(*) FROM messages")?, 1);
    reader.execute("COMMIT")?;
    assert_eq!(reader.integer("SELECT count(*) FROM messages")?, 2);
    assert_integrity(&reader)?;
    Ok(())
}

#[test]
fn wal_writer_lock_contention_and_rollback_have_exact_visibility()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("wal-locks.zsqlite");
    let holder = Connection::open(&path)?;
    holder.execute(
        "PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO events VALUES(1, 'committed');",
    )?;
    let contender = Connection::open(&path)?;
    contender.busy_timeout(0)?;

    holder.execute(
        "BEGIN IMMEDIATE;
         INSERT INTO events VALUES(2, 'must roll back');",
    )?;
    assert_eq!(contender.integer("SELECT count(*) FROM events")?, 1);
    assert_busy(contender.execute_with_code("BEGIN IMMEDIATE"));
    holder.execute("ROLLBACK")?;

    contender.execute(
        "BEGIN IMMEDIATE;
         UPDATE events SET body='updated' WHERE id=1;
         INSERT INTO events VALUES(3, 'committed later');
         COMMIT;",
    )?;
    assert_eq!(holder.integer("SELECT count(*) FROM events")?, 2);
    assert_eq!(
        holder.rows("SELECT id, body FROM events ORDER BY id")?,
        vec![
            vec![Value::Integer(1), Value::Text(b"updated".to_vec())],
            vec![Value::Integer(3), Value::Text(b"committed later".to_vec())],
        ]
    );
    assert_integrity(&holder)?;
    holder.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    Ok(())
}

#[test]
fn rollback_journal_lock_promotion_and_exclusive_locking_are_correct()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("rollback-locks.zsqlite");
    let reader = Connection::open(&path)?;
    reader.execute(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO events VALUES(1, 'original');",
    )?;
    let writer = Connection::open(&path)?;
    writer.busy_timeout(0)?;

    reader.execute("BEGIN")?;
    assert_eq!(reader.integer("SELECT count(*) FROM events")?, 1);
    writer.execute(
        "BEGIN IMMEDIATE;
         INSERT INTO events VALUES(2, 'pending');",
    )?;
    assert_eq!(reader.integer("SELECT count(*) FROM events")?, 1);
    assert_busy(writer.execute_with_code("COMMIT"));
    assert_eq!(reader.integer("SELECT count(*) FROM events")?, 1);
    reader.execute("COMMIT")?;
    writer.execute("COMMIT")?;
    assert_eq!(reader.integer("SELECT count(*) FROM events")?, 2);

    writer.execute("BEGIN EXCLUSIVE")?;
    let blocked_reader = Connection::open(&path)?;
    blocked_reader.busy_timeout(0)?;
    assert_busy(blocked_reader.execute_with_code("SELECT count(*) FROM events"));
    writer.execute("ROLLBACK")?;
    assert_eq!(blocked_reader.integer("SELECT count(*) FROM events")?, 2);
    assert_integrity(&blocked_reader)?;
    Ok(())
}

#[test]
fn readers_never_observe_a_partially_applied_transaction() -> Result<(), Box<dyn std::error::Error>>
{
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("atomic-visibility.zsqlite");
    let observer = Connection::open(&path)?;
    observer.execute(
        "PRAGMA journal_mode=WAL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO events VALUES(0, 'before');",
    )?;

    let (staged_tx, staged_rx) = mpsc::channel();
    let (commit_tx, commit_rx) = mpsc::channel();
    let writer_path = path.clone();
    let writer = std::thread::spawn(move || -> Result<(), String> {
        let connection = Connection::open(&writer_path)?;
        connection.execute("BEGIN IMMEDIATE; DELETE FROM events")?;
        for id in 1..=200 {
            connection.execute(&format!(
                "INSERT INTO events VALUES({id}, printf('%.*c', {}, 'x'))",
                300 + id % 29
            ))?;
        }
        staged_tx.send(()).map_err(|error| error.to_string())?;
        commit_rx.recv().map_err(|error| error.to_string())?;
        connection.execute("COMMIT")
    });

    staged_rx.recv_timeout(Duration::from_secs(10))?;
    for _ in 0..50 {
        assert_eq!(observer.integer("SELECT count(*) FROM events")?, 1);
        assert_eq!(
            observer.text("SELECT body FROM events WHERE id=0")?,
            "before"
        );
        std::thread::yield_now();
    }
    commit_tx.send(())?;
    writer.join().map_err(|_| "writer thread panicked")??;
    assert_eq!(observer.integer("SELECT count(*) FROM events")?, 200);
    assert_eq!(
        observer.integer("SELECT count(*) FROM events WHERE id=0")?,
        0
    );
    assert_integrity(&observer)?;
    observer.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    Ok(())
}

#[test]
fn rollback_journal_parallel_transactions_serialize_without_lost_commits()
-> Result<(), Box<dyn std::error::Error>> {
    const WORKERS: usize = 4;
    const TRANSACTIONS: usize = 24;
    const ROWS_PER_TRANSACTION: usize = 3;
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("rollback-parallel.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA page_size=8192;
             PRAGMA journal_mode=DELETE;
             CREATE TABLE events(
               writer INTEGER,
               transaction_no INTEGER,
               member INTEGER,
               body TEXT,
               PRIMARY KEY(writer, transaction_no, member)
             );",
        )?;
    }

    let barrier = Arc::new(Barrier::new(WORKERS + 1));
    let mut threads = Vec::new();
    for worker in 0..WORKERS {
        let path = path.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || -> Result<(), String> {
            let connection = Connection::open(&path)?;
            barrier.wait();
            for transaction_no in 0..TRANSACTIONS {
                connection.execute("BEGIN IMMEDIATE")?;
                for member in 0..ROWS_PER_TRANSACTION {
                    connection.execute(&format!(
                        "INSERT INTO events VALUES(
                           {worker}, {transaction_no}, {member},
                           printf('{{\"writer\":{worker},\"transaction\":{transaction_no},\"member\":{member},\"body\":\"%.*c\"}}', 700, 'r')
                         )"
                    ))?;
                }
                connection.execute(if transaction_no % 4 == 0 {
                    "ROLLBACK"
                } else {
                    "COMMIT"
                })?;
            }
            Ok(())
        }));
    }
    barrier.wait();
    for thread in threads {
        thread.join().map_err(|_| "writer thread panicked")??;
    }

    let connection = Connection::open(&path)?;
    let committed_transactions = TRANSACTIONS - TRANSACTIONS.div_ceil(4);
    let expected = i64::try_from(WORKERS * committed_transactions * ROWS_PER_TRANSACTION)?;
    assert_eq!(connection.integer("SELECT count(*) FROM events")?, expected);
    assert_eq!(
        connection.integer(
            "SELECT count(*) FROM (
               SELECT writer, transaction_no, count(*) AS members
               FROM events GROUP BY writer, transaction_no HAVING members=3
             )"
        )?,
        i64::try_from(WORKERS * committed_transactions)?
    );
    assert_eq!(
        connection.integer("SELECT count(*) FROM events WHERE transaction_no%4=0")?,
        0
    );
    assert_integrity(&connection)?;
    drop(connection);
    Store::open_existing(&path)?.verify()?;
    Ok(())
}

#[test]
fn wal_checkpoints_race_with_parallel_transactions_without_losing_data()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITERS: usize = 3;
    const TRANSACTIONS: usize = 40;
    const ROWS_PER_TRANSACTION: usize = 4;
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("checkpoint-race.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA page_size=4096;
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE events(
               writer INTEGER,
               transaction_no INTEGER,
               member INTEGER,
               body TEXT,
               PRIMARY KEY(writer, transaction_no, member)
             );",
        )?;
    }

    let running = Arc::new(AtomicBool::new(true));
    let checkpoint_count = Arc::new(AtomicUsize::new(0));
    let checkpointer_path = path.clone();
    let checkpointer_running = Arc::clone(&running);
    let checkpointer_count = Arc::clone(&checkpoint_count);
    let checkpointer = std::thread::spawn(move || -> Result<(), String> {
        let connection = Connection::open(&checkpointer_path)?;
        while checkpointer_running.load(Ordering::Acquire)
            || checkpointer_count.load(Ordering::Relaxed) < 10
        {
            let result = connection.rows("PRAGMA wal_checkpoint(PASSIVE)")?;
            if result.len() != 1
                || result[0].len() != 3
                || !result[0]
                    .iter()
                    .all(|value| matches!(value, Value::Integer(_)))
            {
                return Err(format!("unexpected checkpoint result: {result:?}"));
            }
            checkpointer_count.fetch_add(1, Ordering::Relaxed);
            std::thread::yield_now();
        }
        Ok(())
    });

    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let mut writers = Vec::new();
    for writer in 0..WRITERS {
        let path = path.clone();
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || -> Result<(), String> {
            let connection = Connection::open(&path)?;
            barrier.wait();
            for transaction_no in 0..TRANSACTIONS {
                connection.execute("BEGIN IMMEDIATE")?;
                for member in 0..ROWS_PER_TRANSACTION {
                    connection.execute(&format!(
                        "INSERT INTO events VALUES(
                           {writer}, {transaction_no}, {member},
                           printf('{{\"writer\":{writer},\"transaction\":{transaction_no},\"member\":{member},\"text\":\"%.*c\"}}', {}, 'w')
                         )",
                        1200 + (transaction_no + member) % 97
                    ))?;
                }
                connection.execute("COMMIT")?;
            }
            Ok(())
        }));
    }
    barrier.wait();
    for writer in writers {
        writer.join().map_err(|_| "writer thread panicked")??;
    }
    running.store(false, Ordering::Release);
    checkpointer
        .join()
        .map_err(|_| "checkpointer thread panicked")??;
    assert!(checkpoint_count.load(Ordering::Relaxed) >= 10);

    let connection = Connection::open(&path)?;
    assert_eq!(
        connection.integer("SELECT count(*) FROM events")?,
        i64::try_from(WRITERS * TRANSACTIONS * ROWS_PER_TRANSACTION)?
    );
    assert_eq!(
        connection.integer(
            "SELECT count(*) FROM (
               SELECT writer, transaction_no, count(*) AS members
               FROM events GROUP BY writer, transaction_no HAVING members=4
             )"
        )?,
        i64::try_from(WRITERS * TRANSACTIONS)?
    );
    assert_integrity(&connection)?;
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(connection);
    Store::open_existing(&path)?.verify()?;
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn auto_vacuum_pointer_maps_survive_parallel_wal_checkpoints()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITERS: usize = 4;
    const TRANSACTIONS: usize = 24;
    const ROWS_PER_TRANSACTION: usize = 8;
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("auto-vacuum-checkpoint-race.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA page_size=1024;
             PRAGMA auto_vacuum=FULL;
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE events(
               id INTEGER PRIMARY KEY,
               writer INTEGER,
               transaction_no INTEGER,
               body TEXT
             );",
        )?;
    }

    let running = Arc::new(AtomicBool::new(true));
    let checkpoint_count = Arc::new(AtomicUsize::new(0));
    let checkpointer_path = path.clone();
    let checkpointer_running = Arc::clone(&running);
    let checkpointer_count = Arc::clone(&checkpoint_count);
    let checkpointer = std::thread::spawn(move || -> Result<(), String> {
        let connection = Connection::open(&checkpointer_path)?;
        while checkpointer_running.load(Ordering::Acquire)
            || checkpointer_count.load(Ordering::Relaxed) < 20
        {
            let _ = connection
                .integer("SELECT count(*) FROM events")
                .map_err(|error| format!("checkpointer read: {error}"))?;
            let result = connection
                .rows("PRAGMA wal_checkpoint(PASSIVE)")
                .map_err(|error| format!("checkpointer checkpoint: {error}"))?;
            if result.len() != 1
                || result[0].len() != 3
                || !result[0]
                    .iter()
                    .all(|value| matches!(value, Value::Integer(_)))
            {
                return Err(format!("unexpected checkpoint result: {result:?}"));
            }
            checkpointer_count.fetch_add(1, Ordering::Relaxed);
            std::thread::yield_now();
        }
        Ok(())
    });

    let barrier = Arc::new(Barrier::new(WRITERS + 1));
    let mut writers = Vec::new();
    for writer in 0..WRITERS {
        let path = path.clone();
        let barrier = Arc::clone(&barrier);
        writers.push(std::thread::spawn(move || -> Result<(), String> {
            let connection = Connection::open(&path)?;
            connection.execute("PRAGMA synchronous=NORMAL")?;
            barrier.wait();
            for transaction_no in 0..TRANSACTIONS {
                connection.execute("BEGIN IMMEDIATE").map_err(|error| {
                    format!("writer {writer} transaction {transaction_no} begin: {error}")
                })?;
                for member in 0..ROWS_PER_TRANSACTION {
                    let id = writer * 1_000_000 + transaction_no * ROWS_PER_TRANSACTION + member;
                    connection.execute(&format!(
                        "INSERT INTO events VALUES(
                           {id}, {writer}, {transaction_no},
                           printf('{{\"writer\":{writer},\"transaction\":{transaction_no},\"member\":{member},\"text\":\"%.*c\"}}', {}, 'p')
                         )",
                        3500 + (transaction_no + member) % 113
                    )).map_err(|error| {
                        format!("writer {writer} transaction {transaction_no} member {member}: {error}")
                    })?;
                }
                connection.execute("COMMIT").map_err(|error| {
                    format!("writer {writer} transaction {transaction_no} commit: {error}")
                })?;
            }
            Ok(())
        }));
    }
    barrier.wait();
    for writer in writers {
        writer.join().map_err(|_| "writer thread panicked")??;
    }
    running.store(false, Ordering::Release);
    checkpointer
        .join()
        .map_err(|_| "checkpointer thread panicked")??;
    assert!(checkpoint_count.load(Ordering::Relaxed) >= 20);

    let connection = Connection::open(&path)?;
    assert_eq!(
        connection.integer("SELECT count(*) FROM events")?,
        i64::try_from(WRITERS * TRANSACTIONS * ROWS_PER_TRANSACTION)?
    );
    assert_integrity(&connection)?;
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(connection);
    let reopened = Connection::open(&path)?;
    assert_integrity(&reopened)?;
    assert_eq!(
        reopened.integer("SELECT count(*) FROM events")?,
        i64::try_from(WRITERS * TRANSACTIONS * ROWS_PER_TRANSACTION)?
    );
    Ok(())
}

#[test]
fn concurrent_open_write_close_churn_preserves_registry_and_index_state()
-> Result<(), Box<dyn std::error::Error>> {
    const WORKERS: usize = 4;
    const ITERATIONS: usize = 30;
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("open-close-churn.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=3;
             CREATE TABLE events(worker INTEGER, sequence INTEGER, body TEXT,
                                 PRIMARY KEY(worker, sequence));",
        )?;
    }

    let barrier = Arc::new(Barrier::new(WORKERS + 1));
    let mut threads = Vec::new();
    for worker in 0..WORKERS {
        let path = path.clone();
        let barrier = Arc::clone(&barrier);
        threads.push(std::thread::spawn(move || -> Result<(), String> {
            barrier.wait();
            for sequence in 0..ITERATIONS {
                let connection = Connection::open(&path)?;
                connection.execute(&format!(
                    "INSERT INTO events VALUES(
                       {worker}, {sequence},
                       printf('{{\"worker\":{worker},\"sequence\":{sequence},\"text\":\"%.*c\"}}', 900, 'c')
                     )"
                ))?;
                if sequence % 5 == 0 {
                    let count = connection.integer("SELECT count(*) FROM events")?;
                    if count <= 0 {
                        return Err("committed row disappeared during open/close churn".into());
                    }
                }
            }
            Ok(())
        }));
    }
    barrier.wait();
    for thread in threads {
        thread.join().map_err(|_| "churn thread panicked")??;
    }

    let connection = Connection::open(&path)?;
    assert_eq!(
        connection.integer("SELECT count(*) FROM events")?,
        i64::try_from(WORKERS * ITERATIONS)?
    );
    assert_integrity(&connection)?;
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(connection);
    let mut store = Store::open_existing(&path)?;
    store.verify()?;
    let info = store.inspect()?;
    assert!(info.head_txid > 0);
    Ok(())
}

#[test]
fn transaction_semantics_matrix_across_journals_and_page_sizes()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    for journal_mode in ["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL"] {
        for synchronous in ["OFF", "NORMAL", "FULL", "EXTRA"] {
            for page_size in [512_u32, 4096, 65_536] {
                let path = directory.path().join(format!(
                    "transactions-{journal_mode}-{synchronous}-{page_size}.zsqlite"
                ));
                {
                    let connection = Connection::open(&path)?;
                    connection.execute(&format!(
                    "PRAGMA page_size={page_size};
                     PRAGMA journal_mode={journal_mode};
                         PRAGMA synchronous={synchronous};
                     PRAGMA wal_autocheckpoint=0;
                     CREATE TABLE events(id INTEGER PRIMARY KEY, transaction_no INTEGER, kind TEXT);"
                ))?;
                    for transaction_no in 0..12 {
                        connection.execute(&format!(
                            "BEGIN IMMEDIATE;
                         INSERT INTO events VALUES({}, {transaction_no}, 'outer');
                         SAVEPOINT nested;
                         INSERT INTO events VALUES({}, {transaction_no}, 'discarded');
                         ROLLBACK TO nested;
                         RELEASE nested;
                         INSERT INTO events VALUES({}, {transaction_no}, 'kept');
                         {}",
                            transaction_no * 10,
                            transaction_no * 10 + 1,
                            transaction_no * 10 + 2,
                            if transaction_no % 3 == 0 {
                                "ROLLBACK;"
                            } else {
                                "COMMIT;"
                            }
                        ))?;
                    }
                    assert_eq!(connection.integer("SELECT count(*) FROM events")?, 16);
                    assert_eq!(
                        connection.integer("SELECT count(*) FROM events WHERE kind='discarded'")?,
                        0
                    );
                    assert_eq!(
                        connection
                            .integer("SELECT count(*) FROM events WHERE transaction_no%3=0")?,
                        0
                    );
                    assert_integrity(&connection)?;
                    if journal_mode == "WAL" {
                        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
                    }
                }
                let reopened = Connection::open(&path)?;
                assert_eq!(reopened.integer("SELECT count(*) FROM events")?, 16);
                assert_integrity(&reopened)?;
                drop(reopened);
                Store::open_existing(&path)?.verify()?;
            }
        }
    }
    Ok(())
}

#[test]
fn synchronous_off_publishes_clean_commits_and_rollbacks() -> Result<(), Box<dyn std::error::Error>>
{
    register()?;
    let directory = tempfile::tempdir()?;
    for journal_mode in ["DELETE", "WAL"] {
        for page_size in [512_u32, 4096, 65_536] {
            let path = directory
                .path()
                .join(format!("sync-off-{journal_mode}-{page_size}.zsqlite"));
            {
                let connection = Connection::open(&path)?;
                connection.execute(&format!(
                    "PRAGMA page_size={page_size};
                     PRAGMA journal_mode={journal_mode};
                     PRAGMA synchronous=OFF;
                     PRAGMA wal_autocheckpoint=0;
                     CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT);
                     BEGIN IMMEDIATE;
                     INSERT INTO events VALUES(1, printf('%.*c', 9000, 'a'));
                     COMMIT;
                     BEGIN IMMEDIATE;
                     INSERT INTO events VALUES(2, printf('%.*c', 7000, 'b'));
                     ROLLBACK;
                     BEGIN IMMEDIATE;
                     INSERT INTO events VALUES(3, printf('%.*c', 11000, 'c'));
                     COMMIT;"
                ))?;
                if journal_mode == "WAL" {
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
                }
                assert_integrity(&connection)?;
            }
            let reopened = Connection::open(&path)?;
            assert_eq!(reopened.integer("SELECT count(*) FROM events")?, 2);
            assert_eq!(
                reopened.integer("SELECT count(*) FROM events WHERE id=2")?,
                0
            );
            assert_eq!(
                reopened.integer("SELECT sum(length(body)) FROM events")?,
                20_000
            );
            assert_integrity(&reopened)?;
            drop(reopened);
            Store::open_existing(&path)?.verify()?;
        }
    }
    Ok(())
}

#[test]
fn independent_processes_serialize_stale_writers_without_lost_commits()
-> Result<(), Box<dyn std::error::Error>> {
    const WORKERS: usize = 4;
    const TRANSACTIONS: usize = 30;
    register()?;
    let directory = tempfile::tempdir()?;
    for journal_mode in ["DELETE", "WAL"] {
        let path = directory
            .path()
            .join(format!("multiprocess-{journal_mode}.zsqlite"));
        {
            let connection = Connection::open(&path)?;
            connection.execute(&format!(
                "PRAGMA journal_mode={journal_mode};
                 PRAGMA synchronous=FULL;
                 PRAGMA wal_autocheckpoint=0;
                 CREATE TABLE events(
                   worker INTEGER,
                   sequence INTEGER,
                   body TEXT NOT NULL,
                   PRIMARY KEY(worker, sequence)
                 );"
            ))?;
        }
        let start = directory.path().join(format!("start-{journal_mode}"));
        let mut children = Vec::new();
        for worker in 0..WORKERS {
            let ready = directory
                .path()
                .join(format!("ready-{journal_mode}-{worker}"));
            let child = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("vfs::tests::multiprocess_writer_worker")
                .arg("--nocapture")
                .env("ZSQLITE_MULTIPROCESS_DB", &path)
                .env("ZSQLITE_MULTIPROCESS_READY", &ready)
                .env("ZSQLITE_MULTIPROCESS_START", &start)
                .env("ZSQLITE_MULTIPROCESS_MODE", journal_mode)
                .env("ZSQLITE_MULTIPROCESS_WORKER", worker.to_string())
                .env(
                    "ZSQLITE_MULTIPROCESS_TRANSACTIONS",
                    TRANSACTIONS.to_string(),
                )
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            children.push((child, ready));
        }
        let deadline = Instant::now() + Duration::from_secs(20);
        while children.iter().any(|(_, ready)| !ready.exists()) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        if children.iter().any(|(_, ready)| !ready.exists()) {
            for (child, _) in &mut children {
                let _ = child.kill();
            }
            return Err("multiprocess writers did not become ready".into());
        }
        std::fs::write(&start, b"start")?;
        let mut child_failures = Vec::new();
        for (child, _) in children {
            let output = child.wait_with_output()?;
            if !output.status.success() {
                child_failures.push(format!(
                    "multiprocess {journal_mode} writer failed with {}:\nstdout:\n{}\nstderr:\n{}",
                    output.status,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }
        let connection = Connection::open(&path)?;
        if journal_mode == "WAL" {
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        }
        let rolled_back = (0..TRANSACTIONS).filter(|value| value % 7 == 0).count();
        let expected = WORKERS * (TRANSACTIONS - rolled_back);
        if !child_failures.is_empty() {
            return Err(child_failures.join("\n").into());
        }
        assert_eq!(
            connection.integer("SELECT count(*) FROM events")?,
            i64::try_from(expected)?
        );
        assert_eq!(
            connection.integer("SELECT count(DISTINCT worker) FROM events")?,
            i64::try_from(WORKERS)?
        );
        let final_integrity = connection.rows("PRAGMA integrity_check")?;
        if !child_failures.is_empty() {
            return Err(format!(
                "{}\nfinal integrity: {final_integrity:?}",
                child_failures.join("\n")
            )
            .into());
        }
        assert_eq!(final_integrity, vec![vec![Value::Text(b"ok".to_vec())]]);
        drop(connection);
        Store::open_existing(&path)?.verify()?;
    }
    Ok(())
}

#[test]
fn multiprocess_writer_worker() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(path) = std::env::var("ZSQLITE_MULTIPROCESS_DB") else {
        return Ok(());
    };
    let ready = PathBuf::from(std::env::var("ZSQLITE_MULTIPROCESS_READY")?);
    let start = PathBuf::from(std::env::var("ZSQLITE_MULTIPROCESS_START")?);
    let journal_mode = std::env::var("ZSQLITE_MULTIPROCESS_MODE")?;
    let worker: usize = std::env::var("ZSQLITE_MULTIPROCESS_WORKER")?.parse()?;
    let transactions: usize = std::env::var("ZSQLITE_MULTIPROCESS_TRANSACTIONS")?.parse()?;
    register()?;
    let connection = Connection::open(Path::new(&path))?;
    connection.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0;")?;
    std::fs::write(&ready, b"ready")?;
    let deadline = Instant::now() + Duration::from_secs(20);
    while !start.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    if !start.exists() {
        return Err("multiprocess start barrier timed out".into());
    }
    for sequence in 0..transactions {
        connection.execute("BEGIN IMMEDIATE")?;
        connection.execute(&format!(
            "INSERT INTO events VALUES(
               {worker}, {sequence},
               printf('{{\"worker\":{worker},\"sequence\":{sequence},\"body\":\"%.*c\"}}', 1200, 'm')
             )"
        ))?;
        connection.execute(if sequence % 7 == 0 {
            "ROLLBACK"
        } else {
            "COMMIT"
        })?;
        if journal_mode == "WAL" && sequence % 5 == 0 {
            connection.execute("PRAGMA wal_checkpoint(PASSIVE)")?;
        }
    }
    assert_integrity(&connection)
        .map_err(|error| format!("{journal_mode} worker {worker}: {error}"))?;
    Ok(())
}

#[test]
fn simultaneous_process_creation_produces_one_consistent_identity()
-> Result<(), Box<dyn std::error::Error>> {
    const WORKERS: usize = 6;
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("simultaneous-create.zsqlite");
    let start = directory.path().join("create-start");
    let mut children = Vec::new();
    for worker in 0..WORKERS {
        let ready = directory.path().join(format!("create-ready-{worker}"));
        let child = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("vfs::tests::multiprocess_create_worker")
            .arg("--nocapture")
            .env("ZSQLITE_CREATE_DB", &path)
            .env("ZSQLITE_CREATE_READY", &ready)
            .env("ZSQLITE_CREATE_START", &start)
            .env("ZSQLITE_CREATE_WORKER", worker.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        children.push((child, ready));
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    while children.iter().any(|(_, ready)| !ready.exists()) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    if children.iter().any(|(_, ready)| !ready.exists()) {
        for (child, _) in &mut children {
            let _ = child.kill();
        }
        return Err("create workers did not become ready".into());
    }
    std::fs::write(&start, b"start")?;
    for (child, _) in children {
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(format!(
                "create worker failed with {}:\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
    }
    let connection = Connection::open(&path)?;
    assert_eq!(connection.integer("SELECT count(*) FROM created")?, 6);
    assert_integrity(&connection)?;
    drop(connection);
    Store::open_existing(&path)?.verify()?;
    Ok(())
}

#[test]
fn multiprocess_create_worker() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(path) = std::env::var("ZSQLITE_CREATE_DB") else {
        return Ok(());
    };
    let ready = PathBuf::from(std::env::var("ZSQLITE_CREATE_READY")?);
    let start = PathBuf::from(std::env::var("ZSQLITE_CREATE_START")?);
    let worker: usize = std::env::var("ZSQLITE_CREATE_WORKER")?.parse()?;
    register()?;
    std::fs::write(ready, b"ready")?;
    let deadline = Instant::now() + Duration::from_secs(20);
    while !start.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    if !start.exists() {
        return Err("create start barrier timed out".into());
    }
    let connection = Connection::open(Path::new(&path))
        .map_err(|error| format!("create worker {worker} open: {error}"))?;
    connection
        .execute(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE IF NOT EXISTS created(worker INTEGER PRIMARY KEY, body TEXT);",
        )
        .map_err(|error| format!("create worker {worker} schema: {error}"))?;
    connection
        .execute(&format!(
            "INSERT INTO created VALUES({worker}, printf('%.*c', 2000, 'c'))"
        ))
        .map_err(|error| format!("create worker {worker} insert: {error}"))?;
    Ok(())
}

#[test]
fn multi_megabyte_text_and_blob_survive_checkpoint_and_reopen()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("large-values.zsqlite");
    let expected = {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA page_size=16384;
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE payloads(id INTEGER PRIMARY KEY, body TEXT, bytes BLOB);
             INSERT INTO payloads
             VALUES(1, printf('%.*c', 2097152, 'j'), randomblob(1048576));",
        )?;
        let expected = connection.rows("SELECT body, bytes FROM payloads")?;
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        assert_integrity(&connection)?;
        expected
    };
    let reopened = Connection::open(&path)?;
    assert_eq!(reopened.rows("SELECT body, bytes FROM payloads")?, expected);
    assert_integrity(&reopened)?;
    drop(reopened);
    let mut store = Store::open_existing(&path)?;
    store.verify()?;
    assert_eq!(store.inspect()?.page_size, 16_384);
    Ok(())
}

#[test]
fn transactions_and_wal_checkpoints_larger_than_pending_memory_commit_or_rollback_exactly()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    for journal_mode in ["DELETE", "WAL"] {
        let path = directory
            .path()
            .join(format!("bounded-layer-{journal_mode}.zsqlite"));
        let committed_bytes;
        {
            let connection = Connection::open(&path)?;
            connection.execute(&format!(
                "PRAGMA page_size=4096;
                 PRAGMA journal_mode={journal_mode};
                 PRAGMA synchronous=FULL;
                 PRAGMA cache_size=32;
                 PRAGMA wal_autocheckpoint=0;
                 CREATE TABLE transcript(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
                 BEGIN IMMEDIATE;
                 WITH RECURSIVE n(x) AS (
                   VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1600
                 )
                 INSERT INTO transcript
                 SELECT x, printf('{{\"id\":%d,\"text\":\"%.*c\"}}', x, 8192, char(96+(x%26)+1))
                 FROM n;
                 COMMIT;"
            ))?;
            if journal_mode == "WAL" {
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            }
            assert_eq!(connection.integer("SELECT count(*) FROM transcript")?, 1600);
            committed_bytes = connection.integer("SELECT sum(length(body)) FROM transcript")?;

            // The tiny pager cache forces main-file writes well before this
            // transaction ends in rollback-journal mode. Those writes spill
            // from zsqlite's bounded staging memory, then must be discarded.
            connection.execute(
                "BEGIN IMMEDIATE;
                 UPDATE transcript SET body=body||printf('%.*c', 4096, 'z');
                 DELETE FROM transcript WHERE id%7=0;
                 ROLLBACK;",
            )?;
            assert_eq!(connection.integer("SELECT count(*) FROM transcript")?, 1600);
            assert_eq!(
                connection.integer("SELECT sum(length(body)) FROM transcript")?,
                committed_bytes
            );
            if journal_mode == "WAL" {
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            }
            assert_integrity(&connection)?;
        }

        let reopened = Connection::open(&path)?;
        assert_eq!(reopened.integer("SELECT count(*) FROM transcript")?, 1600);
        assert_eq!(
            reopened.integer("SELECT sum(length(body)) FROM transcript")?,
            committed_bytes
        );
        assert_integrity(&reopened)?;
        drop(reopened);
        Store::open_existing(&path)?.verify()?;
    }
    Ok(())
}

#[test]
fn non_db_sqlite_paths_pass_through_without_adoption() -> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    for page_size in [512_u32, 8192, 65_536] {
        let path = directory.path().join(format!("native-{page_size}.sqlite"));
        {
            let native = Connection::open_native(&path)?;
            native.execute(&format!(
                "PRAGMA page_size={page_size};
                 PRAGMA journal_mode=DELETE;
                 CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
                 INSERT INTO messages VALUES(1, printf('%.*c', 10000, 'a'));"
            ))?;
            assert_eq!(native.integer("PRAGMA page_size")?, i64::from(page_size));
        }
        let passthrough = Connection::open(&path)?;
        assert_eq!(
            passthrough.integer("PRAGMA page_size")?,
            i64::from(page_size)
        );
        assert_eq!(passthrough.integer("SELECT count(*) FROM messages")?, 1);
        passthrough.execute("INSERT INTO messages VALUES(2, printf('%.*c', 12000, 'b'));")?;
        assert_integrity(&passthrough)?;
        drop(passthrough);

        let native = Connection::open_native(&path)?;
        assert_eq!(native.integer("SELECT count(*) FROM messages")?, 2);
        assert_integrity(&native)?;
        drop(native);

        assert!(!append_suffix(&path, ".d").exists());
    }
    Ok(())
}

#[test]
fn offline_conversion_and_export_are_byte_and_sql_compatible()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    for page_size in [512_u32, 4096, 65_536] {
        let native_path = directory.path().join(format!("source-{page_size}.db"));
        let compressed_path = directory
            .path()
            .join(format!("compressed-{page_size}.zsqlite"));
        let exported_path = directory.path().join(format!("exported-{page_size}.db"));
        {
            let native = Connection::open_native(&native_path)?;
            native.execute(&format!(
                "PRAGMA page_size={page_size};
                 PRAGMA journal_mode=DELETE;
                 CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT, data BLOB);
                 WITH RECURSIVE n(x) AS (
                   VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<80
                 )
                 INSERT INTO messages
                 SELECT x, printf('{{\"id\":%d,\"body\":\"%.*c\"}}', x, 1800+x, 'q'),
                        randomblob(x%41)
                 FROM n;"
            ))?;
            assert_integrity(&native)?;
        }
        let original = std::fs::read(&native_path)?;
        let info = crate::convert_to_zsqlite(&native_path, &compressed_path)?;
        assert_eq!(info.logical_size, original.len() as u64);
        assert_eq!(std::fs::read(&native_path)?, original);
        {
            let compressed = Connection::open(&compressed_path)?;
            assert_eq!(compressed.integer("SELECT count(*) FROM messages")?, 80);
            assert_integrity(&compressed)?;
        }
        let exported_bytes = crate::export_to_sqlite(&compressed_path, &exported_path)?;
        assert_eq!(exported_bytes, original.len() as u64);
        assert_eq!(std::fs::read(&exported_path)?, original);
        let exported = Connection::open_native(&exported_path)?;
        assert_eq!(exported.integer("SELECT count(*) FROM messages")?, 80);
        assert_integrity(&exported)?;
        assert!(matches!(
            crate::export_to_sqlite(&compressed_path, &exported_path),
            Err(crate::StoreError::DestinationExists(_))
        ));
    }
    Ok(())
}

#[test]
fn compaction_preserves_the_exact_exported_sqlite_image() -> Result<(), Box<dyn std::error::Error>>
{
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("compact-export.zsqlite");
    let before_path = directory.path().join("before-compact.db");
    let after_path = directory.path().join("after-compact.db");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA page_size=8192;
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE messages(id INTEGER PRIMARY KEY, revision INTEGER, body TEXT, data BLOB);
             CREATE INDEX messages_revision ON messages(revision);
             WITH RECURSIVE n(x) AS (
               VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<500
             )
             INSERT INTO messages
             SELECT x, x%31, printf('{\"id\":%d,\"text\":\"%.*c\"}', x, 1800+x%211, 'q'),
                    CAST(printf('%.*c', x%97, char(65+x%26)) AS BLOB)
             FROM n;
             BEGIN IMMEDIATE;
             UPDATE messages SET revision=revision+100, body=body||'-committed' WHERE id%5=0;
             COMMIT;
             BEGIN IMMEDIATE;
             DELETE FROM messages WHERE id%7=0;
             UPDATE messages SET body='must roll back' WHERE id%11=0;
             ROLLBACK;
             PRAGMA wal_checkpoint(TRUNCATE);",
        )?;
        assert_integrity(&connection)?;
    }

    let before_length = crate::export_to_sqlite(&path, &before_path)?;
    let before = std::fs::read(&before_path)?;
    assert_eq!(before_length, before.len() as u64);
    let compacted = crate::compact(&path)?;
    assert!(compacted.generation > 0);
    let after_length = crate::export_to_sqlite(&path, &after_path)?;
    let after = std::fs::read(&after_path)?;
    assert_eq!(after_length, after.len() as u64);
    assert_eq!(after, before);

    let exported = Connection::open_native(&after_path)?;
    assert_eq!(exported.integer("SELECT count(*) FROM messages")?, 500);
    assert_eq!(
        exported.integer("SELECT count(*) FROM messages WHERE body='must roll back'")?,
        0
    );
    assert_integrity(&exported)?;
    drop(exported);
    Store::open_existing(&path)?.verify()?;
    Ok(())
}

#[test]
fn maintenance_is_exclusive_and_read_only_first_does_not_poison_writers()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("maintenance.zsqlite");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO messages VALUES(1, 'first');",
        )?;
    }

    let read_only = Connection::open_read_only(&path)?;
    let writer = Connection::open(&path)?;
    writer.execute("INSERT INTO messages VALUES(2, 'second')")?;
    assert_eq!(read_only.integer("SELECT count(*) FROM messages")?, 2);
    let online = crate::compact(&path)?;
    assert_eq!(online.indexed_pages, online.page_count as usize);
    crate::verify(&path)?;
    crate::export_to_sqlite(&path, directory.path().join("online-export.db"))?;
    drop(writer);
    drop(read_only);

    let compacted = crate::compact(&path)?;
    assert_eq!(compacted.indexed_pages, compacted.page_count as usize);
    let verified = crate::verify(&path)?;
    assert_eq!(verified.page_count, compacted.page_count);
    let reopened = Connection::open(&path)?;
    assert_eq!(reopened.integer("SELECT count(*) FROM messages")?, 2);
    assert_integrity(&reopened)?;
    Ok(())
}

#[test]
fn changing_page_size_after_creation_fails_without_damaging_database()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("reject-page-change.zsqlite");
    let connection = Connection::open(&path)?;
    connection.execute(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=DELETE;
         CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
         INSERT INTO messages VALUES(1, printf('%.*c', 20000, 'x'));",
    )?;
    let result = connection.execute("PRAGMA page_size=8192; VACUUM;");
    assert!(result.is_err(), "an in-place page-size change was accepted");
    assert_eq!(connection.integer("PRAGMA page_size")?, 4096);
    assert_eq!(
        connection.integer("SELECT length(body) FROM messages")?,
        20_000
    );
    assert_integrity(&connection)?;
    drop(connection);

    let reopened = Connection::open(&path)?;
    assert_eq!(
        reopened.integer("SELECT length(body) FROM messages")?,
        20_000
    );
    assert_integrity(&reopened)?;
    Ok(())
}

#[test]
fn subprocess_kill_and_wal_recovery_remain_consistent() -> Result<(), Box<dyn std::error::Error>> {
    run_subprocess_crash_recovery("WAL")
}

#[test]
fn subprocess_kill_and_rollback_journal_recovery_remain_consistent()
-> Result<(), Box<dyn std::error::Error>> {
    run_subprocess_crash_recovery("DELETE")
}

fn run_subprocess_crash_recovery(journal_mode: &str) -> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory
        .path()
        .join(format!("crash-{journal_mode}.zsqlite"));

    for cycle in 0..6 {
        let ready = directory.path().join(format!("ready-{cycle}"));
        let mut child = Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("vfs::tests::crash_writer_worker")
            .arg("--nocapture")
            .env("ZSQLITE_CRASH_WORKER_DB", &path)
            .env("ZSQLITE_CRASH_WORKER_READY", &ready)
            .env("ZSQLITE_CRASH_WORKER_JOURNAL", journal_mode)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.exists() && Instant::now() < deadline {
            if let Some(status) = child.try_wait()? {
                return Err(format!("crash worker exited early with {status}").into());
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        if !ready.exists() {
            child.kill()?;
            return Err("crash worker did not become ready".into());
        }
        std::thread::sleep(Duration::from_millis(3 + cycle * 4));
        child.kill()?;
        let _ = child.wait()?;

        let connection = Connection::open(&path)?;
        assert_integrity(&connection)?;
        connection.execute("INSERT INTO events(payload) VALUES('parent recovery marker')")?;
        if journal_mode == "WAL" {
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        }
        assert!(connection.integer("SELECT count(*) FROM events")? >= i64::try_from(cycle + 2)?);
    }
    let connection = Connection::open(&path)?;
    assert_integrity(&connection)?;
    drop(connection);
    Store::open_existing(&path)?.verify()?;
    Ok(())
}

#[test]
fn crash_writer_worker() -> Result<(), Box<dyn std::error::Error>> {
    let Ok(path) = std::env::var("ZSQLITE_CRASH_WORKER_DB") else {
        return Ok(());
    };
    let ready = std::env::var("ZSQLITE_CRASH_WORKER_READY")?;
    let journal_mode = std::env::var("ZSQLITE_CRASH_WORKER_JOURNAL")?;
    if !matches!(journal_mode.as_str(), "WAL" | "DELETE") {
        return Err(format!("unexpected journal mode {journal_mode}").into());
    }
    register()?;
    let connection = Connection::open(Path::new(&path))?;
    connection.execute(&format!(
        "PRAGMA page_size=8192;
         PRAGMA journal_mode={journal_mode};
         PRAGMA synchronous=FULL;
         PRAGMA wal_autocheckpoint=5;
         CREATE TABLE IF NOT EXISTS events(
           id INTEGER PRIMARY KEY AUTOINCREMENT,
           payload TEXT NOT NULL
         );
         INSERT INTO events(payload) VALUES('worker ready');"
    ))?;
    std::fs::write(ready, b"ready")?;
    for sequence in 0..100_000_u32 {
        connection.execute(&format!(
            "BEGIN IMMEDIATE;
             INSERT INTO events(payload)
             VALUES(printf('{{\"sequence\":{sequence},\"body\":\"%.*c\"}}', {}, 'x'));
             COMMIT;",
            1000 + sequence % 500
        ))?;
    }
    Ok(())
}
