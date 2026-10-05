#![cfg(feature = "static")]
use libsqlite3_sys as ffi;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::Path;
use std::sync::Arc;
use zsqlite::domain::{BackendId, StoredBytes};
use zsqlite::storage::adapter::{
    BackendError, DeletePermit, ObjectKey, ObjectRange, ObjectWriter, Publication, Revision,
    RootRecord, StorageBackend,
};
use zsqlite::{FilesystemBackend, Storage};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
struct ReadOnly(Arc<FilesystemBackend>);
impl StorageBackend for ReadOnly {
    fn identity(&self) -> BackendId {
        self.0.identity()
    }
    fn read_ranges(
        &self,
        requests: &[ObjectRange],
    ) -> std::result::Result<Vec<Vec<u8>>, BackendError> {
        self.0.read_ranges(requests)
    }
    fn stat(&self, key: ObjectKey) -> std::result::Result<Option<StoredBytes>, BackendError> {
        self.0.stat(key)
    }
    fn read_root(&self) -> std::result::Result<Option<RootRecord>, BackendError> {
        self.0.read_root()
    }
    fn begin_write(&self) -> std::result::Result<Box<dyn ObjectWriter + '_>, BackendError> {
        panic!("read-only snapshot tried to write")
    }
    fn compare_exchange_root(
        &self,
        _expected: Option<&Revision>,
        _bytes: &[u8],
    ) -> std::result::Result<Publication, BackendError> {
        panic!("read-only snapshot tried to publish")
    }
    fn inventory(
        &self,
        _after: Option<ObjectKey>,
        _limit: usize,
    ) -> std::result::Result<Vec<ObjectKey>, BackendError> {
        panic!("read-only snapshot tried to list")
    }
    fn delete(&self, _permit: DeletePermit<'_>) -> std::result::Result<(), BackendError> {
        panic!("read-only snapshot tried to delete")
    }
}
struct Connection(*mut ffi::sqlite3);
impl Connection {
    fn open(path: &Path, vfs: Option<&str>, flags: c_int) -> Result<Self> {
        let path = CString::new(path.to_str().ok_or("invalid path")?)?;
        let vfs = vfs.map(CString::new).transpose()?;
        let mut database = std::ptr::null_mut();
        // SAFETY: Terminated path/VFS names and an exclusive output pointer are
        // valid for the call; SQLite initializes the owned handle on error too.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                path.as_ptr(),
                &raw mut database,
                flags,
                vfs.as_ref().map_or(std::ptr::null(), |name| name.as_ptr()),
            )
        };
        let connection = Self(database);
        if rc != ffi::SQLITE_OK {
            return Err(format!("open returned {rc}").into());
        }
        Ok(connection)
    }
    fn query(&self, sql: &str) -> Result<Vec<String>> {
        unsafe extern "C" fn row(
            context: *mut c_void,
            count: c_int,
            values: *mut *mut c_char,
            _names: *mut *mut c_char,
        ) -> c_int {
            // SAFETY: exec borrows the exclusive Vec context synchronously; our
            // scalar queries produce one non-NULL terminated string per row.
            unsafe {
                assert_eq!(count, 1);
                (*context.cast::<Vec<String>>())
                    .push(CStr::from_ptr(*values).to_string_lossy().into_owned());
            }
            0
        }
        let sql = CString::new(sql)?;
        let mut rows = Vec::<String>::new();
        // SAFETY: Owned live connection and SQL; callback/context are borrowed
        // synchronously. SQLite retains no pointers after exec returns.
        let rc = unsafe {
            ffi::sqlite3_exec(
                self.0,
                sql.as_ptr(),
                Some(row),
                (&raw mut rows).cast(),
                std::ptr::null_mut(),
            )
        };
        if rc != ffi::SQLITE_OK {
            return Err(format!("query returned {rc}").into());
        }
        Ok(rows)
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: This owns the connection and exec leaves no prepared statements.
        unsafe {
            ffi::sqlite3_close(self.0);
        }
    }
}

#[test]
fn sealed_snapshot_is_lazy_read_only_and_stable_across_publications() -> Result {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.db");
    let archive = directory.path().join("archive.zsqlite");
    create_source(&source)?;
    zsqlite::convert_to_zsqlite(&source, &archive)?;
    let backend = Arc::new(FilesystemBackend::open(
        directory.path().join("archive.zsqlite.d"),
    )?);
    let storage = Storage::new(
        Arc::new(ReadOnly(Arc::clone(&backend))),
        directory.path().join("readonly-coord"),
    )?;
    let before = backend.read_root()?.unwrap();
    let view = storage.open_sealed()?;
    assert_eq!(view.logical_size().get(), std::fs::metadata(&source)?.len());
    drop(view);
    storage.register_read_only_vfs("sealed-snapshot-test", 8192)?;
    assert!(
        storage
            .register_read_only_vfs("sealed-snapshot-test", 8192)
            .is_err()
    );
    storage.register_read_only_vfs("sealed-snapshot-uncached-test", 0)?;
    let reader = Connection::open(
        Path::new("snapshot"),
        Some("sealed-snapshot-test"),
        ffi::SQLITE_OPEN_READONLY,
    )?;
    assert_eq!(
        reader.query("SELECT value FROM items WHERE id=1999")?,
        ["item-1999"]
    );
    assert_eq!(reader.query("SELECT count(*) FROM items")?, ["2000"]);
    assert_eq!(reader.query("PRAGMA integrity_check")?, ["ok"]);
    assert!(
        reader
            .query("UPDATE items SET value='changed' WHERE id=1")
            .is_err()
    );
    assert!(
        Connection::open(
            Path::new("snapshot"),
            Some("sealed-snapshot-test"),
            ffi::SQLITE_OPEN_READWRITE
        )
        .is_err()
    );
    let uncached = Connection::open(
        Path::new("snapshot"),
        Some("sealed-snapshot-uncached-test"),
        ffi::SQLITE_OPEN_READONLY,
    )?;
    assert_eq!(
        uncached.query("SELECT value FROM items WHERE id=1999")?,
        ["item-1999"]
    );
    assert_eq!(backend.read_root()?.unwrap().revision(), before.revision());
    assert!(
        !directory
            .path()
            .join("readonly-coord/active-location")
            .exists()
    );

    zsqlite::register_static_vfs().map_err(|rc| format!("registration: {rc}"))?;
    {
        let writer = Connection::open(&archive, Some("zsqlite"), ffi::SQLITE_OPEN_READWRITE)?;
        writer.query("UPDATE items SET value='new version' WHERE id=1999")?;
    }
    // Active, unsealed writes are outside the bucket's query boundary.
    storage.register_read_only_vfs("sealed-before-flush-test", 0)?;
    let unsealed = Connection::open(
        Path::new("snapshot"),
        Some("sealed-before-flush-test"),
        ffi::SQLITE_OPEN_READONLY,
    )?;
    assert_eq!(
        unsealed.query("SELECT value FROM items WHERE id=1999")?,
        ["item-1999"]
    );
    zsqlite::flush(&archive)?;
    storage.register_read_only_vfs("sealed-after-flush-test", 0)?;
    let latest = Connection::open(
        Path::new("snapshot"),
        Some("sealed-after-flush-test"),
        ffi::SQLITE_OPEN_READONLY,
    )?;
    assert_eq!(
        latest.query("SELECT value FROM items WHERE id=1999")?,
        ["new version"]
    );
    assert_eq!(
        reader.query("SELECT value FROM items WHERE id=1999")?,
        ["item-1999"]
    );
    assert_eq!(
        uncached.query("SELECT value FROM items WHERE id=1999")?,
        ["item-1999"]
    );
    Ok(())
}

fn create_source(path: &Path) -> Result {
    let source = Connection::open(
        path,
        None,
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )?;
    source.query("CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)")?;
    source.query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000) INSERT INTO items SELECT x, printf('item-%04d',x) FROM n")?;
    Ok(())
}

#[test]
fn missing_sealed_head_is_rejected() -> Result {
    let directory = tempfile::tempdir()?;
    let storage = Storage::new(Arc::new(zsqlite::MemoryBackend::new()?), directory.path())?;
    assert!(matches!(
        storage.open_sealed(),
        Err(zsqlite::StoreError::NoSealedHead)
    ));
    Ok(())
}
