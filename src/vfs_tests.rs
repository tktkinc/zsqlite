use super::*;
use std::ffi::{CStr, CString};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, mpsc};
use std::time::{Duration, Instant};

const ZSQLITE_VFS: &CStr = c"zsqlite";

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
fn wal_database_round_trip_through_vfs() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    register()?;
    let path = directory.path().join("transcripts.db");
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
    assert!(info.sidecar_bytes < info.logical_size);
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
fn vfs_delete_removes_anchor_and_sidecar_for_unusual_path() -> Result<(), Box<dyn std::error::Error>>
{
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("spaces-and-ünicode.db");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO messages VALUES(1, 'delete me');",
        )?;
    }
    let sidecar = append_suffix(&path, "-zsqlite");
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
    assert!(!append_suffix(&path, "-zsqlite-publish").exists());
    assert!(!append_suffix(&path, "-zsqlite-delete").exists());
    assert!(!append_suffix(&path, "-zsqlite-lock").exists());
    let recreated = Connection::open(&path)?;
    recreated.execute("CREATE TABLE replacement(id INTEGER PRIMARY KEY)")?;
    assert_integrity(&recreated)?;
    Ok(())
}

#[test]
fn hard_linked_anchor_or_sidecar_is_rejected_without_modification()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("hard-link.db");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE messages(id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO messages VALUES(1, 'preserve me');",
        )?;
    }
    let sidecar = append_suffix(&path, "-zsqlite");
    let anchor_alias = directory.path().join("anchor-alias");
    std::fs::hard_link(&path, &anchor_alias)?;
    assert!(matches!(
        Store::open_existing(&path),
        Err(crate::StoreError::Unsupported)
    ));
    std::fs::remove_file(anchor_alias)?;

    let sidecar_alias = directory.path().join("sidecar-alias");
    std::fs::hard_link(&sidecar, &sidecar_alias)?;
    assert!(matches!(
        Store::open_existing(&path),
        Err(crate::StoreError::Unsupported)
    ));
    std::fs::remove_file(sidecar_alias)?;

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
        let path = directory.path().join(format!("pages-{page_size}.db"));
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
    let path = directory.path().join("rollback.db");
    {
        let connection = Connection::open(&path)?;
        connection.execute(
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
        )?;
        assert_eq!(connection.integer("SELECT count(*) FROM transcript")?, 250);
        assert_eq!(
            connection.integer("SELECT count(*) FROM transcript WHERE body LIKE '%discarded%'")?,
            0
        );
        connection.execute(
            "DELETE FROM transcript WHERE id%2=0;
             PRAGMA incremental_vacuum;
             VACUUM;",
        )?;
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
    let source_path = directory.path().join("source.db");
    let attached_path = directory.path().join("attached.db");
    let native_backup_path = directory.path().join("native-backup.db");
    let restored_path = directory.path().join("restored.db");

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
        let destination_path = directory.path().join(format!("destination-{page_size}.db"));
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
    let destination_path = directory.path().join("online-destination.db");
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
    let compressed_path = directory.path().join("random-zsqlite.db");
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
    let path = directory.path().join("concurrent.db");
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
    let path = directory.path().join("snapshot.db");
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
    let path = directory.path().join("wal-locks.db");
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
    let path = directory.path().join("rollback-locks.db");
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
    let path = directory.path().join("atomic-visibility.db");
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
    let path = directory.path().join("rollback-parallel.db");
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
    let path = directory.path().join("checkpoint-race.db");
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
fn auto_vacuum_pointer_maps_survive_parallel_wal_checkpoints()
-> Result<(), Box<dyn std::error::Error>> {
    const WRITERS: usize = 4;
    const TRANSACTIONS: usize = 24;
    const ROWS_PER_TRANSACTION: usize = 8;
    register()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("auto-vacuum-checkpoint-race.db");
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
            let _ = connection.integer("SELECT count(*) FROM events")?;
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
            connection.execute("PRAGMA synchronous=NORMAL")?;
            barrier.wait();
            for transaction_no in 0..TRANSACTIONS {
                connection.execute("BEGIN IMMEDIATE")?;
                for member in 0..ROWS_PER_TRANSACTION {
                    let id = writer * 1_000_000 + transaction_no * ROWS_PER_TRANSACTION + member;
                    connection.execute(&format!(
                        "INSERT INTO events VALUES(
                           {id}, {writer}, {transaction_no},
                           printf('{{\"writer\":{writer},\"transaction\":{transaction_no},\"member\":{member},\"text\":\"%.*c\"}}', {}, 'p')
                         )",
                        3500 + (transaction_no + member) % 113
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
    let path = directory.path().join("open-close-churn.db");
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
    assert_eq!(info.index_generation, info.generation);
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
                    "transactions-{journal_mode}-{synchronous}-{page_size}.db"
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
                .join(format!("sync-off-{journal_mode}-{page_size}.db"));
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
            .join(format!("multiprocess-{journal_mode}.db"));
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
    let path = directory.path().join("simultaneous-create.db");
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
    let path = directory.path().join("large-values.db");
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
            .join(format!("bounded-layer-{journal_mode}.db"));
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
fn ordinary_sqlite_databases_pass_through_without_adoption()
-> Result<(), Box<dyn std::error::Error>> {
    register()?;
    let directory = tempfile::tempdir()?;
    for page_size in [512_u32, 8192, 65_536] {
        let path = directory.path().join(format!("native-{page_size}.db"));
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

        for suffix in [
            "-zsqlite",
            "-zsqlite-lock",
            "-zsqlite-publish",
            "-zsqlite-delete",
        ] {
            assert!(!append_suffix(&path, suffix).exists());
        }
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
        let compressed_path = directory.path().join(format!("compressed-{page_size}.db"));
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
    let path = directory.path().join("compact-export.db");
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
    assert_eq!(compacted.generation, 1);
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
    let path = directory.path().join("maintenance.db");
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
    assert!(matches!(
        crate::compact(&path),
        Err(crate::StoreError::Busy)
    ));
    assert!(matches!(crate::verify(&path), Err(crate::StoreError::Busy)));
    assert!(matches!(
        crate::export_to_sqlite(&path, directory.path().join("busy-export.db")),
        Err(crate::StoreError::Busy)
    ));
    drop(writer);
    drop(read_only);

    let compacted = crate::compact(&path)?;
    assert_eq!(compacted.index_generation, compacted.generation);
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
    let path = directory.path().join("reject-page-change.db");
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
    let path = directory.path().join(format!("crash-{journal_mode}.db"));

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
