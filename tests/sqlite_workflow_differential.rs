#![cfg(all(feature = "static", unix))]

use blake3::Hasher;
use libsqlite3_sys as ffi;
use std::ffi::{CStr, CString, c_int};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::{null, null_mut};
use std::slice;
use std::thread;
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const ZSQLITE_VFS: &CStr = c"zsqlite";

struct Connection(*mut ffi::sqlite3);

impl Connection {
    fn open(path: &Path, vfs: &CStr) -> Result<Self, String> {
        let path = CString::new(path.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
        let mut database = null_mut();
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                path.as_ptr(),
                &raw mut database,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                vfs.as_ptr(),
            )
        };
        if rc != ffi::SQLITE_OK {
            let message = if database.is_null() {
                "open returned a null handle".into()
            } else {
                unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(database)) }
                    .to_string_lossy()
                    .into_owned()
            };
            if !database.is_null() {
                let _ = unsafe { ffi::sqlite3_close(database) };
            }
            return Err(format!("SQLite open failed with {rc}: {message}"));
        }
        let connection = Self(database);
        let timeout_rc = unsafe { ffi::sqlite3_busy_timeout(connection.0, 10_000) };
        if timeout_rc != ffi::SQLITE_OK {
            return Err(connection.error("setting busy timeout", timeout_rc));
        }
        Ok(connection)
    }

    fn execute(&self, sql: &str) -> Result<(), String> {
        let sql = CString::new(sql).map_err(|error| error.to_string())?;
        let mut error = null_mut();
        let rc =
            unsafe { ffi::sqlite3_exec(self.0, sql.as_ptr(), None, null_mut(), &raw mut error) };
        if rc == ffi::SQLITE_OK {
            return Ok(());
        }
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
        Err(format!("SQLite exec failed with {rc}: {message}"))
    }

    fn integer(&self, sql: &str) -> Result<i64, String> {
        let mut statement = self.prepare(sql)?;
        let step = unsafe { ffi::sqlite3_step(statement.0) };
        if step != ffi::SQLITE_ROW {
            return Err(self.error("stepping integer query", step));
        }
        if unsafe { ffi::sqlite3_column_type(statement.0, 0) } != ffi::SQLITE_INTEGER {
            return Err(format!("integer query returned a non-integer: {sql}"));
        }
        let value = unsafe { ffi::sqlite3_column_int64(statement.0, 0) };
        statement.finish(self)?;
        Ok(value)
    }

    fn text(&self, sql: &str) -> Result<String, String> {
        let mut statement = self.prepare(sql)?;
        let step = unsafe { ffi::sqlite3_step(statement.0) };
        if step != ffi::SQLITE_ROW {
            return Err(self.error("stepping text query", step));
        }
        let length = unsafe { ffi::sqlite3_column_bytes(statement.0, 0) };
        let pointer = unsafe { ffi::sqlite3_column_text(statement.0, 0) };
        let bytes = column_bytes(pointer, length)?;
        let value = String::from_utf8(bytes).map_err(|error| error.to_string())?;
        statement.finish(self)?;
        Ok(value)
    }

    fn logical_digest(&self) -> Result<[u8; 32], String> {
        let mut hasher = Hasher::new();
        for query in [
            "SELECT type, name, tbl_name, coalesce(sql, '')
             FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name",
            "SELECT id, label FROM parents ORDER BY id",
            "SELECT id, parent_id, bucket, note, payload FROM items ORDER BY id",
            "SELECT seq, event FROM audit ORDER BY seq",
        ] {
            hash_query(self, query, &mut hasher)?;
        }
        Ok(*hasher.finalize().as_bytes())
    }

    fn assert_healthy(&self) -> Result<(), String> {
        let integrity = self.text("PRAGMA integrity_check")?;
        if integrity != "ok" {
            return Err(format!("integrity_check returned {integrity:?}"));
        }
        let violations = self.integer("SELECT count(*) FROM pragma_foreign_key_check")?;
        if violations != 0 {
            return Err(format!("foreign_key_check returned {violations} rows"));
        }
        Ok(())
    }

    fn prepare(&self, sql: &str) -> Result<Statement, String> {
        let sql = CString::new(sql).map_err(|error| error.to_string())?;
        let mut statement = null_mut();
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(self.0, sql.as_ptr(), -1, &raw mut statement, null_mut())
        };
        if rc == ffi::SQLITE_OK {
            Ok(Statement(statement))
        } else {
            Err(self.error("preparing query", rc))
        }
    }

    fn error(&self, operation: &str, rc: c_int) -> String {
        let message = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) }.to_string_lossy();
        format!("{operation} failed with SQLite error {rc}: {message}")
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = unsafe { ffi::sqlite3_close(self.0) };
    }
}

struct Statement(*mut ffi::sqlite3_stmt);

impl Statement {
    fn finish(&mut self, connection: &Connection) -> Result<(), String> {
        let statement = std::mem::replace(&mut self.0, null_mut());
        let rc = unsafe { ffi::sqlite3_finalize(statement) };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(connection.error("finalizing query", rc))
        }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.0.is_null() {
            let _ = unsafe { ffi::sqlite3_finalize(self.0) };
        }
    }
}

fn column_bytes(pointer: *const u8, length: c_int) -> Result<Vec<u8>, String> {
    let length = usize::try_from(length).map_err(|_| "SQLite returned a negative length")?;
    if length == 0 {
        return Ok(Vec::new());
    }
    if pointer.is_null() {
        return Err("SQLite returned a null pointer for nonempty data".into());
    }
    Ok(unsafe { slice::from_raw_parts(pointer, length) }.to_vec())
}

fn hash_query(connection: &Connection, query: &str, hasher: &mut Hasher) -> Result<(), String> {
    hasher.update(b"query\0");
    hasher.update(query.as_bytes());
    let mut statement = connection.prepare(query)?;
    loop {
        let step = unsafe { ffi::sqlite3_step(statement.0) };
        if step == ffi::SQLITE_DONE {
            break;
        }
        if step != ffi::SQLITE_ROW {
            return Err(connection.error("stepping digest query", step));
        }
        hasher.update(b"row\0");
        let columns = unsafe { ffi::sqlite3_column_count(statement.0) };
        for column in 0..columns {
            let kind = unsafe { ffi::sqlite3_column_type(statement.0, column) };
            hasher.update(&kind.to_le_bytes());
            match kind {
                ffi::SQLITE_NULL => {}
                ffi::SQLITE_INTEGER => {
                    hasher.update(
                        &unsafe { ffi::sqlite3_column_int64(statement.0, column) }.to_le_bytes(),
                    );
                }
                ffi::SQLITE_FLOAT => {
                    hasher.update(
                        &unsafe { ffi::sqlite3_column_double(statement.0, column) }
                            .to_bits()
                            .to_le_bytes(),
                    );
                }
                ffi::SQLITE_TEXT => {
                    let length = unsafe { ffi::sqlite3_column_bytes(statement.0, column) };
                    let pointer = unsafe { ffi::sqlite3_column_text(statement.0, column) };
                    let value = column_bytes(pointer, length)?;
                    hasher.update(&(value.len() as u64).to_le_bytes());
                    hasher.update(&value);
                }
                ffi::SQLITE_BLOB => {
                    let length = unsafe { ffi::sqlite3_column_bytes(statement.0, column) };
                    let pointer = unsafe { ffi::sqlite3_column_blob(statement.0, column) }.cast();
                    let value = column_bytes(pointer, length)?;
                    hasher.update(&(value.len() as u64).to_le_bytes());
                    hasher.update(&value);
                }
                other => return Err(format!("unknown SQLite column type {other}")),
            }
        }
    }
    statement.finish(connection)
}

fn native_vfs_name() -> TestResult<CString> {
    let vfs = unsafe { ffi::sqlite3_vfs_find(null()) };
    let vfs = unsafe { vfs.as_ref() }.ok_or("SQLite has no default VFS")?;
    Ok(unsafe { CStr::from_ptr(vfs.zName) }.to_owned())
}

fn execute_both(native: &Connection, managed: &Connection, sql: &str, phase: &str) -> TestResult {
    native
        .execute(sql)
        .map_err(|error| format!("native {phase}: {error}"))?;
    managed
        .execute(sql)
        .map_err(|error| format!("zsqlite {phase}: {error}"))?;
    Ok(())
}

fn compare_databases(native: &Connection, managed: &Connection, phase: &str) -> TestResult {
    native
        .assert_healthy()
        .map_err(|error| format!("native {phase}: {error}"))?;
    managed
        .assert_healthy()
        .map_err(|error| format!("zsqlite {phase}: {error}"))?;
    assert_eq!(
        native.logical_digest()?,
        managed.logical_digest()?,
        "logical contents diverged during {phase}"
    );
    Ok(())
}

fn maintain(path: &Path) -> TestResult {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match zsqlite::flush(path) {
            Ok(_) => break,
            Err(zsqlite::StoreError::Busy) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
    loop {
        match zsqlite::compact(path) {
            Ok(_) => break,
            Err(zsqlite::StoreError::Busy) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
    let info = zsqlite::verify(path)?;
    assert_eq!(info.indexed_pages, info.page_count as usize);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn realistic_sqlite_lifecycle_matches_native_across_journals_and_page_sizes() -> TestResult {
    let native_vfs = native_vfs_name()?;
    zsqlite::register_static_vfs()
        .map_err(|code| format!("zsqlite VFS registration failed with {code}"))?;
    let directory = tempfile::tempdir()?;

    for journal in ["DELETE", "WAL"] {
        for page_size in [1024_u32, 8192] {
            let case = format!("{journal}/{page_size}");
            let native_path = directory
                .path()
                .join(format!("native-{journal}-{page_size}.db"));
            let managed_path = directory
                .path()
                .join(format!("managed-{journal}-{page_size}.zsqlite"));
            let mut native = Connection::open(&native_path, &native_vfs)
                .map_err(|error| format!("opening native {case}: {error}"))?;
            let mut managed = Connection::open(&managed_path, ZSQLITE_VFS)
                .map_err(|error| format!("opening zsqlite {case}: {error}"))?;

            let setup = format!(
                "PRAGMA page_size={page_size};
                 PRAGMA auto_vacuum=INCREMENTAL;
                 VACUUM;
                 PRAGMA journal_mode={journal};
                 PRAGMA synchronous=FULL;
                 PRAGMA foreign_keys=ON;
                 CREATE TABLE parents(id INTEGER PRIMARY KEY, label TEXT NOT NULL UNIQUE);
                 CREATE TABLE items(
                   id INTEGER PRIMARY KEY,
                   parent_id INTEGER NOT NULL REFERENCES parents(id),
                   bucket INTEGER NOT NULL,
                   note TEXT NOT NULL,
                   payload BLOB NOT NULL
                 );
                 CREATE INDEX items_bucket ON items(bucket, id);
                 CREATE TABLE audit(seq INTEGER PRIMARY KEY, event TEXT NOT NULL);"
            );
            execute_both(&native, &managed, &setup, &format!("setup {case}"))?;
            assert_eq!(native.integer("PRAGMA page_size")?, i64::from(page_size));
            assert_eq!(managed.integer("PRAGMA page_size")?, i64::from(page_size));
            assert_eq!(
                native.text("PRAGMA journal_mode")?.to_ascii_uppercase(),
                journal
            );
            assert_eq!(
                managed.text("PRAGMA journal_mode")?.to_ascii_uppercase(),
                journal
            );

            execute_both(
                &native,
                &managed,
                "BEGIN IMMEDIATE;
                 INSERT INTO parents VALUES
                   (1,'alpha'),(2,'beta'),(3,'gamma'),(4,'delta'),
                   (5,'epsilon'),(6,'zeta'),(7,'eta');
                 WITH RECURSIVE n(x) AS (
                   VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<180
                 )
                 INSERT INTO items(id,parent_id,bucket,note,payload)
                 SELECT x, 1+(x%7), x%13,
                        printf('row-%06d-%.*c', x, 80+(x%7), char(65+(x%26))),
                        CAST(printf('%.*c', 700+(x%11)*73, char(97+(x%26))) AS BLOB)
                 FROM n;
                 SAVEPOINT discarded;
                 UPDATE items SET note='discarded-'||note WHERE id%3=0;
                 DELETE FROM items WHERE id%5=0;
                 INSERT INTO audit(event) VALUES('must disappear');
                 ROLLBACK TO discarded;
                 RELEASE discarded;
                 SAVEPOINT kept;
                 UPDATE items SET bucket=bucket+100, note=note||'-kept' WHERE id%11=0;
                 DELETE FROM items WHERE id%19=0;
                 INSERT INTO audit(event) VALUES('initial commit');
                 RELEASE kept;
                 COMMIT;",
                &format!("initial savepoint transaction {case}"),
            )?;
            compare_databases(&native, &managed, &format!("initial commit {case}"))?;

            let before_rollback = native.logical_digest()?;
            execute_both(
                &native,
                &managed,
                "BEGIN;
                 UPDATE items SET payload=zeroblob(9000), note='rolled back';
                 SAVEPOINT nested;
                 DELETE FROM items;
                 INSERT INTO audit(event) VALUES('outer rollback');
                 RELEASE nested;
                 ROLLBACK;",
                &format!("outer rollback {case}"),
            )?;
            assert_eq!(native.logical_digest()?, before_rollback);
            compare_databases(&native, &managed, &format!("outer rollback {case}"))?;

            drop(native);
            drop(managed);
            maintain(&managed_path)?;
            native = Connection::open(&native_path, &native_vfs)?;
            managed = Connection::open(&managed_path, ZSQLITE_VFS)?;
            execute_both(
                &native,
                &managed,
                "PRAGMA foreign_keys=ON;",
                &format!("first reopen {case}"),
            )?;
            compare_databases(&native, &managed, &format!("first maintenance {case}"))?;

            execute_both(
                &native,
                &managed,
                "BEGIN IMMEDIATE;
                 WITH RECURSIVE n(x) AS (
                   VALUES(1001) UNION ALL SELECT x+1 FROM n WHERE x<1450
                 )
                 INSERT INTO items(id,parent_id,bucket,note,payload)
                 SELECT x, 1+(x%7), x%17, printf('growth-%d',x),
                        CAST(printf('%.*c', 2200+(x%9)*101, char(65+(x%26))) AS BLOB)
                 FROM n;
                 INSERT INTO audit(event) VALUES('growth commit');
                 COMMIT;",
                &format!("growth {case}"),
            )?;
            compare_databases(&native, &managed, &format!("grown database {case}"))?;
            let native_grown_pages = native.integer("PRAGMA page_count")?;
            let managed_grown_pages = managed.integer("PRAGMA page_count")?;

            execute_both(
                &native,
                &managed,
                "DELETE FROM items WHERE id>24;
                 INSERT INTO audit(event) VALUES('shrink commit');
                 PRAGMA incremental_vacuum(100000);
                 VACUUM;",
                &format!("truncate and vacuum {case}"),
            )?;
            let native_shrunk_pages = native.integer("PRAGMA page_count")?;
            let managed_shrunk_pages = managed.integer("PRAGMA page_count")?;
            assert!(
                native_shrunk_pages < native_grown_pages,
                "native file did not shrink in {case}: {native_grown_pages} -> {native_shrunk_pages}"
            );
            assert!(
                managed_shrunk_pages < managed_grown_pages,
                "managed image did not shrink in {case}: {managed_grown_pages} -> {managed_shrunk_pages}"
            );
            compare_databases(&native, &managed, &format!("vacuumed database {case}"))?;

            drop(native);
            drop(managed);
            maintain(&managed_path)?;
            native = Connection::open(&native_path, &native_vfs)?;
            managed = Connection::open(&managed_path, ZSQLITE_VFS)?;
            execute_both(
                &native,
                &managed,
                "PRAGMA foreign_keys=ON;
                 BEGIN IMMEDIATE;
                 SAVEPOINT regrow;
                 WITH RECURSIVE n(x) AS (
                   VALUES(5001) UNION ALL SELECT x+1 FROM n WHERE x<5300
                 )
                 INSERT INTO items(id,parent_id,bucket,note,payload)
                 SELECT x, 1+(x%7), x%23, printf('regrow-%d',x),
                        CAST(printf('%.*c', 1300+(x%13)*89, char(97+(x%26))) AS BLOB)
                 FROM n;
                 SAVEPOINT reverted_inner;
                 UPDATE items SET note='inner-discarded' WHERE id>=5001;
                 DELETE FROM items WHERE id%4=0;
                 ROLLBACK TO reverted_inner;
                 RELEASE reverted_inner;
                 UPDATE items SET note=note||'-final' WHERE id%9=0;
                 INSERT INTO audit(event) VALUES('regrow commit');
                 RELEASE regrow;
                 COMMIT;",
                &format!("nested regrow {case}"),
            )?;
            assert!(native.integer("PRAGMA page_count")? > native_shrunk_pages);
            assert!(managed.integer("PRAGMA page_count")? > managed_shrunk_pages);
            compare_databases(&native, &managed, &format!("regrown database {case}"))?;

            if journal == "WAL" {
                execute_both(
                    &native,
                    &managed,
                    "PRAGMA wal_checkpoint(TRUNCATE);",
                    &format!("final checkpoint {case}"),
                )?;
            }
            drop(native);
            drop(managed);
            maintain(&managed_path)?;
            native = Connection::open(&native_path, &native_vfs)?;
            managed = Connection::open(&managed_path, ZSQLITE_VFS)?;
            execute_both(
                &native,
                &managed,
                "PRAGMA foreign_keys=ON;",
                &format!("final reopen {case}"),
            )?;
            compare_databases(&native, &managed, &format!("final maintenance {case}"))?;
        }
    }
    Ok(())
}
