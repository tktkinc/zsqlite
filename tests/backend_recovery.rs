#![cfg(all(feature = "static", unix))]

use libsqlite3_sys as ffi;
use std::ffi::{CStr, CString};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::ptr::{null, null_mut};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const VFS: &CStr = c"zsqlite";
const GC_HOLDER_DB: &str = "ZSQLITE_GC_HOLDER_DB";
const GC_HOLDER_READY: &str = "ZSQLITE_GC_HOLDER_READY";
const GC_HOLDER_RELEASE: &str = "ZSQLITE_GC_HOLDER_RELEASE";

struct Connection(*mut ffi::sqlite3);

impl Connection {
    fn open_zsqlite(path: &Path) -> Result<Self, String> {
        zsqlite::register_static_vfs()
            .map_err(|code| format!("VFS registration failed with SQLite code {code}"))?;
        Self::open(path, VFS.as_ptr())
    }

    fn open_native(path: &Path) -> Result<Self, String> {
        Self::open(path, null())
    }

    fn open(path: &Path, vfs: *const std::ffi::c_char) -> Result<Self, String> {
        let path = CString::new(path.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
        let mut database = null_mut();
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                path.as_ptr(),
                &raw mut database,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                vfs,
            )
        };
        if rc != ffi::SQLITE_OK {
            let message = if database.is_null() {
                "open returned a null database handle".into()
            } else {
                unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(database)) }
                    .to_string_lossy()
                    .into_owned()
            };
            if !database.is_null() {
                let _ = unsafe { ffi::sqlite3_close(database) };
            }
            return Err(format!("SQLite error {rc}: {message}"));
        }
        let connection = Self(database);
        let timeout = unsafe { ffi::sqlite3_busy_timeout(connection.0, 5_000) };
        if timeout != ffi::SQLITE_OK {
            return Err(connection.error(timeout));
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
            self.error(rc)
        } else {
            let message = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned();
            unsafe { ffi::sqlite3_free(error.cast()) };
            message
        };
        Err(format!("SQLite error {rc}: {message}"))
    }

    fn integer(&self, sql: &str) -> Result<i64, String> {
        let sql = CString::new(sql).map_err(|error| error.to_string())?;
        let mut statement = null_mut();
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(self.0, sql.as_ptr(), -1, &raw mut statement, null_mut())
        };
        if rc != ffi::SQLITE_OK {
            return Err(self.error(rc));
        }
        let step = unsafe { ffi::sqlite3_step(statement) };
        let result = if step == ffi::SQLITE_ROW {
            Ok(unsafe { ffi::sqlite3_column_int64(statement, 0) })
        } else {
            Err(self.error(step))
        };
        let finalize = unsafe { ffi::sqlite3_finalize(statement) };
        if finalize != ffi::SQLITE_OK {
            return Err(self.error(finalize));
        }
        result
    }

    fn error(&self, code: i32) -> String {
        let message = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) }.to_string_lossy();
        format!("SQLite error {code}: {message}")
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let rc = unsafe { ffi::sqlite3_close(self.0) };
        assert_eq!(rc, ffi::SQLITE_OK);
    }
}

fn sidecar(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".d");
    PathBuf::from(value)
}

fn wal_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push("-wal");
    PathBuf::from(value)
}

fn segment_paths(path: &Path) -> TestResult<Vec<PathBuf>> {
    let mut paths = std::fs::read_dir(sidecar(path).join("segments"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "zseg")
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn staging_paths(directory: &Path) -> TestResult<Vec<PathBuf>> {
    let mut paths = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with('.')
                        && (name.contains(".convert.")
                            || name.contains(".export.")
                            || name.contains(".active."))
                })
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

fn create_native(path: &Path) -> TestResult {
    let connection = Connection::open_native(path)?;
    connection.execute(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=DELETE;
         PRAGMA synchronous=FULL;
         CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);
         WITH RECURSIVE sequence(value) AS (
           VALUES(1) UNION ALL SELECT value+1 FROM sequence WHERE value<80
         )
         INSERT INTO events SELECT value, randomblob(900+value) FROM sequence;",
    )?;
    Ok(())
}

fn managed_delete(path: &Path) -> TestResult<i32> {
    zsqlite::register_static_vfs()
        .map_err(|code| format!("VFS registration failed with SQLite code {code}"))?;
    let vfs_pointer = unsafe { ffi::sqlite3_vfs_find(VFS.as_ptr()) };
    let vfs = unsafe { vfs_pointer.as_ref() }.ok_or("registered VFS was not found")?;
    let delete = vfs.xDelete.ok_or("registered VFS has no xDelete")?;
    let path = CString::new(path.as_os_str().as_bytes())?;
    Ok(unsafe { delete(vfs_pointer, path.as_ptr(), 1) })
}

fn wait_for_marker(child: &mut Child, marker: &Path, deadline: Instant) -> TestResult {
    while !marker.exists() {
        if let Some(status) = child.try_wait()? {
            return Err(format!("holder exited with {status} before becoming ready").into());
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("timed out waiting for {}", marker.display()).into());
        }
        thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

fn wait_for_output(mut child: Child, deadline: Instant, label: &str) -> TestResult<Output> {
    loop {
        if child.try_wait()?.is_some() {
            return Ok(child.wait_with_output()?);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child.wait_with_output()?;
            return Err(format!(
                "{label} exceeded its watchdog deadline\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn assert_success(output: &Output, label: &str) -> TestResult {
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{label} failed with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

#[test]
fn export_rejects_live_wal_and_publishes_a_complete_checkpointed_database() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.zsqlite");
    let rejected = directory.path().join("rejected.db");
    let exported = directory.path().join("exported.db");
    let connection = Connection::open_zsqlite(&source)?;
    connection.execute(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         PRAGMA synchronous=FULL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB);
         INSERT INTO events VALUES(1, randomblob(12000));",
    )?;
    assert!(wal_path(&source).metadata()?.len() > 32);
    assert!(matches!(
        zsqlite::export_to_sqlite(&source, &rejected),
        Err(zsqlite::StoreError::Busy)
    ));
    assert!(matches!(
        zsqlite::verify(&source),
        Err(zsqlite::StoreError::Busy)
    ));
    assert!(!rejected.exists());

    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    assert!(matches!(
        zsqlite::export_to_sqlite(&source, &exported),
        Err(zsqlite::StoreError::Busy)
    ));
    drop(connection);
    zsqlite::export_to_sqlite(&source, &exported)?;
    let native = Connection::open_native(&exported)?;
    assert_eq!(native.integer("SELECT count(*) FROM events")?, 1);
    assert_eq!(
        native.integer("SELECT count(*) FROM pragma_integrity_check WHERE integrity_check='ok'")?,
        1
    );
    Ok(())
}

#[test]
fn simultaneous_conversions_publish_exactly_one_complete_bundle() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.db");
    let destination = directory.path().join("destination.zsqlite");
    let exported = directory.path().join("round-trip.db");
    create_native(&source)?;

    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let source = source.clone();
        let destination = destination.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            zsqlite::convert_to_zsqlite(source, destination)
        }));
    }
    barrier.wait();
    let outcomes = workers
        .into_iter()
        .map(|worker| worker.join().map_err(|_| "conversion worker panicked"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(zsqlite::StoreError::DestinationExists(_))))
            .count(),
        1,
        "losing conversion did not fail with DestinationExists: {outcomes:?}"
    );

    zsqlite::verify(&destination)?;
    zsqlite::export_to_sqlite(&destination, &exported)?;
    assert_eq!(std::fs::read(source)?, std::fs::read(exported)?);
    assert_eq!(staging_paths(directory.path())?, Vec::<PathBuf>::new());
    Ok(())
}

#[test]
fn complete_conversion_is_openable_if_crash_leaves_staging_file() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.db");
    let destination = directory.path().join("destination.zsqlite");
    let staging_file = directory
        .path()
        .join(".destination.convert.interrupted.zsqlite");
    let install_file = directory.path().join(".destination.active-install.zsqlite");
    create_native(&source)?;
    zsqlite::convert_to_zsqlite(&source, &staging_file)?;

    // Model install_staged_bundle() through its durable commit point, but die
    // before cleaning the original conversion staging file. The published
    // active file is linked from a separately allocated, fully written temporary;
    // it must not alias the original staging inode.
    std::fs::copy(&staging_file, &install_file)?;
    std::fs::hard_link(&install_file, &destination)?;
    std::fs::remove_file(&install_file)?;
    std::fs::rename(sidecar(&staging_file), sidecar(&destination))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(destination.metadata()?.nlink(), 1);
    }
    if let Err(error) = zsqlite::verify(&destination) {
        return Err(format!(
            "a complete converted bundle was unusable solely because its crash-left staging file still existed: {error}"
        )
        .into());
    }
    Ok(())
}

#[test]
fn incomplete_file_first_conversion_fails_closed_and_can_be_deleted() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.db");
    let staging = directory.path().join("staging.zsqlite");
    let destination = directory.path().join("destination.zsqlite");
    create_native(&source)?;
    zsqlite::convert_to_zsqlite(&source, &staging)?;

    // This is the durable state if install dies after publishing its complete
    // destination file but before renaming the sidecar directory.
    std::fs::copy(&staging, &destination)?;
    assert!(matches!(
        zsqlite::verify(&destination),
        Err(zsqlite::StoreError::MissingSidecar)
    ));
    assert_eq!(managed_delete(&destination)?, ffi::SQLITE_OK);
    assert!(!destination.exists());

    // Recovery of the incomplete destination must not damage the original
    // fully staged bundle.
    zsqlite::verify(&staging)?;
    Ok(())
}

#[test]
fn interrupted_sidecar_first_delete_can_be_retried_idempotently() -> TestResult {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("interrupted-delete.zsqlite");
    {
        let connection = Connection::open_zsqlite(&database)?;
        connection.execute("CREATE TABLE events(id INTEGER PRIMARY KEY)")?;
    }

    // delete_bundle() removes the sidecar tree before the database file. This is the
    // exact persistent state left if the deleting process dies between them.
    std::fs::remove_dir_all(sidecar(&database))?;
    let rc = managed_delete(&database)?;
    assert_eq!(
        rc,
        ffi::SQLITE_OK,
        "retrying an interrupted managed delete returned SQLite code {rc}"
    );
    assert!(
        !database.exists(),
        "retry left the recognizable database file behind"
    );
    Ok(())
}

#[test]
fn interrupted_recursive_sidecar_delete_can_be_retried_idempotently() -> TestResult {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("partial-delete.zsqlite");
    {
        let connection = Connection::open_zsqlite(&database)?;
        connection.execute(
            "CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB);
             INSERT INTO events VALUES(1, randomblob(8000));",
        )?;
    }
    zsqlite::flush(&database)?;

    // remove_dir_all() is not atomic. Model a crash after one required
    // subdirectory has gone but before the sidecar tree itself is unlinked.
    std::fs::remove_dir_all(sidecar(&database).join("segments"))?;
    assert_eq!(managed_delete(&database)?, ffi::SQLITE_OK);
    assert!(!database.exists());
    assert!(!sidecar(&database).exists());
    Ok(())
}

#[test]
fn partial_sidecar_delete_still_respects_an_existing_lifecycle_lease() -> TestResult {
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("partial-delete-with-reader.zsqlite");
    let connection = Connection::open_zsqlite(&database)?;
    connection.execute("CREATE TABLE events(id INTEGER PRIMARY KEY)")?;

    // Required sidecar structure can be partially absent after an interrupted
    // recursive delete, but the surviving lock files must still protect an
    // already-open generation from a second deleter.
    std::fs::remove_dir_all(sidecar(&database).join("segments"))?;
    assert_eq!(managed_delete(&database)?, ffi::SQLITE_BUSY);
    assert!(database.exists());

    drop(connection);
    assert_eq!(managed_delete(&database)?, ffi::SQLITE_OK);
    assert!(!database.exists());
    assert!(!sidecar(&database).exists());
    Ok(())
}

#[test]
fn conversion_never_replaces_an_existing_destination_component() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.db");
    create_native(&source)?;

    let occupied_file = directory.path().join("occupied-file.zsqlite");
    std::fs::write(&occupied_file, b"do not replace")?;
    assert!(matches!(
        zsqlite::convert_to_zsqlite(&source, &occupied_file),
        Err(zsqlite::StoreError::DestinationExists(_))
    ));
    assert_eq!(std::fs::read(&occupied_file)?, b"do not replace");

    let occupied_sidecar = directory.path().join("occupied-sidecar.zsqlite");
    std::fs::create_dir(sidecar(&occupied_sidecar))?;
    std::fs::write(sidecar(&occupied_sidecar).join("sentinel"), b"keep")?;
    assert!(matches!(
        zsqlite::convert_to_zsqlite(&source, &occupied_sidecar),
        Err(zsqlite::StoreError::DestinationExists(_))
    ));
    assert_eq!(
        std::fs::read(sidecar(&occupied_sidecar).join("sentinel"))?,
        b"keep"
    );
    assert!(!occupied_sidecar.exists());
    assert_eq!(staging_paths(directory.path())?, Vec::<PathBuf>::new());
    Ok(())
}

#[test]
fn simultaneous_exports_publish_exactly_one_complete_database() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.zsqlite");
    let destination = directory.path().join("destination.db");
    {
        let connection = Connection::open_zsqlite(&source)?;
        connection.execute(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB);
             INSERT INTO events VALUES(1, randomblob(12000));",
        )?;
    }

    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let source = source.clone();
        let destination = destination.clone();
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            zsqlite::export_to_sqlite(source, destination)
        }));
    }
    barrier.wait();
    let outcomes = workers
        .into_iter()
        .map(|worker| worker.join().map_err(|_| "export worker panicked"))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(zsqlite::StoreError::DestinationExists(_))))
            .count(),
        1,
        "losing export did not fail with DestinationExists: {outcomes:?}"
    );

    let native = Connection::open_native(&destination)?;
    assert_eq!(native.integer("SELECT count(*) FROM events")?, 1);
    assert_eq!(
        native.integer("SELECT count(*) FROM pragma_integrity_check WHERE integrity_check='ok'")?,
        1
    );
    assert_eq!(staging_paths(directory.path())?, Vec::<PathBuf>::new());
    Ok(())
}

#[test]
fn export_never_replaces_an_existing_destination() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.zsqlite");
    let destination = directory.path().join("destination.db");
    {
        let connection = Connection::open_zsqlite(&source)?;
        connection.execute("CREATE TABLE events(id INTEGER PRIMARY KEY)")?;
    }
    std::fs::write(&destination, b"do not replace")?;

    assert!(matches!(
        zsqlite::export_to_sqlite(&source, &destination),
        Err(zsqlite::StoreError::DestinationExists(_))
    ));
    assert_eq!(std::fs::read(destination)?, b"do not replace");
    assert_eq!(staging_paths(directory.path())?, Vec::<PathBuf>::new());
    Ok(())
}

fn write_and_flush(database: &Path, id: i64) -> TestResult {
    {
        let connection = Connection::open_zsqlite(database)?;
        connection.execute(&format!(
            "INSERT INTO events VALUES({id}, randomblob({}))",
            6000 + id * 17
        ))?;
    }
    zsqlite::flush(database)?;
    Ok(())
}

fn gc_holder_worker() -> TestResult {
    let database = PathBuf::from(std::env::var_os(GC_HOLDER_DB).ok_or("missing holder DB")?);
    let ready = PathBuf::from(std::env::var_os(GC_HOLDER_READY).ok_or("missing ready marker")?);
    let release =
        PathBuf::from(std::env::var_os(GC_HOLDER_RELEASE).ok_or("missing release marker")?);
    let connection = Connection::open_zsqlite(&database)?;
    let _ = connection.integer("SELECT count(*) FROM events")?;
    std::fs::write(ready, b"ready")?;
    let deadline = Instant::now() + Duration::from_secs(15);
    while !release.exists() {
        if Instant::now() >= deadline {
            return Err("GC holder release deadline expired".into());
        }
        thread::sleep(Duration::from_millis(5));
    }
    Ok(())
}

#[test]
fn gc_preserves_a_leased_generation_then_reclaims_it_after_release() -> TestResult {
    if std::env::var_os(GC_HOLDER_DB).is_some() {
        return gc_holder_worker();
    }

    let directory = tempfile::tempdir()?;
    let database = directory.path().join("leased-generation.zsqlite");
    {
        let connection = Connection::open_zsqlite(&database)?;
        connection.execute(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB);
             INSERT INTO events VALUES(1, randomblob(6000));",
        )?;
    }
    zsqlite::flush(&database)?;
    let original_segments = segment_paths(&database)?;
    assert_eq!(original_segments.len(), 1);
    let original_segment = original_segments[0].clone();

    let ready = directory.path().join("holder-ready");
    let release = directory.path().join("holder-release");
    let mut holder = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("gc_preserves_a_leased_generation_then_reclaims_it_after_release")
        .arg("--nocapture")
        .env(GC_HOLDER_DB, &database)
        .env(GC_HOLDER_READY, &ready)
        .env(GC_HOLDER_RELEASE, &release)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    wait_for_marker(&mut holder, &ready, Instant::now() + Duration::from_secs(8))?;

    write_and_flush(&database, 2)?;
    zsqlite::compact(&database)?;
    write_and_flush(&database, 3)?;
    assert!(
        original_segment.exists(),
        "GC removed a segment while another process held a lifecycle lease"
    );

    std::fs::write(&release, b"release")?;
    let output = wait_for_output(
        holder,
        Instant::now() + Duration::from_secs(8),
        "GC lease holder",
    )?;
    assert_success(&output, "GC lease holder")?;

    write_and_flush(&database, 4)?;
    write_and_flush(&database, 5)?;
    assert!(
        !original_segment.exists(),
        "obsolete leased segment was not reclaimed after the lease ended"
    );
    zsqlite::verify(&database)?;
    let connection = Connection::open_zsqlite(&database)?;
    assert_eq!(connection.integer("SELECT count(*) FROM events")?, 5);
    Ok(())
}
