#![cfg(all(feature = "static", unix))]

use libsqlite3_sys as ffi;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt::{self, Display, Formatter};
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::ptr::{NonNull, null_mut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const VFS: &CStr = c"zsqlite";
const CHILD_CASE: &str = "ZSQLITE_CONCURRENCY_CHILD_CASE";
const WORKER_ROLE: &str = "ZSQLITE_CONCURRENCY_WORKER_ROLE";
const WORKER_DB: &str = "ZSQLITE_CONCURRENCY_WORKER_DB";
const WORKER_READY: &str = "ZSQLITE_CONCURRENCY_WORKER_READY";
const WORKER_START: &str = "ZSQLITE_CONCURRENCY_WORKER_START";
const WORKER_STOP: &str = "ZSQLITE_CONCURRENCY_WORKER_STOP";
const WORKER_ENTERED: &str = "ZSQLITE_CONCURRENCY_WORKER_ENTERED";

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Debug)]
struct SqlError {
    code: i32,
    message: String,
}

impl SqlError {
    fn is_busy(&self) -> bool {
        matches!(self.code & 0xff, ffi::SQLITE_BUSY | ffi::SQLITE_LOCKED)
    }
}

impl Display for SqlError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "SQLite error {}: {}", self.code, self.message)
    }
}

impl Error for SqlError {}

struct Connection(NonNull<ffi::sqlite3>);

impl Connection {
    fn open(path: &Path) -> Result<Self, SqlError> {
        let path = CString::new(path.as_os_str().as_bytes()).map_err(|error| SqlError {
            code: ffi::SQLITE_CANTOPEN,
            message: error.to_string(),
        })?;
        let mut raw = null_mut();
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                path.as_ptr(),
                &raw mut raw,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                VFS.as_ptr(),
            )
        };
        let Some(raw) = NonNull::new(raw) else {
            return Err(SqlError {
                code: rc,
                message: "sqlite3_open_v2 returned a null connection".into(),
            });
        };
        let connection = Self(raw);
        if rc == ffi::SQLITE_OK {
            connection.busy_timeout(250)?;
            Ok(connection)
        } else {
            Err(connection.error(rc))
        }
    }

    fn busy_timeout(&self, milliseconds: i32) -> Result<(), SqlError> {
        let rc = unsafe { ffi::sqlite3_busy_timeout(self.0.as_ptr(), milliseconds) };
        if rc == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(self.error(rc))
        }
    }

    fn execute(&self, sql: &str) -> Result<(), SqlError> {
        let sql = CString::new(sql).map_err(|error| SqlError {
            code: ffi::SQLITE_ERROR,
            message: error.to_string(),
        })?;
        let mut error = null_mut();
        let rc = unsafe {
            ffi::sqlite3_exec(
                self.0.as_ptr(),
                sql.as_ptr(),
                None,
                null_mut(),
                &raw mut error,
            )
        };
        if rc == ffi::SQLITE_OK {
            return Ok(());
        }
        let message = if error.is_null() {
            self.error(rc).message
        } else {
            let message = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned();
            unsafe { ffi::sqlite3_free(error.cast()) };
            message
        };
        Err(SqlError { code: rc, message })
    }

    fn integer(&self, sql: &str) -> Result<i64, SqlError> {
        match self.integers(sql)?.as_slice() {
            [value] => Ok(*value),
            values => Err(SqlError {
                code: ffi::SQLITE_MISMATCH,
                message: format!("query returned {} rows, expected one", values.len()),
            }),
        }
    }

    fn integers(&self, sql: &str) -> Result<Vec<i64>, SqlError> {
        let sql = CString::new(sql).map_err(|error| SqlError {
            code: ffi::SQLITE_ERROR,
            message: error.to_string(),
        })?;
        let mut statement = null_mut();
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(
                self.0.as_ptr(),
                sql.as_ptr(),
                -1,
                &raw mut statement,
                null_mut(),
            )
        };
        if rc != ffi::SQLITE_OK {
            return Err(self.error(rc));
        }
        let result = (|| {
            let mut values = Vec::new();
            loop {
                let step = unsafe { ffi::sqlite3_step(statement) };
                if step == ffi::SQLITE_DONE {
                    return Ok(values);
                }
                if step != ffi::SQLITE_ROW {
                    return Err(self.error(step));
                }
                if unsafe { ffi::sqlite3_column_count(statement) } != 1
                    || unsafe { ffi::sqlite3_column_type(statement, 0) } != ffi::SQLITE_INTEGER
                {
                    return Err(SqlError {
                        code: ffi::SQLITE_MISMATCH,
                        message: "query did not return exactly one integer column".into(),
                    });
                }
                values.push(unsafe { ffi::sqlite3_column_int64(statement, 0) });
            }
        })();
        let finalize = unsafe { ffi::sqlite3_finalize(statement) };
        if finalize != ffi::SQLITE_OK {
            return Err(self.error(finalize));
        }
        result
    }

    fn error(&self, code: i32) -> SqlError {
        SqlError {
            code,
            message: unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0.as_ptr())) }
                .to_string_lossy()
                .into_owned(),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = unsafe { ffi::sqlite3_close(self.0.as_ptr()) };
    }
}

fn register() -> TestResult {
    zsqlite::register_static_vfs()
        .map_err(|code| format!("zsqlite VFS registration failed with SQLite code {code}").into())
}

fn execute_until(connection: &Connection, sql: &str, deadline: Instant) -> TestResult {
    loop {
        match connection.execute(sql) {
            Ok(()) => return Ok(()),
            Err(error) if error.is_busy() && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn integer_until(connection: &Connection, sql: &str, deadline: Instant) -> TestResult<i64> {
    loop {
        match connection.integer(sql) {
            Ok(value) => return Ok(value),
            Err(error) if error.is_busy() && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2));
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn wait_for_marker(child: &mut Child, marker: &Path, deadline: Instant) -> TestResult {
    loop {
        if marker.exists() {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(format!(
                "worker exited with {status} before creating {}",
                marker.display()
            )
            .into());
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            return Err(format!("timed out waiting for {}", marker.display()).into());
        }
        thread::sleep(Duration::from_millis(5));
    }
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

fn run_self_with_watchdog(test_name: &str, case: &str, timeout: Duration) -> TestResult {
    let child = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(CHILD_CASE, case)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let output = wait_for_output(child, Instant::now() + timeout, test_name)?;
    assert_success(&output, test_name)
}

fn spawn_worker(
    role: &str,
    database: &Path,
    ready: &Path,
    start: &Path,
    stop: &Path,
    entered: &Path,
) -> TestResult<Child> {
    Ok(Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("concurrency_subprocess_worker")
        .arg("--nocapture")
        .env(WORKER_ROLE, role)
        .env(WORKER_DB, database)
        .env(WORKER_READY, ready)
        .env(WORKER_START, start)
        .env(WORKER_STOP, stop)
        .env(WORKER_ENTERED, entered)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?)
}

fn wait_for_start(path: &Path, deadline: Instant) -> TestResult {
    while !path.exists() {
        if Instant::now() >= deadline {
            return Err(format!("worker start barrier timed out: {}", path.display()).into());
        }
        thread::sleep(Duration::from_millis(2));
    }
    Ok(())
}

fn maintenance_worker(role: &str) -> TestResult {
    let database = PathBuf::from(std::env::var_os(WORKER_DB).ok_or("missing worker database")?);
    let ready = PathBuf::from(std::env::var_os(WORKER_READY).ok_or("missing ready marker")?);
    let start = PathBuf::from(std::env::var_os(WORKER_START).ok_or("missing start marker")?);
    let stop = PathBuf::from(std::env::var_os(WORKER_STOP).ok_or("missing stop marker")?);
    let entered = PathBuf::from(std::env::var_os(WORKER_ENTERED).ok_or("missing entered marker")?);
    std::fs::write(&ready, b"ready")?;
    wait_for_start(&start, Instant::now() + Duration::from_secs(8))?;
    std::fs::write(&entered, b"entered")?;

    let deadline = Instant::now() + Duration::from_secs(20);
    let mut attempts = 0_usize;
    let mut successes = 0_usize;
    let mut busy = 0_usize;
    while !stop.exists() || attempts < 24 {
        if Instant::now() >= deadline {
            return Err(format!(
                "{role} worker deadline expired after {attempts} attempts ({successes} successes, {busy} busy)"
            )
            .into());
        }
        let result = match role {
            "flush" => zsqlite::flush(&database),
            "compact" => zsqlite::compact(&database),
            _ => return Err(format!("unknown maintenance role {role}").into()),
        };
        match result {
            Ok(_) => successes += 1,
            Err(zsqlite::StoreError::Busy) => busy += 1,
            Err(error) => return Err(format!("{role} failed: {error}").into()),
        }
        attempts += 1;
        thread::sleep(Duration::from_millis(2));
    }
    if successes == 0 {
        return Err(format!("{role} never acquired the publication lock").into());
    }
    eprintln!("{role}: {attempts} attempts, {successes} successes, {busy} busy");
    Ok(())
}

fn lock_holder_worker() -> TestResult {
    let database = PathBuf::from(std::env::var_os(WORKER_DB).ok_or("missing worker database")?);
    let ready = PathBuf::from(std::env::var_os(WORKER_READY).ok_or("missing ready marker")?);
    let stop = PathBuf::from(std::env::var_os(WORKER_STOP).ok_or("missing stop marker")?);
    let lock_path = append_suffix(&database, ".d/locks/publication.lock");
    let lock = OpenOptions::new().read(true).write(true).open(lock_path)?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    std::fs::write(&ready, b"locked")?;
    wait_for_start(&stop, Instant::now() + Duration::from_secs(15))?;
    if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_UN) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

fn sqlite_lock_probe_worker() -> TestResult {
    register()?;
    let database = PathBuf::from(std::env::var_os(WORKER_DB).ok_or("missing worker database")?);
    let connection = Connection::open(&database)?;
    connection.busy_timeout(50)?;
    match connection.execute("BEGIN IMMEDIATE") {
        Err(error) if error.is_busy() => Ok(()),
        Err(error) => Err(format!("competing BEGIN IMMEDIATE failed unexpectedly: {error}").into()),
        Ok(()) => {
            let _ = connection.execute("ROLLBACK");
            Err("competing BEGIN IMMEDIATE bypassed the live writer lock".into())
        }
    }
}

#[test]
fn concurrency_subprocess_worker() -> TestResult {
    let Some(role) = std::env::var_os(WORKER_ROLE) else {
        return Ok(());
    };
    let role = role.to_string_lossy();
    match role.as_ref() {
        "flush" | "compact" => maintenance_worker(&role),
        "lock-holder" => lock_holder_worker(),
        "sqlite-lock-probe" => sqlite_lock_probe_worker(),
        _ => Err(format!("unknown worker role {role}").into()),
    }
}

fn assert_competing_writer_is_busy(database: &Path, directory: &Path) -> TestResult {
    let unused = directory.join("unused-lock-probe-marker");
    let child = spawn_worker(
        "sqlite-lock-probe",
        database,
        &unused,
        &unused,
        &unused,
        &unused,
    )?;
    let output = wait_for_output(
        child,
        Instant::now() + Duration::from_secs(8),
        "SQLite lock probe",
    )?;
    assert_success(&output, "SQLite lock probe")
}

#[test]
fn store_inspection_does_not_release_sqlite_process_locks() -> TestResult {
    register()?;
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("active-fd-locking.zsqlite");
    let holder = Connection::open(&database)?;
    holder.execute(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE events(id INTEGER PRIMARY KEY);
         BEGIN IMMEDIATE;
         INSERT INTO events VALUES(1);",
    )?;

    assert_competing_writer_is_busy(&database, directory.path())?;
    zsqlite::inspect(&database)?;
    assert_competing_writer_is_busy(&database, directory.path())?;

    holder.execute("COMMIT")?;
    if holder.integer("SELECT count(*) FROM events")? != 1 {
        return Err("lock holder lost its transaction after Store inspection".into());
    }
    Ok(())
}

#[test]
fn wal_read_write_checkpoint_with_cross_process_flush_and_compact_is_bounded() -> TestResult {
    if std::env::var(CHILD_CASE).as_deref() == Ok("wal-maintenance") {
        return wal_maintenance_scenario();
    }
    run_self_with_watchdog(
        "wal_read_write_checkpoint_with_cross_process_flush_and_compact_is_bounded",
        "wal-maintenance",
        Duration::from_secs(45),
    )
}

#[allow(clippy::too_many_lines)]
fn wal_maintenance_scenario() -> TestResult {
    const WRITES: usize = 60;
    register()?;
    let requested_directory = std::env::var_os("ZSQLITE_STRESS_ARTIFACT_DIR").map(PathBuf::from);
    let temporary_directory = if requested_directory.is_none() {
        Some(tempfile::tempdir()?)
    } else {
        None
    };
    let directory = requested_directory.as_deref().unwrap_or_else(|| {
        temporary_directory
            .as_ref()
            .expect("temporary directory exists")
            .path()
    });
    std::fs::create_dir_all(directory)?;
    let database = directory.join("wal-maintenance.zsqlite");
    let enable_flush = std::env::var_os("ZSQLITE_STRESS_DISABLE_FLUSH").is_none();
    let enable_compact = std::env::var_os("ZSQLITE_STRESS_DISABLE_COMPACT").is_none();
    let enable_reader = std::env::var_os("ZSQLITE_STRESS_DISABLE_READER").is_none();
    let enable_checkpoint = std::env::var_os("ZSQLITE_STRESS_DISABLE_CHECKPOINT").is_none();
    {
        let connection = Connection::open(&database)?;
        connection.execute(
            "PRAGMA page_size=4096;
             PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
             INSERT INTO events VALUES(0, printf('%.*c', 3000, 's'));
             PRAGMA wal_checkpoint(TRUNCATE);",
        )?;
    }
    zsqlite::flush(&database)?;

    let start = directory.join("maintenance-start");
    let stop = directory.join("maintenance-stop");
    let flush_ready = directory.join("flush-ready");
    let compact_ready = directory.join("compact-ready");
    let flush_entered = directory.join("flush-entered");
    let compact_entered = directory.join("compact-entered");
    let mut flusher = enable_flush
        .then(|| {
            spawn_worker(
                "flush",
                &database,
                &flush_ready,
                &start,
                &stop,
                &flush_entered,
            )
        })
        .transpose()?;
    let mut compactor = enable_compact
        .then(|| {
            spawn_worker(
                "compact",
                &database,
                &compact_ready,
                &start,
                &stop,
                &compact_entered,
            )
        })
        .transpose()?;
    let startup_deadline = Instant::now() + Duration::from_secs(8);
    if let Some(child) = &mut flusher {
        wait_for_marker(child, &flush_ready, startup_deadline)?;
    }
    if let Some(child) = &mut compactor {
        wait_for_marker(child, &compact_ready, startup_deadline)?;
    }
    std::fs::write(&start, b"start")?;
    if let Some(child) = &mut flusher {
        wait_for_marker(child, &flush_entered, startup_deadline)?;
    }
    if let Some(child) = &mut compactor {
        wait_for_marker(child, &compact_entered, startup_deadline)?;
    }

    let running = Arc::new(AtomicBool::new(true));
    let checkpoints = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(
        2 + usize::from(enable_reader) + usize::from(enable_checkpoint),
    ));

    let writer_path = database.clone();
    let writer_running = Arc::clone(&running);
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || -> Result<(), String> {
        let result = (|| {
            let connection = Connection::open(&writer_path).map_err(|error| error.to_string())?;
            connection
                .busy_timeout(100)
                .map_err(|error| error.to_string())?;
            writer_barrier.wait();
            let deadline = Instant::now() + Duration::from_secs(15);
            for id in 1..=WRITES {
                execute_until(&connection, "BEGIN IMMEDIATE", deadline)
                    .map_err(|error| format!("writer begin {id}: {error}"))?;
                execute_until(
                    &connection,
                    &format!(
                        "INSERT INTO events VALUES({id}, printf('%.*c', {}, 'w'))",
                        2800 + id % 401
                    ),
                    deadline,
                )
                .map_err(|error| format!("writer insert {id}: {error}"))?;
                execute_until(&connection, "COMMIT", deadline)
                    .map_err(|error| format!("writer commit {id}: {error}"))?;
                thread::sleep(Duration::from_millis(2));
            }
            Ok(())
        })();
        writer_running.store(false, Ordering::Release);
        result
    });

    let reader_path = database.clone();
    let reader_running = Arc::clone(&running);
    let reader_count = Arc::clone(&reads);
    let reader_barrier = Arc::clone(&barrier);
    let reader = enable_reader.then(|| {
        thread::spawn(move || -> Result<(), String> {
            let connection = Connection::open(&reader_path).map_err(|error| error.to_string())?;
            connection
                .busy_timeout(100)
                .map_err(|error| error.to_string())?;
            reader_barrier.wait();
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut previous = 1_i64;
            while reader_running.load(Ordering::Acquire)
                || reader_count.load(Ordering::Relaxed) < 24
            {
                if Instant::now() >= deadline {
                    return Err("reader deadline expired".into());
                }
                let count = integer_until(&connection, "SELECT count(*) FROM events", deadline)
                    .map_err(|error| error.to_string())?;
                if !(previous..=i64::try_from(WRITES + 1).unwrap()).contains(&count) {
                    return Err(format!(
                        "reader observed impossible count {count} after {previous}"
                    ));
                }
                previous = count;
                reader_count.fetch_add(1, Ordering::Relaxed);
                thread::yield_now();
            }
            Ok(())
        })
    });

    let checkpoint_path = database.clone();
    let checkpoint_running = Arc::clone(&running);
    let checkpoint_count = Arc::clone(&checkpoints);
    let checkpoint_barrier = Arc::clone(&barrier);
    let checkpointer = enable_checkpoint.then(|| {
        thread::spawn(move || -> Result<(), String> {
            let connection =
                Connection::open(&checkpoint_path).map_err(|error| error.to_string())?;
            connection
                .busy_timeout(100)
                .map_err(|error| error.to_string())?;
            checkpoint_barrier.wait();
            let deadline = Instant::now() + Duration::from_secs(15);
            while checkpoint_running.load(Ordering::Acquire)
                || checkpoint_count.load(Ordering::Relaxed) < 24
            {
                if Instant::now() >= deadline {
                    return Err("checkpointer deadline expired".into());
                }
                execute_until(&connection, "PRAGMA wal_checkpoint(PASSIVE)", deadline)
                    .map_err(|error| error.to_string())?;
                checkpoint_count.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        })
    });

    barrier.wait();
    let writer_result = writer.join().map_err(|_| "writer thread panicked")?;
    running.store(false, Ordering::Release);
    let reader_result = reader
        .map(|reader| reader.join().map_err(|_| "reader thread panicked"))
        .transpose()?;
    let checkpoint_result = checkpointer
        .map(|checkpointer| {
            checkpointer
                .join()
                .map_err(|_| "checkpointer thread panicked")
        })
        .transpose()?;
    std::fs::write(&stop, b"stop")?;

    let worker_deadline = Instant::now() + Duration::from_secs(22);
    if let Some(child) = flusher {
        let output = wait_for_output(child, worker_deadline, "flush worker")?;
        assert_success(&output, "flush worker")?;
    }
    if let Some(child) = compactor {
        let output = wait_for_output(child, worker_deadline, "compact worker")?;
        assert_success(&output, "compact worker")?;
    }
    writer_result?;
    if let Some(result) = reader_result {
        result?;
    }
    if let Some(result) = checkpoint_result {
        result?;
    }
    if (enable_reader && reads.load(Ordering::Relaxed) < 24)
        || (enable_checkpoint && checkpoints.load(Ordering::Relaxed) < 24)
    {
        return Err("reader or checkpointer did not complete its minimum overlap work".into());
    }

    {
        let connection = Connection::open(&database)?;
        let expected = i64::try_from(WRITES + 1)?;
        let before_checkpoint = connection.integers("SELECT id FROM events ORDER BY id")?;
        let integrity_before = connection
            .integer("SELECT count(*) FROM pragma_integrity_check WHERE integrity_check='ok'")?;
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        let after_checkpoint = connection.integers("SELECT id FROM events ORDER BY id")?;
        if i64::try_from(after_checkpoint.len())? != expected {
            let missing_before = (0..=i64::try_from(WRITES)?)
                .filter(|id| before_checkpoint.binary_search(id).is_err())
                .collect::<Vec<_>>();
            let missing_after = (0..=i64::try_from(WRITES)?)
                .filter(|id| after_checkpoint.binary_search(id).is_err())
                .collect::<Vec<_>>();
            return Err(format!(
                "committed-row loss: expected {expected}; before final checkpoint={} missing={missing_before:?}; after={} missing={missing_after:?}; integrity_ok={integrity_before}; artifact={} ",
                before_checkpoint.len(),
                after_checkpoint.len(),
                directory.display()
            )
            .into());
        }
        if integrity_before != 1 {
            return Err("SQLite integrity_check failed".into());
        }
    }
    zsqlite::flush(&database)?;
    zsqlite::compact(&database)?;
    zsqlite::verify(&database)?;
    Ok(())
}

#[test]
fn last_close_checkpoint_blocked_by_publication_lock_preserves_wal() -> TestResult {
    if std::env::var(CHILD_CASE).as_deref() == Ok("wal-close-publication") {
        return last_close_checkpoint_scenario();
    }
    run_self_with_watchdog(
        "last_close_checkpoint_blocked_by_publication_lock_preserves_wal",
        "wal-close-publication",
        Duration::from_secs(30),
    )
}

fn last_close_checkpoint_scenario() -> TestResult {
    register()?;
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("last-close.zsqlite");
    let wal = append_suffix(&database, "-wal");
    let connection = Connection::open(&database)?;
    connection.execute(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=WAL;
         PRAGMA synchronous=FULL;
         PRAGMA wal_autocheckpoint=0;
         CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
         INSERT INTO events VALUES(1, 'checkpointed');
         PRAGMA wal_checkpoint(TRUNCATE);
         INSERT INTO events VALUES(2, 'acknowledged-in-wal');",
    )?;
    if connection.integer("SELECT count(*) FROM events")? != 2 {
        return Err("writer did not observe its acknowledged WAL commit".into());
    }
    if wal.metadata()?.len() == 0 {
        return Err("acknowledged commit did not leave a WAL frame to recover".into());
    }

    let ready = directory.path().join("lock-ready");
    let release = directory.path().join("lock-release");
    let unused = directory.path().join("unused-marker");
    let mut holder = spawn_worker("lock-holder", &database, &ready, &unused, &release, &unused)?;
    wait_for_marker(&mut holder, &ready, Instant::now() + Duration::from_secs(8))?;

    // This is the only SQLite handle. Closing it runs SQLite's implicit
    // last-connection checkpoint while the Store publication lock is busy.
    // SQLite must leave the WAL available for recovery instead of mistaking
    // storage contention for a successfully completed partial checkpoint.
    drop(connection);
    let close_result = (|| -> TestResult {
        if !wal.exists() || wal.metadata()?.len() == 0 {
            return Err("last-close checkpoint removed an unbackfilled WAL".into());
        }
        Ok(())
    })();

    std::fs::write(&release, b"release")?;
    let holder_output = wait_for_output(
        holder,
        Instant::now() + Duration::from_secs(8),
        "publication lock holder",
    )?;
    assert_success(&holder_output, "publication lock holder")?;
    close_result?;

    {
        let recovered = Connection::open(&database)?;
        if recovered.integer("SELECT count(*) FROM events")? != 2
            || recovered.integer("SELECT count(*) FROM events WHERE id=2")? != 1
        {
            return Err("reopen did not recover the acknowledged WAL commit".into());
        }
        recovered.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    }
    zsqlite::flush(&database)?;
    zsqlite::compact(&database)?;
    zsqlite::verify(&database)?;
    Ok(())
}

#[test]
fn stalled_publication_lock_returns_ioerr_without_hanging() -> TestResult {
    if std::env::var(CHILD_CASE).as_deref() == Ok("publication-lock") {
        return stalled_publication_scenario();
    }
    run_self_with_watchdog(
        "stalled_publication_lock_returns_ioerr_without_hanging",
        "publication-lock",
        Duration::from_secs(30),
    )
}

fn stalled_publication_scenario() -> TestResult {
    register()?;
    let directory = tempfile::tempdir()?;
    let database = directory.path().join("publication-lock.zsqlite");
    {
        let connection = Connection::open(&database)?;
        connection.execute(
            "PRAGMA journal_mode=DELETE;
             PRAGMA synchronous=FULL;
             CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT);
             INSERT INTO events VALUES(1, 'committed');",
        )?;
    }

    let ready = directory.path().join("lock-ready");
    let release = directory.path().join("lock-release");
    let unused = directory.path().join("unused-marker");
    let mut holder = spawn_worker("lock-holder", &database, &ready, &unused, &release, &unused)?;
    wait_for_marker(&mut holder, &ready, Instant::now() + Duration::from_secs(8))?;

    let test_result = (|| -> TestResult {
        let connection = Connection::open(&database)?;
        connection.busy_timeout(5_000)?;
        if connection.integer("SELECT count(*) FROM events")? != 1 {
            return Err("reader lost the committed row while publication was stalled".into());
        }

        let started = Instant::now();
        let flush_error = zsqlite::flush(&database).expect_err("flush unexpectedly acquired lock");
        if !matches!(flush_error, zsqlite::StoreError::Busy) {
            return Err(format!("flush returned {flush_error}, expected Busy").into());
        }
        if started.elapsed() >= Duration::from_secs(2) {
            return Err("flush blocked on the stalled publication lock".into());
        }

        let started = Instant::now();
        let compact_error =
            zsqlite::compact(&database).expect_err("compact unexpectedly acquired lock");
        if !matches!(compact_error, zsqlite::StoreError::Busy) {
            return Err(format!("compact returned {compact_error}, expected Busy").into());
        }
        if started.elapsed() >= Duration::from_secs(2) {
            return Err("compact blocked on the stalled publication lock".into());
        }

        let started = Instant::now();
        let sql_error = connection
            .execute("INSERT INTO events VALUES(2, 'must-fail')")
            .expect_err("write unexpectedly acquired the publication lock");
        if sql_error.code & 0xff != ffi::SQLITE_IOERR {
            return Err(format!("write returned {sql_error}, expected IOERR").into());
        }
        if started.elapsed() >= Duration::from_secs(2) {
            return Err(format!(
                "VFS write blocked instead of returning IOERR promptly: {:?}",
                started.elapsed()
            )
            .into());
        }
        Ok(())
    })();

    std::fs::write(&release, b"release")?;
    let holder_output = wait_for_output(
        holder,
        Instant::now() + Duration::from_secs(8),
        "publication lock holder",
    )?;
    assert_success(&holder_output, "publication lock holder")?;
    test_result?;

    {
        let connection = Connection::open(&database)?;
        connection.execute("INSERT INTO events VALUES(2, 'after-release')")?;
        if connection.integer("SELECT count(*) FROM events")? != 2 {
            return Err("write did not recover after publication lock release".into());
        }
    }
    zsqlite::flush(&database)?;
    zsqlite::compact(&database)?;
    zsqlite::verify(&database)?;
    Ok(())
}
