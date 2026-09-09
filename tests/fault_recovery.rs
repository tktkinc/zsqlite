#![cfg(all(feature = "static", unix))]

use libsqlite3_sys as ffi;
use std::ffi::{CStr, CString};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::ptr::null_mut;
use std::sync::mpsc;
use std::time::{Duration, Instant};
use zsqlite::format::{
    COMMIT_HEADER_SIZE, COMMIT_MAGIC, CommitHeader, FRAME_HEADER_SIZE, FrameHeader, SECTOR_SIZE,
    SEGMENT_MAGIC, SegmentHeader, SegmentId,
};

const VFS: &CStr = c"zsqlite";

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct Connection(*mut ffi::sqlite3);

impl Connection {
    fn open(path: &Path) -> Result<Self, String> {
        zsqlite::register_static_vfs()
            .map_err(|code| format!("VFS registration failed with SQLite error {code}"))?;
        let path = CString::new(path.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
        let mut database = null_mut();
        // SAFETY: Both C strings outlive sqlite3_open_v2(), and database is a
        // valid output pointer. SQLite owns any non-null returned handle.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                path.as_ptr(),
                &raw mut database,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                VFS.as_ptr(),
            )
        };
        if rc != ffi::SQLITE_OK {
            let message = if database.is_null() {
                "open returned a null database handle".to_owned()
            } else {
                // SAFETY: A non-null SQLite handle owns a stable error string.
                unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(database)) }
                    .to_string_lossy()
                    .into_owned()
            };
            if !database.is_null() {
                // SAFETY: sqlite3_open_v2() initialized this handle.
                let _ = unsafe { ffi::sqlite3_close(database) };
            }
            return Err(format!("SQLite error {rc}: {message}"));
        }
        let connection = Self(database);
        // SAFETY: connection owns a live SQLite handle.
        let timeout_rc = unsafe { ffi::sqlite3_busy_timeout(connection.0, 10_000) };
        if timeout_rc != ffi::SQLITE_OK {
            return Err(connection.error(timeout_rc));
        }
        Ok(connection)
    }

    fn execute(&self, sql: &str) -> Result<(), String> {
        let sql = CString::new(sql).map_err(|error| error.to_string())?;
        let mut error = null_mut();
        // SAFETY: self owns a live handle, the SQL string outlives the call,
        // and error is a valid output pointer.
        let rc =
            unsafe { ffi::sqlite3_exec(self.0, sql.as_ptr(), None, null_mut(), &raw mut error) };
        if rc == ffi::SQLITE_OK {
            return Ok(());
        }
        let message = if error.is_null() {
            self.error(rc)
        } else {
            // SAFETY: SQLite allocated this nul-terminated error message.
            let message = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned();
            // SAFETY: sqlite3_exec() allocated error with sqlite3_malloc().
            unsafe { ffi::sqlite3_free(error.cast()) };
            message
        };
        Err(format!("SQLite error {rc}: {message}"))
    }

    fn integer(&self, sql: &str) -> Result<i64, String> {
        let sql = CString::new(sql).map_err(|error| error.to_string())?;
        let mut statement = null_mut();
        // SAFETY: The handle and SQL are live and statement is a valid output.
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(self.0, sql.as_ptr(), -1, &raw mut statement, null_mut())
        };
        if rc != ffi::SQLITE_OK {
            return Err(self.error(rc));
        }
        // SAFETY: sqlite3_prepare_v2() returned a live statement.
        let step = unsafe { ffi::sqlite3_step(statement) };
        let result = if step == ffi::SQLITE_ROW {
            // SAFETY: Column zero exists for every scalar query used here.
            Ok(unsafe { ffi::sqlite3_column_int64(statement, 0) })
        } else {
            Err(self.error(step))
        };
        // SAFETY: statement is live and is finalized exactly once.
        let finalize = unsafe { ffi::sqlite3_finalize(statement) };
        if finalize != ffi::SQLITE_OK {
            return Err(self.error(finalize));
        }
        result
    }

    fn error(&self, code: i32) -> String {
        // SAFETY: self owns a live handle with a stable error string.
        let message = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) }.to_string_lossy();
        format!("SQLite error {code}: {message}")
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: self owns this handle and closes it exactly once.
        let rc = unsafe { ffi::sqlite3_close(self.0) };
        assert_eq!(rc, ffi::SQLITE_OK);
    }
}

fn create_database(path: &Path) -> TestResult {
    let connection = Connection::open(path)?;
    connection.execute(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode=DELETE;
         PRAGMA synchronous=FULL;
         CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB NOT NULL);
         INSERT INTO events VALUES(1, randomblob(12000));",
    )?;
    Ok(())
}

fn sidecar(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".d");
    PathBuf::from(value)
}

fn read_exact_at(file: &File, mut offset: u64, mut output: &mut [u8]) -> std::io::Result<()> {
    while !output.is_empty() {
        let amount = file.read_at(output, offset)?;
        if amount == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        offset += amount as u64;
        output = &mut output[amount..];
    }
    Ok(())
}

fn active_header(path: &Path) -> TestResult<SegmentHeader> {
    let file = File::open(path)?;
    let mut encoded = [0; SECTOR_SIZE];
    read_exact_at(&file, 0, &mut encoded)?;
    Ok(SegmentHeader::decode(&encoded)?)
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

fn commits(path: &Path) -> TestResult<Vec<(u64, CommitHeader)>> {
    let file = File::open(path)?;
    let header = active_header(path)?;
    let file_len = file.metadata()?.len();
    let mut output = Vec::new();
    let mut offset = header.records_offset;
    while offset + COMMIT_HEADER_SIZE as u64 <= file_len {
        let mut magic = [0; 4];
        read_exact_at(&file, offset, &mut magic)?;
        if &magic == COMMIT_MAGIC {
            let mut encoded = [0; COMMIT_HEADER_SIZE];
            read_exact_at(&file, offset, &mut encoded)?;
            if let Ok(commit) = CommitHeader::decode(&encoded) {
                output.push((offset, commit));
            }
        }
        offset += SECTOR_SIZE as u64;
    }
    Ok(output)
}

fn flip_byte(path: &Path, offset: u64) -> TestResult {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut byte = [0];
    read_exact_at(&file, offset, &mut byte)?;
    byte[0] ^= 0x80;
    file.write_all_at(&byte, offset)?;
    file.sync_all()?;
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> TestResult {
    std::fs::create_dir(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else {
            std::fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
}

fn copy_bundle(source: &Path, destination: &Path) -> TestResult {
    std::fs::copy(source, destination)?;
    copy_tree(&sidecar(source), &sidecar(destination))
}

#[test]
fn unreachable_active_tail_is_ignored_and_reclaimed_by_the_next_writer() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("active-tail.zsqlite");
    create_database(&path)?;
    let before = zsqlite::verify(&path)?;
    let (commit_offset, commit) = commits(&path)?
        .into_iter()
        .next_back()
        .ok_or("active file has no commit")?;
    let committed_end = commit_offset + u64::from(commit.record_len);
    assert_eq!(path.metadata()?.len(), committed_end);

    let garbage = vec![0xa5; 777];
    let mut file = OpenOptions::new().append(true).open(&path)?;
    file.write_all(&garbage)?;
    file.sync_all()?;
    drop(file);

    // These bytes model frames and a partial commit written before a crash.
    assert_eq!(zsqlite::verify(&path)?.head_txid, before.head_txid);
    let connection = Connection::open(&path)?;
    assert_eq!(connection.integer("SELECT count(*) FROM events")?, 1);
    connection.execute("INSERT INTO events VALUES(2, randomblob(9000))")?;
    drop(connection);

    let after = zsqlite::verify(&path)?;
    assert!(after.head_txid > before.head_txid);
    let mut magic = [0; 4];
    read_exact_at(&File::open(&path)?, committed_end, &mut magic)?;
    assert_ne!(magic, [0xa5; 4], "the unreachable tail was not reclaimed");
    Ok(())
}

#[test]
fn new_bundle_uses_the_database_file_as_its_active_segment() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("active-file.zsqlite");
    drop(Connection::open(&path)?);

    let header = active_header(&path)?;
    assert_ne!(header.database_id, [0; 32]);
    let mut magic = [0; 8];
    read_exact_at(&File::open(&path)?, 0, &mut magic)?;
    assert_eq!(&magic, SEGMENT_MAGIC);
    assert!(sidecar(&path).join("segments").is_dir());
    assert!(sidecar(&path).join("locks").is_dir());
    assert!(!sidecar(&path).join("active").exists());
    assert!(!sidecar(&path).join("roots").exists());
    Ok(())
}

#[test]
fn incomplete_last_commit_recovers_the_previous_valid_prefix() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("commit-header.zsqlite");
    create_database(&path)?;
    let before = zsqlite::verify(&path)?;
    assert!(before.head_txid > 1);
    let (commit_offset, _) = commits(&path)?
        .into_iter()
        .next_back()
        .ok_or("active file has no commit")?;
    OpenOptions::new()
        .write(true)
        .open(&path)?
        .set_len(commit_offset + COMMIT_HEADER_SIZE as u64 / 2)?;

    let recovered = zsqlite::verify(&path)?;
    assert!(recovered.head_txid < before.head_txid);
    Ok(())
}

#[test]
fn torn_last_commit_entries_recover_the_previous_valid_prefix() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("commit-entries.zsqlite");
    create_database(&path)?;
    let before = zsqlite::verify(&path)?;
    assert!(before.head_txid > 1);
    let (commit_offset, commit) = commits(&path)?
        .into_iter()
        .next_back()
        .ok_or("active file has no commit")?;
    assert!(commit.entry_count > 0);
    flip_byte(&path, commit_offset + COMMIT_HEADER_SIZE as u64 + 24)?;

    let recovered = zsqlite::verify(&path)?;
    assert!(recovered.head_txid < before.head_txid);
    Ok(())
}

#[test]
fn a_missing_referenced_segment_fails_closed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("missing-segment.zsqlite");
    create_database(&path)?;
    zsqlite::flush(&path)?;
    let segment = segment_paths(&path)?
        .pop()
        .ok_or("flush produced no sealed segment")?;
    std::fs::remove_file(segment)?;

    assert!(
        zsqlite::verify(&path).is_err(),
        "an active file opened without its referenced predecessor"
    );
    Ok(())
}

#[test]
fn corrupt_active_header_fails_closed() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("corrupt-header.zsqlite");
    create_database(&path)?;
    flip_byte(&path, 300)?;

    assert!(
        zsqlite::verify(&path).is_err(),
        "a corrupt active-segment header was accepted"
    );
    Ok(())
}

#[test]
fn compaction_refuses_to_bless_corrupt_sealed_payloads() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("compact-corrupt.zsqlite");
    create_database(&path)?;
    zsqlite::flush(&path)?;
    {
        let connection = Connection::open(&path)?;
        connection.execute(
            "BEGIN IMMEDIATE;
             UPDATE events SET payload=randomblob(13000) WHERE id=1;
             INSERT INTO events VALUES(2, randomblob(7000));
             COMMIT;",
        )?;
    }
    let flushed = zsqlite::flush(&path)?;
    assert!(flushed.sealed_segments >= 2);
    let segments = segment_paths(&path)?;
    assert!(segments.len() >= 2);

    let damaged = &segments[0];
    let file = File::open(damaged)?;
    let mut encoded = [0; SECTOR_SIZE];
    read_exact_at(&file, 0, &mut encoded)?;
    let header = SegmentHeader::decode(&encoded)?;
    let mut frame_bytes = [0; FRAME_HEADER_SIZE];
    read_exact_at(&file, header.records_offset, &mut frame_bytes)?;
    let frame = FrameHeader::decode(&frame_bytes)?;
    assert!(!frame.free);
    assert!(frame.stored_len > 0);
    drop(file);
    flip_byte(damaged, header.records_offset + FRAME_HEADER_SIZE as u64)?;

    let active_before = std::fs::read(&path)?;
    let segments_before = segment_paths(&path)?;
    assert!(zsqlite::verify(&path).is_err());
    assert!(
        zsqlite::compact(&path).is_err(),
        "compaction accepted and re-published corrupt source bytes"
    );
    assert_eq!(std::fs::read(&path)?, active_before);
    assert_eq!(segment_paths(&path)?, segments_before);
    Ok(())
}

#[test]
fn rollover_hardlinks_the_old_active_inode_and_publishes_a_new_one() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("hardlink-rollover.zsqlite");
    create_database(&path)?;
    let old_active = path.metadata()?;
    let before = zsqlite::verify(&path)?;
    let flushed = zsqlite::flush(&path)?;
    assert_eq!(flushed.head_txid, before.head_txid);

    let sealed = segment_paths(&path)?
        .pop()
        .ok_or("flush produced no sealed segment")?;
    let sealed_metadata = sealed.metadata()?;
    assert_eq!(sealed_metadata.dev(), old_active.dev());
    assert_eq!(sealed_metadata.ino(), old_active.ino());
    assert_ne!(path.metadata()?.ino(), old_active.ino());

    // A parseable but unreferenced filename is not part of the lineage, even
    // when it repeats the live segment's physical-digest field.
    let live_id = SegmentId::parse_filename(
        sealed
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("non-UTF-8 segment name")?,
    )?;
    let noise = SegmentId {
        start_txid: live_id.end_txid + 100,
        end_txid: live_id.end_txid + 100,
        end_history: [0x77; 32],
        physical_digest: live_id.physical_digest,
    };
    std::fs::copy(
        &sealed,
        sidecar(&path).join("segments").join(noise.filename()),
    )?;
    assert_eq!(zsqlite::verify(&path)?.head_txid, before.head_txid);

    let clone = directory.path().join("hardlink-rollover-copy.zsqlite");
    copy_bundle(&path, &clone)?;
    assert_eq!(zsqlite::verify(&clone)?.head_txid, before.head_txid);
    let connection = Connection::open(&clone)?;
    assert_eq!(connection.integer("SELECT count(*) FROM events")?, 1);
    Ok(())
}

#[test]
fn crash_left_sealed_active_alias_is_rolled_forward() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("sealed-active-crash.zsqlite");
    create_database(&path)?;
    let flushed = zsqlite::flush(&path)?;
    let sealed = segment_paths(&path)?
        .pop()
        .ok_or("flush produced no sealed segment")?;

    // Model a crash after the hardlink was durable but before the fresh active
    // inode was renamed over `.zsqlite`.
    std::fs::remove_file(&path)?;
    std::fs::hard_link(&sealed, &path)?;
    assert_eq!(path.metadata()?.ino(), sealed.metadata()?.ino());
    assert_eq!(zsqlite::inspect(&path)?.head_txid, flushed.head_txid);

    let connection = Connection::open(&path)?;
    connection.execute("INSERT INTO events VALUES(2, randomblob(5000))")?;
    assert_eq!(connection.integer("SELECT count(*) FROM events")?, 2);
    drop(connection);
    assert_ne!(path.metadata()?.ino(), sealed.metadata()?.ino());
    zsqlite::verify(&path)?;
    Ok(())
}

#[test]
fn application_acknowledged_commits_survive_process_crashes() -> TestResult {
    if std::env::var_os("ZSQLITE_ACK_CRASH_DB").is_some() {
        return acknowledged_writer_worker();
    }

    for journal_mode in ["WAL", "DELETE"] {
        let directory = tempfile::tempdir()?;
        let path = directory
            .path()
            .join(format!("acknowledged-{journal_mode}.zsqlite"));
        let mut greatest_ack = 0_i64;
        for cycle in 0..3_u64 {
            let mut child = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("application_acknowledged_commits_survive_process_crashes")
                .arg("--nocapture")
                .env("ZSQLITE_ACK_CRASH_DB", &path)
                .env("ZSQLITE_ACK_CRASH_JOURNAL", journal_mode)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()?;
            let stdout = child.stdout.take().ok_or("child stdout was not piped")?;
            let (sender, receiver) = mpsc::channel();
            let reader = std::thread::spawn(move || {
                for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                    let _ = sender.send(line);
                }
            });
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut ready = false;
            let mut received_ack = false;
            while Instant::now() < deadline && !(ready && received_ack) {
                match receiver.recv_timeout(Duration::from_millis(50)) {
                    Ok(line) if line == "READY" => ready = true,
                    Ok(line) if line.starts_with("ACK ") => {
                        greatest_ack = greatest_ack.max(line[4..].parse()?);
                        received_ack = true;
                    }
                    Ok(_) => {}
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let Some(status) = child.try_wait()? {
                            return Err(format!("crash worker exited early with {status}").into());
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        return Err(
                            "crash worker output disconnected before an acknowledgement".into()
                        );
                    }
                }
            }
            if !(ready && received_ack) {
                child.kill()?;
                let _ = child.wait();
                return Err("crash worker did not acknowledge a commit".into());
            }
            std::thread::sleep(Duration::from_millis(2 + cycle * 3));
            child.kill()?;
            let _ = child.wait()?;
            reader.join().map_err(|_| "stdout reader panicked")?;
            for line in receiver.try_iter() {
                if let Some(value) = line.strip_prefix("ACK ") {
                    greatest_ack = greatest_ack.max(value.parse()?);
                }
            }

            let connection = Connection::open(&path)?;
            assert_eq!(
                connection.integer(&format!(
                    "SELECT count(*) FROM events WHERE id <= {greatest_ack}"
                ))?,
                greatest_ack,
                "{journal_mode} lost an application-acknowledged commit"
            );
            assert_eq!(
                connection.integer(
                    "SELECT count(*) FROM pragma_integrity_check WHERE integrity_check <> 'ok'",
                )?,
                0,
                "{journal_mode} recovery did not pass integrity_check"
            );
            if journal_mode == "WAL" {
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
            }
        }
        zsqlite::verify(&path)?;
    }
    Ok(())
}

fn acknowledged_writer_worker() -> TestResult {
    let path = PathBuf::from(std::env::var_os("ZSQLITE_ACK_CRASH_DB").ok_or("missing DB path")?);
    let journal_mode = std::env::var("ZSQLITE_ACK_CRASH_JOURNAL")?;
    if !matches!(journal_mode.as_str(), "WAL" | "DELETE") {
        return Err(format!("invalid journal mode {journal_mode}").into());
    }
    let connection = Connection::open(&path)?;
    let auto_checkpoint = if journal_mode == "WAL" { 3 } else { 0 };
    connection.execute(&format!(
        "PRAGMA page_size=4096;
         PRAGMA journal_mode={journal_mode};
         PRAGMA synchronous=FULL;
         PRAGMA wal_autocheckpoint={auto_checkpoint};
         CREATE TABLE IF NOT EXISTS events(
           id INTEGER PRIMARY KEY,
           payload BLOB NOT NULL
         );"
    ))?;
    let mut next = connection.integer("SELECT coalesce(max(id), 0) FROM events")? + 1;
    println!("READY");
    std::io::stdout().flush()?;
    loop {
        connection.execute(&format!(
            "BEGIN IMMEDIATE;
             INSERT INTO events VALUES({next}, randomblob({}));
             COMMIT;",
            4096 + next.rem_euclid(3000)
        ))?;
        println!("ACK {next}");
        std::io::stdout().flush()?;
        next += 1;
    }
}
