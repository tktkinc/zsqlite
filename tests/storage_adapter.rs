//! The forwarding adapter intentionally lives outside the library crate: none
//! of its implementation can access private catalog or authentication proofs.
use std::io::Read;
use std::sync::Arc;
use zsqlite::domain::{BackendId, StoredBytes};
use zsqlite::storage::adapter::{
    BackendError, DeletePermit, ObjectKey, ObjectRange, Publication, Revision, RootRecord,
    StorageBackend,
};
use zsqlite::{MemoryBackend, Storage};
type TestResult = Result<(), Box<dyn std::error::Error>>;
struct ExternalAdapter(Arc<dyn StorageBackend>);
impl StorageBackend for ExternalAdapter {
    fn identity(&self) -> BackendId {
        self.0.identity()
    }
    fn begin_write(
        &self,
    ) -> Result<Box<dyn zsqlite::storage::adapter::ObjectWriter + '_>, BackendError> {
        self.0.begin_write()
    }
    fn put(
        &self,
        key: ObjectKey,
        length: StoredBytes,
        input: &mut dyn Read,
    ) -> Result<(), BackendError> {
        self.0.put(key, length, input)
    }
    fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError> {
        self.0.read_ranges(requests)
    }
    fn stat(&self, key: ObjectKey) -> Result<Option<StoredBytes>, BackendError> {
        self.0.stat(key)
    }
    fn read_root(&self) -> Result<Option<RootRecord>, BackendError> {
        self.0.read_root()
    }
    fn compare_exchange_root(
        &self,
        expected: Option<&Revision>,
        bytes: &[u8],
    ) -> Result<Publication, BackendError> {
        self.0.compare_exchange_root(expected, bytes)
    }
    fn inventory(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, BackendError> {
        self.0.inventory(after, limit)
    }
    fn delete(&self, permit: DeletePermit<'_>) -> Result<(), BackendError> {
        self.0.delete(permit)
    }
}
#[test]
fn root_cas_and_immutable_put_contract() -> TestResult {
    use zsqlite::domain::{BlobId, FileOffset, StoredRange};
    let directory = tempfile::tempdir()?;
    let backends: [Arc<dyn StorageBackend>; 2] = [
        Arc::new(MemoryBackend::new()?),
        Arc::new(zsqlite::FilesystemBackend::open(directory.path())?),
    ];
    for backend in backends {
        let backend = ExternalAdapter(backend);
        let first = backend.compare_exchange_root(None, b"first")?;
        assert!(matches!(first, Publication::Applied(_)));
        assert!(matches!(
            backend.compare_exchange_root(None, b"stale")?,
            Publication::Stale
        ));
        let root = backend.read_root()?.unwrap();
        assert!(matches!(
            backend.compare_exchange_root(Some(root.revision()), b"second")?,
            Publication::Applied(_)
        ));
        assert!(matches!(
            backend.compare_exchange_root(Some(root.revision()), b"third")?,
            Publication::Stale
        ));
        let key = ObjectKey::Blob(BlobId::from_bytes(*blake3::hash(b"payload").as_bytes()));
        backend.put(key, StoredBytes::new(7), &mut &b"payload"[..])?;
        backend.put(key, StoredBytes::new(7), &mut &b"payload"[..])?;
        assert!(matches!(
            backend.put(key, StoredBytes::new(7), &mut &b"changed"[..]),
            Err(BackendError::IdentityMismatch(_))
        ));
        let ranges = [ObjectRange::new(
            key,
            StoredRange::new(FileOffset::new(1), StoredBytes::new(3))?,
        )?];
        assert_eq!(backend.read_ranges(&ranges)?, vec![b"ayl".to_vec()]);
        assert_eq!(backend.inventory(None, 1)?, vec![key]);
        assert!(backend.inventory(Some(key), 1)?.is_empty());
        assert!(
            backend
                .put(key, StoredBytes::new(6), &mut &b"payload"[..])
                .is_err()
        );
        assert!(
            backend
                .put(key, StoredBytes::new(8), &mut &b"payload"[..])
                .is_err()
        );
        assert!(backend.inventory(None, 0).is_err());
        let out_of_bounds = ObjectRange::new(
            key,
            StoredRange::new(FileOffset::new(6), StoredBytes::new(2))?,
        )?;
        assert!(backend.read_ranges(&[out_of_bounds]).is_err());
        assert!(backend.read_ranges(&[]).is_err());
        let second = backend.read_root()?.unwrap();
        backend.compare_exchange_root(Some(second.revision()), b"first")?;
        assert_ne!(backend.read_root()?.unwrap().revision(), root.revision());
    }
    Ok(())
}
#[test]
fn bootstrap_without_a_finalized_head_fails() -> TestResult {
    let directory = tempfile::tempdir()?;
    let storage = Storage::new(
        Arc::new(MemoryBackend::new()?),
        directory.path().join("coord"),
    )?;
    assert!(matches!(
        storage.bootstrap(directory.path().join("missing.zsqlite")),
        Err(zsqlite::StoreError::NoSealedHead)
    ));
    assert!(!directory.path().join("missing.zsqlite").exists());
    Ok(())
}

#[cfg(feature = "static")]
mod sqlite {
    use super::*;
    use libsqlite3_sys as ffi;
    use std::ffi::{CStr, CString};
    use std::path::Path;
    struct Connection(*mut ffi::sqlite3);
    impl Connection {
        fn open(path: &Path, vfs: &str) -> Result<Self, Box<dyn std::error::Error>> {
            let path = CString::new(path.to_str().ok_or("path")?)?;
            let vfs = CString::new(vfs)?;
            let mut database = std::ptr::null_mut();
            // SAFETY: The path/VFS C strings outlive this call and the output slot is
            // exclusive. SQLite initializes the owned handle even when opening fails.
            let rc = unsafe {
                ffi::sqlite3_open_v2(
                    path.as_ptr(),
                    &raw mut database,
                    ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                    vfs.as_ptr(),
                )
            };
            let connection = Self(database);
            if rc != ffi::SQLITE_OK {
                return Err(connection.error().into());
            }
            Ok(connection)
        }
        fn error(&self) -> String {
            // SAFETY: The owned connection is live and accessed on this thread only.
            // SQLite returns a terminated message, copied before another SQLite call.
            unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) }
                .to_string_lossy()
                .into_owned()
        }
        fn exec(&self, sql: &str) -> TestResult {
            let sql = CString::new(sql)?;
            // SAFETY: The owned connection is live on this thread and sql is a
            // terminated CString. Any error output is an exclusive local pointer slot;
            // SQLite copies SQL and retains no Rust callback or buffer.
            let rc = unsafe {
                ffi::sqlite3_exec(
                    self.0,
                    sql.as_ptr(),
                    None,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            };
            if rc != ffi::SQLITE_OK {
                return Err(self.error().into());
            }
            Ok(())
        }
        fn integer(&self, sql: &str) -> Result<i64, Box<dyn std::error::Error>> {
            let sql = CString::new(sql)?;
            let mut statement = std::ptr::null_mut();
            // SAFETY: The connection and terminated SQL string remain live. SQLite
            // writes the statement to an exclusive local slot; its owner finalizes it
            // before the connection is closed.
            let rc = unsafe {
                ffi::sqlite3_prepare_v2(
                    self.0,
                    sql.as_ptr(),
                    -1,
                    &raw mut statement,
                    std::ptr::null_mut(),
                )
            };
            if rc != ffi::SQLITE_OK {
                return Err(self.error().into());
            }
            // SAFETY: The prepared statement and its connection remain live and
            // exclusive to this thread; prior column borrows have ended before stepping.
            let rc = unsafe { ffi::sqlite3_step(statement) };
            let value = if rc == ffi::SQLITE_ROW {
                // SAFETY: This test's scalar query has a first column, and step
                // returned ROW. The statement remains live until finalize below.
                unsafe { ffi::sqlite3_column_int64(statement, 0) }
            } else {
                0
            };
            // SAFETY: This owner releases the prepared statement exactly once,
            // while its connection is still live and all column borrows have ended.
            unsafe {
                ffi::sqlite3_finalize(statement);
            }
            if rc != ffi::SQLITE_ROW {
                return Err(self.error().into());
            }
            Ok(value)
        }
    }
    impl Drop for Connection {
        fn drop(&mut self) {
            // SAFETY: This owner consumes its live SQLite connection exactly once;
            // no statement or buffer is used after the close.
            unsafe {
                ffi::sqlite3_close(self.0);
            }
        }
    }

    #[test]
    fn configured_vfs_opens_and_bootstraps_without_a_setup_call() -> TestResult {
        for filesystem in [false, true] {
            let directory = tempfile::tempdir()?;
            let backend: Arc<dyn StorageBackend> = if filesystem {
                Arc::new(zsqlite::FilesystemBackend::open(
                    directory.path().join("sealed"),
                )?)
            } else {
                Arc::new(MemoryBackend::new()?)
            };
            let measured = Arc::new(zsqlite::storage::FaultBackend::new(backend));
            let storage = Storage::new(
                Arc::new(ExternalAdapter(measured.clone())),
                directory.path().join("coord"),
            )?;
            let name = if filesystem {
                "open-filesystem"
            } else {
                "open-memory"
            };
            storage.register_vfs(name)?;
            storage.register_vfs(name)?;
            let source = directory.path().join("source.db");
            let connection = Connection::open(&source, name)?;
            connection.exec("CREATE TABLE data(id INTEGER PRIMARY KEY, value BLOB); WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2048) INSERT INTO data SELECT x,randomblob(2000) FROM n;")?;
            drop(connection);
            let database = storage.open(&source)?;
            let sealed = database.flush()?;
            drop(database);
            let destination = directory.path().join("restored.db");
            measured.reset_statistics();
            let start = std::time::Instant::now();
            let connection = Connection::open(&destination, name)?;
            let opened = start.elapsed();
            let open_bytes = measured.statistics().read_bytes;
            assert_eq!(
                connection.integer("SELECT length(value) FROM data WHERE id=1234")?,
                2000
            );
            let queried = start.elapsed();
            let query_bytes = measured.statistics().read_bytes;
            assert!(
                query_bytes < sealed.logical_size / 4,
                "lazy open fetched {query_bytes} of {}",
                sealed.logical_size
            );
            assert!(
                directory
                    .path()
                    .join("restored.db.zsqlite")
                    .metadata()?
                    .len()
                    < 32 * 1024
            );
            eprintln!(
                "{name}: open {opened:?} ({open_bytes} backend bytes), first indexed query {queried:?} ({query_bytes} backend bytes), database {} bytes",
                sealed.logical_size
            );
            connection.exec("INSERT INTO data VALUES(2049,zeroblob(2000))")?;
            drop(connection);
            let database = storage.open(&destination)?;
            database.flush()?;
            drop(database);
            let connection = Connection::open(&destination, name)?;
            assert_eq!(connection.integer("SELECT count(*) FROM data")?, 2049);
        }
        Ok(())
    }

    #[test]
    fn external_adapter_runs_divergent_sqlite_forks_in_shared_storage() -> TestResult {
        let directory = tempfile::tempdir()?;
        let adapter = Arc::new(ExternalAdapter(Arc::new(MemoryBackend::new()?)));
        let storage = Storage::new(adapter, directory.path().join("coord"))?;
        storage.register_vfs("fork-source")?;
        let source = directory.path().join("source.zsqlite");
        let connection = Connection::open(&source, "fork-source")?;
        connection.exec("CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO items VALUES(1,0);")?;
        drop(connection);
        storage.open(&source)?.flush()?;
        let left = storage.fork("left")?;
        let right = storage.fork("right")?;
        left.register_vfs("fork-left")?;
        right.register_vfs("fork-right")?;
        let mut threads = Vec::new();
        for (head, vfs, value) in [
            (left.clone(), "fork-left", 41),
            (right.clone(), "fork-right", 42),
        ] {
            let path = directory.path().join(format!("{value}.zsqlite"));
            threads.push(std::thread::spawn(move || -> Result<_, String> {
                let connection = Connection::open(&path, vfs).map_err(|e| e.to_string())?;
                connection
                    .exec(&format!(
                        "PRAGMA journal_mode=WAL; UPDATE items SET value={value} WHERE id=1;"
                    ))
                    .map_err(|e| e.to_string())?;
                drop(connection);
                let database = head.open(&path).map_err(|e| e.to_string())?;
                let info = database.flush().map_err(|e| e.to_string())?;
                database.collect(usize::MAX).map_err(|e| e.to_string())?;
                Ok((info.head_history, path))
            }));
        }
        let (left_hash, left_path) = threads.remove(0).join().expect("left fork thread")?;
        let (right_hash, right_path) = threads.remove(0).join().expect("right fork thread")?;
        assert_ne!(left_hash, right_hash);
        let source_connection = Connection::open(&source, "fork-source")?;
        assert_eq!(source_connection.integer("SELECT value FROM items")?, 0);
        let left_connection = Connection::open(&left_path, "fork-left")?;
        assert_eq!(left_connection.integer("SELECT value FROM items")?, 41);
        std::fs::remove_file(&right_path)?;
        let restored_path = directory.path().join("right-restored.zsqlite");
        let restored = right.bootstrap(&restored_path)?;
        let connection = Connection::open(&restored_path, "fork-right")?;
        assert_eq!(connection.integer("SELECT value FROM items")?, 42);
        restored.verify()?;
        assert_eq!(left_connection.integer("SELECT value FROM items")?, 41);
        Ok(())
    }

    #[test]
    fn external_adapter_bootstraps_lazily_and_continues_writing() -> TestResult {
        let directory = tempfile::tempdir()?;
        let memory = Arc::new(MemoryBackend::new()?);
        let measured = Arc::new(zsqlite::storage::FaultBackend::new(memory));
        let adapter = Arc::new(ExternalAdapter(measured.clone()));
        let storage = Storage::new(adapter.clone(), directory.path().join("coord"))?;
        let source = directory.path().join("original.zsqlite");
        let database = storage.create(&source)?;
        storage.register_vfs("adapter-bootstrap")?;
        let connection = Connection::open(&source, "adapter-bootstrap")?;
        connection.exec("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; CREATE TABLE items(id INTEGER PRIMARY KEY, value BLOB); WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1024) INSERT INTO items SELECT x,randomblob(2000) FROM n;")?;
        drop(connection);
        let original = database.flush()?;
        let before = directory.path().join("before.sqlite");
        zsqlite::export_to_sqlite(&source, &before)?;
        let pin = database.retain(zsqlite::RetentionName::new("saved")?)?;
        let destination = directory.path().join("restored.zsqlite");
        assert!(matches!(
            storage.bootstrap(&destination),
            Err(zsqlite::StoreError::Busy)
        ));
        drop(database);
        std::fs::remove_file(&source)?;
        std::fs::remove_dir_all(directory.path().join("original.zsqlite.d"))?;
        // Lose every local control/cache file too: the adapter alone contains
        // the recoverable database identity, policy and history.
        std::fs::remove_dir_all(storage.coordination_directory())?;
        let storage = Storage::new(adapter, directory.path().join("coord"))?;
        measured.reset_statistics();
        let restored = storage.bootstrap(&destination)?;
        let fetched_on_bootstrap = measured.statistics().read_bytes;
        assert!(
            fetched_on_bootstrap < original.logical_size / 2,
            "bootstrap fetched {fetched_on_bootstrap} of {} bytes",
            original.logical_size
        );
        assert!(destination.metadata()?.len() < 32 * 1024);
        let info = restored.inspect()?;
        assert_eq!(info.head_txid, original.head_txid);
        assert_eq!(info.head_history, original.head_history);
        let after = directory.path().join("after.sqlite");
        zsqlite::export_to_sqlite(&destination, &after)?;
        assert_eq!(std::fs::read(before)?, std::fs::read(after)?);
        let retained = restored.open_retained(&zsqlite::RetentionName::new("saved")?)?;
        assert_eq!(retained.logical_size().get(), original.logical_size);
        drop(retained);
        let connection = Connection::open(&destination, "adapter-bootstrap")?;
        assert_eq!(connection.integer("SELECT count(*) FROM items")?, 1024);
        connection.exec("INSERT INTO items VALUES(1025,zeroblob(2000));")?;
        drop(connection);
        let updated = restored.flush()?;
        assert!(updated.head_txid > original.head_txid);
        restored.release_retention(pin)?;
        drop(restored);
        let final_path = directory.path().join("again.zsqlite");
        let final_database = storage.bootstrap(&final_path)?;
        let connection = Connection::open(&final_path, "adapter-bootstrap")?;
        assert_eq!(connection.integer("SELECT count(*) FROM items")?, 1025);
        drop(connection);
        assert!(matches!(
            storage.open(&destination),
            Err(zsqlite::StoreError::StaleAttachment)
        ));
        final_database.verify()?;
        Ok(())
    }
}

#[test]
fn staged_objects_are_invisible_until_consumed_finish() -> TestResult {
    use std::io::Write;
    use zsqlite::domain::BlobId;
    let directory = tempfile::tempdir()?;
    let backends: [Arc<dyn StorageBackend>; 2] = [
        Arc::new(MemoryBackend::new()?),
        Arc::new(zsqlite::FilesystemBackend::open(directory.path())?),
    ];
    for backend in backends {
        let backend = ExternalAdapter(backend);
        let mut writer = backend.begin_write()?;
        writer.write_all(b"first ")?;
        writer.write_all(b"second")?;
        assert!(backend.inventory(None, 10)?.is_empty());
        let key = ObjectKey::Blob(BlobId::from_bytes(
            *blake3::hash(b"first second").as_bytes(),
        ));
        writer.finish(key, StoredBytes::new(12))?;
        assert_eq!(backend.stat(key)?, Some(StoredBytes::new(12)));
        let mut aborted = backend.begin_write()?;
        aborted.write_all(b"unfinished")?;
        drop(aborted);
        assert_eq!(backend.inventory(None, 10)?, [key]);
        let mut conflict = backend.begin_write()?;
        conflict.write_all(b"other bytes!")?;
        assert!(matches!(
            conflict.finish(key, StoredBytes::new(12)),
            Err(BackendError::IdentityMismatch(_))
        ));
        let missing = ObjectKey::Blob(BlobId::from_bytes([42; 32]));
        let mut wrong_length = backend.begin_write()?;
        wrong_length.write_all(b"short")?;
        assert!(wrong_length.finish(missing, StoredBytes::new(6)).is_err());
        assert_eq!(backend.stat(missing)?, None);
    }
    Ok(())
}
