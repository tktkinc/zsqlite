//! Reproducible GC request accounting against a simulated object store.
//!
//! Run with `cargo bench --bench gc_object_storage --no-default-features --features static`.
//! Environment: `GC_MIB=8`, `GC_LATENCY_US=0`, `GC_READ_CONCURRENCY=16`,
//! `GC_CACHE_MODES=cold,warm,mixed`, `GC_PATTERNS=fragmented,clustered`,
//! `GC_MODEL=local_async` (or `write_through` for an uncached-remote comparison).
//! `GC_MIB` supports 1..=128 so the fragmented survivors fit one maintenance pass.
//!
//! The remote tier is `MemoryBackend` with request latency, not a real network.
//! A `read_ranges` batch costs ceil(remote requests / concurrency) latency rounds;
//! HEAD, root GET, LIST, PUT, DELETE, and CAS each cost one round. Streaming
//! writer calls count as one completed PUT (no multipart-upload model).
//! In `write_through`, optional local disk caching fetches complete objects on miss and
//! serves both ranges and stat locally thereafter. Cache modes are reset before
//! each measured operation; warm caches every existing object, mixed caches
//! every second object in inventory order, and cold starts empty. Writes are
//! synchronously copied to both tiers.
//!
//! The default `local_async` uses a durable `FilesystemBackend` as authoritative
//! storage. Metadata and roots remain local; only uploaded blobs can be evicted.
//! `maintain` and `collect` complete locally, followed by a separately timed sync
//! that uploads pending objects before publishing one remote root. Remote deletes
//! are deferred and counted, never executed: remote retention/reader coordination
//! is outside this harness. The final remote checkpoint is bootstrapped and fully
//! verified outside timed regions. This is a cost model, not a production uploader.
use libsqlite3_sys as ffi;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use zsqlite::domain::{BackendId, DecodedBytes, FileOffset, StoredBytes, StoredRange};
use zsqlite::layout::LayoutPolicy;
use zsqlite::storage::adapter::{
    BackendError, DeletePermit, ObjectKey, ObjectRange, ObjectWriter, Publication, Revision,
    RootRecord, StorageBackend,
};
use zsqlite::{DictionaryPolicy, FilesystemBackend, MemoryBackend, Storage, StoragePolicy};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Default)]
struct Counts {
    batches: u64,
    ranges: u64,
    requested_bytes: u64,
    remote_gets: u64,
    remote_blob_gets: u64,
    remote_bytes: u64,
    local_ranges: u64,
    local_bytes: u64,
    heads: u64,
    roots: u64,
    lists: u64,
    puts: u64,
    put_bytes: u64,
    deletes: u64,
    publications: u64,
    latency_rounds: u64,
    local_roots: u64,
    local_publications: u64,
    deferred_remote_deletes: u64,
}

struct SimulatedStore {
    remote: Arc<MemoryBackend>,
    local: Option<FilesystemBackend>,
    known: Mutex<BTreeMap<ObjectKey, StoredBytes>>,
    pending: Mutex<BTreeSet<ObjectKey>>,
    uploaded: Mutex<BTreeSet<ObjectKey>>,
    remote_root: Mutex<Option<RootRecord>>,
    cache_directory: PathBuf,
    cached: Mutex<BTreeMap<ObjectKey, (PathBuf, StoredBytes)>>,
    counts: Mutex<Counts>,
    latency: Mutex<Duration>,
    concurrency: usize,
    cache_enabled: bool,
}
impl SimulatedStore {
    fn model(&self) -> &'static str {
        if self.local.is_some() {
            "local_async"
        } else {
            "write_through"
        }
    }
    fn count(&self, change: impl FnOnce(&mut Counts)) {
        change(&mut self.counts.lock().unwrap());
    }
    fn delay(&self, requests: usize, concurrency: usize) {
        let rounds = requests.div_ceil(concurrency) as u64;
        self.count(|count| count.latency_rounds += rounds);
        let delay = *self.latency.lock().unwrap();
        if !delay.is_zero() && rounds != 0 {
            std::thread::sleep(delay.saturating_mul(u32::try_from(rounds).unwrap()));
        }
    }
    fn cache_bytes(&self, key: ObjectKey, bytes: &[u8]) -> std::io::Result<()> {
        let path = self.cache_directory.join(format!("{key:?}"));
        std::fs::write(&path, bytes)?;
        self.cached
            .lock()
            .unwrap()
            .insert(key, (path, StoredBytes::new(bytes.len() as u64)));
        Ok(())
    }
    fn prepare(&self, mode: &str, latency: Duration) -> Result<()> {
        *self.latency.lock().unwrap() = Duration::ZERO;
        if let Some(local) = &self.local {
            let pending = self.pending.lock().unwrap();
            for (index, (&key, &size)) in self.known.lock().unwrap().iter().enumerate() {
                if !matches!(key, ObjectKey::Blob(_)) || pending.contains(&key) {
                    continue;
                }
                let keep = mode == "warm" || (mode == "mixed" && index % 2 == 0);
                if keep && local.stat(key)?.is_none() {
                    let request =
                        ObjectRange::new(key, StoredRange::new(FileOffset::new(0), size)?)?;
                    let bytes = self.remote.read_ranges(&[request])?;
                    local.put(key, size, &mut &bytes[0][..])?;
                } else if !keep && local.stat(key)?.is_some() {
                    std::fs::remove_file(local.object_path(key))?;
                }
            }
            self.reset(latency);
            return Ok(());
        }
        self.cached.lock().unwrap().clear();
        for entry in std::fs::read_dir(&self.cache_directory)? {
            std::fs::remove_file(entry?.path())?;
        }
        if mode == "warm" || mode == "mixed" {
            let mut after = None;
            let mut index = 0;
            loop {
                let keys = self.remote.inventory(after, 4096)?;
                if keys.is_empty() {
                    break;
                }
                for key in &keys {
                    if mode == "warm" || index % 2 == 0 {
                        let size = self.remote.stat(*key)?.ok_or("missing cache object")?;
                        let request =
                            ObjectRange::new(*key, StoredRange::new(FileOffset::new(0), size)?)?;
                        let bytes = self.remote.read_ranges(&[request])?;
                        self.cache_bytes(*key, &bytes[0])?;
                    }
                    index += 1;
                }
                after = keys.last().copied();
            }
        }
        self.reset(latency);
        Ok(())
    }
    fn reset(&self, latency: Duration) {
        *self.counts.lock().unwrap() = Counts::default();
        *self.latency.lock().unwrap() = latency;
    }
    fn stop(&self) -> Counts {
        *self.latency.lock().unwrap() = Duration::ZERO;
        *self.counts.lock().unwrap()
    }

    fn synchronize(&self) -> Result<()> {
        let Some(local) = &self.local else {
            return Ok(());
        };
        let mut pending = self.pending.lock().unwrap();
        let known = self.known.lock().unwrap();
        for &key in pending.iter() {
            let size = known[&key];
            self.count(|count| {
                count.puts += 1;
                count.put_bytes += size.get();
            });
            self.delay(1, 1);
            self.remote
                .put(key, size, &mut File::open(local.object_path(key))?)?;
            self.uploaded.lock().unwrap().insert(key);
        }
        let root = local.read_root()?.ok_or("local root missing")?;
        let mut previous = self.remote_root.lock().unwrap();
        if previous
            .as_ref()
            .is_none_or(|old| old.bytes() != root.bytes())
        {
            self.count(|count| count.publications += 1);
            self.delay(1, 1);
            match self
                .remote
                .compare_exchange_root(previous.as_ref().map(RootRecord::revision), root.bytes())?
            {
                Publication::Applied(revision) => {
                    *previous = Some(RootRecord::new(revision, root.bytes().to_vec())?);
                }
                other => {
                    return Err(format!("unexpected simulated sync publication: {other:?}").into());
                }
            }
        }
        pending.clear();
        Ok(())
    }

    fn local_read_ranges(
        &self,
        local: &FilesystemBackend,
        requests: &[ObjectRange],
    ) -> std::result::Result<Vec<Vec<u8>>, BackendError> {
        let mut missing = BTreeSet::new();
        for request in requests {
            if local.stat(request.key())?.is_some() {
                self.count(|count| {
                    count.local_ranges += 1;
                    count.local_bytes += request.range().length().get();
                });
            } else {
                missing.insert(request.key());
            }
        }
        if !self.cache_enabled {
            self.delay(
                requests
                    .iter()
                    .filter(|request| missing.contains(&request.key()))
                    .count(),
                self.concurrency,
            );
            return requests
                .iter()
                .map(|request| {
                    if missing.contains(&request.key()) {
                        self.count(|count| {
                            count.remote_gets += 1;
                            count.remote_blob_gets +=
                                u64::from(matches!(request.key(), ObjectKey::Blob(_)));
                            count.remote_bytes += request.range().length().get();
                        });
                        Ok(self.remote.read_ranges(&[*request])?.remove(0))
                    } else {
                        Ok(local.read_ranges(&[*request])?.remove(0))
                    }
                })
                .collect();
        }
        self.delay(missing.len(), self.concurrency);
        for key in missing {
            let size = *self
                .known
                .lock()
                .unwrap()
                .get(&key)
                .ok_or(BackendError::Missing(key))?;
            let request = ObjectRange::new(
                key,
                StoredRange::new(FileOffset::new(0), size).map_err(|_| BackendError::Range)?,
            )?;
            let bytes = self.remote.read_ranges(&[request])?;
            self.count(|count| {
                count.remote_gets += 1;
                count.remote_blob_gets += u64::from(matches!(key, ObjectKey::Blob(_)));
                count.remote_bytes += size.get();
            });
            local.put(key, size, &mut &bytes[0][..])?;
        }
        local.read_ranges(requests)
    }
}

struct MeasuredWriter<'a> {
    inner: Box<dyn ObjectWriter + 'a>,
    owner: &'a SimulatedStore,
    bytes: Vec<u8>,
}
impl Write for MeasuredWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        if self.owner.cache_enabled && self.owner.local.is_none() {
            self.bytes.extend_from_slice(&bytes[..written]);
        }
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
impl ObjectWriter for MeasuredWriter<'_> {
    fn finish(
        self: Box<Self>,
        key: ObjectKey,
        length: StoredBytes,
    ) -> std::result::Result<(), BackendError> {
        if self.owner.local.is_some() {
            self.inner.finish(key, length)?;
            self.owner.known.lock().unwrap().insert(key, length);
            if !self.owner.uploaded.lock().unwrap().contains(&key) {
                self.owner.pending.lock().unwrap().insert(key);
            }
            return Ok(());
        }
        self.owner.count(|count| {
            count.puts += 1;
            count.put_bytes += length.get();
        });
        self.owner.delay(1, 1);
        self.inner.finish(key, length)?;
        if self.owner.cache_enabled {
            self.owner.cache_bytes(key, &self.bytes)?;
        }
        Ok(())
    }
}

impl StorageBackend for SimulatedStore {
    fn identity(&self) -> BackendId {
        self.local
            .as_ref()
            .map_or_else(|| self.remote.identity(), StorageBackend::identity)
    }
    fn begin_write(&self) -> std::result::Result<Box<dyn ObjectWriter + '_>, BackendError> {
        Ok(Box::new(MeasuredWriter {
            inner: if let Some(local) = &self.local {
                local.begin_write()?
            } else {
                self.remote.begin_write()?
            },
            owner: self,
            bytes: Vec::new(),
        }))
    }
    fn read_ranges(
        &self,
        requests: &[ObjectRange],
    ) -> std::result::Result<Vec<Vec<u8>>, BackendError> {
        self.count(|count| {
            count.batches += 1;
            count.ranges += requests.len() as u64;
            count.requested_bytes += requests
                .iter()
                .map(|r| r.range().length().get())
                .sum::<u64>();
        });
        if let Some(local) = &self.local {
            return self.local_read_ranges(local, requests);
        }
        if !self.cache_enabled {
            self.count(|count| {
                count.remote_gets += requests.len() as u64;
                count.remote_blob_gets += requests
                    .iter()
                    .filter(|r| matches!(r.key(), ObjectKey::Blob(_)))
                    .count() as u64;
                count.remote_bytes += requests
                    .iter()
                    .map(|r| r.range().length().get())
                    .sum::<u64>();
            });
            self.delay(requests.len(), self.concurrency);
            return self.remote.read_ranges(requests);
        }
        let missing: BTreeSet<_> = {
            let cached = self.cached.lock().unwrap();
            for request in requests
                .iter()
                .filter(|request| cached.contains_key(&request.key()))
            {
                self.count(|count| {
                    count.local_ranges += 1;
                    count.local_bytes += request.range().length().get();
                });
            }
            requests
                .iter()
                .map(|r| r.key())
                .filter(|key| !cached.contains_key(key))
                .collect()
        };
        // Count actual object GETs independently from core batches/range count.
        // HEAD and GET run as two bounded-concurrency waves for cache misses.
        self.count(|count| count.heads += missing.len() as u64);
        self.delay(missing.len(), self.concurrency);
        self.delay(missing.len(), self.concurrency);
        for key in missing {
            let length = self.remote.stat(key)?.ok_or(BackendError::Missing(key))?;
            let range = ObjectRange::new(
                key,
                StoredRange::new(FileOffset::new(0), length).map_err(|_| BackendError::Range)?,
            )?;
            let bytes = self.remote.read_ranges(&[range])?;
            self.count(|count| {
                count.remote_gets += 1;
                count.remote_blob_gets += u64::from(matches!(key, ObjectKey::Blob(_)));
                count.remote_bytes += length.get();
            });
            self.cache_bytes(key, &bytes[0])?;
        }
        let cached = self.cached.lock().unwrap();
        requests
            .iter()
            .map(|request| {
                let (path, _) = &cached[&request.key()];
                let mut file = File::open(path)?;
                file.seek(SeekFrom::Start(request.range().offset().get()))?;
                let length = usize::try_from(request.range().length().get())
                    .map_err(|_| BackendError::Range)?;
                let mut bytes = vec![0; length];
                file.read_exact(&mut bytes)?;
                Ok(bytes)
            })
            .collect()
    }
    fn stat(&self, key: ObjectKey) -> std::result::Result<Option<StoredBytes>, BackendError> {
        if self.local.is_some() {
            return Ok(self.known.lock().unwrap().get(&key).copied());
        }
        if let Some((_, size)) = self.cached.lock().unwrap().get(&key) {
            return Ok(Some(*size));
        }
        self.count(|count| count.heads += 1);
        self.delay(1, 1);
        self.remote.stat(key)
    }
    fn read_root(&self) -> std::result::Result<Option<RootRecord>, BackendError> {
        if let Some(local) = &self.local {
            self.count(|count| count.local_roots += 1);
            return local.read_root();
        }
        self.count(|count| count.roots += 1);
        self.delay(1, 1);
        self.remote.read_root()
    }
    fn compare_exchange_root(
        &self,
        expected: Option<&Revision>,
        bytes: &[u8],
    ) -> std::result::Result<Publication, BackendError> {
        if let Some(local) = &self.local {
            self.count(|count| count.local_publications += 1);
            return local.compare_exchange_root(expected, bytes);
        }
        self.count(|count| count.publications += 1);
        self.delay(1, 1);
        self.remote.compare_exchange_root(expected, bytes)
    }
    fn inventory(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> std::result::Result<Vec<ObjectKey>, BackendError> {
        if self.local.is_some() {
            if limit == 0 || limit > 4096 {
                return Err(BackendError::Range);
            }
            return Ok(self
                .known
                .lock()
                .unwrap()
                .keys()
                .copied()
                .filter(|key| after.is_none_or(|after| *key > after))
                .take(limit)
                .collect());
        }
        self.count(|count| count.lists += 1);
        self.delay(1, 1);
        self.remote.inventory(after, limit)
    }
    fn delete(&self, permit: DeletePermit<'_>) -> std::result::Result<(), BackendError> {
        let key = permit.key();
        if let Some(local) = &self.local {
            local.delete(permit)?;
            self.known.lock().unwrap().remove(&key);
            self.pending.lock().unwrap().remove(&key);
            if self.uploaded.lock().unwrap().contains(&key) {
                self.count(|count| count.deferred_remote_deletes += 1);
            }
            return Ok(());
        }
        self.count(|count| count.deletes += 1);
        self.delay(1, 1);
        self.remote.delete(permit)?;
        if let Some((path, _)) = self.cached.lock().unwrap().remove(&key) {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }
}

struct Connection(*mut ffi::sqlite3);
impl Connection {
    fn open(path: &Path, vfs: &str) -> Result<Self> {
        let path = CString::new(path.to_str().ok_or("invalid path")?)?;
        let vfs = CString::new(vfs)?;
        let mut database = std::ptr::null_mut();
        // SAFETY: C strings and the exclusive output slot outlive this call.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                path.as_ptr(),
                &raw mut database,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                vfs.as_ptr(),
            )
        };
        let result = Self(database);
        result.check(rc)?;
        Ok(result)
    }
    fn check(&self, rc: i32) -> Result<()> {
        if rc != ffi::SQLITE_OK && rc != ffi::SQLITE_DONE {
            // SAFETY: The owned live connection supplies a terminated message.
            let message = unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) };
            return Err(message.to_string_lossy().into_owned().into());
        }
        Ok(())
    }
    fn exec(&self, sql: &str) -> Result<()> {
        let sql = CString::new(sql)?;
        // SAFETY: The connection and terminated SQL remain live for this call.
        self.check(unsafe {
            ffi::sqlite3_exec(
                self.0,
                sql.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        })
    }
    fn rows(&self, ids: impl Iterator<Item = usize>, generation: u64, update: bool) -> Result<()> {
        let sql = CString::new(if update {
            "UPDATE data SET value=?2 WHERE id=?1"
        } else {
            "INSERT INTO data VALUES(?1,?2)"
        })?;
        let mut statement = std::ptr::null_mut();
        // SAFETY: The connection/SQL remain live, and SQLite initializes this slot.
        self.check(unsafe {
            ffi::sqlite3_prepare_v2(
                self.0,
                sql.as_ptr(),
                -1,
                &raw mut statement,
                std::ptr::null_mut(),
            )
        })?;
        let result = (|| {
            for id in ids {
                let mut bytes = [0_u8; 3000];
                let mut hash = blake3::Hasher::new();
                hash.update(&(id as u64).to_le_bytes());
                hash.update(&generation.to_le_bytes());
                hash.finalize_xof().fill(&mut bytes);
                // SAFETY: The statement is exclusively owned here; TRANSIENT copies
                // the live byte buffer before the bind returns. Parameters are valid.
                unsafe {
                    self.check(ffi::sqlite3_bind_int64(statement, 1, i64::try_from(id)?))?;
                    self.check(ffi::sqlite3_bind_blob(
                        statement,
                        2,
                        bytes.as_ptr().cast(),
                        3000,
                        ffi::SQLITE_TRANSIENT(),
                    ))?;
                    self.check(ffi::sqlite3_step(statement))?;
                    self.check(ffi::sqlite3_reset(statement))?;
                }
            }
            Ok(())
        })();
        // SAFETY: This owns the initialized statement and releases it exactly once.
        unsafe { ffi::sqlite3_finalize(statement) };
        result
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: All statements are finalized before this owned connection closes.
        unsafe { ffi::sqlite3_close(self.0) };
    }
}

#[derive(Default)]
struct Work {
    packs: usize,
    copied_frames: usize,
    copied_bytes: u64,
    decoded_bytes: u64,
    deleted_objects: usize,
    deleted_bytes: u64,
}

fn measure(
    backend: &SimulatedStore,
    mode: &str,
    pattern: &str,
    phase: &str,
    latency: Duration,
    operation: impl FnOnce() -> Result<Work>,
) -> Result<()> {
    if phase.starts_with("sync") {
        backend.reset(latency);
    } else {
        backend.prepare(mode, latency)?;
    }
    let start = Instant::now();
    let work = operation()?;
    let elapsed = start.elapsed();
    let c = backend.stop();
    println!(
        "{},{mode},{pattern},{phase},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        backend.model(),
        elapsed.as_secs_f64() * 1000.0,
        c.batches,
        c.ranges,
        c.requested_bytes,
        c.remote_gets,
        c.remote_blob_gets,
        c.remote_bytes,
        c.local_ranges,
        c.local_bytes,
        c.heads,
        c.roots,
        c.lists,
        c.puts,
        c.put_bytes,
        c.deletes,
        c.publications,
        c.latency_rounds,
        work.packs,
        work.copied_frames,
        work.copied_bytes,
        work.decoded_bytes,
        work.deleted_objects,
        work.deleted_bytes,
        latency.as_micros(),
        backend.concurrency,
        c.local_roots,
        c.local_publications,
        c.deferred_remote_deletes,
    );
    Ok(())
}

fn populate(database: &zsqlite::Database, name: &str, count: usize, pattern: &str) -> Result<()> {
    let path = database.path();
    // Keep request-accounting focused on pack GC rather than dictionary training.
    zsqlite::configure(
        path,
        StoragePolicy::default()
            .with_dictionary(DictionaryPolicy::new(0, 1024 * 1024)?)
            .with_layout(
                LayoutPolicy::default().with_maintenance(DecodedBytes::new(64 * 1024 * 1024), 0)?,
            ),
    )?;
    let per_seal = count / 4;
    for seal in 0..4 {
        let connection = Connection::open(path, name)?;
        if seal == 0 {
            connection.exec("PRAGMA page_size=4096; PRAGMA journal_mode=DELETE; CREATE TABLE data(id INTEGER PRIMARY KEY,value BLOB NOT NULL)")?;
        }
        connection.exec("BEGIN")?;
        connection.rows((seal * per_seal + 1)..=((seal + 1) * per_seal), 0, false)?;
        connection.exec("COMMIT")?;
        drop(connection);
        database.flush()?;
    }
    let connection = Connection::open(path, name)?;
    connection.exec("BEGIN")?;
    connection.rows(
        (1..=count).filter(|id| match pattern {
            "fragmented" => id % 4 != 0,
            "clustered" => (id - 1) % per_seal < per_seal * 3 / 4,
            _ => unreachable!(),
        }),
        1,
        true,
    )?;
    connection.exec("COMMIT")?;
    drop(connection);
    database.flush()?;
    Ok(())
}

#[allow(clippy::too_many_lines)] // Keep setup, measured stages, and checkpoint verification together.
fn fixture(
    mib: usize,
    model: &str,
    mode: &str,
    pattern: &str,
    latency: Duration,
    concurrency: usize,
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let cache_directory = directory.path().join("disk-cache");
    std::fs::create_dir(&cache_directory)?;
    let backend = Arc::new(SimulatedStore {
        remote: Arc::new(MemoryBackend::new()?),
        local: if model == "local_async" {
            Some(FilesystemBackend::open(
                directory.path().join("local-primary"),
            )?)
        } else {
            None
        },
        known: Mutex::new(BTreeMap::new()),
        pending: Mutex::new(BTreeSet::new()),
        uploaded: Mutex::new(BTreeSet::new()),
        remote_root: Mutex::new(None),
        cache_directory,
        cached: Mutex::new(BTreeMap::new()),
        counts: Mutex::new(Counts::default()),
        latency: Mutex::new(Duration::ZERO),
        concurrency,
        cache_enabled: mode != "none",
    });
    let storage = Storage::new(backend.clone(), directory.path().join("coord"))?;
    let name = format!("gc-{model}-{mode}-{pattern}");
    storage.register_vfs(&name)?;
    let path = directory.path().join("database.zsqlite");
    let database = storage.create(&path)?;
    let count = mib.checked_mul(256).ok_or("GC_MIB too large")?;
    populate(&database, &name, count, pattern)?;
    backend.synchronize()?;
    let before = database.inspect()?;
    eprintln!(
        "fixture cache={mode} pattern={pattern} logical_bytes={} packs={} sparse_packs={}",
        before.logical_size,
        before.pack_occupancy.len(),
        before
            .pack_occupancy
            .iter()
            .filter(|pack| pack.live_pages * 2 < pack.total_pages)
            .count()
    );
    measure(&backend, mode, pattern, "maintain", latency, || {
        let report = database.maintain()?;
        assert!(
            report.repacked_packs > 1,
            "fixture must batch multiple packs"
        );
        assert_eq!(
            report.decoded_input.get(),
            0,
            "page frames should copy encoded"
        );
        Ok(Work {
            packs: report.repacked_packs,
            copied_frames: report.copied_frames,
            copied_bytes: report.copied_bytes.get(),
            decoded_bytes: report.decoded_input.get(),
            deleted_objects: report.gc.deleted_objects,
            deleted_bytes: report.gc.deleted_bytes,
        })
    })?;
    measure(&backend, mode, pattern, "collect", latency, || {
        let report = database.collect(usize::MAX)?;
        Ok(Work {
            deleted_objects: report.deleted_objects,
            deleted_bytes: report.deleted_bytes,
            ..Work::default()
        })
    })?;
    if backend.local.is_some() {
        measure(&backend, mode, pattern, "sync_gc", latency, || {
            backend.synchronize()?;
            Ok(Work::default())
        })?;
    }
    measure(&backend, mode, pattern, "noop_maintain", latency, || {
        let report = database.maintain()?;
        assert_eq!(report.repacked_packs, 0);
        Ok(Work::default())
    })?;
    let connection = Connection::open(&path, &name)?;
    connection.exec("BEGIN")?;
    connection.rows(1..=count, 2, true)?;
    connection.exec("COMMIT")?;
    drop(connection);
    database.flush()?;
    measure(
        &backend,
        mode,
        pattern,
        "fully_dead_collect",
        latency,
        || {
            let report = database.collect(usize::MAX)?;
            assert!(report.deleted_bytes > 0);
            Ok(Work {
                deleted_objects: report.deleted_objects,
                deleted_bytes: report.deleted_bytes,
                ..Work::default()
            })
        },
    )?;
    if backend.local.is_some() {
        measure(&backend, mode, pattern, "sync_dead", latency, || {
            backend.synchronize()?;
            Ok(Work::default())
        })?;
    }
    let final_view = database.verify()?;
    if backend.local.is_some() {
        let restored = Storage::new(
            backend.remote.clone(),
            directory.path().join("remote-verification"),
        )?
        .bootstrap(directory.path().join("restored.zsqlite"))?;
        let verified = restored.verify()?;
        assert_eq!(verified.head_history, final_view.head_history);
        assert_eq!(verified.logical_size, final_view.logical_size);
    }
    Ok(())
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn main() -> Result<()> {
    let mib: usize = env("GC_MIB", "8").parse()?;
    let latency = Duration::from_micros(env("GC_LATENCY_US", "0").parse()?);
    let concurrency: usize = env("GC_READ_CONCURRENCY", "16").parse()?;
    let model = env("GC_MODEL", "local_async");
    if !["local_async", "write_through"].contains(&model.as_str()) {
        return Err(format!("unknown model {model}").into());
    }
    if !(1..=128).contains(&mib) || concurrency == 0 {
        return Err("GC_MIB must be 1..=128 and GC_READ_CONCURRENCY must be positive".into());
    }
    println!(
        "model,cache,pattern,phase,elapsed_ms,batch_calls,range_requests,requested_bytes,remote_gets,remote_blob_gets,remote_read_bytes,local_hit_ranges,local_hit_bytes,remote_heads,root_gets,lists,puts,put_bytes,deletes,cas,latency_rounds,repacked_packs,copied_frames,copied_bytes,decoded_bytes,deleted_objects,deleted_bytes,latency_us,read_concurrency,local_root_gets,local_cas,deferred_remote_deletes"
    );
    for mode in env("GC_CACHE_MODES", "cold,warm,mixed").split(',') {
        if !["none", "cold", "warm", "mixed"].contains(&mode) {
            return Err(format!("unknown cache mode {mode}").into());
        }
        for pattern in env("GC_PATTERNS", "fragmented,clustered").split(',') {
            if !["fragmented", "clustered"].contains(&pattern) {
                return Err(format!("unknown pattern {pattern}").into());
            }
            fixture(mib, &model, mode, pattern, latency, concurrency)?;
        }
    }
    Ok(())
}
