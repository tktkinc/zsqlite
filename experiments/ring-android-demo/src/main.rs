//! Spike: a `ring` (AES-256-GCM) encrypting `StorageBackend` over zsqlite's VFS, proven on-device.
//!
//! `RingBackend` wraps another backend (here `FilesystemBackend`) and encrypts every immutable sealed
//! object it stores. The nonce is derived from the plaintext (blake3), so encrypting the same
//! content-addressed object twice yields identical bytes — zsqlite's de-dup requires that — while the
//! (key, nonce) pair never repeats across distinct plaintexts. The active pagefile and the local
//! extracted-page cache stay plaintext (Android FBE covers them, as everywhere else in the app); only
//! the sealed objects are ring-encrypted.
//!
//! The key here is a fixed demo key. In the app it comes from the same Keystore-backed `EncryptedFile`
//! vault the IMAP credential uses (a random 32-byte key generated once, read out in Kotlin, passed to
//! native as bytes) — this crate is just the cipher.
//!
//! `main` opens a DB through the encrypting VFS, writes rows, seals to the backend, reopens and reads
//! them back (proving decrypt round-trips), then scans the sealed object files and asserts a known
//! plaintext marker never appears in them.

use std::ffi::{CStr, CString};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr::{null_mut, NonNull};
use std::sync::Arc;

use libsqlite3_sys as ffi;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use zsqlite::domain::{BackendId, FileOffset, StoredBytes, StoredRange};
use zsqlite::storage::adapter::{
    BackendError, DeletePermit, ObjectKey, ObjectRange, ObjectWriter, Publication, Revision,
    RootRecord,
};
use zsqlite::{FilesystemBackend, Storage, StorageBackend};

const TAG_LEN: usize = 16;
const OVERHEAD: u64 = (NONCE_LEN + TAG_LEN) as u64;

/// An encrypting decorator: every stored object is `nonce(12) || AES-256-GCM(plaintext) || tag(16)`.
struct RingBackend {
    inner: Arc<dyn StorageBackend>,
    key: LessSafeKey,
}

impl RingBackend {
    fn new(inner: Arc<dyn StorageBackend>, key_bytes: &[u8; 32]) -> Self {
        let unbound = UnboundKey::new(&AES_256_GCM, key_bytes).expect("valid AES-256 key");
        Self { inner, key: LessSafeKey::new(unbound) }
    }

    /// Deterministic nonce = blake3(plaintext)[..12], so the same object encrypts identically
    /// (idempotent, for content-addressed de-dup) while distinct plaintexts get distinct nonces.
    fn encrypt(&self, plain: &[u8]) -> Vec<u8> {
        let digest = blake3::hash(plain);
        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(&digest.as_bytes()[..NONCE_LEN]);
        let mut buf = plain.to_vec();
        self.key
            .seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce_bytes), Aad::empty(), &mut buf)
            .expect("seal");
        let mut out = Vec::with_capacity(NONCE_LEN + buf.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&buf);
        out
    }

    fn decrypt(&self, object: &[u8]) -> Result<Vec<u8>, BackendError> {
        if (object.len() as u64) < OVERHEAD {
            return Err(BackendError::InvalidData);
        }
        let (nonce, rest) = object.split_at(NONCE_LEN);
        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(nonce);
        let mut buf = rest.to_vec();
        let plain = self
            .key
            .open_in_place(Nonce::assume_unique_for_key(nonce_bytes), Aad::empty(), &mut buf)
            .map_err(|_| BackendError::InvalidData)?;
        Ok(plain.to_vec())
    }

    /// Fetch and decrypt a whole object. GCM is not seekable, so a range read decrypts the object and
    /// slices it — correct, at the cost of read amplification (a real impl would use a block cipher).
    fn read_full(&self, key: ObjectKey) -> Result<Option<Vec<u8>>, BackendError> {
        let physical = match self.inner.stat(key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        if physical.get() < OVERHEAD {
            return Err(BackendError::InvalidData);
        }
        let range = StoredRange::new(FileOffset::new(0), physical).map_err(|_| BackendError::Range)?;
        let object = self
            .inner
            .read_ranges(&[ObjectRange::new(key, range)?])?
            .into_iter()
            .next()
            .ok_or(BackendError::InvalidData)?;
        Ok(Some(self.decrypt(&object)?))
    }
}

/// Buffers the plaintext object, encrypts it whole on `finish`.
struct RingWriter<'a> {
    backend: &'a RingBackend,
    buffer: Vec<u8>,
}
impl std::io::Write for RingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl ObjectWriter for RingWriter<'_> {
    fn finish(self: Box<Self>, key: ObjectKey, length: StoredBytes) -> Result<(), BackendError> {
        if self.buffer.len() as u64 != length.get() {
            return Err(BackendError::Range);
        }
        let encrypted = self.backend.encrypt(&self.buffer);
        let encrypted_len = StoredBytes::new(encrypted.len() as u64);
        self.backend.inner.put(key, encrypted_len, &mut &encrypted[..])
    }
}

impl StorageBackend for RingBackend {
    fn identity(&self) -> BackendId {
        // Delegate so a DeletePermit minted against this backend authorizes on the inner one.
        self.inner.identity()
    }
    fn begin_write(&self) -> Result<Box<dyn ObjectWriter + '_>, BackendError> {
        Ok(Box::new(RingWriter { backend: self, buffer: Vec::new() }))
    }
    fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError> {
        requests
            .iter()
            .map(|request| {
                let plain = self
                    .read_full(request.key())?
                    .ok_or(BackendError::Missing(request.key()))?;
                let offset = request.range().offset().as_usize().map_err(|_| BackendError::Range)?;
                let length = request.range().length().as_usize().map_err(|_| BackendError::Range)?;
                let end = offset.checked_add(length).ok_or(BackendError::Range)?;
                if end > plain.len() {
                    return Err(BackendError::Range);
                }
                Ok(plain[offset..end].to_vec())
            })
            .collect()
    }
    fn stat(&self, key: ObjectKey) -> Result<Option<StoredBytes>, BackendError> {
        // Present the LOGICAL (plaintext) length zsqlite expects; the file on disk is 28 bytes larger.
        Ok(self.inner.stat(key)?.map(|b| StoredBytes::new(b.get().saturating_sub(OVERHEAD))))
    }
    fn read_root(&self) -> Result<Option<RootRecord>, BackendError> {
        match self.inner.read_root()? {
            None => Ok(None),
            Some(record) => {
                let plain = self.decrypt(record.bytes())?;
                let revision =
                    Revision::new(record.revision().bytes().to_vec()).map_err(|_| BackendError::InvalidData)?;
                RootRecord::new(revision, plain).map(Some).map_err(|_| BackendError::InvalidData)
            }
        }
    }
    fn compare_exchange_root(
        &self,
        expected: Option<&Revision>,
        bytes: &[u8],
    ) -> Result<Publication, BackendError> {
        let encrypted = self.encrypt(bytes);
        self.inner.compare_exchange_root(expected, &encrypted)
    }
    fn inventory(&self, after: Option<ObjectKey>, limit: usize) -> Result<Vec<ObjectKey>, BackendError> {
        self.inner.inventory(after, limit)
    }
    fn delete(&self, permit: DeletePermit<'_>) -> Result<(), BackendError> {
        self.inner.delete(permit)
    }
}

// ── a tiny raw-FFI SQLite connection, matching zsqlite's own test helpers ────────────────────────

struct Connection(NonNull<ffi::sqlite3>);
impl Connection {
    fn open(path: &Path, vfs: &str) -> Result<Self, String> {
        let cpath = CString::new(path.as_os_str().as_bytes()).map_err(|e| e.to_string())?;
        let cvfs = CString::new(vfs).map_err(|e| e.to_string())?;
        let mut raw = null_mut();
        // SAFETY: path/vfs C strings outlive the call; the out-slot is exclusive.
        let rc = unsafe {
            ffi::sqlite3_open_v2(
                cpath.as_ptr(),
                &raw mut raw,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                cvfs.as_ptr(),
            )
        };
        let handle = NonNull::new(raw).ok_or("sqlite3_open_v2 returned null")?;
        let connection = Self(handle);
        if rc != ffi::SQLITE_OK {
            return Err(format!("open rc={rc}"));
        }
        // SAFETY: live handle, synchronous config call.
        unsafe { ffi::sqlite3_busy_timeout(connection.0.as_ptr(), 2000) };
        Ok(connection)
    }
    fn execute(&self, sql: &str) -> Result<(), String> {
        let csql = CString::new(sql).map_err(|e| e.to_string())?;
        let mut error = null_mut();
        // SAFETY: live handle, terminated SQL; sqlite copies it and retains nothing of ours.
        let rc = unsafe {
            ffi::sqlite3_exec(self.0.as_ptr(), csql.as_ptr(), None, null_mut(), &raw mut error)
        };
        if rc == ffi::SQLITE_OK {
            return Ok(());
        }
        let message = if error.is_null() {
            format!("exec rc={rc}")
        } else {
            // SAFETY: sqlite returned a terminated error string; copy then free with its allocator.
            let m = unsafe { CStr::from_ptr(error) }.to_string_lossy().into_owned();
            unsafe { ffi::sqlite3_free(error.cast()) };
            m
        };
        Err(message)
    }
    fn integer(&self, sql: &str) -> Result<i64, String> {
        let csql = CString::new(sql).map_err(|e| e.to_string())?;
        let mut stmt = null_mut();
        // SAFETY: live handle, terminated SQL, exclusive out-slot.
        let rc = unsafe {
            ffi::sqlite3_prepare_v2(self.0.as_ptr(), csql.as_ptr(), -1, &raw mut stmt, null_mut())
        };
        if rc != ffi::SQLITE_OK {
            return Err(format!("prepare rc={rc}"));
        }
        // SAFETY: stmt is a live prepared statement owned here.
        let step = unsafe { ffi::sqlite3_step(stmt) };
        let value = if step == ffi::SQLITE_ROW {
            unsafe { ffi::sqlite3_column_int64(stmt, 0) }
        } else {
            -1
        };
        unsafe { ffi::sqlite3_finalize(stmt) };
        Ok(value)
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: sole owner; closes the handle exactly once.
        unsafe { ffi::sqlite3_close(self.0.as_ptr()) };
    }
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    needle.len() <= haystack.len() && haystack.windows(needle.len()).any(|w| w == needle)
}

fn main() {
    let base = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/data/local/tmp/zsqlite-ring".to_string());
    let base = Path::new(&base);
    let sealed = base.join("sealed");
    let coord = base.join("coord");
    let localdb = base.join("mail.zsqlite");
    // Fresh run.
    let _ = std::fs::remove_dir_all(base);
    std::fs::create_dir_all(&sealed).expect("mkdir sealed");
    std::fs::create_dir_all(&coord).expect("mkdir coord");

    // Demo key (production: the Keystore-backed EncryptedFile vault, passed from Kotlin).
    let key = [0x5Au8; 32];
    let marker = "SECRET_MARKER_reimbursement_A1B2C3D4";

    let backend = FilesystemBackend::open(&sealed).expect("filesystem backend");
    let ring = Arc::new(RingBackend::new(Arc::new(backend), &key));
    let storage = Storage::new(ring, &coord).expect("storage");
    storage.register_vfs("ringzs").expect("register vfs");

    // Write a small mailbox through the encrypting VFS, including a distinctive plaintext marker.
    {
        let c = Connection::open(&localdb, "ringzs").expect("open write");
        c.execute("PRAGMA journal_mode=WAL;").expect("wal");
        c.execute("CREATE TABLE mail(id INTEGER PRIMARY KEY, subject TEXT, body TEXT);").expect("create");
        c.execute(&format!(
            "INSERT INTO mail(subject, body) VALUES('quarterly', '{marker} the numbers are attached');"
        ))
        .expect("insert marker");
        c.execute(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<500) \
             INSERT INTO mail(subject, body) SELECT 'row'||x, 'filler body '||x FROM n;",
        )
        .expect("bulk insert");
    }

    // Seal to the (encrypting) backend, then reopen and read back — proving decrypt round-trips.
    storage.open(&localdb).expect("open storage").flush().expect("flush");
    let rows = {
        let c = Connection::open(&localdb, "ringzs").expect("reopen");
        c.integer("SELECT count(*) FROM mail;").expect("count")
    };
    let marker_row = {
        let c = Connection::open(&localdb, "ringzs").expect("reopen2");
        c.integer(&format!(
            "SELECT count(*) FROM mail WHERE body LIKE '%{}%';",
            &marker[..20]
        ))
        .expect("marker query")
    };

    // The plaintext marker must not appear in any sealed object on disk.
    let mut objects = Vec::new();
    walk(&sealed, &mut objects);
    let mut leaked = false;
    let mut object_count = 0usize;
    for path in &objects {
        if path.extension().is_some_and(|e| e == "identity") {
            continue;
        }
        object_count += 1;
        if let Ok(bytes) = std::fs::read(path) {
            if contains(&bytes, marker.as_bytes()) {
                leaked = true;
                println!("LEAK: plaintext marker found in {}", path.display());
            }
        }
    }

    println!("ROWS={rows} MARKER_ROWS={marker_row} SEALED_OBJECTS={object_count} MARKER_LEAKED={leaked}");
    let pass = rows == 501 && marker_row == 1 && !leaked && object_count > 0;
    println!("RESULT={}", if pass { "PASS ring-encrypted zsqlite VFS works on device" } else { "FAIL" });
    std::process::exit(if pass { 0 } else { 1 });
}
