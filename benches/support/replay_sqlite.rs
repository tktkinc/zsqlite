//! Shared owned `SQLite` adapter for benchmarks. Statement borrows cannot
//! outlive their connection; column buffers are consumed before the next step.
use libsqlite3_sys as ffi;
use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};
use std::ptr::{null, null_mut};
use std::time::Instant;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub type Rows = Vec<Vec<(i32, Vec<u8>)>>;

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

pub fn write_values(path: &Path, values: &[Value]) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create_new(path)?;
    file.write_all(b"ZSQLPAR1")?;
    file.write_all(&u32::try_from(values.len())?.to_le_bytes())?;
    for value in values {
        match value {
            Value::Null => file.write_all(&[0])?,
            Value::Integer(value) => {
                file.write_all(&[1])?;
                file.write_all(&value.to_le_bytes())?;
            }
            Value::Real(value) => {
                file.write_all(&[2])?;
                file.write_all(&value.to_bits().to_le_bytes())?;
            }
            Value::Text(value) => {
                file.write_all(&[3])?;
                file.write_all(&u64::try_from(value.len())?.to_le_bytes())?;
                file.write_all(value)?;
            }
            Value::Blob(value) => {
                file.write_all(&[4])?;
                file.write_all(&u64::try_from(value.len())?.to_le_bytes())?;
                file.write_all(value)?;
            }
        }
    }
    Ok(())
}

pub fn read_values(path: &Path) -> Result<Vec<Value>> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != b"ZSQLPAR1" {
        return Err("invalid SQL parameter file".into());
    }
    let mut count = [0; 4];
    file.read_exact(&mut count)?;
    let count = u32::from_le_bytes(count);
    if count > 64 {
        return Err("too many SQL parameters".into());
    }
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mut tag = [0];
        file.read_exact(&mut tag)?;
        let value = match tag[0] {
            0 => Value::Null,
            1 | 2 => {
                let mut bytes = [0; 8];
                file.read_exact(&mut bytes)?;
                if tag[0] == 1 {
                    Value::Integer(i64::from_le_bytes(bytes))
                } else {
                    Value::Real(f64::from_bits(u64::from_le_bytes(bytes)))
                }
            }
            3 | 4 => {
                let mut length = [0; 8];
                file.read_exact(&mut length)?;
                let length = usize::try_from(u64::from_le_bytes(length))?;
                if length > 64 * 1024 * 1024 {
                    return Err("SQL parameter is too large".into());
                }
                let mut bytes = vec![0; length];
                file.read_exact(&mut bytes)?;
                if tag[0] == 3 {
                    Value::Text(bytes)
                } else {
                    Value::Blob(bytes)
                }
            }
            _ => return Err("invalid SQL parameter type".into()),
        };
        values.push(value);
    }
    let mut trailing = [0];
    if file.read(&mut trailing)? != 0 {
        return Err("trailing SQL parameter data".into());
    }
    Ok(values)
}

pub struct Connection(*mut ffi::sqlite3);
impl Connection {
    pub fn open(path: &Path, managed: bool, readonly: bool) -> Result<Self> {
        if readonly && !path.is_file() {
            return Err(format!("missing snapshot: {}", path.display()).into());
        }
        let name = CString::new(if readonly && !managed {
            uri(path).into_bytes()
        } else {
            path.as_os_str().as_encoded_bytes().to_vec()
        })?;
        let mut raw = null_mut();
        let flags = ffi::SQLITE_OPEN_URI
            | if readonly {
                ffi::SQLITE_OPEN_READONLY
            } else {
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE
            };
        // SAFETY: The path/VFS C strings outlive this call and the output slot is
        // exclusive. SQLite initializes the owned handle even when opening fails.
        let code = unsafe {
            ffi::sqlite3_open_v2(
                name.as_ptr(),
                &raw mut raw,
                flags,
                if managed { c"zsqlite".as_ptr() } else { null() },
            )
        };
        let result = Self(raw);
        result.check(code)?;
        result.execute("PRAGMA mmap_size=0; PRAGMA busy_timeout=10000;")?;
        Ok(result)
    }
    fn check(&self, code: i32) -> Result<()> {
        if code == ffi::SQLITE_OK {
            return Ok(());
        }
        if self.0.is_null() {
            return Err(format!("SQLite open error {code}").into());
        }
        Err(format!(
            "SQLite {code}: {}",
            // SAFETY: The owned connection is live and accessed on this thread only.
            // SQLite returns a terminated message, copied before another SQLite call.
            unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(self.0)) }.to_string_lossy()
        )
        .into())
    }
    pub fn execute(&self, sql: &str) -> Result<()> {
        let sql = CString::new(sql)?;
        // SAFETY: The owned connection is live on this thread and sql is a
        // terminated CString. Any error output is an exclusive local pointer slot;
        // SQLite copies SQL and retains no Rust callback or buffer.
        self.check(unsafe { ffi::sqlite3_exec(self.0, sql.as_ptr(), None, null_mut(), null_mut()) })
    }
    pub fn rows(&self, sql: &str) -> Result<Vec<Vec<String>>> {
        let mut statement = self.prepare(sql)?;
        let mut rows = Vec::new();
        while statement.next()? {
            let mut row = Vec::new();
            for column in 0..statement.columns() {
                row.push(std::str::from_utf8(statement.bytes(column)?)?.to_owned());
            }
            rows.push(row);
        }
        Ok(rows)
    }
    /// Materialize typed results; callers may fingerprint them outside timing.
    pub fn query(&self, sql: &str) -> Result<Rows> {
        let mut statement = self.prepare(sql)?;
        let mut rows = Vec::new();
        while statement.next()? {
            let mut row = Vec::new();
            for column in 0..statement.columns() {
                // SAFETY: The statement is positioned on SQLITE_ROW and column is
                // within its result columns; it has not been stepped or finalized.
                let kind = unsafe { ffi::sqlite3_column_type(statement.raw, column) };
                row.push((kind, statement.bytes(column)?.to_vec()));
            }
            rows.push(row);
        }
        Ok(rows)
    }
    pub fn values(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let mut statement = self.prepare(sql)?;
        let mut rows = Vec::new();
        while statement.next()? {
            let mut row = Vec::new();
            for column in 0..statement.columns() {
                row.push(statement.value(column)?);
            }
            rows.push(row);
        }
        Ok(rows)
    }
    pub fn query_params(&self, sql: &str, parameters: &[Value]) -> Result<Rows> {
        let mut statement = self.prepare(sql)?;
        statement.bind(parameters)?;
        let mut rows = Vec::new();
        while statement.next()? {
            let mut row = Vec::new();
            for column in 0..statement.columns() {
                // SAFETY: The statement is positioned on SQLITE_ROW and column is
                // within its result columns; it has not been stepped or finalized.
                let kind = unsafe { ffi::sqlite3_column_type(statement.raw, column) };
                row.push((kind, statement.bytes(column)?.to_vec()));
            }
            rows.push(row);
        }
        Ok(rows)
    }
    pub fn execute_params(&self, sql: &str, parameters: &[Value]) -> Result<()> {
        let mut statement = self.prepare(sql)?;
        statement.bind(parameters)?;
        while statement.next()? {}
        Ok(())
    }
    pub fn execute_params_changes(&self, sql: &str, parameters: &[Value]) -> Result<u64> {
        self.execute_params(sql, parameters)?;
        // SAFETY: The owned connection is live and exclusive to this thread;
        // the call returns a scalar and retains no Rust memory.
        Ok(u64::try_from(unsafe { ffi::sqlite3_changes64(self.0) })?)
    }
    pub fn replay(
        &self,
        pages: &[u32],
        size: u32,
    ) -> Result<(Vec<u64>, zsqlite::statistics::ConnectionStatistics)> {
        self.execute("PRAGMA cache_size=-2048; BEGIN; SELECT count(*) FROM sqlite_schema;")?;
        let mut file: *mut ffi::sqlite3_file = null_mut();
        // SAFETY: The connection and schema string remain live on this thread.
        // The output allocation has the type and size required by this file-control
        // opcode and remains exclusively accessible during the synchronous call.
        let code = unsafe {
            ffi::sqlite3_file_control(
                self.0,
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                (&raw mut file).cast(),
            )
        };
        if code != ffi::SQLITE_OK || file.is_null() {
            return Err("no main-file handle".into());
        }
        // SAFETY: FILE_POINTER returned this live main file inside an active
        // transaction; its method table remains valid until connection close.
        let read = unsafe { (*(*file).pMethods).xRead }.ok_or("no xRead")?;
        // SAFETY: This thread exclusively uses the live connection, linked
        // against the same SQLite ABI; the schema is a static terminated string.
        let before = unsafe { zsqlite::statistics::connection_statistics(self.0, c"main") }
            .map_err(|code| format!("stats: {code}"))?;
        let mut buffer = vec![0; size as usize];
        let mut latencies = Vec::with_capacity(pages.len());
        for page in pages {
            let offset = i64::from(page - 1) * i64::from(size);
            let start = Instant::now();
            // SAFETY: The active transaction retains the file and its method
            // table. buffer owns size initialized writable bytes and this thread is the
            // sole user of the connection during the synchronous xRead call.
            let code = unsafe {
                read(
                    file,
                    buffer.as_mut_ptr().cast(),
                    i32::try_from(size)?,
                    offset,
                )
            };
            latencies.push(u64::try_from(start.elapsed().as_nanos())?);
            if code != ffi::SQLITE_OK {
                return Err(format!("page {page}: {code}").into());
            }
            std::hint::black_box(&buffer);
        }
        // SAFETY: This thread exclusively uses the live connection, linked
        // against the same SQLite ABI; the schema is a static terminated string.
        let mut after = unsafe { zsqlite::statistics::connection_statistics(self.0, c"main") }
            .map_err(|code| format!("stats: {code}"))?;
        after.handle.requested_bytes -= before.handle.requested_bytes;
        after.handle.fetched_bytes -= before.handle.fetched_bytes;
        after.handle.inflated_bytes -= before.handle.inflated_bytes;
        after.handle.decode_nanoseconds -= before.handle.decode_nanoseconds;
        after.handle.cache_hits -= before.handle.cache_hits;
        after.handle.cache_misses -= before.handle.cache_misses;
        after.database.extra_pages_requested -= before.database.extra_pages_requested;
        after.database.extra_pages_evicted_unused -= before.database.extra_pages_evicted_unused;
        self.execute("ROLLBACK")?;
        Ok((latencies, after))
    }
    pub fn scalar(&self, sql: &str) -> Result<i64> {
        let rows = self.rows(sql)?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .ok_or("missing scalar")?
            .parse()?)
    }
    pub fn scalar_params(&self, sql: &str, parameters: &[Value]) -> Result<i64> {
        let rows = self.query_params(sql, parameters)?;
        Ok(std::str::from_utf8(
            &rows
                .first()
                .and_then(|row| row.first())
                .ok_or("missing scalar")?
                .1,
        )?
        .parse()?)
    }
    pub fn fingerprint(&self, sql: &str) -> Result<Fingerprint> {
        let mut statement = self.prepare(sql)?;
        Self::fingerprint_statement(&mut statement)
    }
    pub fn fingerprint_params(&self, sql: &str, parameters: &[Value]) -> Result<Fingerprint> {
        let mut statement = self.prepare(sql)?;
        statement.bind(parameters)?;
        Self::fingerprint_statement(&mut statement)
    }
    fn fingerprint_statement(statement: &mut Statement<'_>) -> Result<Fingerprint> {
        let mut hash = blake3::Hasher::new();
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        while statement.next()? {
            rows += 1;
            hash.update(&statement.columns().to_le_bytes());
            for column in 0..statement.columns() {
                // SAFETY: The statement is positioned on SQLITE_ROW and column is
                // within its result columns; it has not been stepped or finalized.
                let kind = unsafe { ffi::sqlite3_column_type(statement.raw, column) };
                let value = statement.bytes(column)?;
                hash.update(&kind.to_le_bytes());
                hash.update(&(value.len() as u64).to_le_bytes());
                hash.update(value);
                bytes += value.len() as u64;
            }
        }
        Ok(Fingerprint {
            hash: hash.finalize(),
            rows,
            bytes,
        })
    }
    fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
        let sql = CString::new(sql)?;
        let mut raw = null_mut();
        let code =
            // SAFETY: The connection and terminated SQL string remain live. SQLite
            // writes the statement to an exclusive local slot; its owner finalizes it
            // before the connection is closed.
            unsafe { ffi::sqlite3_prepare_v2(self.0, sql.as_ptr(), -1, &raw mut raw, null_mut()) };
        let statement = Statement {
            connection: self,
            raw,
        };
        self.check(code)?;
        if statement.raw.is_null() {
            return Err("empty SQL".into());
        }
        Ok(statement)
    }
    pub fn stats(&self, managed: bool) -> Result<Stats> {
        if managed {
            // SAFETY: This thread exclusively uses the live connection, linked
            // against the same SQLite ABI; the schema is a static terminated string.
            let stats = unsafe { zsqlite::statistics::connection_statistics(self.0, c"main") }
                .map_err(|code| format!("VFS statistics: {code}"))?;
            return Ok(Stats {
                io: stats.handle,
                cache: stats.database,
                sqlite: stats.sqlite,
            });
        }
        let counter = |operation| -> Result<u64> {
            let mut value = 0;
            let mut high = 0;
            // SAFETY: The connection is live on this thread; both output counters
            // are aligned, exclusive c_int locals valid for the synchronous call.
            self.check(unsafe {
                ffi::sqlite3_db_status(self.0, operation, &raw mut value, &raw mut high, 0)
            })?;
            Ok(u64::try_from(value)?)
        };
        Ok(Stats {
            sqlite: zsqlite::statistics::ConnectionCacheStats {
                hits: counter(ffi::SQLITE_DBSTATUS_CACHE_HIT)?,
                misses: counter(ffi::SQLITE_DBSTATUS_CACHE_MISS)?,
                used_bytes: counter(ffi::SQLITE_DBSTATUS_CACHE_USED)?,
                writes: counter(ffi::SQLITE_DBSTATUS_CACHE_WRITE)?,
                spills: counter(ffi::SQLITE_DBSTATUS_CACHE_SPILL)?,
            },
            ..Stats::default()
        })
    }
    pub fn checkpoint(&self) -> Result<()> {
        let row = self.rows("PRAGMA wal_checkpoint(TRUNCATE)")?;
        if row.first().is_none_or(|row| row != &["0", "0", "0"]) {
            return Err(format!("incomplete checkpoint: {row:?}").into());
        }
        Ok(())
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: This owner consumes its live SQLite connection exactly once;
            // no statement or buffer is used after the close.
            unsafe {
                ffi::sqlite3_close(self.0);
            }
        }
    }
}
struct Statement<'connection> {
    connection: &'connection Connection,
    raw: *mut ffi::sqlite3_stmt,
}
impl Statement<'_> {
    fn bind(&mut self, values: &[Value]) -> Result<()> {
        // SAFETY: The prepared statement and its connection remain live;
        // this metadata query is synchronous and does not retain any Rust pointers.
        if usize::try_from(unsafe { ffi::sqlite3_bind_parameter_count(self.raw) })? != values.len()
        {
            return Err("parameter count mismatch".into());
        }
        for (offset, value) in values.iter().enumerate() {
            let index = i32::try_from(offset + 1)?;
            // SAFETY: The statement is live and parameter indices were bounded by
            // bind_parameter_count. Scalar values are copied; text/blob lengths match
            // their slices and SQLITE_TRANSIENT makes SQLite copy bytes before returning.
            let code = unsafe {
                match value {
                    Value::Null => ffi::sqlite3_bind_null(self.raw, index),
                    Value::Integer(value) => ffi::sqlite3_bind_int64(self.raw, index, *value),
                    Value::Real(value) => ffi::sqlite3_bind_double(self.raw, index, *value),
                    Value::Text(value) => ffi::sqlite3_bind_text64(
                        self.raw,
                        index,
                        value.as_ptr().cast(),
                        value.len() as u64,
                        ffi::SQLITE_TRANSIENT(),
                        u8::try_from(ffi::SQLITE_UTF8)?,
                    ),
                    Value::Blob(value) => ffi::sqlite3_bind_blob64(
                        self.raw,
                        index,
                        value.as_ptr().cast(),
                        value.len() as u64,
                        ffi::SQLITE_TRANSIENT(),
                    ),
                }
            };
            self.connection.check(code)?;
        }
        Ok(())
    }
    fn next(&mut self) -> Result<bool> {
        // SAFETY: The prepared statement and its connection remain live and
        // exclusive to this thread; prior column borrows have ended before stepping.
        match unsafe { ffi::sqlite3_step(self.raw) } {
            ffi::SQLITE_ROW => Ok(true),
            ffi::SQLITE_DONE => Ok(false),
            code => {
                self.connection.check(code)?;
                unreachable!()
            }
        }
    }
    fn columns(&self) -> i32 {
        // SAFETY: The prepared statement remains live; querying its column
        // count borrows it synchronously and retains no Rust memory.
        unsafe { ffi::sqlite3_column_count(self.raw) }
    }
    fn bytes(&self, column: i32) -> Result<&[u8]> {
        // SAFETY: The statement is on a live row and the column is in range.
        // SQLite owns the returned bytes, consumed before conversion, step or finalize
        // can invalidate them; null/empty results are checked by the buffer reader.
        let pointer = unsafe { ffi::sqlite3_column_blob(self.raw, column) };
        // SAFETY: The statement is live on its current row and the column is
        // in range; the resulting byte length is consumed before stepping/finalizing.
        let length = usize::try_from(unsafe { ffi::sqlite3_column_bytes(self.raw, column) })?;
        if length == 0 {
            return Ok(&[]);
        }
        if pointer.is_null() {
            return Err("SQLite column allocation failed".into());
        }
        // SAFETY: The caller obtained pointer and length from the same live
        // SQLite column, checked null/empty cases, and consumes these bytes before
        // stepping, conversion or finalization can invalidate the allocation.
        Ok(unsafe { std::slice::from_raw_parts(pointer.cast(), length) })
    }
    fn value(&self, column: i32) -> Result<Value> {
        Ok(
            // SAFETY: The statement is positioned on SQLITE_ROW and column is
            // within its result columns; it has not been stepped or finalized.
            match unsafe { ffi::sqlite3_column_type(self.raw, column) } {
                ffi::SQLITE_NULL => Value::Null,
                ffi::SQLITE_INTEGER => {
                    // SAFETY: The live statement is positioned on SQLITE_ROW with this
                    // column in range. SQLite returns the scalar by value without retaining data.
                    Value::Integer(unsafe { ffi::sqlite3_column_int64(self.raw, column) })
                }
                ffi::SQLITE_FLOAT => {
                    // SAFETY: The live statement is positioned on SQLITE_ROW with this
                    // column in range. SQLite returns the scalar by value without retaining data.
                    Value::Real(unsafe { ffi::sqlite3_column_double(self.raw, column) })
                }
                ffi::SQLITE_TEXT => Value::Text(self.bytes(column)?.to_vec()),
                ffi::SQLITE_BLOB => Value::Blob(self.bytes(column)?.to_vec()),
                _ => return Err("unknown SQLite value type".into()),
            },
        )
    }
}
impl Drop for Statement<'_> {
    fn drop(&mut self) {
        // SAFETY: This owner releases the prepared statement exactly once,
        // while its connection is still live and all column borrows have ended.
        unsafe {
            ffi::sqlite3_finalize(self.raw);
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fingerprint {
    pub hash: blake3::Hash,
    pub rows: u64,
    pub bytes: u64,
}
#[derive(Default)]
pub struct Stats {
    pub io: zsqlite::statistics::HandleIoStats,
    pub cache: zsqlite::statistics::DatabaseCacheStats,
    pub sqlite: zsqlite::statistics::ConnectionCacheStats,
}
pub fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
pub fn uri(path: &Path) -> String {
    let mut uri = String::from("file:");
    for byte in path.as_os_str().as_encoded_bytes() {
        if byte.is_ascii_alphanumeric() || b"/-_.".contains(byte) {
            uri.push(char::from(*byte));
        } else {
            use std::fmt::Write;
            write!(uri, "%{byte:02X}").expect("string formatting");
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    uri
}
pub fn native_vfs() -> Result<String> {
    // SAFETY: SQLite is initialized; the optional VFS name is terminated.
    // The test/benchmark retains registrations while using the returned pointer.
    let vfs = unsafe { ffi::sqlite3_vfs_find(null()) };
    if vfs.is_null() {
        return Err("no native VFS".into());
    }
    // SAFETY: The registered VFS remains live for this call and owns a
    // terminated name, which is copied before releasing the registration borrow.
    Ok(unsafe { CStr::from_ptr((*vfs).zName) }.to_str()?.to_owned())
}

/// Process-lifetime high-water mark, not an isolated per-profile allocation.
pub fn peak_rss_bytes() -> Option<u64> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: usage is an aligned writable rusage allocation. getrusage
        // initializes it on success and retains no pointer into Rust memory.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return None;
        }
        // SAFETY: The immediately preceding getrusage call succeeded and
        // initialized every field of this aligned rusage output allocation.
        let usage = unsafe { usage.assume_init() };
        let value = u64::try_from(usage.ru_maxrss).ok()?;
        #[cfg(target_os = "linux")]
        {
            value.checked_mul(1024)
        }
        #[cfg(target_os = "macos")]
        {
            Some(value)
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

pub fn fingerprint_rows(rows: &Rows) -> (blake3::Hash, usize) {
    let mut hash = blake3::Hasher::new();
    let mut bytes = 0;
    for row in rows {
        hash.update(&(row.len() as u64).to_le_bytes());
        for (kind, value) in row {
            hash.update(&kind.to_le_bytes());
            hash.update(&(value.len() as u64).to_le_bytes());
            hash.update(value);
            bytes += value.len();
        }
    }
    (hash.finalize(), bytes)
}

pub fn sidecar(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".d");
    name.into()
}
pub fn tree_bytes(path: &Path) -> Result<u64> {
    let metadata = match path.metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_dir() {
        return Ok(metadata.len());
    }
    std::fs::read_dir(path)?.try_fold(0_u64, |total, entry| {
        total
            .checked_add(tree_bytes(&entry?.path())?)
            .ok_or_else(|| "size overflow".into())
    })
}

/// Bytes attributable to the database on disk, excluding the advisory
/// dictionary-training reservoir. The reservoir is not referenced by a sealed
/// view and is not required to open, verify, or query the database.
pub fn bundle_bytes(path: &Path) -> Result<u64> {
    let sidecar = sidecar(path);
    let mut bytes = tree_bytes(path)?
        .checked_add(tree_bytes(&sidecar)?)
        .ok_or("size overflow")?;
    bytes = bytes
        .checked_sub(tree_bytes(&sidecar.join("dictionary.samples"))?)
        .ok_or("size underflow")?;
    for suffix in ["-wal", "-shm", "-journal"] {
        bytes = bytes
            .checked_add(tree_bytes(&PathBuf::from(format!(
                "{}{suffix}",
                path.display()
            )))?)
            .ok_or("size overflow")?;
    }
    Ok(bytes)
}
