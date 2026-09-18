//! Cumulative snapshots with explicitly distinct statistics scopes.
use libsqlite3_sys as ffi;
use std::ffi::CStr;

pub const FILE_CONTROL_STATS_V1: i32 = 0x5a53_0101;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HandleIoStats {
    pub requested_bytes: u64,
    /// Source active/frame bytes fetched; excludes plaintext-cache file I/O.
    pub fetched_bytes: u64,
    pub inflated_bytes: u64,
    pub decode_nanoseconds: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DatabaseCacheStats {
    /// Bytes in occupied disk-cache slots (including alignment padding), not RAM/RSS.
    pub resident_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub extra_pages_requested: u64,
    pub extra_pages_evicted_unused: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectionCacheStats {
    pub used_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub writes: u64,
    pub spills: u64,
}

#[repr(C)]
pub(crate) struct StatsHeader {
    pub version: u32,
    pub size: u32,
}

/// V1 file-control wire buffer; all fields have a fixed C ABI.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FileControlStatsV1 {
    pub version: u32,
    pub size: u32,
    pub requested_bytes: u64,
    pub fetched_bytes: u64,
    pub inflated_bytes: u64,
    pub decode_nanoseconds: u64,
    pub handle_hits: u64,
    pub handle_misses: u64,
    /// Retained disk-cache slot bytes, not process resident memory.
    pub resident_bytes: u64,
    pub database_hits: u64,
    pub database_misses: u64,
    pub extra_pages_requested: u64,
    pub extra_pages_evicted_unused: u64,
}
impl Default for FileControlStatsV1 {
    fn default() -> Self {
        Self {
            version: 1,
            size: u32::try_from(std::mem::size_of::<Self>()).expect("small ABI struct"),
            requested_bytes: 0,
            fetched_bytes: 0,
            inflated_bytes: 0,
            decode_nanoseconds: 0,
            handle_hits: 0,
            handle_misses: 0,
            resident_bytes: 0,
            database_hits: 0,
            database_misses: 0,
            extra_pages_requested: 0,
            extra_pages_evicted_unused: 0,
        }
    }
}
impl FileControlStatsV1 {
    pub(crate) fn snapshot(handle: HandleIoStats, cache: DatabaseCacheStats) -> Self {
        Self {
            requested_bytes: handle.requested_bytes,
            fetched_bytes: handle.fetched_bytes,
            inflated_bytes: handle.inflated_bytes,
            decode_nanoseconds: handle.decode_nanoseconds,
            handle_hits: handle.cache_hits,
            handle_misses: handle.cache_misses,
            resident_bytes: cache.resident_bytes,
            database_hits: cache.hits,
            database_misses: cache.misses,
            extra_pages_requested: cache.extra_pages_requested,
            extra_pages_evicted_unused: cache.extra_pages_evicted_unused,
            ..Self::default()
        }
    }
    #[must_use]
    pub fn handle(self) -> HandleIoStats {
        HandleIoStats {
            requested_bytes: self.requested_bytes,
            fetched_bytes: self.fetched_bytes,
            inflated_bytes: self.inflated_bytes,
            decode_nanoseconds: self.decode_nanoseconds,
            cache_hits: self.handle_hits,
            cache_misses: self.handle_misses,
        }
    }
    #[must_use]
    pub fn database(self) -> DatabaseCacheStats {
        DatabaseCacheStats {
            resident_bytes: self.resident_bytes,
            hits: self.database_hits,
            misses: self.database_misses,
            extra_pages_requested: self.extra_pages_requested,
            extra_pages_evicted_unused: self.extra_pages_evicted_unused,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ConnectionStatistics {
    pub handle: HandleIoStats,
    pub database: DatabaseCacheStats,
    pub sqlite: ConnectionCacheStats,
}

/// Read VFS and `SQLite` pager counters without resetting either scope.
///
/// # Safety
/// `connection` must be a live `SQLite` connection exclusively available for the
/// duration of this call, using this build's `SQLite` ABI. No reference is retained.
pub unsafe fn connection_statistics(
    connection: *mut ffi::sqlite3,
    schema: &CStr,
) -> Result<ConnectionStatistics, i32> {
    if connection.is_null() {
        return Err(ffi::SQLITE_MISUSE);
    }
    let mut wire = FileControlStatsV1::default();
    // SAFETY: The caller guarantees a live, exclusive connection with this ABI.
    // schema is a terminated C string; wire is the initialized, correctly sized
    // V1 output required by our custom opcode and lives through the call.
    let result = unsafe {
        ffi::sqlite3_file_control(
            connection,
            schema.as_ptr(),
            FILE_CONTROL_STATS_V1,
            (&raw mut wire).cast(),
        )
    };
    if result != ffi::SQLITE_OK {
        return Err(result);
    }
    let counter = |operation| {
        let mut current = 0;
        let mut highwater = 0;
        // SAFETY: The caller retains exclusive access to the live connection;
        // db_status writes only the two aligned c_int locals for this call.
        let result = unsafe {
            ffi::sqlite3_db_status(
                connection,
                operation,
                &raw mut current,
                &raw mut highwater,
                0,
            )
        };
        if result == ffi::SQLITE_OK {
            Ok(u64::try_from(current).unwrap_or(0))
        } else {
            Err(result)
        }
    };
    Ok(ConnectionStatistics {
        handle: wire.handle(),
        database: wire.database(),
        sqlite: ConnectionCacheStats {
            used_bytes: counter(ffi::SQLITE_DBSTATUS_CACHE_USED)?,
            hits: counter(ffi::SQLITE_DBSTATUS_CACHE_HIT)?,
            misses: counter(ffi::SQLITE_DBSTATUS_CACHE_MISS)?,
            writes: counter(ffi::SQLITE_DBSTATUS_CACHE_WRITE)?,
            spills: counter(ffi::SQLITE_DBSTATUS_CACHE_SPILL)?,
        },
    })
}
