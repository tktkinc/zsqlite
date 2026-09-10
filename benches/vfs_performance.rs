#[cfg(not(feature = "static"))]
fn main() {
    eprintln!(
        "vfs_performance requires the statically linked SQLite feature; run:\n\
         cargo bench --no-default-features --features static --bench vfs_performance"
    );
    std::process::exit(2);
}

#[cfg(feature = "static")]
fn main() {
    if let Err(error) = benchmark::run() {
        eprintln!("vfs_performance: {error}");
        std::process::exit(1);
    }
}

#[cfg(feature = "static")]
mod benchmark {
    use libsqlite3_sys as ffi;
    use std::error::Error;
    use std::ffi::{CStr, CString, c_int};
    use std::fmt::Write as _;
    use std::path::{Path, PathBuf};
    use std::ptr::null_mut;
    use std::time::{Duration, Instant};

    const ZSQLITE_VFS: &CStr = c"zsqlite";
    const PHASE_COUNT: usize = 9;
    type Result<T> = std::result::Result<T, Box<dyn Error>>;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Engine {
        Native,
        Zsqlite,
    }

    impl Engine {
        const fn name(self) -> &'static str {
            match self {
                Self::Native => "native",
                Self::Zsqlite => "zsqlite",
            }
        }
    }

    #[derive(Clone, Copy)]
    enum Phase {
        InsertCommit,
        InitialCheckpoint,
        CloseAfterLoad,
        Reopen,
        PointRead,
        UpdateCommit,
        UpdateCheckpoint,
        Scan,
        FinalClose,
    }

    impl Phase {
        const ALL: [Self; PHASE_COUNT] = [
            Self::InsertCommit,
            Self::InitialCheckpoint,
            Self::CloseAfterLoad,
            Self::Reopen,
            Self::PointRead,
            Self::UpdateCommit,
            Self::UpdateCheckpoint,
            Self::Scan,
            Self::FinalClose,
        ];

        const fn index(self) -> usize {
            self as usize
        }

        const fn name(self) -> &'static str {
            match self {
                Self::InsertCommit => "insert + commit",
                Self::InitialCheckpoint => "initial checkpoint",
                Self::CloseAfterLoad => "close after load",
                Self::Reopen => "reopen",
                Self::PointRead => "random point read",
                Self::UpdateCommit => "update + commit",
                Self::UpdateCheckpoint => "update checkpoint",
                Self::Scan => "aggregate scan",
                Self::FinalClose => "final close",
            }
        }

        const fn operations(self, config: &Config) -> Option<usize> {
            match self {
                Self::InsertCommit => Some(config.rows),
                Self::PointRead => Some(config.reads),
                Self::UpdateCommit => Some(config.updates),
                _ => None,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct Config {
        rows: usize,
        reads: usize,
        updates: usize,
        batch_size: usize,
        payload_bytes: usize,
        samples: usize,
        page_size: usize,
        pressure_cache_mib: usize,
        resident_cache_mib: usize,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                rows: 20_000,
                reads: 50_000,
                updates: 10_000,
                batch_size: 100,
                payload_bytes: 1024,
                samples: 5,
                page_size: 4096,
                pressure_cache_mib: 8,
                resident_cache_mib: 64,
            }
        }
    }

    impl Config {
        fn parse() -> std::result::Result<Option<Self>, String> {
            let mut config = Self::default();
            let mut arguments = std::env::args().skip(1);
            while let Some(argument) = arguments.next() {
                if argument == "--help" || argument == "-h" {
                    println!("{}", usage());
                    return Ok(None);
                }
                // Cargo appends this libtest-compatible marker even when the
                // benchmark declares `harness = false`.
                if argument == "--bench" {
                    continue;
                }
                if argument == "--quick" {
                    config.rows = 2_000;
                    config.reads = 5_000;
                    config.updates = 1_000;
                    config.samples = 1;
                    config.pressure_cache_mib = 2;
                    continue;
                }
                let target = match argument.as_str() {
                    "--rows" => &mut config.rows,
                    "--reads" => &mut config.reads,
                    "--updates" => &mut config.updates,
                    "--batch-size" => &mut config.batch_size,
                    "--payload-bytes" => &mut config.payload_bytes,
                    "--samples" => &mut config.samples,
                    "--page-size" => &mut config.page_size,
                    "--pressure-cache-mib" => &mut config.pressure_cache_mib,
                    "--resident-cache-mib" => &mut config.resident_cache_mib,
                    _ => return Err(format!("unknown option {argument:?}\n\n{}", usage())),
                };
                let value = arguments
                    .next()
                    .ok_or_else(|| format!("{argument} requires a value"))?;
                *target = value
                    .parse()
                    .map_err(|_| format!("invalid value {value:?} for {argument}"))?;
            }
            config.validate()?;
            Ok(Some(config))
        }

        fn validate(&self) -> std::result::Result<(), String> {
            if self.rows == 0
                || self.reads == 0
                || self.updates == 0
                || self.batch_size == 0
                || self.payload_bytes == 0
                || self.samples == 0
                || self.pressure_cache_mib == 0
                || self.resident_cache_mib == 0
            {
                return Err(
                    "row, operation, batch, payload, and sample counts must be nonzero".into(),
                );
            }
            if i64::try_from(self.rows).is_err() {
                return Err("--rows is too large for SQLite integer keys".into());
            }
            if !matches!(
                self.page_size,
                512 | 1024 | 2048 | 4096 | 8192 | 16_384 | 32_768 | 65_536
            ) {
                return Err(
                    "--page-size must be a supported SQLite page size from 512 to 65536".into(),
                );
            }
            for cache_mib in [self.pressure_cache_mib, self.resident_cache_mib] {
                let cache_kib = cache_mib
                    .checked_mul(1024)
                    .ok_or("SQLite cache size is too large")?;
                i64::try_from(cache_kib).map_err(|_| "SQLite cache size is too large")?;
            }
            Ok(())
        }

        const fn cache_profiles(&self) -> [CacheProfile; 2] {
            [
                CacheProfile {
                    name: "pressure",
                    mebibytes: self.pressure_cache_mib,
                },
                CacheProfile {
                    name: "cache-resident",
                    mebibytes: self.resident_cache_mib,
                },
            ]
        }
    }

    #[derive(Clone, Copy)]
    struct CacheProfile {
        name: &'static str,
        mebibytes: usize,
    }

    impl CacheProfile {
        fn kibibytes(self) -> Result<usize> {
            self.mebibytes
                .checked_mul(1024)
                .ok_or_else(|| "SQLite cache size is too large".into())
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct Validation {
        rows: i64,
        revision_sum: i64,
        payload_sum: i64,
        read_checksum: u64,
    }

    #[derive(Clone, Copy)]
    struct Storage {
        logical: u64,
        allocated: u64,
        segment_bytes: u64,
        segments: u64,
    }

    #[derive(Clone, Copy)]
    struct CacheStats {
        used: u64,
        hits: u64,
        misses: u64,
        writes: u64,
        spills: u64,
    }

    struct Sample {
        timings: [Duration; PHASE_COUNT],
        storage: Storage,
        cache: CacheStats,
        validation: Validation,
    }

    pub fn run() -> Result<()> {
        let Some(config) = Config::parse()? else {
            return Ok(());
        };
        zsqlite::register_static_vfs()
            .map_err(|code| format!("zsqlite VFS registration failed with SQLite code {code}"))?;
        let directory = tempfile::tempdir()?;

        for profile in config.cache_profiles() {
            let (native, compressed) = run_profile(directory.path(), profile, &config)?;
            print_report(&config, profile, &native, &compressed);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
        Ok(())
    }

    fn run_profile(
        directory: &Path,
        profile: CacheProfile,
        config: &Config,
    ) -> Result<(Vec<Sample>, Vec<Sample>)> {
        let mut native = Vec::with_capacity(config.samples);
        let mut compressed = Vec::with_capacity(config.samples);

        eprintln!(
            "running {} samples per VFS ({} profile: {} MiB SQLite cache, WAL, synchronous=FULL, page_size={})",
            config.samples, profile.name, profile.mebibytes, config.page_size
        );
        for sample_index in 0..config.samples {
            let order = if sample_index % 2 == 0 {
                [Engine::Native, Engine::Zsqlite]
            } else {
                [Engine::Zsqlite, Engine::Native]
            };
            let mut native_sample = None;
            let mut compressed_sample = None;
            for engine in order {
                eprintln!(
                    "  sample {}/{}: {}",
                    sample_index + 1,
                    config.samples,
                    engine.name()
                );
                let path = directory.join(format!(
                    "{}-{}-{sample_index}.zsqlite",
                    profile.name,
                    engine.name()
                ));
                let sample = run_sample(&path, engine, profile, config)?;
                match engine {
                    Engine::Native => native_sample = Some(sample),
                    Engine::Zsqlite => compressed_sample = Some(sample),
                }
            }
            let native_sample = native_sample.ok_or("native sample was not recorded")?;
            let compressed_sample = compressed_sample.ok_or("zsqlite sample was not recorded")?;
            if native_sample.validation != compressed_sample.validation {
                return Err(format!(
                    "{} cache sample {} produced different results: native={:?}, zsqlite={:?}",
                    profile.name,
                    sample_index + 1,
                    native_sample.validation,
                    compressed_sample.validation
                )
                .into());
            }
            native.push(native_sample);
            compressed.push(compressed_sample);
        }
        Ok((native, compressed))
    }

    #[allow(clippy::too_many_lines)]
    fn run_sample(
        path: &Path,
        engine: Engine,
        profile: CacheProfile,
        config: &Config,
    ) -> Result<Sample> {
        let mut timings = [Duration::ZERO; PHASE_COUNT];
        let cache_kib = profile.kibibytes()?;
        let connection = Connection::open(path, engine, config)?;
        connection.execute(&format!(
            "PRAGMA page_size={};
             PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA wal_autocheckpoint=0;
             PRAGMA cache_size=-{cache_kib};
             CREATE TABLE records(
               id INTEGER PRIMARY KEY,
               revision INTEGER NOT NULL,
               body BLOB NOT NULL
             );
             CREATE INDEX records_revision ON records(revision);",
            config.page_size
        ))?;

        let payload = payload(config.payload_bytes);
        let mut insert =
            connection.prepare("INSERT INTO records(id, revision, body) VALUES(?1, ?2, ?3)")?;
        insert.bind_blob(3, &payload)?;
        let started = Instant::now();
        for row in 0..config.rows {
            if row % config.batch_size == 0 {
                connection.execute("BEGIN IMMEDIATE")?;
            }
            let id = to_sql_integer(row + 1)?;
            insert.bind_integer(1, id)?;
            insert.bind_integer(2, id % 997)?;
            insert.execute()?;
            if row % config.batch_size == config.batch_size - 1 || row + 1 == config.rows {
                connection.execute("COMMIT")?;
            }
        }
        timings[Phase::InsertCommit.index()] = started.elapsed();
        drop(insert);

        timings[Phase::InitialCheckpoint.index()] =
            measure(|| connection.execute("PRAGMA wal_checkpoint(TRUNCATE)"))?;
        timings[Phase::CloseAfterLoad.index()] = measure(|| connection.close())?;
        if engine == Engine::Zsqlite {
            // Seal the active segment so reopen and random reads exercise
            // immutable per-page frames.
            zsqlite::flush(path)?;
        }

        let started = Instant::now();
        let connection = Connection::open(path, engine, config)?;
        timings[Phase::Reopen.index()] = started.elapsed();
        connection.execute(&format!(
            "PRAGMA cache_size=-{cache_kib}; PRAGMA wal_autocheckpoint=0"
        ))?;
        connection.warm_page_cache()?;
        connection.reset_cache_counters()?;

        let mut select =
            connection.prepare("SELECT revision, length(body) FROM records WHERE id=?1")?;
        let mut random = XorShift64(0x4d59_5df4_d0f3_3173);
        let mut read_checksum = 0_u64;
        let started = Instant::now();
        let row_count = u64::try_from(config.rows)?;
        for _ in 0..config.reads {
            let id = random.next() % row_count + 1;
            select.bind_integer(1, i64::try_from(id)?)?;
            let (revision, length) = select.query_pair()?;
            read_checksum = read_checksum
                .wrapping_mul(1_099_511_628_211)
                .wrapping_add(id)
                .wrapping_add(revision.cast_unsigned())
                .wrapping_add(length.cast_unsigned());
        }
        timings[Phase::PointRead.index()] = started.elapsed();
        drop(select);

        let mut update =
            connection.prepare("UPDATE records SET revision=revision+1 WHERE id=?1")?;
        let started = Instant::now();
        for operation in 0..config.updates {
            if operation % config.batch_size == 0 {
                connection.execute("BEGIN IMMEDIATE")?;
            }
            let id = operation % config.rows + 1;
            update.bind_integer(1, to_sql_integer(id)?)?;
            update.execute()?;
            if operation % config.batch_size == config.batch_size - 1
                || operation + 1 == config.updates
            {
                connection.execute("COMMIT")?;
            }
        }
        timings[Phase::UpdateCommit.index()] = started.elapsed();
        drop(update);

        timings[Phase::UpdateCheckpoint.index()] =
            measure(|| connection.execute("PRAGMA wal_checkpoint(TRUNCATE)"))?;
        let started = Instant::now();
        let (rows, revision_sum, payload_sum) = connection.aggregate()?;
        timings[Phase::Scan.index()] = started.elapsed();
        let cache = connection.cache_stats()?;
        timings[Phase::FinalClose.index()] = measure(|| connection.close())?;
        if engine == Engine::Zsqlite {
            zsqlite::flush(path)?;
        }

        Ok(Sample {
            timings,
            storage: storage(path, engine)?,
            cache,
            validation: Validation {
                rows,
                revision_sum,
                payload_sum,
                read_checksum,
            },
        })
    }

    #[allow(clippy::cast_precision_loss)]
    fn print_report(
        config: &Config,
        profile: CacheProfile,
        native: &[Sample],
        compressed: &[Sample],
    ) {
        println!();
        println!(
            "zsqlite VFS performance comparison (median of {} samples)",
            config.samples
        );
        println!(
            "cache profile={} ({} MiB); rows={}, reads={}, updates={}, batch={}, payload={} B, page_size={}, WAL, synchronous=FULL",
            profile.name,
            profile.mebibytes,
            config.rows,
            config.reads,
            config.updates,
            config.batch_size,
            config.payload_bytes,
            config.page_size
        );
        println!("random point reads begin after a sequential table-cache warmup");
        println!();
        println!(
            "{:<22} {:>12} {:>12} {:>11} {:>14} {:>14}",
            "phase", "native ms", "zsqlite ms", "z/native", "native ops/s", "zsqlite ops/s"
        );
        println!("{}", "-".repeat(91));
        for phase in Phase::ALL {
            let native_time = median_duration(native, phase);
            let compressed_time = median_duration(compressed, phase);
            let ratio = compressed_time.as_secs_f64() / native_time.as_secs_f64();
            let (native_rate, compressed_rate) = phase.operations(config).map_or_else(
                || ("-".into(), "-".into()),
                |operations| {
                    (
                        rate(operations, native_time),
                        rate(operations, compressed_time),
                    )
                },
            );
            println!(
                "{:<22} {:>12.3} {:>12.3} {:>10.2}x {:>14} {:>14}",
                phase.name(),
                milliseconds(native_time),
                milliseconds(compressed_time),
                ratio,
                native_rate,
                compressed_rate
            );
        }

        let native_logical = median_storage(native, |value| value.logical);
        let compressed_logical = median_storage(compressed, |value| value.logical);
        let native_allocated = median_storage(native, |value| value.allocated);
        let compressed_allocated = median_storage(compressed, |value| value.allocated);
        println!();
        println!(
            "storage after final close: native={} logical / {} allocated; zsqlite={} logical / {} allocated",
            bytes(native_logical),
            bytes(native_allocated),
            bytes(compressed_logical),
            bytes(compressed_allocated)
        );
        println!(
            "storage ratios (z/native): logical={:.3}x, allocated={:.3}x",
            compressed_logical as f64 / native_logical as f64,
            compressed_allocated as f64 / native_allocated as f64
        );
        let segment_bytes = median_storage(compressed, |value| value.segment_bytes);
        let segments = median_storage(compressed, |value| value.segments);
        let average = if segments == 0 {
            0
        } else {
            segment_bytes / segments
        };
        println!(
            "zsqlite segment bytes: {} across {} segments, {} average",
            bytes(segment_bytes),
            segments,
            bytes(average)
        );
        let native_cache = median_cache_stats(native);
        let compressed_cache = median_cache_stats(compressed);
        println!(
            "SQLite page cache after warmup: native={} used, {} hits / {} misses ({:.2}% hit), {} writes, {} spills",
            bytes(native_cache.used),
            native_cache.hits,
            native_cache.misses,
            hit_rate(native_cache),
            native_cache.writes,
            native_cache.spills
        );
        println!(
            "SQLite page cache after warmup: zsqlite={} used, {} hits / {} misses ({:.2}% hit), {} writes, {} spills",
            bytes(compressed_cache.used),
            compressed_cache.hits,
            compressed_cache.misses,
            hit_rate(compressed_cache),
            compressed_cache.writes,
            compressed_cache.spills
        );
        println!(
            "Lower timing and storage ratios are better. Results are not CI pass/fail thresholds."
        );
    }

    fn median_duration(samples: &[Sample], phase: Phase) -> Duration {
        let mut values: Vec<_> = samples
            .iter()
            .map(|sample| sample.timings[phase.index()])
            .collect();
        values.sort_unstable();
        values[values.len() / 2]
    }

    fn median_storage(samples: &[Sample], select: impl Fn(Storage) -> u64) -> u64 {
        let mut values: Vec<_> = samples
            .iter()
            .map(|sample| select(sample.storage))
            .collect();
        values.sort_unstable();
        values[values.len() / 2]
    }

    fn median_cache_stats(samples: &[Sample]) -> CacheStats {
        let median = |select: fn(CacheStats) -> u64| {
            let mut values = samples
                .iter()
                .map(|sample| select(sample.cache))
                .collect::<Vec<_>>();
            values.sort_unstable();
            values[values.len() / 2]
        };
        CacheStats {
            used: median(|stats| stats.used),
            hits: median(|stats| stats.hits),
            misses: median(|stats| stats.misses),
            writes: median(|stats| stats.writes),
            spills: median(|stats| stats.spills),
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn hit_rate(stats: CacheStats) -> f64 {
        let accesses = stats.hits.saturating_add(stats.misses);
        if accesses == 0 {
            0.0
        } else {
            stats.hits as f64 * 100.0 / accesses as f64
        }
    }

    fn milliseconds(duration: Duration) -> f64 {
        duration.as_secs_f64() * 1000.0
    }

    #[allow(clippy::cast_precision_loss)]
    fn rate(operations: usize, duration: Duration) -> String {
        format!("{:.0}", operations as f64 / duration.as_secs_f64())
    }

    #[allow(clippy::cast_precision_loss)]
    fn bytes(value: u64) -> String {
        const KIB: f64 = 1024.0;
        const MIB: f64 = 1024.0 * KIB;
        const GIB: f64 = 1024.0 * MIB;
        let mut output = String::new();
        if value as f64 >= GIB {
            let _ = write!(output, "{:.2} GiB", value as f64 / GIB);
        } else if value as f64 >= MIB {
            let _ = write!(output, "{:.2} MiB", value as f64 / MIB);
        } else if value as f64 >= KIB {
            let _ = write!(output, "{:.2} KiB", value as f64 / KIB);
        } else {
            let _ = write!(output, "{value} B");
        }
        output
    }

    fn payload(length: usize) -> Vec<u8> {
        let template = br#"{"kind":"vfs-benchmark","body":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tag":"repeatable"}"#;
        template.iter().copied().cycle().take(length).collect()
    }

    fn to_sql_integer(value: usize) -> Result<i64> {
        Ok(i64::try_from(value)?)
    }

    fn measure(operation: impl FnOnce() -> Result<()>) -> Result<Duration> {
        let started = Instant::now();
        operation()?;
        Ok(started.elapsed())
    }

    fn storage(path: &Path, engine: Engine) -> Result<Storage> {
        let paths = vec![
            path.to_path_buf(),
            suffix(path, "-wal"),
            suffix(path, "-shm"),
        ];
        let mut logical = 0_u64;
        let mut allocated = 0_u64;
        for candidate in paths {
            match candidate.metadata() {
                Ok(metadata) => {
                    logical = logical.saturating_add(metadata.len());
                    allocated = allocated.saturating_add(allocated_bytes(&metadata));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        let (segment_bytes, segments) = if engine == Engine::Zsqlite {
            let info = zsqlite::inspect(path)?;
            // The ordinary path walk already counted the active `.zsqlite`
            // file. Only sealed segment files live in the sidecar.
            logical = logical.saturating_add(info.segment_bytes);
            allocated = allocated.saturating_add(info.segment_allocated_bytes);
            (
                info.segment_bytes.saturating_add(info.file_bytes),
                u64::try_from(info.sealed_segments)? + u64::from(info.active),
            )
        } else {
            (0, 0)
        };
        Ok(Storage {
            logical,
            allocated,
            segment_bytes,
            segments,
        })
    }

    fn suffix(path: &Path, value: &str) -> PathBuf {
        let mut result = path.as_os_str().to_os_string();
        result.push(value);
        PathBuf::from(result)
    }

    #[cfg(unix)]
    fn allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
        use std::os::unix::fs::MetadataExt as _;
        metadata.blocks().saturating_mul(512)
    }

    fn usage() -> &'static str {
        "Usage: cargo bench --no-default-features --features static --bench vfs_performance -- [OPTIONS]\n\
         \n\
         Options:\n\
           --quick            one small smoke-test sample per cache profile\n\
           --rows N           inserted rows per sample (default: 20000)\n\
           --reads N          random point reads per sample (default: 50000)\n\
           --updates N        updated rows per sample (default: 10000)\n\
           --batch-size N     rows per transaction (default: 100)\n\
           --payload-bytes N  compressible BLOB bytes per row (default: 1024)\n\
           --samples N        fresh databases measured per VFS (default: 5)\n\
           --page-size N      SQLite page size (default: 4096)\n\
           --pressure-cache-mib N\n\
                              constrained-cache profile size (default: 8)\n\
           --resident-cache-mib N\n\
                              cache-resident profile size (default: 64)\n\
           -h, --help         show this help"
    }

    struct Connection {
        raw: *mut ffi::sqlite3,
    }

    impl Connection {
        fn open(path: &Path, engine: Engine, _config: &Config) -> Result<Self> {
            let filename = path.to_string_lossy().into_owned();
            let path = CString::new(filename)?;
            let mut raw = null_mut();
            let vfs = match engine {
                Engine::Native => std::ptr::null(),
                Engine::Zsqlite => ZSQLITE_VFS.as_ptr(),
            };
            let flags = ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_URI;
            let code = unsafe { ffi::sqlite3_open_v2(path.as_ptr(), &raw mut raw, flags, vfs) };
            if code != ffi::SQLITE_OK {
                let message = sqlite_message(raw);
                if !raw.is_null() {
                    unsafe { ffi::sqlite3_close(raw) };
                }
                return Err(format!(
                    "opening {} failed ({code}): {message}",
                    path.to_string_lossy()
                )
                .into());
            }
            let connection = Self { raw };
            let code = unsafe { ffi::sqlite3_busy_timeout(connection.raw, 30_000) };
            connection.check(code, "setting busy timeout")?;
            Ok(connection)
        }

        fn execute(&self, sql: &str) -> Result<()> {
            let sql = CString::new(sql)?;
            let mut error = null_mut();
            let code = unsafe {
                ffi::sqlite3_exec(self.raw, sql.as_ptr(), None, null_mut(), &raw mut error)
            };
            if code == ffi::SQLITE_OK {
                return Ok(());
            }
            let message = if error.is_null() {
                sqlite_message(self.raw)
            } else {
                let message = unsafe { CStr::from_ptr(error) }
                    .to_string_lossy()
                    .into_owned();
                unsafe { ffi::sqlite3_free(error.cast()) };
                message
            };
            Err(format!("executing SQL failed ({code}): {message}").into())
        }

        fn prepare<'connection>(&'connection self, sql: &str) -> Result<Statement<'connection>> {
            let sql = CString::new(sql)?;
            let mut raw = null_mut();
            let code = unsafe {
                ffi::sqlite3_prepare_v2(self.raw, sql.as_ptr(), -1, &raw mut raw, null_mut())
            };
            self.check(code, "preparing statement")?;
            Ok(Statement {
                connection: self,
                raw,
            })
        }

        fn aggregate(&self) -> Result<(i64, i64, i64)> {
            let mut statement =
                self.prepare("SELECT count(*), sum(revision), sum(length(body)) FROM records")?;
            let code = unsafe { ffi::sqlite3_step(statement.raw) };
            if code != ffi::SQLITE_ROW {
                return Err(self.error(code, "reading aggregate"));
            }
            let values = unsafe {
                (
                    ffi::sqlite3_column_int64(statement.raw, 0),
                    ffi::sqlite3_column_int64(statement.raw, 1),
                    ffi::sqlite3_column_int64(statement.raw, 2),
                )
            };
            statement.expect_done()?;
            Ok(values)
        }

        fn warm_page_cache(&self) -> Result<()> {
            let mut statement =
                self.prepare("SELECT sum(length(body)) FROM records NOT INDEXED")?;
            let code = unsafe { ffi::sqlite3_step(statement.raw) };
            if code != ffi::SQLITE_ROW {
                return Err(self.error(code, "warming SQLite page cache"));
            }
            let payload_bytes = unsafe { ffi::sqlite3_column_int64(statement.raw, 0) };
            if payload_bytes <= 0 {
                return Err("page-cache warmup returned an invalid payload size".into());
            }
            statement.expect_done()?;
            Ok(())
        }

        fn reset_cache_counters(&self) -> Result<()> {
            for operation in [
                ffi::SQLITE_DBSTATUS_CACHE_HIT,
                ffi::SQLITE_DBSTATUS_CACHE_MISS,
                ffi::SQLITE_DBSTATUS_CACHE_WRITE,
                ffi::SQLITE_DBSTATUS_CACHE_SPILL,
            ] {
                let _ = self.cache_status(operation, true)?;
            }
            Ok(())
        }

        fn cache_stats(&self) -> Result<CacheStats> {
            Ok(CacheStats {
                used: self.cache_status(ffi::SQLITE_DBSTATUS_CACHE_USED, false)?,
                hits: self.cache_status(ffi::SQLITE_DBSTATUS_CACHE_HIT, false)?,
                misses: self.cache_status(ffi::SQLITE_DBSTATUS_CACHE_MISS, false)?,
                writes: self.cache_status(ffi::SQLITE_DBSTATUS_CACHE_WRITE, false)?,
                spills: self.cache_status(ffi::SQLITE_DBSTATUS_CACHE_SPILL, false)?,
            })
        }

        fn cache_status(&self, operation: c_int, reset: bool) -> Result<u64> {
            let mut current = 0;
            let mut highwater = 0;
            let code = unsafe {
                ffi::sqlite3_db_status(
                    self.raw,
                    operation,
                    &raw mut current,
                    &raw mut highwater,
                    c_int::from(reset),
                )
            };
            self.check(code, "reading SQLite page-cache status")?;
            Ok(u64::try_from(current).unwrap_or(0))
        }

        fn close(mut self) -> Result<()> {
            let code = unsafe { ffi::sqlite3_close(self.raw) };
            if code != ffi::SQLITE_OK {
                return Err(self.error(code, "closing database"));
            }
            self.raw = null_mut();
            Ok(())
        }

        fn check(&self, code: c_int, operation: &str) -> Result<()> {
            if code == ffi::SQLITE_OK {
                Ok(())
            } else {
                Err(self.error(code, operation))
            }
        }

        fn error(&self, code: c_int, operation: &str) -> Box<dyn Error> {
            format!("{operation} failed ({code}): {}", sqlite_message(self.raw)).into()
        }
    }

    impl Drop for Connection {
        fn drop(&mut self) {
            if !self.raw.is_null() {
                let code = unsafe { ffi::sqlite3_close(self.raw) };
                assert_eq!(code, ffi::SQLITE_OK, "closing benchmark database failed");
            }
        }
    }

    struct Statement<'connection> {
        connection: &'connection Connection,
        raw: *mut ffi::sqlite3_stmt,
    }

    impl Statement<'_> {
        fn bind_integer(&mut self, index: c_int, value: i64) -> Result<()> {
            let code = unsafe { ffi::sqlite3_bind_int64(self.raw, index, value) };
            self.connection.check(code, "binding integer")
        }

        fn bind_blob(&mut self, index: c_int, value: &[u8]) -> Result<()> {
            let code = unsafe {
                ffi::sqlite3_bind_blob64(
                    self.raw,
                    index,
                    value.as_ptr().cast(),
                    value.len() as u64,
                    ffi::SQLITE_STATIC(),
                )
            };
            self.connection.check(code, "binding blob")
        }

        fn execute(&mut self) -> Result<()> {
            let code = unsafe { ffi::sqlite3_step(self.raw) };
            if code != ffi::SQLITE_DONE {
                return Err(self.connection.error(code, "executing statement"));
            }
            self.reset()
        }

        fn query_pair(&mut self) -> Result<(i64, i64)> {
            let code = unsafe { ffi::sqlite3_step(self.raw) };
            if code != ffi::SQLITE_ROW {
                return Err(self.connection.error(code, "reading point query"));
            }
            let values = unsafe {
                (
                    ffi::sqlite3_column_int64(self.raw, 0),
                    ffi::sqlite3_column_int64(self.raw, 1),
                )
            };
            self.expect_done()?;
            self.reset()?;
            Ok(values)
        }

        fn expect_done(&mut self) -> Result<()> {
            let code = unsafe { ffi::sqlite3_step(self.raw) };
            if code == ffi::SQLITE_DONE {
                Ok(())
            } else {
                Err(self.connection.error(code, "finishing query"))
            }
        }

        fn reset(&mut self) -> Result<()> {
            let code = unsafe { ffi::sqlite3_reset(self.raw) };
            self.connection.check(code, "resetting statement")
        }
    }

    impl Drop for Statement<'_> {
        fn drop(&mut self) {
            let code = unsafe { ffi::sqlite3_finalize(self.raw) };
            assert_eq!(
                code,
                ffi::SQLITE_OK,
                "finalizing benchmark statement failed"
            );
        }
    }

    fn sqlite_message(database: *mut ffi::sqlite3) -> String {
        if database.is_null() {
            return "SQLite did not return a database handle".into();
        }
        unsafe { CStr::from_ptr(ffi::sqlite3_errmsg(database)) }
            .to_string_lossy()
            .into_owned()
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
}
