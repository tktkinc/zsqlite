//! Process-local controls for conversion, sealing, and repack compression.

use crate::StoreError;
use std::num::NonZeroUsize;
use std::sync::{PoisonError, RwLock};

/// Scheduling priority of compression and dictionary-training workers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CompressionPriority {
    /// Inherit the spawning thread's OS priority.
    #[default]
    Inherit,
    /// Use background `QoS` on macOS, or a nice value of at least 10 on Linux
    /// and Android. The caller's priority is unaffected. Other platforms reject
    /// this setting rather than silently ignoring it.
    Background,
}

/// Runtime execution settings, separate from the persisted storage format.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompressionOptions {
    /// Maximum workers per compression batch. `None` uses the available CPUs;
    /// explicit limits are also capped by CPU availability and the work size.
    pub workers: Option<NonZeroUsize>,
    pub priority: CompressionPriority,
}

static OPTIONS: RwLock<CompressionOptions> = RwLock::new(CompressionOptions {
    workers: None,
    priority: CompressionPriority::Inherit,
});

/// Read the process-wide compression execution settings.
pub fn compression_options() -> CompressionOptions {
    *OPTIONS.read().unwrap_or_else(PoisonError::into_inner)
}

/// Change execution settings for all databases in this process, returning the
/// previous settings. A host can call this while conversion or background
/// sealing runs. Changes take effect at the next bounded frame batch or
/// dictionary-training operation; already-running work finishes at its current
/// priority. Fresh worker threads restore inherited priority without changing
/// existing worker niceness or foreground thread priority.
///
/// These settings are neither persisted nor shared with other processes.
/// `pause_conversion()` and `resume_conversion()` separately control durable,
/// per-database source conversion.
pub fn set_compression_options(
    options: CompressionOptions,
) -> Result<CompressionOptions, StoreError> {
    if options.priority == CompressionPriority::Background
        && !cfg!(any(
            target_os = "macos",
            target_os = "linux",
            target_os = "android"
        ))
    {
        return Err(StoreError::InvalidConfiguration(
            "background compression priority is unsupported on this platform",
        ));
    }
    let mut current = OPTIONS.write().unwrap_or_else(PoisonError::into_inner);
    Ok(std::mem::replace(&mut *current, options))
}

impl CompressionOptions {
    pub(crate) fn worker_count(self, jobs: usize) -> usize {
        // The browser build has no pthread support.
        let available = if cfg!(target_os = "emscripten") {
            1
        } else {
            std::thread::available_parallelism().map_or(1, NonZeroUsize::get)
        };
        self.workers
            .map_or(available, NonZeroUsize::get)
            .min(available)
            .min(jobs.max(1))
    }
}

/// Run background-priority training on a private thread, so lowering its
/// priority never changes the SQLite/host caller's priority.
pub(crate) fn run<T: Send>(
    work: impl FnOnce() -> Result<T, StoreError> + Send,
) -> Result<T, StoreError> {
    run_with_priority(compression_options().priority, work)
}

fn run_with_priority<T: Send>(
    priority: CompressionPriority,
    work: impl FnOnce() -> Result<T, StoreError> + Send,
) -> Result<T, StoreError> {
    if priority == CompressionPriority::Inherit {
        return work();
    }
    #[cfg(target_os = "emscripten")]
    {
        priority.apply()?;
        work()
    }
    #[cfg(not(target_os = "emscripten"))]
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("zsqlite-training".into())
            .spawn_scoped(scope, move || {
                priority.apply()?;
                work()
            })?
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

impl CompressionPriority {
    pub(crate) fn apply(self) -> Result<(), StoreError> {
        if self == Self::Inherit {
            return Ok(());
        }
        background_priority()
    }
}

#[cfg(target_os = "macos")]
fn background_priority() -> Result<(), StoreError> {
    // SAFETY: changes only the calling worker's QoS; valid class/relative priority.
    let error =
        unsafe { libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_BACKGROUND, 0) };
    if error != 0 {
        return Err(std::io::Error::from_raw_os_error(error).into());
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn background_priority() -> Result<(), StoreError> {
    // SAFETY: errno is thread-local; the pointer is valid for this thread.
    #[cfg(target_os = "linux")]
    let errno = unsafe { libc::__errno_location() };
    // SAFETY: Android's errno accessor returns this thread's valid errno pointer.
    #[cfg(target_os = "android")]
    let errno = unsafe { libc::__errno() };
    // SAFETY: valid thread-local errno pointer; PRIO_PROCESS/0 addresses only
    // this worker under Linux/NPTL. Never raise an inherited lower priority.
    unsafe {
        *errno = 0;
        let current = libc::getpriority(libc::PRIO_PROCESS, 0);
        if current == -1 && *errno != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if libc::setpriority(libc::PRIO_PROCESS, 0, current.max(10)) != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    Ok(())
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "android")))]
fn background_priority() -> Result<(), StoreError> {
    Err(StoreError::InvalidConfiguration(
        "background compression priority is unsupported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_limits_follow_cpu_availability_and_work_size() {
        let available = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        assert_eq!(CompressionOptions::default().worker_count(1), 1);
        assert_eq!(
            CompressionOptions::default().worker_count(usize::MAX),
            available
        );
        let single = CompressionOptions {
            workers: NonZeroUsize::new(1),
            ..CompressionOptions::default()
        };
        assert_eq!(single.worker_count(usize::MAX), 1);
        let excessive = CompressionOptions {
            workers: NonZeroUsize::new(usize::MAX),
            ..CompressionOptions::default()
        };
        assert_eq!(excessive.worker_count(usize::MAX), available);
    }

    #[cfg(target_os = "macos")]
    fn current_priority() -> i32 {
        let mut class = libc::qos_class_t::QOS_CLASS_UNSPECIFIED;
        // SAFETY: the current pthread is valid; class is a valid output pointer.
        let error = unsafe {
            libc::pthread_get_qos_class_np(
                libc::pthread_self(),
                &raw mut class,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(error, 0);
        class as i32
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn current_priority() -> i32 {
        // SAFETY: queries this thread's priority, retaining no pointers.
        unsafe { libc::getpriority(libc::PRIO_PROCESS, 0) }
    }

    #[test]
    #[cfg(any(target_os = "macos", target_os = "linux", target_os = "android"))]
    fn background_workers_leave_caller_priority_unchanged() -> Result<(), StoreError> {
        let before = current_priority();
        let caller = std::thread::current().id();
        let (worker_priority, worker) = run_with_priority(CompressionPriority::Background, || {
            Ok((current_priority(), std::thread::current().id()))
        })?;
        assert_ne!(worker, caller);
        #[cfg(target_os = "macos")]
        assert_eq!(
            worker_priority,
            libc::qos_class_t::QOS_CLASS_BACKGROUND as i32
        );
        #[cfg(any(target_os = "linux", target_os = "android"))]
        assert!(worker_priority >= 10 && worker_priority >= before);
        assert_eq!(current_priority(), before);
        // Restoring inherited priority requires no privileged priority increase.
        assert_eq!(
            run_with_priority(CompressionPriority::Inherit, || Ok(current_priority()))?,
            before
        );
        Ok(())
    }
}
