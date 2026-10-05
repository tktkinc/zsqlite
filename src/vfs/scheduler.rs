//! One process-wide thread runs each open writable database's background work
//! at the deadline that database last reported. Commits, seals and reloads
//! re-arm a database through its notifier; with nothing due, nothing wakes.
#![forbid(unsafe_code)]
use crate::store::{SealWork, Store};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, PoisonError, TryLockError, Weak};
use std::time::{Duration, Instant};

/// A store busy with a `SQLite` callback is retried no sooner than this.
const RETRY: Duration = Duration::from_secs(1);

#[derive(Default)]
struct Queue {
    due: BTreeSet<(Instant, u64)>,
    stores: HashMap<u64, (Instant, Weak<Mutex<Store>>)>,
    worker: bool,
}

struct Scheduler {
    queue: Mutex<Queue>,
    wake: Condvar,
}

static SCHEDULER: OnceLock<Scheduler> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn scheduler() -> &'static Scheduler {
    SCHEDULER.get_or_init(|| Scheduler {
        queue: Mutex::new(Queue::default()),
        wake: Condvar::new(),
    })
}

/// Keep `store`'s background deadline scheduled for as long as it is open.
/// The caller must not hold the store's lock.
pub(super) fn register(store: &Arc<Mutex<Store>>) -> Result<(), crate::StoreError> {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let weak = Arc::downgrade(store);
    store
        .lock()
        .map_err(|_| crate::StoreError::Range)?
        .set_maintenance_notifier(Box::new(move |deadline| arm(id, deadline, &weak)));
    Ok(())
}

/// Replace a store's deadline. Stores call this with their own lock held, so
/// the worker never holds the queue while it waits for a store.
fn arm(id: u64, deadline: Option<Instant>, store: &Weak<Mutex<Store>>) {
    let scheduler = scheduler();
    let mut queue = scheduler
        .queue
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some((previous, _)) = queue.stores.remove(&id) {
        queue.due.remove(&(previous, id));
    }
    let Some(deadline) = deadline else {
        return;
    };
    let earliest = queue.due.first().is_none_or(|(first, _)| deadline < *first);
    queue.due.insert((deadline, id));
    queue.stores.insert(id, (deadline, Weak::clone(store)));
    if !queue.worker {
        queue.worker = std::thread::Builder::new()
            .name("zsqlite-maintenance".into())
            .spawn(run)
            .is_ok();
    } else if earliest {
        scheduler.wake.notify_one();
    }
}

fn run() {
    let scheduler = scheduler();
    let lock = || {
        scheduler
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    };
    let mut queue = lock();
    loop {
        let Some(&(deadline, id)) = queue.due.first() else {
            queue = scheduler
                .wake
                .wait(queue)
                .unwrap_or_else(PoisonError::into_inner);
            continue;
        };
        let now = Instant::now();
        if deadline > now {
            queue = scheduler
                .wake
                .wait_timeout(queue, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
            continue;
        }
        queue.due.remove(&(deadline, id));
        let Some((_, store)) = queue.stores.remove(&id) else {
            continue;
        };
        drop(queue);
        if let Some(opened) = store.upgrade() {
            let preparation = match opened.try_lock() {
                Ok(mut database) => database.prepare_background_work(),
                Err(TryLockError::WouldBlock) => {
                    arm(id, Some(Instant::now() + RETRY), &store);
                    queue = lock();
                    continue;
                }
                Err(TryLockError::Poisoned(_)) => {
                    queue = lock();
                    continue;
                }
            };
            // Compression/training owns immutable input, with no Store mutex
            // or publication/catalogue guard held by this thread.
            let prepared = preparation.and_then(|work| work.map(SealWork::prepare).transpose());
            if let Ok(mut database) = opened.lock() {
                let _ = database.complete_background_work(prepared);
            }
        }
        // The last handle may have dropped above; Store::drop re-enters `arm`,
        // so the queue is relocked only afterwards.
        queue = lock();
    }
}
