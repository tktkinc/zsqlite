//! Failures inside streamed object creation, including a lost finish response.
//! The ordinary `FaultBackend` fails before `begin_write`; these cases exercise
//! partially written streams and completely installed, unacknowledged objects.
use super::adapter::{
    BackendError, DeletePermit, ObjectKey, ObjectRange, ObjectWriter, Publication, Revision,
    RootRecord, StorageBackend,
};
use super::tests::{TestResult, page};
use super::{FaultBackend, MemoryBackend, Storage};
use crate::StoragePolicy;
use crate::domain::{BackendId, DecodedBytes, StoredBytes};
use crate::layout::LayoutPolicy;
use crate::store::Store;
use std::io::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
enum Failure {
    WriteAfter(usize),
    BeforeFinish,
    AfterFinish,
}

#[derive(Default)]
struct State {
    injection: Option<(usize, Failure)>,
    started: usize,
    finished: Vec<(usize, ObjectKey, StoredBytes)>,
    fired: bool,
    short_writes: bool,
    shortened: usize,
}

struct StreamBackend {
    inner: FaultBackend,
    state: Mutex<State>,
}

impl StreamBackend {
    fn reset(&self, injection: Option<(usize, Failure)>, short_writes: bool) {
        *self.state.lock().unwrap() = State {
            injection,
            short_writes,
            ..State::default()
        };
        self.inner.reset_statistics();
    }
}

struct StreamWriter<'a> {
    backend: &'a StreamBackend,
    inner: Box<dyn ObjectWriter + 'a>,
    failure: Option<Failure>,
    written: usize,
    short_writes: bool,
    call: usize,
}

impl Write for StreamWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut length = bytes.len();
        if let Some(Failure::WriteAfter(limit)) = self.failure {
            if self.written >= limit && !bytes.is_empty() {
                self.backend.state.lock().unwrap().fired = true;
                return Err(std::io::Error::other("injected mid-stream failure"));
            }
            length = length.min(limit.saturating_sub(self.written));
        }
        if self.short_writes && length > 7 {
            length = 7;
            self.backend.state.lock().unwrap().shortened += 1;
        }
        let count = self.inner.write(&bytes[..length])?;
        self.written += count;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl ObjectWriter for StreamWriter<'_> {
    fn finish(self: Box<Self>, key: ObjectKey, length: StoredBytes) -> Result<(), BackendError> {
        let Self {
            backend,
            inner,
            failure,
            call,
            ..
        } = *self;
        if matches!(failure, Some(Failure::BeforeFinish)) {
            backend.state.lock().unwrap().fired = true;
            return Err(std::io::Error::other("injected before durable installation").into());
        }
        inner.finish(key, length)?;
        backend
            .state
            .lock()
            .unwrap()
            .finished
            .push((call, key, length));
        if matches!(failure, Some(Failure::AfterFinish)) {
            backend.state.lock().unwrap().fired = true;
            return Err(BackendError::Transport {
                message: "lost response after durable object installation".into(),
                retryable: true,
            });
        }
        Ok(())
    }
}

impl StorageBackend for StreamBackend {
    fn identity(&self) -> BackendId {
        self.inner.identity()
    }

    fn begin_write(&self) -> Result<Box<dyn ObjectWriter + '_>, BackendError> {
        let mut state = self.state.lock().unwrap();
        state.started += 1;
        let failure = state
            .injection
            .filter(|(call, _)| *call == state.started)
            .map(|(_, failure)| failure);
        Ok(Box::new(StreamWriter {
            backend: self,
            inner: self.inner.begin_write()?,
            failure,
            written: 0,
            short_writes: state.short_writes,
            call: state.started,
        }))
    }

    fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError> {
        self.inner.read_ranges(requests)
    }

    fn stat(&self, key: ObjectKey) -> Result<Option<StoredBytes>, BackendError> {
        self.inner.stat(key)
    }

    fn read_root(&self) -> Result<Option<RootRecord>, BackendError> {
        self.inner.read_root()
    }

    fn compare_exchange_root(
        &self,
        expected: Option<&Revision>,
        bytes: &[u8],
    ) -> Result<Publication, BackendError> {
        self.inner.compare_exchange_root(expected, bytes)
    }

    fn inventory(
        &self,
        after: Option<ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectKey>, BackendError> {
        self.inner.inventory(after, limit)
    }

    fn delete(&self, permit: DeletePermit<'_>) -> Result<(), BackendError> {
        self.inner.delete(permit)
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    backend: Arc<StreamBackend>,
    store: Store,
    expected: Vec<u8>,
}

fn fixture(partial_frames: bool) -> Result<Fixture, Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let backend = Arc::new(StreamBackend {
        inner: FaultBackend::new(Arc::new(MemoryBackend::new()?)),
        state: Mutex::new(State::default()),
    });
    let storage = Storage::new(backend.clone(), directory.path().join("coord"))?;
    let path = storage.bind(&directory.path().join("stream.zsqlite"))?;
    let mut store = Store::open(&path, true)?;
    let mut layout =
        LayoutPolicy::default().with_maintenance(DecodedBytes::new(64 * 1024 * 1024), 0)?;
    if partial_frames {
        layout = layout.fixed(DecodedBytes::new(4 * 4096))?;
    }
    store.set_storage_policy(StoragePolicy::default().with_layout(layout))?;
    let mut expected = Vec::new();
    for base in [1, 5, 9] {
        let bytes = (base..base + 4).map(page).collect::<Vec<_>>().concat();
        store.write_at(expected.len() as u64, &bytes)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        expected.extend(bytes);
    }
    for pack in 0..3 {
        let bytes = (0..3)
            .map(|offset| page(100 + pack * 4 + offset))
            .collect::<Vec<_>>()
            .concat();
        let offset = usize::from(pack) * 4 * 4096;
        store.write_at(offset as u64, &bytes)?;
        expected[offset..offset + bytes.len()].copy_from_slice(&bytes);
    }
    store.publish(true)?;
    store.flush_sidecars()?;
    // Separate replacement frames into multiple output objects; failures in
    // later objects must also preserve the previously acknowledged endpoint.
    store.set_storage_policy(
        StoragePolicy::default().with_layout(layout.with_pack_target(StoredBytes::new(1))?),
    )?;
    backend.reset(None, false);
    Ok(Fixture {
        directory,
        backend,
        store,
        expected,
    })
}

fn assert_contents(store: &mut Store, expected: &[u8]) -> TestResult {
    let mut actual = vec![0; expected.len()];
    assert_eq!(store.read_at(0, &mut actual)?, expected.len());
    assert_eq!(actual, expected);
    store.verify()?;
    Ok(())
}

#[test]
fn every_streamed_gc_object_survives_partial_writes_and_lost_finish_responses() -> TestResult {
    for partial_frames in [false, true] {
        let mut baseline = fixture(partial_frames)?;
        assert_eq!(baseline.store.repack_once()?.repacked_packs, 3);
        let mut objects = baseline.backend.state.lock().unwrap().finished.clone();
        let started = baseline.backend.state.lock().unwrap().started;
        // Writers may overlap: the manifest writer starts before a payload
        // writer and finishes after it. Match faults by start ordinal.
        objects.sort_unstable_by_key(|(call, _, _)| *call);
        assert!(
            objects
                .iter()
                .any(|(_, key, _)| matches!(key, ObjectKey::Blob(_)))
        );
        assert!(
            objects
                .iter()
                .any(|(_, key, _)| matches!(key, ObjectKey::Manifest(_)))
        );
        assert!(
            objects
                .iter()
                .any(|(_, key, _)| matches!(key, ObjectKey::Index(_)))
        );
        drop(baseline);
        for call in 1..=started {
            let mut failures = vec![Failure::WriteAfter(1)];
            if let Some((_, _, length)) = objects.iter().find(|(start, _, _)| *start == call) {
                failures.extend([
                    Failure::WriteAfter(usize::try_from(length.get() / 2)?),
                    Failure::BeforeFinish,
                    Failure::AfterFinish,
                ]);
            }
            // Empty trailing pack writers are abandoned, but their writes can
            // still fail after earlier objects were installed. Include them.
            for failure in failures {
                let mut fixture = fixture(partial_frames)?;
                let before = fixture.store.inspect()?;
                fixture.backend.reset(Some((call, failure)), false);
                let result = fixture.store.repack_once();
                assert!(
                    fixture.backend.state.lock().unwrap().fired,
                    "unreached fault: partial={partial_frames}, object={call}, {failure:?}"
                );
                assert!(result.is_err(), "object={call}, {failure:?}");
                assert_eq!(fixture.backend.inner.statistics().deletes, 0);
                fixture.backend.reset(None, false);
                // Drop all in-process reader/cache state before checking the
                // durable endpoint, then retry and reclaim abandoned objects.
                drop(fixture.store);
                let mut reopened =
                    Store::open_existing(fixture.directory.path().join("stream.zsqlite"))?;
                let after = reopened.inspect()?;
                assert_eq!(after.head_txid, before.head_txid);
                assert_eq!(after.head_history, before.head_history);
                assert_eq!(
                    after.manifest.unwrap().logical_hash(),
                    before.manifest.unwrap().logical_hash()
                );
                assert_contents(&mut reopened, &fixture.expected)?;
                reopened.repack_once()?;
                for _ in 0..4 {
                    reopened.gc_report(usize::MAX)?;
                }
                assert_eq!(reopened.gc_report(0)?.collectible_bytes, 0);
                assert_contents(&mut reopened, &fixture.expected)?;
            }
        }
    }
    Ok(())
}

#[test]
fn gc_retries_short_stream_writes_until_each_object_is_complete() -> TestResult {
    for partial_frames in [false, true] {
        let mut fixture = fixture(partial_frames)?;
        fixture.backend.reset(None, true);
        let report = fixture.store.repack_once()?;
        assert_eq!(report.repacked_packs, 3);
        assert!(fixture.backend.state.lock().unwrap().shortened > 0);
        fixture.backend.reset(None, false);
        fixture.store.gc_report(usize::MAX)?;
        drop(fixture.store);
        let mut reopened = Store::open_existing(fixture.directory.path().join("stream.zsqlite"))?;
        assert_contents(&mut reopened, &fixture.expected)?;
    }
    Ok(())
}
