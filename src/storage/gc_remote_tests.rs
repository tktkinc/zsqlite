use super::adapter::{
    BackendError, DeletePermit, ObjectKey, ObjectRange, ObjectWriter, Publication, Revision,
    RootRecord, StorageBackend,
};
use super::tests::{TestResult, page};
use super::{Catalog, FaultBackend, MemoryBackend, Storage};
use crate::domain::{BackendId, StoredBytes};
use crate::store::Store;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug)]
enum Damage {
    Byte(ObjectKey, u64),
    Short,
    Long,
    Missing,
    Extra,
}

// Damage blob replies at the transport boundary. A coalesced range starts with
// a metadata prefix that is intentionally rebuilt from the manifest; corrupting
// only that prefix does not test authentication of any copied frame's payload.
struct DamagingBackend {
    inner: MemoryBackend,
    damage: Mutex<Option<Damage>>,
}

impl StorageBackend for DamagingBackend {
    fn identity(&self) -> BackendId {
        self.inner.identity()
    }
    fn begin_write(&self) -> Result<Box<dyn ObjectWriter + '_>, BackendError> {
        self.inner.begin_write()
    }
    fn read_ranges(&self, requests: &[ObjectRange]) -> Result<Vec<Vec<u8>>, BackendError> {
        let mut reply = self.inner.read_ranges(requests)?;
        let mut pending = self.damage.lock().expect("damage lock");
        let Some(damage) = *pending else {
            return Ok(reply);
        };
        let Some(index) = requests.iter().position(|request| match damage {
            Damage::Byte(key, offset) => {
                request.key() == key
                    && offset >= request.range().offset().get()
                    && offset < request.range().offset().get() + request.range().length().get()
            }
            _ => matches!(request.key(), ObjectKey::Blob(_)),
        }) else {
            return Ok(reply);
        };
        *pending = None;
        match damage {
            Damage::Byte(_, offset) => {
                let relative = usize::try_from(offset - requests[index].range().offset().get())
                    .map_err(|_| BackendError::Range)?;
                reply[index][relative] ^= 1;
            }
            Damage::Short => {
                reply[index].pop();
            }
            Damage::Long => reply[index].push(0),
            Damage::Missing => {
                reply.remove(index);
            }
            Damage::Extra => reply.push(reply[index].clone()),
        }
        Ok(reply)
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
    _storage: Storage,
    backend: Arc<FaultBackend>,
    damage: Arc<DamagingBackend>,
    store: Store,
    expected: Vec<u8>,
}

impl Fixture {
    fn new(pages: Vec<Vec<u8>>) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let damage = Arc::new(DamagingBackend {
            inner: MemoryBackend::new()?,
            damage: Mutex::new(None),
        });
        let backend = Arc::new(FaultBackend::new(damage.clone()));
        let storage = Storage::new(backend.clone(), directory.path().join("coord"))?;
        let path = storage.bind(&directory.path().join("active.zsqlite"))?;
        let mut store = Store::open(&path, true)?;
        let expected: Vec<u8> = pages.into_iter().flatten().collect();
        store.write_at(0, &expected)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        Ok(Self {
            directory,
            _storage: storage,
            backend,
            damage,
            store,
            expected,
        })
    }

    fn overwrite_prefix(&mut self, pages: Vec<Vec<u8>>) -> TestResult {
        let bytes: Vec<u8> = pages.into_iter().flatten().collect();
        self.store.write_at(0, &bytes)?;
        self.expected[..bytes.len()].copy_from_slice(&bytes);
        self.store.publish(true)?;
        self.store.flush_sidecars()?;
        Ok(())
    }

    fn assert_contents(&mut self) -> TestResult {
        let mut actual = vec![0; self.expected.len()];
        self.store.read_at(0, &mut actual)?;
        assert_eq!(actual, self.expected);
        self.store.verify()?;
        Ok(())
    }
}

fn random_page(seed: u64) -> Vec<u8> {
    let mut bytes = page(1);
    let mut state = seed;
    for byte in &mut bytes[18..] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state.to_le_bytes()[0];
    }
    bytes
}

#[test]
fn collection_reports_occupancy_without_blob_reads_or_repeated_blob_stats() -> TestResult {
    let mut fixture = Fixture::new((1..=4).map(page).collect())?;
    fixture.overwrite_prefix(vec![page(101)])?;
    let blobs = fixture
        .backend
        .inventory(None, 4096)?
        .into_iter()
        .filter(|key| matches!(key, ObjectKey::Blob(_)))
        .count() as u64;
    assert_eq!(blobs, 2);

    for _ in 0..2 {
        fixture.backend.reset_statistics();
        let report = fixture.store.gc_report(0)?;
        let statistics = fixture.backend.statistics();
        assert_eq!(statistics.blob_reads, 0, "{statistics:?}");
        assert_eq!(statistics.blob_stat_calls, blobs, "{statistics:?}");
        assert_eq!(statistics.publications, 0, "{statistics:?}");
        assert_eq!(report.deleted_objects, 0);
        assert!(report.partially_obsolete_bytes > 0);
    }
    fixture.assert_contents()
}

#[test]
fn repeated_noop_repack_does_not_fetch_fully_live_blobs() -> TestResult {
    let mut fixture = Fixture::new((1..=8).map(page).collect())?;
    for _ in 0..2 {
        fixture.backend.reset_statistics();
        assert_eq!(fixture.store.repack_once()?.repacked_packs, 0);
        let statistics = fixture.backend.statistics();
        assert_eq!(statistics.blob_reads, 0, "{statistics:?}");
        assert_eq!(statistics.publications, 0, "{statistics:?}");
        assert_eq!(statistics.puts, 0, "{statistics:?}");
    }
    Ok(())
}

#[test]
fn repack_selection_uses_stored_bytes_instead_of_page_counts() -> TestResult {
    for (dead_pages, live_pages, dead_random) in [(4_u8, 8_u8, true), (8, 4, false)] {
        let pages = (0..dead_pages + live_pages)
            .map(|index| {
                if (index < dead_pages) == dead_random {
                    random_page(u64::from(index) + 1)
                } else {
                    page(index + 1)
                }
            })
            .collect();
        let mut fixture = Fixture::new(pages)?;
        fixture.overwrite_prefix((0..dead_pages).map(|index| page(index + 101)).collect())?;
        let occupancy = fixture.store.inspect()?.pack_occupancy;
        let source = occupancy
            .iter()
            .find(|pack| pack.live_pages < pack.total_pages)
            .ok_or("expected a partially obsolete source pack")?;
        assert_eq!(source.total_pages, u64::from(dead_pages + live_pages));
        assert_eq!(source.live_pages, u64::from(live_pages));
        assert_eq!(source.live_pages * 2 > source.total_pages, dead_random);
        assert_eq!(
            source.fully_obsolete_frame_bytes.get() > source.fully_live_frame_bytes.get(),
            dead_random
        );

        fixture.backend.reset_statistics();
        let report = fixture.store.repack_once()?;
        assert_eq!(report.repacked_packs, usize::from(dead_random));
        assert_eq!(report.decoded_input.get(), 0);
        if dead_random {
            assert_eq!(report.copied_frames, usize::from(live_pages));
        } else {
            assert_eq!(fixture.backend.statistics().blob_reads, 0);
        }
        fixture.assert_contents()?;
    }
    Ok(())
}

#[test]
fn contiguous_surviving_frames_are_copied_in_coalesced_reads() -> TestResult {
    let mut fixture = Fixture::new(
        (0..256)
            .map(|index| page(u8::try_from(index % 250).unwrap() + 1))
            .collect(),
    )?;
    fixture.overwrite_prefix((1..=192).map(page).collect())?;
    fixture.backend.reset_statistics();
    let report = fixture.store.repack_once()?;
    let statistics = fixture.backend.statistics();
    assert_eq!(report.repacked_packs, 1);
    assert_eq!(report.copied_frames, 64);
    assert_eq!(report.decoded_input.get(), 0);
    assert!(statistics.blob_reads <= 4, "{statistics:?}");
    assert!(statistics.publications <= 4, "{statistics:?}");
    fixture.assert_contents()
}

#[test]
fn every_coalesced_live_frame_authenticates_its_header_and_payload() -> TestResult {
    // The surviving four records share a range. Test every record, including
    // corruption after the first record has already been copied to the writer.
    for frame_index in 0..4 {
        for field in 0..3 {
            let mut fixture = Fixture::new((1..=16).map(page).collect())?;
            fixture.overwrite_prefix((101..=112).map(page).collect())?;
            let info = fixture.store.inspect()?;
            let manifest = info.manifest.unwrap().id();
            let source = info
                .pack_occupancy
                .into_iter()
                .find(|pack| pack.live_pages < pack.total_pages)
                .unwrap()
                .pack;
            let catalog = Catalog::open(
                &crate::backend::sidecar_dir(&fixture.directory.path().join("active.zsqlite")),
                false,
            )?;
            let guard = catalog.lock()?;
            let view = guard.pin(manifest)?;
            let mut frames: Vec<_> = view
                .metadata
                .frames
                .values()
                .filter(|frame| frame.pack == source)
                .collect();
            frames.sort_by_key(|frame| frame.payload.offset());
            assert_eq!(frames.len(), 4);
            let frame = frames[frame_index];
            let extent = guard.placement(source)?.preferred().extent;
            let start = extent.offset().get() + frame.payload.offset().get();
            let offset = match field {
                0 => start - crate::format::FRAME_HEADER_SIZE as u64,
                1 => start,
                _ => start + frame.payload.length().get() - 1,
            };
            let damage = Damage::Byte(ObjectKey::Blob(extent.blob()), offset);
            drop(guard);
            *fixture.damage.damage.lock().unwrap() = Some(damage);
            fixture.backend.reset_statistics();
            assert!(fixture.store.repack_once().is_err(), "{damage:?}");
            assert!(
                fixture.damage.damage.lock().unwrap().is_none(),
                "{damage:?}"
            );
            let statistics = fixture.backend.statistics();
            assert_eq!(statistics.publications, 0, "{damage:?}");
            assert_eq!(statistics.deletes, 0, "{damage:?}");
            assert_eq!(statistics.blob_reads, 1, "{damage:?}: {statistics:?}");
            fixture.assert_contents()?;
            assert_eq!(fixture.store.repack_once()?.copied_frames, 4);
            fixture.assert_contents()?;
        }
    }
    Ok(())
}

#[test]
fn malformed_coalesced_replies_never_publish_or_delete() -> TestResult {
    for damage in [Damage::Short, Damage::Long, Damage::Missing, Damage::Extra] {
        let mut fixture = Fixture::new((1..=16).map(page).collect())?;
        fixture.overwrite_prefix((101..=112).map(page).collect())?;
        *fixture.damage.damage.lock().unwrap() = Some(damage);
        fixture.backend.reset_statistics();
        assert!(fixture.store.repack_once().is_err(), "{damage:?}");
        assert!(
            fixture.damage.damage.lock().unwrap().is_none(),
            "{damage:?}"
        );
        assert_eq!(fixture.backend.statistics().publications, 0, "{damage:?}");
        assert_eq!(fixture.backend.statistics().deletes, 0, "{damage:?}");
        fixture.assert_contents()?;
        assert_eq!(fixture.store.repack_once()?.copied_frames, 4);
        fixture.assert_contents()?;
    }
    Ok(())
}
