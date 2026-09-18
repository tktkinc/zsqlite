use super::adapter::{BackendStatistics, Fault, ObjectKey, Operation, StorageBackend};
use super::tests::{TestResult, page};
use super::{Catalog, FaultBackend, MemoryBackend, Storage};
use crate::domain::{
    CacheBytes, DecodedBytes, FileOffset, FrameId, PackId, PageNumber, StoredBytes, StoredRange,
};
use crate::layout::LayoutPolicy;
use crate::store::Store;
use crate::{StoragePolicy, StoreError};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const SOURCE_PACKS: usize = 4;
const PAGES_PER_PACK: usize = 4;
const PAGE_BYTES: usize = 4096;

struct Fixture {
    directory: tempfile::TempDir,
    storage: Storage,
    backend: Arc<FaultBackend>,
    store: Store,
    expected: Vec<u8>,
}

fn fixture(layout: LayoutPolicy) -> Result<Fixture, Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new()?)));
    let storage = Storage::new(backend.clone(), directory.path().join("coord"))?;
    let path = storage.bind(&directory.path().join("active.zsqlite"))?;
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(StoragePolicy::default().with_layout(layout))?;
    let mut expected = Vec::new();
    // Each seal gets its own pack containing four pages.
    for source in 0..SOURCE_PACKS {
        let bytes = (0..PAGES_PER_PACK)
            .map(|offset| page(u8::try_from(1 + source * PAGES_PER_PACK + offset).unwrap()))
            .collect::<Vec<_>>()
            .concat();
        store.write_at(expected.len() as u64, &bytes)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        expected.extend(bytes);
    }
    // Leave one page in each source pack alive, and put all replacements in
    // one fully live pack that should never be selected for maintenance.
    for source in 0..SOURCE_PACKS {
        let offset = source * PAGES_PER_PACK * PAGE_BYTES;
        let bytes = (0..PAGES_PER_PACK - 1)
            .map(|index| page(u8::try_from(100 + source * PAGES_PER_PACK + index).unwrap()))
            .collect::<Vec<_>>()
            .concat();
        store.write_at(offset as u64, &bytes)?;
        expected[offset..offset + bytes.len()].copy_from_slice(&bytes);
    }
    store.publish(true)?;
    store.flush_sidecars()?;
    Ok(Fixture {
        directory,
        storage,
        backend,
        store,
        expected,
    })
}

fn sparse_packs(store: &Store) -> Result<BTreeSet<PackId>, StoreError> {
    Ok(store
        .inspect()?
        .pack_occupancy
        .into_iter()
        .filter(|pack| pack.live_pages * 2 < pack.total_pages)
        .map(|pack| pack.pack)
        .collect())
}

type FrameSnapshot = BTreeMap<FrameId, (PackId, Vec<u8>)>;

fn frame_snapshot(fixture: &Fixture) -> Result<FrameSnapshot, StoreError> {
    let id = fixture.store.inspect()?.manifest.unwrap().id();
    let catalog = Catalog::open(
        &crate::backend::sidecar_dir(&fixture.directory.path().join("active.zsqlite")),
        false,
    )?;
    let guard = catalog.lock()?;
    let view = guard.pin(id)?;
    view.metadata
        .frames
        .iter()
        .map(|(id, location)| {
            let payload = view.placement.read(
                location.pack,
                StoredRange::new(
                    FileOffset::new(location.payload.offset().get()),
                    location.payload.length(),
                )?,
            )?;
            Ok((*id, (location.pack, payload)))
        })
        .collect()
}

fn source_blobs(fixture: &Fixture, packs: &BTreeSet<PackId>) -> Result<Vec<ObjectKey>, StoreError> {
    let catalog = Catalog::open(
        &crate::backend::sidecar_dir(&fixture.directory.path().join("active.zsqlite")),
        false,
    )?;
    let guard = catalog.lock()?;
    packs
        .iter()
        .map(|pack| {
            Ok(ObjectKey::Blob(
                guard.placement(*pack)?.preferred().extent.blob(),
            ))
        })
        .collect()
}

fn assert_pages(store: &mut Store, expected: &[u8]) -> TestResult {
    let mut bytes = vec![0; expected.len()];
    store.read_at(0, &mut bytes)?;
    assert_eq!(bytes, expected);
    store.verify()?;
    Ok(())
}

fn pin_current(fixture: &Fixture) -> Result<super::PinnedView, StoreError> {
    let manifest = fixture.store.inspect()?.manifest.unwrap().id();
    let catalog = Catalog::open(
        &crate::backend::sidecar_dir(&fixture.directory.path().join("active.zsqlite")),
        false,
    )?;
    catalog.lock()?.pin(manifest)
}

fn assert_reader(reader: &super::PinnedView, expected: &[u8]) -> TestResult {
    for (index, bytes) in expected.chunks_exact(PAGE_BYTES).enumerate() {
        let number = PageNumber::new(u32::try_from(index + 1)?)?;
        assert_eq!(reader.resolve(number)?.read()?, bytes);
    }
    Ok(())
}

fn backend_calls(statistics: &BackendStatistics) -> [(Operation, u64); 7] {
    [
        (Operation::Put, statistics.write_starts),
        (Operation::Read, statistics.batches),
        (Operation::Stat, statistics.stat_calls),
        (Operation::RootRead, statistics.root_reads),
        (Operation::Publish, statistics.publications),
        (Operation::Inventory, statistics.inventory_calls),
        (Operation::Delete, statistics.deletes),
    ]
}

fn multiple_outputs(fixture: &mut Fixture) -> TestResult {
    let layout = LayoutPolicy::default().with_pack_target(StoredBytes::new(1))?;
    fixture
        .store
        .set_storage_policy(StoragePolicy::default().with_layout(layout))?;
    Ok(())
}

// Enumerate the successful trace instead of choosing the first few failures:
// later output packs, retirement indexes, and deletion are part of the pass.
#[test]
fn every_batched_repack_backend_failure_preserves_readers_and_retry() -> TestResult {
    let mut baseline = fixture(LayoutPolicy::default())?;
    multiple_outputs(&mut baseline)?;
    let reader = pin_current(&baseline)?;
    baseline.backend.reset_statistics();
    assert_eq!(baseline.store.repack_once()?.repacked_packs, SOURCE_PACKS);
    let statistics = baseline.backend.statistics();
    eprintln!(
        "batched repack fault positions: {:?}",
        backend_calls(&statistics)
    );
    assert!(statistics.puts > SOURCE_PACKS as u64);
    assert_eq!(
        statistics
            .put_keys
            .iter()
            .filter(|key| matches!(key, ObjectKey::Blob(_)))
            .count(),
        SOURCE_PACKS
    );
    drop(reader);
    for (operation, calls) in backend_calls(&statistics) {
        for call in 1..=calls {
            let mut fixture = fixture(LayoutPolicy::default())?;
            multiple_outputs(&mut fixture)?;
            let reader = pin_current(&fixture)?;
            let sources = source_blobs(&fixture, &sparse_packs(&fixture.store)?)?;
            let before = fixture.store.inspect()?;
            fixture.backend.reset_statistics();
            fixture
                .backend
                .inject(operation, usize::try_from(call)?, Fault::Before);
            assert!(
                fixture.store.repack_once().is_err(),
                "repack {operation:?} call {call}/{calls} did not fail"
            );
            // These exact source objects remain needed by the pre-repack reader
            // even when the failing operation follows successful publication.
            for key in sources {
                assert!(fixture.backend.stat(key)?.is_some(), "{operation:?} {call}");
            }
            assert_reader(&reader, &fixture.expected)?;
            assert_pages(&mut fixture.store, &fixture.expected)?;
            let path = fixture.directory.path().join("active.zsqlite");
            drop(fixture.store);
            let mut reopened = Store::open_existing(&path)?;
            let after = reopened.inspect()?;
            assert_eq!(after.head_history, before.head_history);
            assert_eq!(after.head_txid, before.head_txid);
            assert_eq!(
                after.manifest.unwrap().logical_hash(),
                before.manifest.unwrap().logical_hash()
            );
            reopened.repack_once()?;
            reopened.gc_report(usize::MAX)?;
            assert_reader(&reader, &fixture.expected)?;
            drop(reader);
            reopened.gc_report(usize::MAX)?;
            assert_pages(&mut reopened, &fixture.expected)?;
        }
    }
    Ok(())
}

#[test]
fn every_collection_backend_failure_preserves_live_objects_and_retry() -> TestResult {
    let prepare = || -> Result<Fixture, Box<dyn std::error::Error>> {
        let layout = LayoutPolicy::default().with_maintenance(DecodedBytes::new(65536), 0)?;
        let mut fixture = fixture(layout)?;
        // Budget zero leaves the obsolete blobs and discovery records in place
        // so the standalone collector must retire and physically delete them.
        assert_eq!(fixture.store.repack_once()?.repacked_packs, SOURCE_PACKS);
        fixture.backend.reset_statistics();
        Ok(fixture)
    };
    let mut baseline = prepare()?;
    let report = baseline.store.gc_report(usize::MAX)?;
    let statistics = baseline.backend.statistics();
    eprintln!(
        "collection fault positions: {:?}",
        backend_calls(&statistics)
    );
    assert!(report.deleted_objects > SOURCE_PACKS);
    assert!(statistics.publications > 0);
    for (operation, calls) in backend_calls(&statistics) {
        for call in 1..=calls {
            for fault in [Fault::Before, Fault::AfterPublish].into_iter().take(
                if operation == Operation::Publish {
                    2
                } else {
                    1
                },
            ) {
                let mut fixture = prepare()?;
                fixture
                    .backend
                    .inject(operation, usize::try_from(call)?, fault);
                assert!(
                    fixture.store.gc_report(usize::MAX).is_err(),
                    "collect {operation:?} call {call}/{calls}: {fault:?} did not fail"
                );
                if operation != Operation::Delete {
                    assert_eq!(fixture.backend.statistics().deletes, 0);
                }
                assert_pages(&mut fixture.store, &fixture.expected)?;
                let path = fixture.directory.path().join("active.zsqlite");
                drop(fixture.store);
                let mut reopened = Store::open_existing(&path)?;
                reopened.gc_report(usize::MAX)?;
                assert_pages(&mut reopened, &fixture.expected)?;
                assert_eq!(reopened.gc_report(usize::MAX)?.deleted_objects, 0);
            }
        }
    }
    Ok(())
}

#[test]
fn batches_sparse_packs_in_one_checkpoint_and_copies_live_frames() -> TestResult {
    let mut fixture = fixture(LayoutPolicy::default())?;
    let layout = LayoutPolicy::default().with_pack_target(StoredBytes::new(1))?;
    fixture
        .store
        .set_storage_policy(StoragePolicy::default().with_layout(layout))?;
    let sources = sparse_packs(&fixture.store)?;
    assert_eq!(sources.len(), SOURCE_PACKS);
    let blobs = source_blobs(&fixture, &sources)?;
    let before = fixture.store.inspect()?;
    let frames = frame_snapshot(&fixture)?;
    let copied_bytes: u64 = frames
        .values()
        .filter(|(pack, _)| sources.contains(pack))
        .map(|(_, bytes)| bytes.len() as u64)
        .sum();
    fixture.backend.reset_statistics();
    let report = fixture.store.repack_once()?;
    assert_eq!(report.repacked_packs, SOURCE_PACKS);
    assert_eq!(report.copied_frames, SOURCE_PACKS);
    assert_eq!(report.copied_bytes.get(), copied_bytes);
    assert_eq!(report.decoded_input.get(), 0);
    let statistics = fixture.backend.statistics();
    assert!(statistics.publications <= 4, "{statistics:?}");
    assert_eq!(
        statistics
            .put_keys
            .iter()
            .filter(|key| matches!(key, ObjectKey::Blob(_)))
            .count(),
        SOURCE_PACKS
    );
    assert_eq!(
        statistics
            .put_keys
            .iter()
            .filter(|key| matches!(key, ObjectKey::Manifest(_)))
            .count(),
        1
    );

    let after = fixture.store.inspect()?;
    assert_eq!(after.head_history, before.head_history);
    assert_eq!(after.head_txid, before.head_txid);
    assert_eq!(after.logical_size, before.logical_size);
    assert_eq!(
        after.manifest.as_ref().unwrap().logical_hash(),
        before.manifest.unwrap().logical_hash()
    );
    assert_eq!(after.manifest.unwrap().run_depth(), 0);
    assert!(
        after
            .pack_occupancy
            .iter()
            .all(|pack| pack.live_pages == pack.total_pages)
    );
    let rewritten = frame_snapshot(&fixture)?;
    assert_eq!(frames.len(), rewritten.len());
    for (id, (pack, bytes)) in &frames {
        let (new_pack, new_bytes) = &rewritten[id];
        assert_eq!(new_bytes, bytes);
        if sources.contains(pack) {
            assert_ne!(new_pack, pack);
        } else {
            assert_eq!(new_pack, pack);
        }
    }
    fixture.store.gc_report(usize::MAX)?;
    for blob in blobs {
        assert!(fixture.backend.stat(blob)?.is_none());
    }
    assert_pages(&mut fixture.store, &fixture.expected)?;
    fixture.backend.reset_statistics();
    assert_eq!(fixture.store.repack_once()?.repacked_packs, 0);
    assert_eq!(fixture.backend.statistics().publications, 0);
    assert_eq!(fixture.backend.statistics().puts, 0);
    Ok(())
}

#[test]
fn pass_budget_bounds_total_live_frame_work() -> TestResult {
    for (budget, selected) in [(0, 0), (4095, 0), (4096, 1), (8192, 2), (16384, 4)] {
        let layout = LayoutPolicy::default().with_maintenance(DecodedBytes::new(budget), 0)?;
        let mut fixture = fixture(layout)?;
        fixture.backend.reset_statistics();
        let report = fixture.store.repack_once()?;
        assert_eq!(report.repacked_packs, selected, "budget {budget}");
        assert_eq!(report.copied_frames, selected, "budget {budget}");
        assert_eq!(report.decoded_input.get(), 0);
        assert_eq!(sparse_packs(&fixture.store)?.len(), SOURCE_PACKS - selected);
        if selected == 0 {
            assert_eq!(fixture.backend.statistics().puts, 0);
            assert_eq!(fixture.backend.statistics().publications, 0);
        }
        assert_pages(&mut fixture.store, &fixture.expected)?;
    }
    Ok(())
}

#[test]
fn batches_partial_frames_and_charges_each_decode_once() -> TestResult {
    let layout = LayoutPolicy::default().fixed(DecodedBytes::new(16384))?;
    let mut fixture = fixture(layout)?;
    let sources = sparse_packs(&fixture.store)?;
    assert_eq!(sources.len(), SOURCE_PACKS);
    let before = fixture.store.inspect()?;
    let report = fixture.store.repack_once()?;
    assert_eq!(report.repacked_packs, SOURCE_PACKS);
    assert_eq!(report.copied_frames, 0);
    assert_eq!(report.copied_bytes.get(), 0);
    assert_eq!(report.decoded_input.get(), (SOURCE_PACKS * 16384) as u64);
    let after = fixture.store.inspect()?;
    assert_eq!(after.head_history, before.head_history);
    assert_eq!(after.head_txid, before.head_txid);
    assert!(sparse_packs(&fixture.store)?.is_empty());
    assert_pages(&mut fixture.store, &fixture.expected)?;
    Ok(())
}

#[test]
fn mixed_frames_decode_only_partial_records() -> TestResult {
    let layout = LayoutPolicy::default().fixed(DecodedBytes::new(8192))?;
    let mut fixture = fixture(layout)?;
    let offset = fixture.expected.len();
    let bytes = (201..=208).map(page).collect::<Vec<_>>().concat();
    fixture.store.write_at(offset as u64, &bytes)?;
    fixture.store.publish(true)?;
    fixture.store.flush_sidecars()?;
    fixture.expected.extend(bytes);
    // Four two-page frames: two dead, one partial, and one fully live. With
    // only three live pages this pack is eligible alongside the four others.
    let replacements = (211..=215).map(page).collect::<Vec<_>>().concat();
    fixture.store.write_at(offset as u64, &replacements)?;
    fixture.store.publish(true)?;
    fixture.store.flush_sidecars()?;
    fixture.expected[offset..offset + replacements.len()].copy_from_slice(&replacements);
    assert_eq!(sparse_packs(&fixture.store)?.len(), SOURCE_PACKS + 1);
    let report = fixture.store.repack_once()?;
    assert_eq!(report.repacked_packs, SOURCE_PACKS + 1);
    assert_eq!(report.copied_frames, 1);
    assert!(report.copied_bytes.get() > 0);
    assert_eq!(
        report.decoded_input.get(),
        ((SOURCE_PACKS + 1) * 8192) as u64
    );
    assert_pages(&mut fixture.store, &fixture.expected)?;
    Ok(())
}

#[test]
fn current_view_reader_retains_source_blobs_until_its_lease_ends() -> TestResult {
    let mut fixture = fixture(LayoutPolicy::default())?;
    let sources = sparse_packs(&fixture.store)?;
    let blobs = source_blobs(&fixture, &sources)?;
    let manifest = fixture.store.inspect()?.manifest.unwrap().id();
    let reader = {
        let catalog = Catalog::open(
            &crate::backend::sidecar_dir(&fixture.directory.path().join("active.zsqlite")),
            false,
        )?;
        let guard = catalog.lock()?;
        guard.pin(manifest)?
    };

    assert_eq!(fixture.store.repack_once()?.repacked_packs, SOURCE_PACKS);
    assert_ne!(fixture.store.inspect()?.manifest.unwrap().id(), manifest);
    fixture.store.gc_report(usize::MAX)?;
    for blob in &blobs {
        assert!(fixture.backend.stat(*blob)?.is_some());
    }
    for (index, expected) in fixture.expected.chunks_exact(PAGE_BYTES).enumerate() {
        let page = PageNumber::new(u32::try_from(index + 1)?)?;
        assert_eq!(reader.resolve(page)?.read()?, expected);
    }
    assert_pages(&mut fixture.store, &fixture.expected)?;

    drop(reader);
    fixture.store.gc_report(usize::MAX)?;
    for blob in blobs {
        assert!(fixture.backend.stat(blob)?.is_none());
    }
    assert_pages(&mut fixture.store, &fixture.expected)?;
    Ok(())
}

#[test]
fn repack_faults_keep_sources_and_a_readable_endpoint() -> TestResult {
    let failures = [
        (Operation::Put, 1, Fault::Before),
        (Operation::Put, 2, Fault::Before),
        (Operation::Put, 3, Fault::Before),
        (Operation::Publish, 1, Fault::Before),
        (Operation::Publish, 2, Fault::Before),
        (Operation::Publish, 3, Fault::Before),
        (Operation::Publish, 1, Fault::AfterPublish),
        (Operation::Publish, 2, Fault::AfterPublish),
        (Operation::Publish, 3, Fault::AfterPublish),
    ];
    for (operation, call, fault) in failures {
        let mut fixture = fixture(LayoutPolicy::default())?;
        multiple_outputs(&mut fixture)?;
        let sources = sparse_packs(&fixture.store)?;
        let blobs = source_blobs(&fixture, &sources)?;
        let before = fixture.store.inspect()?;
        fixture.backend.reset_statistics();
        fixture.backend.inject(operation, call, fault);
        assert!(
            fixture.store.repack_once().is_err(),
            "{operation:?} call {call}: {fault:?}"
        );
        assert_eq!(fixture.backend.statistics().deletes, 0);
        for blob in &blobs {
            assert!(fixture.backend.stat(*blob)?.is_some());
        }
        assert_pages(&mut fixture.store, &fixture.expected)?;
        let path = fixture.directory.path().join("active.zsqlite");
        drop(fixture.store);
        let mut reopened = Store::open_existing(&path)?;
        let after = reopened.inspect()?;
        assert_eq!(after.head_history, before.head_history);
        assert_eq!(after.head_txid, before.head_txid);
        assert_eq!(
            after.manifest.unwrap().logical_hash(),
            before.manifest.unwrap().logical_hash()
        );
        reopened.repack_once()?;
        reopened.gc_report(usize::MAX)?;
        assert_pages(&mut reopened, &fixture.expected)?;
    }
    Ok(())
}

fn next_random(state: &mut u64) -> usize {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    (*state >> 32) as usize
}

fn mutate_oracle(store: &mut Store, expected: &mut Vec<u8>, state: &mut u64) -> TestResult {
    let pages = expected.len() / PAGE_BYTES;
    match next_random(state) % 4 {
        0 => {
            let number = next_random(state) % pages;
            let bytes = page(u8::try_from(next_random(state) % 250 + 1)?);
            store.write_at((number * PAGE_BYTES) as u64, &bytes)?;
            expected[number * PAGE_BYTES..(number + 1) * PAGE_BYTES].copy_from_slice(&bytes);
        }
        1 => {
            let length = (next_random(state) % 20 + 1) * PAGE_BYTES;
            store.truncate(length as u64)?;
            expected.resize(length, 0);
        }
        2 => {
            // Shrink and regrow in the same transaction. Old packed bytes must
            // not reappear in the zero-filled tail, even after a later repack.
            let length = (next_random(state) % pages + 1) * PAGE_BYTES;
            store.truncate(length as u64)?;
            expected.truncate(length);
            store.truncate((pages * PAGE_BYTES) as u64)?;
            expected.resize(pages * PAGE_BYTES, 0);
        }
        _ => {
            // Also exercise writes smaller than a page, including a boundary
            // between two pages, without changing the first page's format.
            let start = 18 + next_random(state) % (expected.len() - 18);
            let length = (1 + next_random(state) % 5000).min(expected.len() - start);
            let bytes = vec![u8::try_from(next_random(state) % 256)?; length];
            store.write_at(start as u64, &bytes)?;
            expected[start..start + length].copy_from_slice(&bytes);
        }
    }
    store.publish(true)?;
    Ok(())
}

// Fixed seeds make failures reproducible. The model is a plain byte vector;
// it never asks the storage implementation how truncation or retention works.
#[test]
fn seeded_gc_histories_match_independent_bytes_with_readers_pins_and_forks() -> TestResult {
    for (seed, frame_bytes) in [
        (0x5eed_u64, 4096),
        (0x00c0_ffee, 8192),
        (0xdead_beef, 16384),
    ] {
        run_gc_history(seed, frame_bytes)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn run_gc_history(seed: u64, frame_bytes: u64) -> TestResult {
    let layout = LayoutPolicy::default()
        .fixed(DecodedBytes::new(frame_bytes))?
        .with_cache(CacheBytes::new(0))?
        .with_maintenance(DecodedBytes::new(32768), 3)?;
    let mut fixture = fixture(layout)?;
    let mut state = seed;
    let first = fixture.store.repack_once()?;
    assert!(first.repacked_packs > 0, "seed {seed:x}");
    let fork_storage = fixture.storage.fork("oracle-fork")?;
    let fork_path = fixture.directory.path().join("fork.zsqlite");
    drop(fork_storage.bootstrap(&fork_path)?);
    let mut fork = Some(Store::open_existing(&fork_path)?);
    let mut fork_bytes = fixture.expected.clone();
    let mut readers = Vec::new();
    let mut retained: Option<(super::DurablePin, Vec<u8>)> = None;
    let path = fixture.directory.path().join("active.zsqlite");
    for step in 0..48 {
        mutate_oracle(&mut fixture.store, &mut fixture.expected, &mut state)?;
        // Alternate sealed and active-only commits. Maintenance must preserve
        // both, including commits acknowledged before any pack was uploaded.
        if step % 3 != 1 {
            fixture.store.flush_sidecars()?;
        }
        if step % 8 == 0 {
            fixture.store.flush_sidecars()?;
            readers.push((pin_current(&fixture)?, fixture.expected.clone()));
            retained = Some((
                fixture
                    .store
                    .retain_view(super::RetentionName::new(format!("oracle-{step}"))?, false)?,
                fixture.expected.clone(),
            ));
        }
        if step % 8 == 4 {
            readers.clear();
            if let Some((pin, _)) = retained.take() {
                fixture.store.release_view(pin)?;
            }
        }
        if let Some(store) = &mut fork {
            if step % 5 == 0 {
                mutate_oracle(store, &mut fork_bytes, &mut state)?;
                store.flush_sidecars()?;
                store.repack_once()?;
            }
            assert_eq!(store.logical_size(), fork_bytes.len() as u64);
            assert_pages(store, &fork_bytes)?;
        }
        if step == 23 {
            drop(fork.take());
            fork_storage.remove_head()?;
        }
        let maintenance = fixture.store.repack_once()?;
        assert!(
            maintenance.decoded_input.get() <= 32768,
            "seed {seed:x}, step {step}"
        );
        assert!(
            maintenance.gc.deleted_objects <= 3,
            "seed {seed:x}, step {step}"
        );
        let budget = next_random(&mut state) % 5;
        assert!(fixture.store.gc_report(budget)?.deleted_objects <= budget);
        assert_eq!(fixture.store.logical_size(), fixture.expected.len() as u64);
        assert_pages(&mut fixture.store, &fixture.expected)?;
        for (reader, bytes) in &readers {
            assert_reader(reader, bytes)?;
        }
        if let Some((pin, bytes)) = &retained {
            assert_reader(&fixture.store.retained_view(pin.name())?, bytes)?;
        }
        if step % 7 == 6 {
            drop(fixture.store);
            fixture.store = Store::open_existing(&path)?;
            assert_pages(&mut fixture.store, &fixture.expected)?;
        }
    }
    readers.clear();
    if let Some((pin, _)) = retained.take() {
        fixture.store.release_view(pin)?;
    }
    fixture.store.flush_sidecars()?;
    let mut converged = false;
    for _ in 0..128 {
        if fixture.store.repack_once()?.repacked_packs == 0 {
            converged = true;
            break;
        }
    }
    assert!(converged, "maintenance did not converge for seed {seed:x}");
    fixture.store.gc_report(usize::MAX)?;
    assert_pages(&mut fixture.store, &fixture.expected)?;
    assert_eq!(fixture.store.gc_report(usize::MAX)?.deleted_objects, 0);
    Ok(())
}
