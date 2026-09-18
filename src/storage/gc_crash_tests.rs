//! Real process exits through batched repacking, active replacement, and GC.
//! The parent process keeps no view lease: OS lock release and reopen recovery
//! must protect the acknowledged endpoint without Rust destructors running.
use super::adapter::ObjectKey;
use super::faults::{self, Mode, Point};
use super::tests::{TestResult, page};
use super::{Catalog, FilesystemBackend, Storage, StorageBackend};
use crate::domain::{DecodedBytes, StoredBytes};
use crate::layout::LayoutPolicy;
use crate::store::Store;
use crate::{Inspect, StoragePolicy};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SOURCE_PACKS: usize = 3;
const PAGES_PER_PACK: usize = 4;
const PAGE_BYTES: usize = 4096;
const POINTS: [Point; 11] = [
    Point::ObjectDataSynced,
    Point::ObjectLinked,
    Point::ObjectDirectorySynced,
    Point::ManifestReady,
    Point::RootRenamed,
    Point::RootDirectorySynced,
    Point::ActiveDataSynced,
    Point::ActiveRenamed,
    Point::ActiveDirectorySynced,
    Point::ObjectRemoved,
    Point::ObjectDeletionSynced,
];

struct Fixture {
    directory: tempfile::TempDir,
    storage: Storage,
    backend: Arc<FilesystemBackend>,
    backend_path: PathBuf,
    path: PathBuf,
    expected: Vec<u8>,
    before: Inspect,
    source_blobs: Vec<ObjectKey>,
}

fn fixture(partial_frames: bool) -> Result<Fixture, Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let backend_path = directory.path().join("sealed");
    let backend = Arc::new(FilesystemBackend::open(&backend_path)?);
    let storage = Storage::new(backend.clone(), directory.path().join("coord"))?;
    let path = storage.bind(&directory.path().join("active.zsqlite"))?;
    let mut store = Store::open(&path, true)?;
    let layout = LayoutPolicy::default()
        .fixed(DecodedBytes::new(if partial_frames { 16384 } else { 4096 }))?
        .with_maintenance(DecodedBytes::new(1024 * 1024), 0)?;
    store.set_storage_policy(StoragePolicy::default().with_layout(layout))?;
    let mut expected = Vec::new();
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
    for source in 0..SOURCE_PACKS {
        let offset = source * PAGES_PER_PACK * PAGE_BYTES;
        let bytes = (0..PAGES_PER_PACK - 1)
            .map(|index| page(u8::try_from(101 + source * PAGES_PER_PACK + index).unwrap()))
            .collect::<Vec<_>>()
            .concat();
        store.write_at(offset as u64, &bytes)?;
        expected[offset..offset + bytes.len()].copy_from_slice(&bytes);
    }
    store.publish(true)?;
    store.flush_sidecars()?;
    // Force several output blobs, one replacement manifest, all publication
    // phases, and physical deletion during the same maintenance pass.
    let layout = layout
        .with_pack_target(StoredBytes::new(1))?
        .with_maintenance(DecodedBytes::new(1024 * 1024), usize::MAX)?;
    store.set_storage_policy(StoragePolicy::default().with_layout(layout))?;
    let before = store.inspect()?;
    let source_blobs = {
        let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
        let guard = catalog.lock()?;
        before
            .pack_occupancy
            .iter()
            .filter(|pack| pack.live_pages * 2 < pack.total_pages)
            .map(|pack| {
                Ok(ObjectKey::Blob(
                    guard.placement(pack.pack)?.preferred().extent.blob(),
                ))
            })
            .collect::<Result<Vec<_>, crate::StoreError>>()?
    };
    assert_eq!(source_blobs.len(), SOURCE_PACKS);
    drop(store);
    Ok(Fixture {
        directory,
        storage,
        backend,
        backend_path,
        path,
        expected,
        before,
        source_blobs,
    })
}

fn assert_endpoint(store: &mut Store, fixture: &Fixture) -> TestResult {
    let mut actual = vec![0; fixture.expected.len()];
    store.read_at(0, &mut actual)?;
    assert_eq!(actual, fixture.expected);
    store.verify()?;
    let after = store.inspect()?;
    assert_eq!(after.logical_size, fixture.before.logical_size);
    assert_eq!(after.head_txid, fixture.before.head_txid);
    assert_eq!(after.head_history, fixture.before.head_history);
    assert_eq!(
        after.manifest.unwrap().logical_hash(),
        fixture.before.manifest.as_ref().unwrap().logical_hash()
    );
    Ok(())
}

fn copy_directory(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory(&entry.path(), &target)?;
        } else {
            assert!(entry.file_type()?.is_file());
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn crash_at(fixture: &Fixture, point: Point, occurrence: usize) -> TestResult {
    let output = tempfile::NamedTempFile::new()?;
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "storage::gc_crash_tests::gc_crash_child",
            "--nocapture",
        ])
        .env("ZSQLITE_GC_CRASH_BACKEND", &fixture.backend_path)
        .env(
            "ZSQLITE_GC_CRASH_COORD",
            fixture.storage.coordination_directory(),
        )
        .env("ZSQLITE_GC_CRASH_PATH", &fixture.path)
        .env("ZSQLITE_GC_CRASH_POINT", format!("{point:?}"))
        .env("ZSQLITE_GC_CRASH_OCCURRENCE", occurrence.to_string())
        .stdout(output.as_file().try_clone()?)
        .stderr(output.as_file().try_clone()?)
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            panic!(
                "GC child timed out: {point:?} #{occurrence}: {}",
                std::fs::read_to_string(output.path())?
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        status.code(),
        Some(73),
        "{point:?} #{occurrence}: {}",
        std::fs::read_to_string(output.path())?
    );
    Ok(())
}

fn crash_matrix(partial_frames: bool) -> TestResult {
    let started = Instant::now();
    let fixture = fixture(partial_frames)?;
    // Keep the exact same acknowledged disk image and paths for every case.
    // All view/catalog handles are closed before snapshot/restore. These are
    // process-exit tests, so restoring fixture files need not model power loss.
    let snapshot = tempfile::tempdir()?;
    copy_directory(fixture.directory.path(), snapshot.path())?;
    let mut store = Store::open(&fixture.path, true)?;
    let recording = faults::record();
    let report = store.repack_once()?;
    let points = recording.finish();
    assert_eq!(report.repacked_packs, SOURCE_PACKS);
    if partial_frames {
        assert_eq!(report.copied_frames, 0);
        assert_eq!(report.decoded_input.get(), (SOURCE_PACKS * 16384) as u64);
    } else {
        assert_eq!(report.copied_frames, SOURCE_PACKS);
        assert_eq!(report.decoded_input.get(), 0);
    }
    assert_endpoint(&mut store, &fixture)?;
    drop(store);
    // Discover counts from the operation rather than assuming only the first
    // blob or first catalog CAS matters. In particular, GC retirement is a
    // fourth publication and its index writes also need crash coverage.
    let mut occurrences = BTreeMap::<Point, usize>::new();
    for point in points {
        *occurrences.entry(point).or_default() += 1;
    }
    for point in POINTS {
        assert!(
            occurrences.contains_key(&point),
            "missing boundary {point:?}"
        );
    }
    assert_eq!(occurrences[&Point::RootRenamed], 4);
    assert!(occurrences[&Point::ObjectLinked] > SOURCE_PACKS + 1);
    assert!(occurrences[&Point::ObjectRemoved] >= SOURCE_PACKS);
    let cases: usize = occurrences.values().sum();
    for (point, count) in occurrences {
        for occurrence in 1..=count {
            std::fs::remove_dir_all(fixture.directory.path())?;
            copy_directory(snapshot.path(), fixture.directory.path())?;
            crash_at(&fixture, point, occurrence)?;
            if !matches!(point, Point::ObjectRemoved | Point::ObjectDeletionSynced) {
                for blob in &fixture.source_blobs {
                    assert!(
                        fixture.backend.stat(*blob)?.is_some(),
                        "source removed before collection: {point:?} #{occurrence}"
                    );
                }
            }
            let mut recovered = Store::open(&fixture.path, true)?;
            assert_endpoint(&mut recovered, &fixture)?;
            recovered.repack_once()?;
            // A second collection can retire index runs superseded by the
            // first. Repetition must converge and reclaim installed orphans.
            for _ in 0..4 {
                if recovered.gc_report(usize::MAX)?.deleted_objects == 0 {
                    break;
                }
            }
            assert_eq!(recovered.gc_report(0)?.collectible_bytes, 0);
            for blob in &fixture.source_blobs {
                assert!(fixture.backend.stat(*blob)?.is_none());
            }
            assert_endpoint(&mut recovered, &fixture)?;
            assert_eq!(recovered.repack_once()?.repacked_packs, 0);
            drop(recovered);
            // The successfully retried maintenance must itself survive close
            // and reopen, not merely remain readable from an existing cache.
            assert_endpoint(&mut Store::open(&fixture.path, true)?, &fixture)?;
        }
    }
    eprintln!(
        "GC crash matrix partial={partial_frames}: {cases} process exits in {:?}",
        started.elapsed()
    );
    Ok(())
}

#[test]
fn copied_frame_gc_survives_every_process_exit_boundary() -> TestResult {
    crash_matrix(false)
}

#[test]
fn partial_frame_gc_survives_every_process_exit_boundary() -> TestResult {
    crash_matrix(true)
}

#[test]
fn gc_crash_child() -> TestResult {
    let Some(backend) = std::env::var_os("ZSQLITE_GC_CRASH_BACKEND") else {
        return Ok(());
    };
    let coordination = std::env::var_os("ZSQLITE_GC_CRASH_COORD").unwrap();
    let path = std::env::var_os("ZSQLITE_GC_CRASH_PATH").unwrap();
    let selected = std::env::var("ZSQLITE_GC_CRASH_POINT")?;
    let occurrence = std::env::var("ZSQLITE_GC_CRASH_OCCURRENCE")?.parse()?;
    let point = POINTS
        .into_iter()
        .find(|point| format!("{point:?}") == selected)
        .expect("known crash boundary");
    let storage = Storage::new(Arc::new(FilesystemBackend::open(backend)?), coordination)?;
    let mut store = storage.open(path)?.into_store()?;
    let _injection = faults::inject_nth(point, Mode::Crash, occurrence);
    store.repack_once()?;
    panic!("GC did not reach {point:?} #{occurrence}");
}
