use crate::domain::{CacheBytes, DecodedBytes, PageNumber, StoredBytes};
use crate::layout::LayoutPolicy;
use crate::store::Store;
use crate::{RetentionName, StoragePolicy};
use std::collections::BTreeSet;
use std::path::Path;

pub(super) type TestResult = Result<(), Box<dyn std::error::Error>>;
pub(super) fn page(value: u8) -> Vec<u8> {
    let mut bytes = vec![value; 4096];
    bytes[..16].copy_from_slice(b"SQLite format 3\0");
    bytes[16..18].copy_from_slice(&4096_u16.to_be_bytes());
    bytes
}
pub(super) fn packs(path: &Path) -> Result<BTreeSet<std::path::PathBuf>, std::io::Error> {
    std::fs::read_dir(crate::backend::sidecar_dir(path).join("objects"))?
        .filter_map(|entry| match entry {
            Ok(entry) if entry.path().extension().is_some_and(|ext| ext == "blob") => {
                Some(Ok(entry.path()))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}
pub(super) fn tiny_packs() -> StoragePolicy {
    let policy = LayoutPolicy::default()
        .with_pack_target(StoredBytes::new(1))
        .unwrap();
    StoragePolicy::default().with_layout(policy)
}

#[test]
fn whole_pack_collection_does_not_rewrite_stable_data() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("gc.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(tiny_packs())?;
    store.write_at(0, &[page(1), page(2), page(3)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let checkpoint = store.inspect()?.manifest.unwrap();
    let before = packs(&path)?;
    assert_eq!(before.len(), 3);
    store.write_at(0, &page(4))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let after = packs(&path)?;
    let delta = store.inspect()?.manifest.unwrap();
    assert_eq!(delta.resolved_parent(), Some(checkpoint.id()));
    assert_eq!(after.len(), 3);
    assert_eq!(before.intersection(&after).count(), 2);
    // Ancestor metadata remains even though its overwritten pack was collected.
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let guard = catalog.lock()?;
    assert!(guard.path::<super::Manifest>(checkpoint.id()).exists());
    assert!(guard.pin(checkpoint.id()).is_err());
    drop(guard);
    drop(store);
    let mut store = Store::open_existing(&path)?;
    store.verify()?;
    Ok(())
}

#[test]
fn idle_maintenance_does_not_contend_with_a_writer() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("idle-maintenance.zsqlite");
    let mut reader = Store::open(&path, true)?;
    reader.write_at(0, &page(1))?;
    reader.publish(true)?;
    reader.flush_sidecars()?;
    let mut writer = Store::open_existing(&path)?;
    writer.write_at(0, &page(2))?;
    // The pending writer owns publication. A fully live pack requires no
    // rewrite, so the idle reader must not reserve that lock just to discover it.
    reader.try_background_maintenance()?;
    writer.publish(true)?;
    writer.verify()?;
    Ok(())
}

#[test]
fn offline_fork_and_manifest_lease_each_retain_their_own_view() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("fork.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &page(1))?;
    store.publish(true)?;
    let name = RetentionName::new("offline-fork")?;
    let pin = store.retain_view(name.clone(), false)?;
    let original_pack = packs(&path)?;
    drop(pin);
    drop(store);
    let mut store = Store::open_existing(&path)?;
    store.write_at(0, &page(2))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    assert!(original_pack.iter().all(|pack| pack.exists()));
    assert!(store.gc_report(100)?.retained_bytes > 0);
    let reader = crate::open_retained(&path, &name)?;
    assert_eq!(reader.resolve(PageNumber::new(1)?)?.read()?, page(1));
    let pin = crate::retention(&path, &name)?;
    store.release_view(pin)?;
    store.gc_report(100)?;
    assert!(original_pack.iter().all(|pack| pack.exists()));
    drop(reader);
    store.gc_report(100)?;
    assert!(original_pack.iter().all(|pack| !pack.exists()));
    store.verify()?;
    Ok(())
}

#[test]
fn stale_pin_cannot_release_advanced_root_and_bad_roots_stop_gc() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("pin-cas.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &page(1))?;
    store.publish(true)?;
    let name = RetentionName::new("backup")?;
    let stale = store.retain_view(name.clone(), false)?;
    store.write_at(0, &page(2))?;
    store.publish(true)?;
    let current = store.retain_view(name.clone(), true)?;
    assert!(matches!(
        store.release_view(stale),
        Err(crate::StoreError::Busy)
    ));
    let before = packs(&path)?;
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let guard = catalog.lock()?;
    guard.state_mut().retentions.insert(
        name.as_str().as_bytes().to_vec(),
        b"corrupt retained root".to_vec(),
    );
    guard.publish_catalog()?;
    drop(guard);
    assert!(store.gc_report(100).is_err());
    assert_eq!(before, packs(&path)?);
    drop(current);
    Ok(())
}

#[test]
fn multi_page_frames_reconstruct_partial_obsolescence_and_compaction() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("frames.zsqlite");
    let mut store = Store::open(&path, true)?;
    let layout = LayoutPolicy::default().fixed(DecodedBytes::new(65536))?;
    store.set_storage_policy(StoragePolicy::default().with_layout(layout))?;
    let mut expected = [page(1), page(2), page(3), page(4)].concat();
    store.write_at(0, &expected)?;
    store.publish(true)?;
    store.flush_sidecars()?;
    store.write_at(4096, &page(5))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    expected[4096..8192].copy_from_slice(&page(5));
    store.compact()?;
    drop(store);
    let mut store = Store::open_existing(&path)?;
    assert_eq!(store.inspect()?.policy.layout(), layout);
    let mut actual = vec![0; expected.len()];
    store.read_at(0, &mut actual)?;
    assert_eq!(actual, expected);
    store.verify()?;
    Ok(())
}

#[test]
fn bounded_repack_skips_forks_then_removes_partial_obsolescence() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("repack.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(
        StoragePolicy::default()
            .with_layout(LayoutPolicy::default().fixed(DecodedBytes::new(65536))?),
    )?;
    let expected = (1..=8).map(page).collect::<Vec<_>>().concat();
    store.write_at(0, &expected)?;
    store.publish(true)?;
    let fork = store.retain_view(RetentionName::new("fork")?, false)?;
    let original = packs(&path)?;
    store.write_at(0, &(20..=25).map(page).collect::<Vec<_>>().concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let before = store.inspect()?;
    assert!(store.gc_report(0)?.partially_obsolete_bytes > 0);
    store.try_background_maintenance()?;
    assert!(original.iter().all(|path| path.exists()));
    store.release_view(fork)?;
    store.try_background_maintenance()?;
    assert!(original.iter().all(|path| !path.exists()));
    let after = store.inspect()?;
    assert_eq!(before.head_history, after.head_history);
    assert_eq!(before.head_txid, after.head_txid);
    assert!(
        after
            .pack_occupancy
            .iter()
            .all(|pack| pack.live_pages == pack.total_pages)
    );
    assert_eq!(store.gc_report(0)?.partially_obsolete_bytes, 0);
    store.verify()?;
    Ok(())
}

#[test]
fn durable_roots_prevent_whole_bundle_deletion_without_live_processes() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("durable-delete.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &page(1))?;
    store.publish(true)?;
    let pin = store.retain_view(RetentionName::new("offline")?, false)?;
    drop(store);
    assert!(matches!(
        Store::delete_bundle(&path),
        Err(crate::StoreError::Busy)
    ));
    crate::release_retention(&path, pin)?;
    Store::delete_bundle(&path)?;
    assert!(!path.exists());
    Ok(())
}

#[test]
fn cached_old_slots_cannot_replace_new_versions_and_zero_budget_bypasses() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("cache.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(
        StoragePolicy::default()
            .with_layout(LayoutPolicy::default().fixed(DecodedBytes::new(65536))?),
    )?;
    store.write_at(0, &[page(1), page(2)].concat())?;
    store.publish(true)?;
    let name = RetentionName::new("view")?;
    let _pin = store.retain_view(name.clone(), false)?;
    let old = crate::open_retained(&path, &name)?;
    let mut cache = super::PageCache::new(crate::domain::CacheBytes::new(65536))?;
    let mut io = crate::statistics::HandleIoStats::default();
    assert_eq!(
        cache.read(&old, PageNumber::new(1)?, &mut io, |_| false)?,
        page(1)
    );
    assert_eq!(
        cache.read(&old, PageNumber::new(2)?, &mut io, |_| false)?,
        page(2)
    );
    assert_eq!(cache.stats().extra_pages_requested, 1);
    store.write_at(4096, &page(3))?;
    store.publish(true)?;
    let _pin = store.retain_view(name.clone(), true)?;
    let current = crate::open_retained(&path, &name)?;
    assert_eq!(
        cache.read(&current, PageNumber::new(2)?, &mut io, |_| false)?,
        page(3)
    );
    cache.set_budget(crate::domain::CacheBytes::new(0))?;
    let before = io.cache_misses;
    cache.read(&old, PageNumber::new(1)?, &mut io, |_| false)?;
    cache.read(&old, PageNumber::new(1)?, &mut io, |_| false)?;
    assert_eq!(cache.stats().resident_bytes, 0);
    assert_eq!(io.cache_misses - before, 2);
    assert!(cache.stats().extra_pages_evicted_unused >= 2);
    Ok(())
}

#[test]
fn accepted_writes_and_truncation_invalidate_cached_pages_before_publication() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("write-cache.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(
        StoragePolicy::default().with_layout(
            LayoutPolicy::default()
                .fixed(DecodedBytes::new(65536))?
                .with_cache(CacheBytes::new(65536))?,
        ),
    )?;
    store.write_at(0, &[page(1), page(2), page(3)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let mut bytes = vec![0; 4096];
    store.read_at(0, &mut bytes)?;
    let full = store.cache_stats().resident_bytes;
    assert!(full >= 3 * 4096);
    store.write_at(4096, &page(9))?;
    assert_eq!(store.cache_stats().resident_bytes, full * 2 / 3);
    store.read_at(4096, &mut bytes)?;
    assert_eq!(bytes, page(9));
    store.discard_pending();
    store.read_at(4096, &mut bytes)?;
    assert_eq!(bytes, page(2));
    assert_eq!(store.cache_stats().resident_bytes, full);
    store.truncate(4096)?;
    assert_eq!(store.cache_stats().resident_bytes, full / 3);
    store.truncate(12288)?;
    store.read_at(8192, &mut bytes)?;
    assert_eq!(bytes, vec![0; 4096]);
    store.discard_pending();
    store.read_at(8192, &mut bytes)?;
    assert_eq!(bytes, page(3));
    Ok(())
}

#[test]
fn refreshing_another_writers_publication_invalidates_only_shadowed_slots() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("refresh-cache.zsqlite");
    let mut writer = Store::open(&path, true)?;
    writer.set_storage_policy(
        StoragePolicy::default().with_layout(
            LayoutPolicy::default()
                .fixed(DecodedBytes::new(65536))?
                .with_cache(CacheBytes::new(65536))?,
        ),
    )?;
    writer.write_at(0, &[page(1), page(2), page(3)].concat())?;
    writer.publish(true)?;
    writer.flush_sidecars()?;
    let mut reader = Store::open_existing(&path)?;
    let mut bytes = vec![0; 4096];
    reader.read_at(0, &mut bytes)?;
    let full = reader.cache_stats().resident_bytes;
    writer.write_at(4096, &page(9))?;
    writer.publish(true)?;
    reader.refresh()?;
    assert_eq!(reader.cache_stats().resident_bytes, full * 2 / 3);
    reader.read_at(4096, &mut bytes)?;
    assert_eq!(bytes, page(9));
    let hits = reader.cache_stats().hits;
    reader.read_at(8192, &mut bytes)?;
    assert_eq!(bytes, page(3));
    assert_eq!(reader.cache_stats().hits, hits + 1);
    writer.flush_sidecars()?;
    reader.refresh()?;
    assert_eq!(
        reader.cache_stats().resident_bytes,
        0,
        "new manifest discards the old cache namespace"
    );
    reader.read_at(4096, &mut bytes)?;
    assert_eq!(bytes, page(9));
    Ok(())
}

pub(super) const SEAL_POINTS: [super::faults::Point; 7] = [
    super::faults::Point::ObjectDataSynced,
    super::faults::Point::ObjectLinked,
    super::faults::Point::ObjectDirectorySynced,
    super::faults::Point::ManifestReady,
    super::faults::Point::ActiveDataSynced,
    super::faults::Point::ActiveRenamed,
    super::faults::Point::ActiveDirectorySynced,
];

#[test]
fn seal_io_failures_preserve_published_pages_and_offline_roots() -> TestResult {
    use super::faults::{Mode, inject};
    for point in SEAL_POINTS {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("failure.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &[page(1), page(3), page(4)].concat())?;
        store.publish(true)?;
        let name = RetentionName::new("offline")?;
        let pin = store.retain_view(name.clone(), false)?;
        store.write_at(0, &page(2))?;
        store.publish(true)?;
        let injection = inject(point, Mode::Error);
        assert!(store.flush_sidecars().is_err(), "{point:?}");
        drop(injection);
        drop(store);
        let mut store = Store::open_existing(&path)?;
        let mut bytes = vec![0; 4096];
        store.read_at(0, &mut bytes)?;
        assert_eq!(bytes, page(2), "{point:?}");
        let reader = crate::open_retained(&path, &name)?;
        assert_eq!(reader.resolve(PageNumber::new(1)?)?.read()?, page(1));
        drop(reader);
        store.verify()?;
        store.release_view(pin)?;
        store.flush_sidecars()?;
        store.gc_report(100)?;
        store.verify()?;
    }
    Ok(())
}

#[test]
fn uncertain_state_sync_never_rolls_pages_back_under_a_new_endpoint() -> TestResult {
    use super::faults::{Mode, Point, inject};
    for point in [Point::ActiveStateWritten, Point::ActiveStateSynced] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("state-sync.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &page(1))?;
        store.publish(true)?;
        store.write_at(0, &page(2))?;
        let injection = inject(point, Mode::Error);
        assert!(matches!(
            store.publish(true),
            Err(crate::StoreError::PublicationUncertain(_))
        ));
        drop(injection);
        drop(store);
        let mut store = Store::open_existing(&path)?;
        let mut bytes = vec![0; 4096];
        store.read_at(0, &mut bytes)?;
        assert_eq!(bytes, page(2));
        assert_eq!(store.inspect()?.head_txid, 2);
    }
    Ok(())
}

#[test]
fn uncertain_root_replacement_retains_the_visible_root() -> TestResult {
    use super::faults::{Mode, Point, inject};
    for point in [Point::RootRenamed, Point::RootDirectorySynced] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("root-sync.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &page(1))?;
        store.publish(true)?;
        store.flush_sidecars()?;
        let name = RetentionName::new("offline")?;
        let injection = inject(point, Mode::Error);
        assert!(matches!(
            store.retain_view(name.clone(), false),
            Err(crate::StoreError::Backend(
                super::adapter::BackendError::Uncertain(_)
            ))
        ));
        drop(injection);
        drop(store);
        let pin = crate::retention(&path, &name)?;
        let reader = crate::open_retained(&path, &name)?;
        assert_eq!(reader.resolve(PageNumber::new(1)?)?.read()?, page(1));
        drop(reader);
        crate::release_retention(&path, pin)?;
    }
    Ok(())
}

#[test]
fn publication_crash_child() -> TestResult {
    let Some(path) = std::env::var_os("ZSQLITE_V1_CRASH_PATH") else {
        return Ok(());
    };
    let point: usize = std::env::var("ZSQLITE_V1_CRASH_POINT")?.parse()?;
    let mut store = Store::open_existing(path)?;
    store.write_at(0, &page(2))?;
    store.publish(true)?;
    let _injection = super::faults::inject(SEAL_POINTS[point], super::faults::Mode::Crash);
    store.flush_sidecars()?;
    Err("crash boundary was not reached".into())
}

#[test]
fn abrupt_exit_at_every_seal_boundary_reopens_consistently() -> TestResult {
    for (index, point) in SEAL_POINTS.iter().enumerate() {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("crash.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &[page(1), page(3), page(4)].concat())?;
        store.publish(true)?;
        let name = RetentionName::new("fork")?;
        let pin = store.retain_view(name.clone(), false)?;
        drop(store);
        let status = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "storage::tests::publication_crash_child",
                "--nocapture",
            ])
            .env("ZSQLITE_V1_CRASH_PATH", &path)
            .env("ZSQLITE_V1_CRASH_POINT", index.to_string())
            .stdout(std::process::Stdio::null())
            .status()?;
        assert_eq!(status.code(), Some(73), "{point:?}");
        let mut store = Store::open_existing(&path)?;
        let mut bytes = vec![0; 4096];
        store.read_at(0, &mut bytes)?;
        assert_eq!(bytes, page(2), "{point:?}");
        let reader = crate::open_retained(&path, &name)?;
        assert_eq!(reader.resolve(PageNumber::new(1)?)?.read()?, page(1));
        drop(reader);
        store.release_view(pin)?;
        store.flush_sidecars()?;
        store.gc_report(100)?;
        store.verify()?;
    }
    Ok(())
}

#[test]
fn tiny_seals_reuse_shared_dictionaries_and_pins_retain_decoding_dependencies() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("dictionaries.zsqlite");
    let mut state = 17_u64;
    let mut random = |length| {
        (0..length)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[0]
            })
            .collect::<Vec<_>>()
    };
    let common = random(3072);
    let mut pages = Vec::new();
    for _ in 0..320 {
        let mut bytes = common.clone();
        bytes.extend(random(1024));
        pages.push(bytes);
    }
    pages[0][..18].copy_from_slice(&page(0)[..18]);
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &pages.concat())?;
    store.publish(true)?;
    let name = RetentionName::new("dictionary-fork")?;
    let pin = store.retain_view(name.clone(), false)?;
    let first = store.inspect()?;
    assert!(first.dictionary_bytes > 0);
    assert!(
        first
            .frame_distribution
            .iter()
            .any(|bin| bin.dictionary_frames > 0)
    );
    let original = pages[1].clone();
    std::fs::write(
        crate::backend::sidecar_dir(&path).join("dictionary.samples"),
        b"corrupt advisory reservoir",
    )?;
    pages[1][4000] ^= 1;
    store.write_at(4096, &pages[1])?;
    store.publish(true)?;
    store.flush_sidecars()?;
    assert_eq!(first.dictionary_bytes, store.inspect()?.dictionary_bytes);
    drop(store);
    let reader = crate::open_retained(&path, &name)?;
    assert_eq!(reader.resolve(PageNumber::new(2)?)?.read()?, original);
    drop(reader);
    crate::release_retention(&path, pin)?;
    crate::collect(&path, 100)?;
    crate::verify(&path)?;
    let dictionary = std::fs::read_dir(crate::backend::sidecar_dir(&path).join("objects"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "dict")
        })
        .ok_or("dictionary missing")?;
    let mut bytes = std::fs::read(&dictionary)?;
    bytes[0] ^= 1;
    std::fs::write(dictionary, bytes)?;
    assert!(crate::verify(&path).is_err());
    assert!(crate::collect(&path, 100).is_err());
    Ok(())
}

#[test]
fn large_shared_dictionaries_decode_reopen_and_survive_offline_retention() -> TestResult {
    use super::frame::install_dictionary;
    use super::view::{ManifestBuilder, ViewMetadata};
    use crate::domain::{
        DatabaseId, HistoryHash, LineageId, LogicalBytes, PageSize, TransactionId,
    };
    for capacity in [512 * 1024, 768 * 1024] {
        let directory = tempfile::tempdir()?;
        let active_path = directory.path().join("large.zsqlite");
        drop(Store::open(&active_path, true)?);
        let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&active_path), false)?;
        let mut guard = catalog.lock()?;
        // Raw-content dictionaries are valid Zstandard dictionaries too. This
        // exercises the full large-object path without a heavyweight trainer.
        let dictionary: Vec<_> = (0..u32::try_from(capacity / 32)?)
            .flat_map(|index| *blake3::hash(&index.to_le_bytes()).as_bytes())
            .collect();
        let page_bytes = dictionary[capacity - 4096..].to_vec();
        let endpoint = super::SealEndpoint {
            dictionary: crate::DictionaryPolicy::new(0, 1024 * 1024)?,
            database: DatabaseId::from_bytes([1; 32]),
            lineage: LineageId::from_bytes([2; 32]),
            size: LogicalBytes::new(4096, PageSize::new(4096)?)?,
            txid: TransactionId::new(2)?,
            history: HistoryHash::from_bytes([3; 32]),
            truncate: None,
        };
        let name = RetentionName::new("offline-large-dictionary")?;
        let (manifest, dictionary_path) = {
            assert!(install_dictionary(&guard, &[]).is_err());
            assert!(
                install_dictionary(
                    &guard,
                    &vec![0; crate::dictionary::MAX_DICTIONARY_BYTES as usize + 1]
                )
                .is_err()
            );
            let receipt = install_dictionary(&guard, &dictionary)?;
            let mut metadata = ViewMetadata::empty(
                endpoint.database,
                endpoint.lineage,
                LogicalBytes::new(0, endpoint.size.page_size())?,
                TransactionId::new(1)?,
                endpoint.history,
            );
            metadata.preferred.push(receipt.id());
            let dictionary_path = guard.path::<super::Dictionary>(receipt.id());
            let mut builder = ManifestBuilder::new(&guard, metadata, None)?;
            builder.dictionary(&receipt)?;
            let empty = builder.finalize()?;
            let source = guard.pin(empty.id())?;
            let sealed = super::seal(
                &guard,
                Some(&source),
                endpoint,
                &[(PageNumber::new(1)?, endpoint.txid)],
                |_| Ok(page_bytes.clone()),
                LayoutPolicy::default(),
                super::ManifestMode::Incremental,
            )?;
            let view = guard.pin(sealed.id())?;
            assert_eq!(view.resolve(PageNumber::new(1)?)?.read()?, page_bytes);
            assert_eq!(view.frame_distribution()[0].dictionary_frames, 1);
            drop(guard.retain(name.clone(), &view, false)?);
            (sealed.id(), dictionary_path)
        };
        assert!(guard.collect(100)?.fork_retained_bytes >= capacity as u64);
        let reopened = guard.pin(manifest)?;
        assert_eq!(reopened.resolve(PageNumber::new(1)?)?.read()?, page_bytes);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..8 {
                        assert_eq!(
                            reopened
                                .resolve(PageNumber::new(1).unwrap())
                                .unwrap()
                                .read()
                                .unwrap(),
                            page_bytes
                        );
                    }
                });
            }
        });
        drop(reopened);
        let mut corrupted = dictionary.clone();
        corrupted[0] ^= 1;
        std::fs::write(&dictionary_path, corrupted)?;
        assert!(guard.pin(manifest).is_err());
        assert!(guard.collect(100).is_err());
        std::fs::write(&dictionary_path, &dictionary)?;
        guard.release(guard.read_root(&name)?)?;
        assert!(guard.collect(100)?.deleted_bytes >= capacity as u64);
        assert!(!dictionary_path.exists());
    }
    Ok(())
}

#[test]
fn seal_sampling_reaches_distinct_pages_after_a_repetitive_prefix() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("representative.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(
        StoragePolicy::default().with_dictionary(crate::DictionaryPolicy::new(8192, 1024 * 1024)?),
    )?;
    // The first 1.2 MiB has just one distinct sample. A prefix-limited sampler
    // cannot train here, despite plenty of useful pages later in the image.
    let mut image = page(7).repeat(300);
    let common: Vec<_> = (0..96_u32)
        .flat_map(|index| *blake3::hash(&index.to_le_bytes()).as_bytes())
        .collect();
    for index in 0..320_u32 {
        image.extend(&common);
        for part in 0..32_u32 {
            image.extend(blake3::hash(&(index * 32 + part + 96).to_le_bytes()).as_bytes());
        }
    }
    store.write_at(0, &image)?;
    store.publish(true)?;
    store.flush_sidecars()?;
    assert_eq!(store.inspect()?.dictionary_bytes, 8192);
    drop(store);
    let mut reopened = Store::open_existing(&path)?;
    let mut actual = vec![0; image.len()];
    reopened.read_at(0, &mut actual)?;
    assert_eq!(actual, image);
    Ok(())
}
