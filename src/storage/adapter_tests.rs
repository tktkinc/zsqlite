use super::adapter::{Fault, ObjectKey, Operation, StorageBackend};
use super::faults::{self, Mode, Point};
use super::tests::{TestResult, page, tiny_packs};
use super::{Catalog, FaultBackend, MemoryBackend, Storage};
use crate::StoreError;
use crate::domain::{BlobBytes, BlobId, BlobOffset, PackId, PackOffset, StoredBytes};
use crate::store::Store;
use std::sync::Arc;

type Fixture = (tempfile::TempDir, Storage, Arc<FaultBackend>, Store);
fn fixture() -> Result<Fixture, Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new()?)));
    let storage = Storage::new(backend.clone(), directory.path().join("coord"))?;
    let path = storage.bind(&directory.path().join("active.zsqlite"))?;
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(tiny_packs())?;
    store.write_at(0, &[page(1), page(2), page(3)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    Ok((directory, storage, backend, store))
}

#[test]
fn bootstrap_ignores_unsealed_and_unfinished_candidates() -> TestResult {
    for unfinished in [false, true] {
        let (directory, storage, _, mut store) = fixture()?;
        let original = store.inspect()?;
        store.write_at(4096, &page(99))?;
        store.publish(true)?;
        if unfinished {
            let _fault = faults::inject(Point::ActiveDataSynced, Mode::Error);
            assert!(store.flush_sidecars().is_err());
        }
        drop(store);
        let restored = storage.bootstrap(directory.path().join("restored.zsqlite"))?;
        let info = restored.inspect()?;
        assert_eq!(info.head_txid, original.head_txid);
        assert_eq!(info.head_history, original.head_history);
        assert_eq!(info.policy, original.policy);
        let mut bytes = vec![0; 3 * 4096];
        restored.read_at(0, &mut bytes)?;
        assert_eq!(bytes, [page(1), page(2), page(3)].concat());
    }
    Ok(())
}

#[test]
fn interrupted_bootstrap_is_retryable_or_already_installed() -> TestResult {
    for point in [
        Point::BootstrapDataSynced,
        Point::BootstrapClaimed,
        Point::BootstrapInstalled,
    ] {
        let (directory, storage, _, store) = fixture()?;
        drop(store);
        let path = directory.path().join("restored.zsqlite");
        {
            let _fault = faults::inject(point, Mode::Error);
            assert!(storage.bootstrap(&path).is_err(), "{point:?}");
        }
        let restored = if path.exists() {
            storage.open(&path)?
        } else {
            storage.bootstrap(&path)?
        };
        restored.verify()?;
        assert!(matches!(
            storage.bootstrap(&path),
            Err(StoreError::DestinationExists(_))
        ));
        let mut bytes = vec![0; 3 * 4096];
        restored.read_at(0, &mut bytes)?;
        assert_eq!(bytes, [page(1), page(2), page(3)].concat());
    }
    Ok(())
}

#[test]
fn uncertain_attachment_claim_is_reread_and_confirmed() -> TestResult {
    let (directory, storage, backend, store) = fixture()?;
    drop(store);
    let path = directory.path().join("restored.zsqlite");
    backend.inject(Operation::Publish, 1, Fault::AfterPublish);
    storage.bootstrap(&path)?.verify()?;
    assert!(path.exists());
    Ok(())
}

#[test]
fn bootstrap_excludes_other_openers_and_fences_copied_pagefiles() -> TestResult {
    let (directory, storage, _, store) = fixture()?;
    let destination = directory.path().join("other.zsqlite");
    assert!(matches!(
        storage.bootstrap(&destination),
        Err(StoreError::Busy)
    ));
    drop(store);
    let restored = storage.bootstrap(&destination)?;
    let copy = directory.path().join("copied.zsqlite");
    std::fs::copy(&destination, &copy)?;
    std::fs::create_dir(crate::backend::sidecar_dir(&copy))?;
    assert!(matches!(
        storage.open(&copy),
        Err(StoreError::StaleAttachment)
    ));
    assert!(matches!(
        storage.open(directory.path().join("active.zsqlite")),
        Err(StoreError::StaleAttachment)
    ));
    drop(restored);
    Ok(())
}

#[test]
fn an_empty_namespace_cannot_have_two_writable_pagefiles() -> TestResult {
    let directory = tempfile::tempdir()?;
    let storage = Storage::new(
        Arc::new(MemoryBackend::new()?),
        directory.path().join("coord"),
    )?;
    let first = storage.create(directory.path().join("one.zsqlite"))?;
    assert!(matches!(
        storage.create(directory.path().join("two.zsqlite")),
        Err(StoreError::Busy)
    ));
    drop(first);
    let path = storage.bind(&directory.path().join("two.zsqlite"))?;
    assert!(matches!(
        Store::open(&path, true),
        Err(StoreError::StaleAttachment)
    ));
    Ok(())
}

#[test]
fn relocation_preserves_manifests_forks_and_exact_old_blob_leases() -> TestResult {
    let (directory, storage, backend, mut store) = fixture()?;
    let path = directory.path().join("active.zsqlite");
    let before = store.inspect()?.manifest.unwrap();
    let fork = store.retain_view(super::RetentionName::new("saved")?, false)?;
    let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let guard = catalog.lock()?;
    let reader = guard.pin(before.id())?;
    let packs: Vec<_> = guard
        .state()
        .placements
        .entries
        .keys()
        .map(|bytes| PackId::from_bytes(bytes.as_slice().try_into().unwrap()))
        .collect();
    assert_eq!(packs.len(), 3);
    let old_blobs: Vec<_> = packs
        .iter()
        .map(|pack| guard.placement(*pack).unwrap().preferred().extent.blob())
        .collect();
    assert!(guard.relocate(&packs, StoredBytes::new(1)).is_err());
    let relocated = guard.relocate(&packs, StoredBytes::new(1024 * 1024))?;
    let blob = relocated.blob.unwrap();
    assert_eq!(relocated.packs, 3);
    let bytes = super::catalog::read_all(backend.as_ref(), ObjectKey::Blob(blob), 1024 * 1024)?;
    assert_eq!(
        super::BlobIndex::authenticate(blob, &bytes)?
            .extents()
            .len(),
        3
    );
    let mut corrupt = bytes.clone();
    corrupt[33] ^= 1;
    assert!(super::BlobIndex::authenticate(blob, &corrupt).is_err());
    drop(guard);
    drop(store);
    let mut store = Store::open_existing(&path)?;
    assert_eq!(store.inspect()?.manifest.unwrap().id(), before.id());
    store.gc_report(usize::MAX)?;
    for blob in &old_blobs {
        assert!(backend.stat(ObjectKey::Blob(*blob))?.is_some());
    }
    assert_eq!(
        reader.resolve(crate::domain::PageNumber::new(2)?)?.read()?,
        page(2)
    );
    drop(reader);
    store.gc_report(usize::MAX)?;
    for blob in &old_blobs {
        assert!(backend.stat(ObjectKey::Blob(*blob))?.is_none());
    }
    let retained = store.retained_view(&super::RetentionName::new("saved")?)?;
    assert_eq!(
        retained
            .resolve(crate::domain::PageNumber::new(3)?)?
            .read()?,
        page(3)
    );
    drop(retained);
    store.release_view(fork)?;
    store.write_at(0, &page(4))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    assert!(store.gc_report(usize::MAX)?.dead_extent_bytes > 0);
    assert!(backend.stat(ObjectKey::Blob(blob))?.is_some());
    store.write_at(4096, &[page(5), page(6)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    store.gc_report(usize::MAX)?;
    assert!(backend.stat(ObjectKey::Blob(blob))?.is_none());
    store.verify()?;
    drop(storage);
    Ok(())
}

#[test]
fn manifest_compaction_does_not_rewrite_placements() -> TestResult {
    let (directory, _storage, backend, mut store) = fixture()?;
    store.write_at(4096, &page(4))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let path = directory.path().join("active.zsqlite");
    let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let placement = catalog.lock()?.state().placements.head;
    backend.reset_statistics();
    store.compact()?;
    assert!(
        backend
            .statistics()
            .put_keys
            .iter()
            .all(|key| matches!(key, ObjectKey::Index(_) | ObjectKey::Manifest(_)))
    );
    assert_eq!(catalog.lock()?.state().placements.head, placement);
    Ok(())
}

#[test]
fn corrupt_required_metadata_blocks_all_deletion() -> TestResult {
    let (_directory, _storage, backend, mut store) = fixture()?;
    backend.reset_statistics();
    backend.inject(Operation::Read, 1, Fault::CorruptRead);
    assert!(store.gc_report(usize::MAX).is_err());
    assert_eq!(backend.statistics().deletes, 0);
    backend.inject(Operation::Read, 1, Fault::ShortRead);
    assert!(store.gc_report(usize::MAX).is_err());
    assert_eq!(backend.statistics().deletes, 0);
    Ok(())
}

#[test]
fn placement_ranges_reject_overflow_containment_and_length_mismatch() {
    let blob = BlobId::from_bytes([1; 32]);
    assert!(super::PackRange::new(PackOffset::new(u64::MAX), StoredBytes::new(1)).is_err());
    assert!(super::PackRange::new(PackOffset::new(0), StoredBytes::new(0)).is_err());
    for (offset, length, blob_length, pack_length) in [
        (u64::MAX, 1, u64::MAX, 1),
        (32, 8, 39, 8),
        (32, 8, 40, 7),
        (32, 0, 40, 0),
    ] {
        assert!(
            super::BlobExtent::new(
                blob,
                BlobOffset::new(offset),
                BlobBytes::new(length),
                BlobBytes::new(blob_length),
                StoredBytes::new(pack_length)
            )
            .is_err()
        );
    }
}

#[test]
fn range_batches_preserve_order_and_merge_only_overlaps_or_neighbors() -> TestResult {
    use crate::domain::PackOffset;
    let (directory, _storage, backend, store) = fixture()?;
    let catalog = Catalog::open(
        &crate::backend::sidecar_dir(&directory.path().join("active.zsqlite")),
        false,
    )?;
    let guard = catalog.lock()?;
    let packs: Vec<_> = guard
        .state()
        .placements
        .entries
        .keys()
        .map(|bytes| PackId::from_bytes(bytes.as_slice().try_into().unwrap()))
        .collect();
    let pin = super::PlacementPin::new(&guard, packs.iter().copied())?;
    let other = super::PlacementPin::new(&guard, packs.iter().copied())?;
    let range = |offset, length| {
        super::PackRange::new(PackOffset::new(offset), StoredBytes::new(length)).unwrap()
    };
    let requests = [
        pin.locate(packs[0], range(8, 8))?,
        pin.locate(packs[0], range(0, 8))?,
        pin.locate(packs[0], range(4, 8))?,
        pin.locate(packs[0], range(24, 8))?,
        pin.locate(packs[1], range(0, 8))?,
    ];
    backend.reset_statistics();
    let result = pin.read_ranges(&requests)?;
    assert_eq!(backend.statistics().batches, 1);
    assert_eq!(backend.statistics().reads, 3);
    assert_eq!(backend.statistics().read_bytes, 32);
    assert_eq!(result[1], b"ZPACK001");
    assert_eq!(result[4], b"ZPACK001");
    assert_eq!(result[2], [&result[1][4..], &result[0][..4]].concat());
    assert!(other.read_ranges(&requests).is_err());
    assert!(
        pin.locate(packs[0], range(pin.length(packs[0])?.get(), 1))
            .is_err()
    );
    backend.inject(Operation::Read, 1, Fault::ShortRead);
    assert!(pin.read_ranges(&requests).is_err());
    drop(guard);
    drop(store);
    Ok(())
}

#[test]
fn bootstrap_rejects_malformed_descriptor_without_claiming_attachment() -> TestResult {
    for damage in 0..3 {
        let (directory, storage, backend, store) = fixture()?;
        drop(store);
        let catalog = Catalog::open(
            &crate::backend::sidecar_dir(&directory.path().join("active.zsqlite")),
            false,
        )?;
        let guard = catalog.lock()?;
        let mut descriptor = guard.state().sealed.unwrap();
        match damage {
            0 => descriptor.base_logical_size += 4096,
            1 => descriptor.start_txid += 1,
            _ => descriptor.base_history[0] ^= 1,
        }
        guard.state_mut().sealed = Some(descriptor);
        guard.publish_catalog()?;
        let revision = backend.read_root()?.unwrap().revision().clone();
        drop(guard);
        let destination = directory.path().join("bad.zsqlite");
        assert!(storage.bootstrap(&destination).is_err());
        assert!(!destination.exists());
        assert_eq!(backend.read_root()?.unwrap().revision(), &revision);
    }
    Ok(())
}

#[test]
fn stale_catalog_writers_cannot_publish_or_collect() -> TestResult {
    let (directory, _storage, backend, store) = fixture()?;
    let catalog = Catalog::open(
        &crate::backend::sidecar_dir(&directory.path().join("active.zsqlite")),
        false,
    )?;
    let mut guard = catalog.lock()?;
    let current = backend.read_root()?.unwrap();
    backend.compare_exchange_root(Some(current.revision()), current.bytes())?;
    assert!(matches!(
        guard.publish_catalog(),
        Err(StoreError::Backend(super::BackendError::Stale))
    ));
    backend.reset_statistics();
    assert!(guard.collect(usize::MAX).is_err());
    assert_eq!(backend.statistics().deletes, 0);
    drop(store);
    Ok(())
}

#[test]
fn concurrent_bootstraps_install_only_one_attachment() -> TestResult {
    let (directory, storage, _, store) = fixture()?;
    drop(store);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let jobs: Vec<_> = (0..2)
        .map(|index| {
            let storage = storage.clone();
            let barrier = barrier.clone();
            let path = directory.path().join(format!("restore-{index}.zsqlite"));
            std::thread::spawn(move || {
                barrier.wait();
                storage.bootstrap(path)
            })
        })
        .collect();
    let results: Vec<_> = jobs.into_iter().map(|job| job.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(
        results
            .iter()
            .any(|result| matches!(result, Err(StoreError::Busy)))
    );
    for database in results.into_iter().flatten() {
        database.verify()?;
    }
    Ok(())
}

#[test]
fn bootstrap_survives_process_exit_at_every_install_boundary() -> TestResult {
    for index in 0..3 {
        let directory = tempfile::tempdir()?;
        let backend = directory.path().join("sealed");
        let coordination = directory.path().join("coord");
        let storage = Storage::new(
            Arc::new(super::FilesystemBackend::open(&backend)?),
            &coordination,
        )?;
        let source = directory.path().join("source.zsqlite");
        storage.bind(&source)?;
        let mut store = Store::open(&source, true)?;
        store.write_at(0, &page(42))?;
        store.publish(true)?;
        store.flush_sidecars()?;
        drop(store);
        let destination = directory.path().join("restore.zsqlite");
        let status = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "storage::adapter_tests::bootstrap_crash_child",
                "--nocapture",
            ])
            .env("ZSQLITE_BOOTSTRAP_CRASH_BACKEND", &backend)
            .env("ZSQLITE_BOOTSTRAP_CRASH_COORD", &coordination)
            .env("ZSQLITE_BOOTSTRAP_CRASH_DEST", &destination)
            .env("ZSQLITE_BOOTSTRAP_CRASH_POINT", index.to_string())
            .stdout(std::process::Stdio::null())
            .status()?;
        assert_eq!(status.code(), Some(73));
        let restored = if destination.exists() {
            storage.open(&destination)?
        } else {
            storage.bootstrap(&destination)?
        };
        restored.verify()?;
        let mut bytes = vec![0; 4096];
        restored.read_at(0, &mut bytes)?;
        assert_eq!(bytes, page(42));
    }
    Ok(())
}

#[test]
fn bootstrap_crash_child() -> TestResult {
    let Some(backend) = std::env::var_os("ZSQLITE_BOOTSTRAP_CRASH_BACKEND") else {
        return Ok(());
    };
    let coordination = std::env::var_os("ZSQLITE_BOOTSTRAP_CRASH_COORD").unwrap();
    let destination = std::env::var_os("ZSQLITE_BOOTSTRAP_CRASH_DEST").unwrap();
    let index: usize = std::env::var("ZSQLITE_BOOTSTRAP_CRASH_POINT")?.parse()?;
    let storage = Storage::new(
        Arc::new(super::FilesystemBackend::open(backend)?),
        coordination,
    )?;
    let _injection = faults::inject(
        [
            Point::BootstrapDataSynced,
            Point::BootstrapClaimed,
            Point::BootstrapInstalled,
        ][index],
        Mode::Crash,
    );
    let _database = storage.bootstrap(destination)?;
    panic!("bootstrap did not reach injected crash boundary");
}

#[test]
fn bootstrap_starts_with_an_empty_cache_and_preserves_active_invalidation() -> TestResult {
    use crate::domain::CacheBytes;
    let (directory, storage, backend, store) = fixture()?;
    drop(store);
    let restored = storage.bootstrap(directory.path().join("cache.zsqlite"))?;
    let mut store = restored.into_store()?;
    assert_eq!(store.cache_stats().resident_bytes, 0);
    let policy = store.inspect()?.policy;
    store.set_storage_policy(
        policy.with_layout(policy.layout().with_cache(CacheBytes::new(4096))?),
    )?;
    let mut bytes = vec![0; 4096];
    backend.reset_statistics();
    store.read_at(4096, &mut bytes)?;
    assert_eq!(bytes, page(2));
    assert_eq!(store.cache_stats().resident_bytes, 4096);
    let reads = backend.statistics().reads;
    store.read_at(4096, &mut bytes)?;
    assert_eq!(backend.statistics().reads, reads);
    assert!(store.cache_stats().hits > 0);
    store.read_at(8192, &mut bytes)?;
    assert_eq!(bytes, page(3));
    assert_eq!(store.cache_stats().resident_bytes, 4096);
    store.read_at(4096, &mut bytes)?;
    assert!(backend.statistics().reads > reads);
    store.write_at(4096, &page(99))?;
    assert_eq!(store.cache_stats().resident_bytes, 0);
    store.read_at(4096, &mut bytes)?;
    assert_eq!(bytes, page(99));
    store.publish(true)?;
    backend.reset_statistics();
    assert!(matches!(store.compact(), Err(StoreError::Busy)));
    assert_eq!(backend.statistics().puts, 0);
    assert_eq!(backend.statistics().publications, 0);
    store.flush_sidecars()?;
    store.compact()?;
    store.verify()?;
    Ok(())
}

#[test]
fn relocation_faults_never_delete_sources_or_advance_logical_history() -> TestResult {
    for (operation, fault) in [
        (Operation::Put, Fault::Before),
        (Operation::Publish, Fault::Before),
        (Operation::Publish, Fault::AfterPublish),
    ] {
        let (directory, _storage, backend, mut store) = fixture()?;
        let before = store.inspect()?;
        let path = directory.path().join("active.zsqlite");
        let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
        let mut guard = catalog.lock()?;
        let packs: Vec<_> = guard
            .state()
            .placements
            .entries
            .keys()
            .map(|bytes| PackId::from_bytes(bytes.as_slice().try_into().unwrap()))
            .collect();
        let sources: Vec<_> = packs
            .iter()
            .map(|pack| ObjectKey::Blob(guard.placement(*pack).unwrap().preferred().extent.blob()))
            .collect();
        backend.inject(operation, 1, fault);
        assert!(
            guard
                .relocate(&packs, StoredBytes::new(1024 * 1024))
                .is_err()
        );
        if operation == Operation::Publish {
            assert!(guard.collect(usize::MAX).is_err());
        }
        for source in &sources {
            assert!(backend.stat(*source)?.is_some());
        }
        drop(guard);
        store.verify()?;
        assert_eq!(store.inspect()?.head_history, before.head_history);
        assert_eq!(
            store.inspect()?.manifest.unwrap().logical_hash(),
            before.manifest.unwrap().logical_hash()
        );
        drop(store);
        let mut store = Store::open_existing(&path)?;
        store.gc_report(usize::MAX)?;
        store.verify()?;
    }
    Ok(())
}

#[test]
fn writable_forks_share_objects_and_keep_independent_sealed_heads() -> TestResult {
    let (directory, storage, backend, mut main) = fixture()?;
    let initial = main.inspect()?.head_history;
    let parent = main.inspect()?.manifest.unwrap().logical_hash();
    let fork_storage = storage.fork("experiment")?;
    assert!(storage.fork("experiment").is_err());
    assert!(storage.head("../bad").is_err());
    assert_eq!(storage.heads()?, ["experiment", "main"]);
    let fork_path = directory.path().join("fork.zsqlite");
    let fork_db = fork_storage.bootstrap(&fork_path)?;
    assert!(matches!(fork_storage.remove_head(), Err(StoreError::Busy)));
    drop(fork_db);
    let mut fork = Store::open_existing(&fork_path)?;
    assert_eq!(fork.inspect()?.head_history, initial);
    main.write_at(4096, &page(41))?;
    main.publish(true)?;
    fork.write_at(4096, &page(42))?;
    fork.publish(true)?;
    // Mutable publication only changes the transaction counter. It does not
    // read/hash every modified page to construct an intermediate history.
    assert_eq!(main.inspect()?.head_history, initial);
    assert_eq!(fork.inspect()?.head_history, initial);
    main.flush_sidecars()?;
    fork.flush_sidecars()?;
    let main_info = main.inspect()?;
    let fork_info = fork.inspect()?;
    assert_eq!(main_info.head_txid, fork_info.head_txid);
    assert_eq!(
        main_info.manifest.as_ref().unwrap().sealed_parent(),
        Some(parent)
    );
    assert_eq!(
        fork_info.manifest.as_ref().unwrap().sealed_parent(),
        Some(parent)
    );
    main.compact()?;
    assert_eq!(
        main.inspect()?.manifest.unwrap().sealed_parent(),
        Some(parent)
    );
    assert_ne!(main_info.head_history, fork_info.head_history);
    assert_ne!(
        main_info.manifest.as_ref().unwrap().id(),
        fork_info.manifest.as_ref().unwrap().id()
    );
    main.gc_report(usize::MAX)?;
    fork.gc_report(usize::MAX)?;
    main.verify()?;
    fork.verify()?;
    drop(fork);
    // Replacing one fork's local instance leaves the other writable head live.
    let restored = fork_storage.bootstrap(directory.path().join("fork-again.zsqlite"))?;
    let mut bytes = vec![0; 4096];
    restored.read_at(4096, &mut bytes)?;
    assert_eq!(bytes, page(42));
    main.read_at(4096, &mut bytes)?;
    assert_eq!(bytes, page(41));
    assert!(matches!(
        fork_storage.open(&fork_path),
        Err(StoreError::StaleAttachment)
    ));
    drop(restored);
    // The source remains usable after the fork head and its exact readers die.
    fork_storage.remove_head()?;
    let objects_before = backend.statistics().deletes;
    main.gc_report(usize::MAX)?;
    assert!(backend.statistics().deletes > objects_before);
    main.verify()?;
    assert_eq!(storage.heads()?, ["main"]);
    Ok(())
}

#[test]
fn intermediate_writes_do_not_change_an_equivalent_sealed_result() -> TestResult {
    let (directory, storage, _, mut main) = fixture()?;
    let left = storage.fork("left")?;
    let right = storage.fork("right")?;
    let left_path = directory.path().join("left.zsqlite");
    let right_path = directory.path().join("right.zsqlite");
    drop(left.bootstrap(&left_path)?);
    drop(right.bootstrap(&right_path)?);
    let mut left = Store::open_existing(&left_path)?;
    let mut right = Store::open_existing(&right_path)?;
    for (a, b) in [(11, 12), (99, 99)] {
        left.write_at(4096, &page(a))?;
        right.write_at(4096, &page(b))?;
        left.publish(true)?;
        right.publish(true)?;
    }
    left.flush_sidecars()?;
    right.flush_sidecars()?;
    assert_eq!(left.inspect()?.head_history, right.inspect()?.head_history);
    assert_eq!(
        left.inspect()?.manifest.unwrap().id(),
        right.inspect()?.manifest.unwrap().id()
    );
    main.verify()?;
    Ok(())
}

#[test]
fn concurrent_and_uncertain_fork_creation_never_replace_a_head() -> TestResult {
    let (_directory, storage, backend, _main) = fixture()?;
    backend.inject(Operation::Publish, 1, Fault::AfterPublish);
    let existing = storage.fork("uncertain")?;
    assert_eq!(existing.head_name(), "uncertain");
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let storage = storage.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                storage.fork("raced").is_ok()
            })
        })
        .collect();
    let applied = threads
        .into_iter()
        .map(|thread| usize::from(thread.join().expect("fork creator")))
        .sum::<usize>();
    assert_eq!(applied, 1);
    assert_eq!(storage.heads()?, ["main", "raced", "uncertain"]);
    Ok(())
}
