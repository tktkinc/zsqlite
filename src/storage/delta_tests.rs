use super::objects::Manifest;
use super::tests::{TestResult, packs, page};
use crate::domain::PageNumber;
use crate::store::Store;
use crate::{DictionaryPolicy, RetentionName, StoragePolicy};

#[test]
fn metadata_compaction_never_rewrites_payload_packs() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("range-only.zsqlite");
    let mut store = Store::open(&path, true)?;
    let dictionary = DictionaryPolicy::new(0, 1024 * 1024)?;
    store.set_storage_policy(StoragePolicy::default().with_dictionary(dictionary))?;
    store.write_at(0, &[page(1), page(2), page(3), page(4)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let initial = store.inspect()?.manifest.unwrap();
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let objects = catalog.lock()?.all_objects()?;
    let layout = crate::layout::LayoutPolicy::default()
        .fixed(crate::domain::DecodedBytes::new(65536))?
        .with_level(9)?;
    store.set_storage_policy(
        StoragePolicy::default()
            .with_layout(layout)
            .with_dictionary(dictionary),
    )?;
    store.flush_sidecars()?;
    store.compact()?;
    assert_eq!(store.inspect()?.manifest.unwrap().id(), initial.id());
    assert_eq!(catalog.lock()?.all_objects()?, objects);

    store.write_at(4096, &page(5))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let delta = store.inspect()?.manifest.unwrap();
    let delta_packs = packs(&path)?;
    let delta_pack_bytes = delta_packs
        .iter()
        .map(|path| Ok((path.clone(), std::fs::read(path)?)))
        .collect::<Result<std::collections::BTreeMap<_, _>, std::io::Error>>()?;
    assert_eq!(delta.run_depth(), 1);
    store.flush_sidecars()?;
    store.compact()?;
    let info = store.inspect()?;
    let rolled = info.manifest.unwrap();
    assert_eq!(rolled.run_depth(), 0);
    assert_eq!(rolled.logical_hash(), delta.logical_hash());
    assert_eq!(rolled.transaction_span(), delta.transaction_span());
    assert_ne!(rolled.id(), delta.id());
    assert_eq!(packs(&path)?, delta_packs);
    for (path, expected) in delta_pack_bytes {
        assert_eq!(std::fs::read(path)?, expected);
    }
    let objects = catalog.lock()?.all_objects()?;
    store.flush_sidecars()?;
    store.compact()?;
    assert_eq!(store.inspect()?.manifest.unwrap().id(), rolled.id());
    assert_eq!(catalog.lock()?.all_objects()?, objects);
    drop(store);
    let mut store = Store::open_existing(&path)?;
    let mut actual = vec![0; 4 * 4096];
    store.read_at(0, &mut actual)?;
    assert_eq!(actual, [page(1), page(5), page(3), page(4)].concat());
    Ok(())
}

#[test]
fn invalid_segment_encodings_are_rejected() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("latest-only.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &[page(1), page(2), page(3)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let current = store.inspect()?.manifest.unwrap();
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let guard = catalog.lock()?;
    let original = std::fs::read(guard.path::<Manifest>(current.id()))?;
    for magic in [b"BADSEG!!", b"SEGMENT?"] {
        let mut wire = original.clone();
        wire[..8].copy_from_slice(magic);
        let mut builder = guard.build::<Manifest>()?;
        builder.append(&wire)?;
        assert!(
            builder.finalize().is_err(),
            "accepted invalid segment magic"
        );
    }
    let name = guard.path::<Manifest>(current.id());
    let prefixed = name.with_file_name(format!(
        "L00-{}",
        name.file_name().unwrap().to_str().unwrap()
    ));
    std::fs::rename(&name, &prefixed)?;
    assert!(guard.pin(current.id()).is_err());
    assert!(guard.all_objects().is_err());
    std::fs::rename(prefixed, name)?;
    let view = guard.pin(current.id())?;
    assert_eq!(view.resolve(PageNumber::new(2)?)?.read()?, page(2));
    Ok(())
}

#[test]
fn sealed_manifests_are_metadata_only_and_compaction_preserves_packs() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("metadata-only.zsqlite");
    let mut store = Store::open(&path, true)?;
    let layout = crate::layout::LayoutPolicy::default()
        .with_pack_target(crate::domain::StoredBytes::new(65536))?;
    store.set_storage_policy(
        StoragePolicy::default()
            .with_layout(layout)
            .with_dictionary(DictionaryPolicy::new(0, 1024 * 1024)?),
    )?;
    let mut bytes = Vec::new();
    for number in 0..2048_u32 {
        bytes.extend(
            blake3::hash(&number.to_le_bytes())
                .as_bytes()
                .repeat(4096 / 32),
        );
    }
    bytes[..18].copy_from_slice(&page(1)[..18]);
    store.write_at(0, &bytes)?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let base = store.inspect()?.manifest.unwrap();
    let before_packs = packs(&path)?;
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let guard = catalog.lock()?;
    let segment = std::fs::read(guard.path::<Manifest>(base.id()))?;
    let footer_offset = u64::from_le_bytes(
        segment[segment.len() - 16..segment.len() - 8]
            .try_into()
            .expect("fixed trailer offset"),
    );
    assert_eq!(footer_offset, super::segment::HEADER_SIZE as u64);
    drop(guard);
    // A size-only run changes metadata without changing the payload set.
    store.write_at(bytes.len() as u64, &[0; 4096])?;
    bytes.extend([0; 4096]);
    store.publish(true)?;
    store.flush_sidecars()?;
    assert_eq!(packs(&path)?, before_packs);
    store.compact()?;
    let after = store.inspect()?;
    assert_eq!(after.manifest.as_ref().unwrap().run_depth(), 0);
    assert_eq!(packs(&path)?, before_packs);
    assert!(
        after
            .pack_occupancy
            .iter()
            .all(|pack| pack.live_pages == pack.total_pages)
    );
    store.gc_report(100)?;
    assert!(!catalog.lock()?.path::<Manifest>(base.id()).exists());
    drop(store);
    let mut store = Store::open_existing(&path)?;
    let mut actual = vec![0; bytes.len()];
    store.read_at(0, &mut actual)?;
    assert_eq!(actual, bytes);
    store.verify()?;
    Ok(())
}

#[test]
fn logical_parent_selects_widest_rollup_without_breaking_existing_readers() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("logical-parent.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(
        StoragePolicy::default().with_dictionary(DictionaryPolicy::new(0, 1024 * 1024)?),
    )?;
    store.write_at(0, &[page(1), page(2), page(3), page(4)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let base_id = store.inspect()?.manifest.unwrap().id();
    store.write_at(4096, &page(5))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let parent_info = store.inspect()?.manifest.unwrap();
    store.write_at(8192, &page(6))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let child_id = store.inspect()?.manifest.unwrap().id();
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let old_reader = catalog.lock()?.pin(child_id)?;
    drop(store);
    let mut guard = catalog.lock()?;
    let rolled_id = {
        let parent = guard.pin(parent_info.id())?;
        let endpoint = super::SealEndpoint {
            dictionary: DictionaryPolicy::new(0, 1024 * 1024)?,
            database: parent.metadata.database,
            lineage: parent.metadata.lineage,
            size: parent.metadata.size,
            txid: parent.metadata.txid,
            history: parent.metadata.history,
            truncate: None,
        };
        let rolled = super::seal(
            &guard,
            Some(&parent),
            endpoint,
            &parent.versions(),
            |number| parent.resolve(number)?.read(),
            crate::layout::LayoutPolicy::default(),
            super::ManifestMode::Rollup,
        )?;
        let info = guard.pin(rolled.id())?.manifest_statistics()?;
        assert_eq!(info.logical_hash(), parent_info.logical_hash());
        assert_ne!(info.id(), parent_info.id());
        assert_eq!(info.run_depth(), 0);
        let header = guard
            .read_manifest(rolled.id(), 512 * 1024 * 1024)?
            .container()
            .header;
        assert_eq!(header.logical_hash(), parent_info.logical_hash());
        assert!(header.coverage().represented().begin() < header.coverage().full().end());
        rolled.id()
    };
    let new_reader = guard.pin(child_id)?;
    assert_eq!(
        new_reader.manifest_statistics()?.resolved_parent(),
        Some(rolled_id)
    );
    assert_eq!(
        old_reader.manifest_statistics()?.resolved_parent(),
        Some(parent_info.id())
    );
    guard.collect(100)?;
    assert!(guard.path::<Manifest>(base_id).exists());
    assert!(guard.path::<Manifest>(parent_info.id()).exists());
    for (number, expected) in [(1, 1), (2, 5), (3, 6), (4, 4)] {
        assert_eq!(
            old_reader.resolve(PageNumber::new(number)?)?.read()?,
            page(expected)
        );
        assert_eq!(
            new_reader.resolve(PageNumber::new(number)?)?.read()?,
            page(expected)
        );
    }
    drop(old_reader);
    guard.collect(100)?;
    assert!(!guard.path::<Manifest>(base_id).exists());
    assert!(!guard.path::<Manifest>(parent_info.id()).exists());
    assert!(guard.path::<Manifest>(rolled_id).exists());
    drop((new_reader, guard));
    Store::open_existing(&path)?.verify()?;
    Ok(())
}

#[test]
fn offline_descendant_retains_transitive_parents_across_a_rollup() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("descendant.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(
        StoragePolicy::default().with_dictionary(DictionaryPolicy::new(0, 1024 * 1024)?),
    )?;
    let mut bytes = [page(1), page(2), page(3), page(4)].concat();
    store.write_at(0, &bytes)?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let mut ancestors = vec![store.inspect()?.manifest.unwrap().id()];
    for (index, value) in [(1, 5), (2, 6)] {
        bytes[index * 4096..(index + 1) * 4096].copy_from_slice(&page(value));
        store.write_at((index * 4096) as u64, &page(value))?;
        store.publish(true)?;
        store.flush_sidecars()?;
        ancestors.push(store.inspect()?.manifest.unwrap().id());
    }
    let name = RetentionName::new("offline-descendant")?;
    let pin = store.retain_view(name.clone(), false)?;
    let retained = store.retained_view(&name)?;
    drop(store);
    let mut store = Store::open_existing(&path)?;
    store.flush_sidecars()?;
    store.compact()?;
    assert_eq!(store.inspect()?.manifest.unwrap().run_depth(), 0);
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    {
        let guard = catalog.lock()?;
        for id in &ancestors {
            assert!(guard.path::<Manifest>(*id).exists());
        }
    }
    for index in 0..4 {
        assert_eq!(
            retained.resolve(PageNumber::new(index + 1)?)?.read()?,
            bytes[index as usize * 4096..(index as usize + 1) * 4096]
        );
    }
    store.gc_report(100)?;
    // A temporary reader still needs exactly this old parent chain.
    for id in &ancestors {
        assert!(catalog.lock()?.path::<Manifest>(*id).exists());
    }
    drop(retained);
    store.gc_report(100)?;
    for id in ancestors {
        assert!(!catalog.lock()?.path::<Manifest>(id).exists());
    }
    // The durable logical pin now uses the wider rollup, with no dependency on
    // the old chain. It survives later writes and a fully independent rollup.
    let rolled = store.retained_view(&name)?;
    assert_eq!(
        rolled.manifest_statistics()?.logical_hash(),
        pin.logical_hash()
    );
    assert_eq!(rolled.endpoint().1, pin.txid());
    let rolled_id = rolled.id();
    drop(rolled);
    store.write_at(0, &page(9))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    store.compact()?;
    drop(store);
    let mut store = Store::open_existing(&path)?;
    store.gc_report(100)?;
    let retained = store.retained_view(&name)?;
    assert_eq!(retained.id(), rolled_id);
    for index in 0..4 {
        assert_eq!(
            retained.resolve(PageNumber::new(index + 1)?)?.read()?,
            bytes[index as usize * 4096..(index as usize + 1) * 4096]
        );
    }
    // A second named owner of the same hash also keeps its last representation.
    let second = catalog
        .lock()?
        .retain(RetentionName::new("second-owner")?, &retained, false)?;
    store.release_view(pin)?;
    drop(retained);
    store.gc_report(100)?;
    assert!(catalog.lock()?.path::<Manifest>(rolled_id).exists());
    store.release_view(second)?;
    store.gc_report(100)?;
    assert!(!catalog.lock()?.path::<Manifest>(rolled_id).exists());
    store.verify()?;
    Ok(())
}

#[test]
fn tiny_delta_and_metadata_compaction_preserve_pages_packs_and_old_readers() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("delta.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.set_storage_policy(
        StoragePolicy::default().with_dictionary(DictionaryPolicy::new(0, 1024 * 1024)?),
    )?;
    let mut bytes = Vec::new();
    for number in 0..2048_u32 {
        let hash = blake3::hash(&number.to_le_bytes());
        bytes.extend(hash.as_bytes().repeat(4096 / 32));
    }
    bytes[..18].copy_from_slice(&page(1)[..18]);
    store.write_at(0, &bytes)?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let base = store.inspect()?.manifest.unwrap();
    assert_eq!(base.run_depth(), 0);
    let name = RetentionName::new("base")?;
    let pin = store.retain_view(name.clone(), false)?;
    let changed_offset = 123 * 4096;
    bytes[changed_offset] ^= 1;
    store.write_at(
        changed_offset as u64,
        &bytes[changed_offset..changed_offset + 4096],
    )?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let delta = store.inspect()?.manifest.unwrap();
    assert_eq!(delta.resolved_parent(), Some(base.id()));
    assert_eq!(delta.parent(), Some(base.logical_hash()));
    assert_eq!(
        delta.transaction_span().begin(),
        base.transaction_span().begin()
    );
    assert_eq!(
        delta.transaction_span().end().get(),
        store.inspect()?.head_txid
    );
    assert_eq!(delta.run_depth(), 1);
    assert!(delta.head_bytes().get() * 100 < base.head_bytes().get());
    eprintln!(
        "manifest checkpoint={} delta={}",
        base.head_bytes().get(),
        delta.head_bytes().get()
    );
    let before_packs = packs(&path)?;
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let old_reader = catalog.lock()?.pin(delta.id())?;
    let before = store.inspect()?;
    store.compact()?;
    let compacted = store.inspect()?;
    assert_eq!(compacted.head_txid, before.head_txid);
    assert_eq!(compacted.head_history, before.head_history);
    let flat = compacted.manifest.unwrap();
    assert_eq!(flat.run_depth(), 0);
    assert_eq!(flat.transaction_span(), delta.transaction_span());
    assert_eq!(flat.logical_hash(), delta.logical_hash());
    assert_eq!(flat.ancestor_bytes().get(), 0);
    assert_eq!(packs(&path)?, before_packs);
    assert_eq!(
        old_reader.resolve(PageNumber::new(124)?)?.read()?,
        bytes[changed_offset..changed_offset + 4096]
    );
    store.gc_report(100)?;
    assert!(catalog.lock()?.path::<Manifest>(delta.id()).exists());
    drop(old_reader);
    store.release_view(pin)?;
    store.gc_report(100)?;
    let guard = catalog.lock()?;
    // Metadata runs can be collected independently because their referenced
    // payload packs remain separate, content-addressed objects.
    assert!(!guard.path::<Manifest>(delta.id()).exists());
    assert!(!guard.path::<Manifest>(base.id()).exists());
    drop(guard);
    drop(store);
    let mut store = Store::open_existing(&path)?;
    let mut actual = vec![0; bytes.len()];
    store.read_at(0, &mut actual)?;
    assert_eq!(actual, bytes);
    store.verify()?;
    Ok(())
}

#[test]
fn delta_zero_truncate_regrow_and_replacement_cover_every_page_size() -> TestResult {
    for size in [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("zeros.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.set_storage_policy(
            StoragePolicy::default().with_dictionary(DictionaryPolicy::new(0, 1024 * 1024)?),
        )?;
        let mut bytes: Vec<_> = (1..=6).flat_map(|value| vec![value; size]).collect();
        bytes[..16].copy_from_slice(b"SQLite format 3\0");
        bytes[16..18].copy_from_slice(
            &(if size == 65536 {
                1
            } else {
                u16::try_from(size)?
            })
            .to_be_bytes(),
        );
        store.write_at(0, &bytes)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        let pin = store.retain_view(RetentionName::new("old")?, false)?;
        bytes[size..size * 2].fill(0);
        store.write_at(size as u64, &vec![0; size])?;
        store.publish(true)?;
        store.flush_sidecars()?;
        assert_eq!(store.inspect()?.manifest.unwrap().run_depth(), 1);
        store.truncate((size * 4) as u64)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        assert_eq!(store.inspect()?.manifest.unwrap().run_depth(), 2);
        store.truncate((size * 6) as u64)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        bytes[size * 4..].fill(0);
        assert_eq!(store.inspect()?.manifest.unwrap().run_depth(), 3);
        store.write_at((size * 5) as u64, &vec![9; size])?;
        store.discard_pending();
        store.write_at((size * 5) as u64, &vec![8; size])?;
        store.publish(true)?;
        store.flush_sidecars()?;
        bytes[size * 5..].fill(8);
        store.release_view(pin)?;
        store.gc_report(100)?;
        drop(store);
        let mut store = Store::open_existing(&path)?;
        let mut actual = vec![0; bytes.len()];
        store.read_at(0, &mut actual)?;
        assert_eq!(actual, bytes, "page size {size}");
        store.verify()?;
    }
    Ok(())
}

#[test]
fn missing_or_corrupt_ancestor_blocks_open_and_gc_before_any_deletion() -> TestResult {
    for corrupt in [false, true] {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("ancestor.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &[page(1), page(2), page(3)].concat())?;
        store.publish(true)?;
        store.flush_sidecars()?;
        let base = store.inspect()?.manifest.unwrap();
        store.write_at(4096, &page(4))?;
        store.publish(true)?;
        store.flush_sidecars()?;
        assert_eq!(
            store.inspect()?.manifest.unwrap().resolved_parent(),
            Some(base.id())
        );
        let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
        let ancestor = catalog.lock()?.path::<Manifest>(base.id());
        let original = std::fs::read(&ancestor)?;
        if corrupt {
            std::fs::write(&ancestor, b"damaged manifest")?;
        } else {
            std::fs::remove_file(&ancestor)?;
        }
        let objects = catalog.lock()?.all_objects()?;
        assert!(catalog.lock()?.collect(100).is_err());
        assert_eq!(catalog.lock()?.all_objects()?, objects);
        assert!(Store::open_existing(&path).is_err());
        // Restore the test fixture so Store's normal close path can complete.
        std::fs::write(ancestor, original)?;
    }
    Ok(())
}

#[test]
fn ancestor_preferred_dictionaries_are_not_implicit_decoding_roots() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("pool.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &[page(1), page(2), page(3)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let base_id = store.inspect()?.manifest.unwrap().id();
    let catalog = super::Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let mut guard = catalog.lock()?;
    let base = guard.pin(base_id)?;
    let receipt = super::frame::install_dictionary(&guard, &[17; 8192])?;
    let dict_path = guard.path::<super::Dictionary>(receipt.id());
    let mut draft = base.metadata.draft();
    draft.preferred.push(receipt.id());
    let mut builder = super::view::ManifestBuilder::new(&guard, draft, Some(&base))?;
    builder.dictionary(&receipt)?;
    let parent = builder.finalize()?;
    let parent_view = guard.pin(parent.id())?;
    let parent_pin = guard.retain(RetentionName::new("old-pool")?, &parent_view, false)?;
    let mut draft = parent_view.metadata.draft();
    draft.preferred.clear();
    let child = super::view::ManifestBuilder::new(&guard, draft, Some(&parent_view))?.finalize()?;
    let child_id = child.id();
    let child_view = guard.pin(child_id)?;
    drop(guard.retain(RetentionName::new("new-pool")?, &child_view, false)?);
    let parent_id = parent.id();
    drop((receipt, parent, child, parent_view, child_view, base));
    guard.collect(100)?;
    // Pins preserve logical content, not a preferred dictionary pool in an
    // otherwise equivalent metadata-only representation.
    assert!(!dict_path.exists());
    // Equivalent endpoint lookup can bypass this metadata-only pool update.
    assert!(!guard.path::<Manifest>(parent_id).exists());
    assert_eq!(
        guard
            .pin_retained(&parent_pin)?
            .resolve(PageNumber::new(1)?)?
            .read()?,
        page(1)
    );
    guard.release(parent_pin)?;
    Ok(())
}

#[test]
fn compact_failures_preserve_visible_view_and_never_rewrite_packs() -> TestResult {
    for point in super::tests::SEAL_POINTS {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("compact-fault.zsqlite");
        let mut store = Store::open(&path, true)?;
        store.write_at(0, &[page(1), page(2), page(3)].concat())?;
        store.publish(true)?;
        store.flush_sidecars()?;
        store.write_at(4096, &page(4))?;
        assert!(matches!(store.compact(), Err(crate::StoreError::Busy)));
        store.publish(true)?;
        assert!(matches!(store.compact(), Err(crate::StoreError::Busy)));
        store.flush_sidecars()?;
        assert_eq!(store.inspect()?.manifest.unwrap().run_depth(), 1);
        let before = packs(&path)?;
        let injection = super::faults::inject(point, super::faults::Mode::Error);
        assert!(store.compact().is_err(), "{point:?}");
        drop(injection);
        drop(store);
        let mut store = Store::open_existing(&path)?;
        let mut actual = vec![0; 3 * 4096];
        store.read_at(0, &mut actual)?;
        assert_eq!(actual, [page(1), page(4), page(3)].concat(), "{point:?}");
        store.compact()?;
        store.gc_report(100)?;
        assert_eq!(before, packs(&path)?);
        store.verify()?;
    }
    Ok(())
}
