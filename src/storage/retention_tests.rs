use super::super::tests::{TestResult, page};
use super::*;
use crate::domain::PageNumber;
use crate::storage::Catalog;
use crate::store::Store;

fn root_bytes(guard: &CatalogGuard, name: &RetentionName) -> Vec<u8> {
    guard.state().retentions.entries[name.as_str().as_bytes()].clone()
}
fn write_root_bytes(
    guard: &CatalogGuard,
    name: &RetentionName,
    bytes: &[u8],
) -> Result<(), StoreError> {
    guard
        .state_mut()
        .retentions
        .insert(name.as_str().as_bytes().to_vec(), bytes.to_vec());
    guard.publish_catalog()
}

#[test]
fn logical_roots_are_small_and_same_endpoint_replacement_rejects_stale_handles() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("logical-pin.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &page(1))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    let info = store.inspect()?.manifest.unwrap();
    let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let before = catalog.lock()?.all_objects()?;
    let name = RetentionName::new("backup")?;
    let stale = store.retain_view(name.clone(), false)?;
    assert!(matches!(
        store.retain_view(name.clone(), false),
        Err(StoreError::RetentionExists(existing)) if existing == "backup"
    ));
    assert_eq!(stale.logical_hash(), info.logical_hash());
    assert_eq!(stale.txid(), info.transaction_span().end());
    assert_eq!(catalog.lock()?.all_objects()?, before);
    let bytes = root_bytes(&catalog.lock()?, &name);
    assert_eq!(bytes.len(), 144);
    assert_eq!(&bytes[..8], b"ZROOT001");
    assert_eq!(&bytes[40..72], stale.logical_hash().as_bytes());

    let current = store.retain_view(name.clone(), true)?;
    assert_eq!(stale.logical_hash(), current.logical_hash());
    assert_ne!(stale.revision, current.revision);
    assert!(matches!(store.release_view(stale), Err(StoreError::Busy)));
    let stale = catalog.lock()?.read_root(&name)?;
    store.release_view(current)?;
    let recreated = store.retain_view(name.clone(), false)?;
    assert_eq!(stale.logical_hash(), recreated.logical_hash());
    assert!(matches!(store.release_view(stale), Err(StoreError::Busy)));
    store.release_view(recreated)?;
    Ok(())
}

#[test]
fn invalid_or_missing_logical_roots_stop_gc_before_any_deletion() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("invalid-logical-pin.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &page(1))?;
    store.publish(true)?;
    let name = RetentionName::new("offline")?;
    let pin = store.retain_view(name.clone(), false)?;
    let original = store.inspect()?.manifest.unwrap();
    store.write_at(0, &page(2))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    store.compact()?;
    let current = store.inspect()?.manifest.unwrap();
    assert!(current.transaction_span().begin() <= pin.txid());
    assert!(current.transaction_span().end() > pin.txid());
    assert_ne!(current.logical_hash(), pin.logical_hash());
    let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let guard = catalog.lock()?;
    let original_path = guard.path::<Manifest>(original.id());
    // An unrelated unreachable object makes zero-deletion assertions meaningful.
    drop(super::super::frame::install_dictionary(
        &guard,
        &[99; 8192],
    )?);
    let before = guard.all_objects()?;
    drop(guard);
    let root = root_bytes(&catalog.lock()?, &name);
    let mut bad_roots = Vec::new();
    for offset in [8, 40, 72] {
        let mut corrupt = root.clone();
        corrupt[offset] ^= 0x80;
        let digest = blake3::hash(&corrupt[..112]);
        corrupt[112..].copy_from_slice(digest.as_bytes());
        bad_roots.push(corrupt);
    }
    let mut zero_txid = root.clone();
    zero_txid[72..80].fill(0);
    let digest = blake3::hash(&zero_txid[..112]);
    zero_txid[112..].copy_from_slice(digest.as_bytes());
    bad_roots.extend([zero_txid, root[..143].to_vec(), vec![0; 4096]]);
    for bad in bad_roots {
        write_root_bytes(&catalog.lock()?, &name, &bad)?;
        assert!(store.retained_view(&name).is_err());
        assert!(store.gc_report(100).is_err());
        assert_eq!(catalog.lock()?.all_objects()?, before);
    }
    write_root_bytes(&catalog.lock()?, &name, &root)?;
    // A newer hash with wider coverage cannot replace this missing endpoint.
    let saved = directory.path().join("saved-segment");
    std::fs::rename(&original_path, &saved)?;
    let missing = catalog.lock()?.all_objects()?;
    assert!(store.retained_view(&name).is_err());
    assert!(store.gc_report(100).is_err());
    assert_eq!(catalog.lock()?.all_objects()?, missing);
    std::fs::rename(saved, original_path)?;
    store.gc_report(100)?;
    assert_eq!(
        store
            .retained_view(&name)?
            .resolve(PageNumber::new(1)?)?
            .read()?,
        page(1)
    );
    store.release_view(pin)?;
    Ok(())
}

#[test]
fn corrupt_wider_representation_cannot_authorize_gc_or_root_replacement() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("bad-rollup.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &[page(1), page(2)].concat())?;
    store.publish(true)?;
    store.flush_sidecars()?;
    store.write_at(4096, &page(3))?;
    store.publish(true)?;
    let name = RetentionName::new("offline")?;
    let pin = store.retain_view(name.clone(), false)?;
    let old_reader = store.retained_view(&name)?;
    store.flush_sidecars()?;
    store.compact()?;
    let rolled = store.inspect()?.manifest.unwrap();
    assert_ne!(rolled.id(), old_reader.id());
    store.write_at(0, &page(4))?;
    store.publish(true)?;
    store.flush_sidecars()?;
    store.compact()?;
    let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let mut guard = catalog.lock()?;
    let rolled_path = guard.path::<Manifest>(rolled.id());
    let original = std::fs::read(&rolled_path)?;
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 1;
    std::fs::write(&rolled_path, corrupt)?;
    drop(super::super::frame::install_dictionary(
        &guard,
        &[99; 8192],
    )?);
    let before = guard.all_objects()?;
    let old_root = root_bytes(&guard, &name);
    assert!(guard.pin_retained(&pin).is_err());
    // The supplied narrower reader is valid, but replacement must also validate
    // the wider representation that future logical lookup would actually use.
    assert!(guard.retain(name.clone(), &old_reader, true).is_err());
    assert_eq!(root_bytes(&guard, &name), old_root);
    assert!(guard.collect(100).is_err());
    assert_eq!(guard.all_objects()?, before);
    std::fs::write(rolled_path, original)?;
    assert_eq!(guard.pin_retained(&pin)?.id(), rolled.id());
    guard.release(pin)?;
    Ok(())
}

#[test]
fn invalid_root_records_stop_collection_without_mutation() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("invalid-root.zsqlite");
    let mut store = Store::open(&path, true)?;
    store.write_at(0, &[page(1), page(2)].concat())?;
    store.publish(true)?;
    let name = RetentionName::new("offline")?;
    let pin = store.retain_view(name.clone(), false)?;
    let catalog = Catalog::open(&crate::backend::sidecar_dir(&path), false)?;
    let mut guard = catalog.lock()?;
    let original = root_bytes(&guard, &name);
    let before = guard.all_objects()?;
    let mut truncated = b"ZROOT001".to_vec();
    truncated.extend(pin.database().as_bytes());
    truncated.extend([0_u8; 32]);
    truncated.extend(*blake3::hash(&truncated).as_bytes());
    for bytes in [truncated, {
        let mut wrong_magic = original.clone();
        wrong_magic[..8].copy_from_slice(b"BADROOT!");
        let hash = *blake3::hash(&wrong_magic[..112]).as_bytes();
        wrong_magic[112..].copy_from_slice(&hash);
        wrong_magic
    }] {
        write_root_bytes(&guard, &name, &bytes)?;
        assert!(guard.read_root(&name).is_err());
        assert!(guard.collect(100).is_err());
        assert_eq!(guard.all_objects()?, before);
        assert_eq!(root_bytes(&guard, &name), bytes);
    }
    write_root_bytes(&guard, &name, &original)?;
    assert_eq!(guard.pin_retained(&pin)?.logical_size().get(), 8192);
    guard.release(pin)?;
    Ok(())
}
