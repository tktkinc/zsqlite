//! Executable specification for asynchronous tier publication. This is a test
//! protocol harness, not a production tier adapter. It uses real immutable
//! objects, catalog traversal, OS leases, GC, CAS, and bootstrap verification.
//! Scheduling is deterministic: dropping an Upload models a stopped worker;
//! delayed requests remain explicit values and can complete after restart.

use super::adapter::{
    BackendError, Fault, ObjectKey, Operation, Publication, Revision, RootRecord,
};
use super::catalog::{put_bytes, read_all};
use super::objects::ObjectKey as LogicalKey;
use super::tests::{TestResult, page};
use super::{Catalog, FaultBackend, FilesystemBackend, MemoryBackend, Storage, StorageBackend};
use crate::domain::{ManifestId, PageNumber};
use crate::fs::SharedLock;
use crate::store::Store;
use crate::{RetentionName, StoreError};
use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Arc;

const OBJECT_LIMIT: u64 = 64 * 1024 * 1024;

struct Local {
    directory: tempfile::TempDir,
    storage: Storage,
    store: Store,
    expected: Vec<u8>,
}

impl Local {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let backend = Arc::new(FilesystemBackend::open(directory.path().join("objects"))?);
        let storage = Storage::new(backend, directory.path().join("coord"))?;
        let path = storage.bind(&directory.path().join("local.zsqlite"))?;
        let mut store = Store::open(&path, true)?;
        let expected = [page(1), page(2), page(3), page(4)].concat();
        store.write_at(0, &expected)?;
        store.publish(true)?;
        store.flush_sidecars()?;
        Ok(Self {
            directory,
            storage,
            store,
            expected,
        })
    }

    fn overwrite(&mut self, value: u8) -> TestResult {
        let bytes = [page(value), page(value + 1), page(value + 2)].concat();
        self.store.write_at(0, &bytes)?;
        self.store.publish(true)?;
        self.store.flush_sidecars()?;
        self.expected[..bytes.len()].copy_from_slice(&bytes);
        Ok(())
    }

    fn snapshot(&self) -> Result<Arc<Snapshot>, StoreError> {
        let catalog = Catalog::configured(
            self.storage.clone(),
            self.directory.path().join("local.zsqlite"),
        );
        let guard = catalog.lock()?;
        // The root and all exact dependencies are captured while GC/publication
        // are excluded. Logical retention alone would not retain old indexes.
        assert!(!guard.state().has_pending()?);
        let mut keys = guard.state().index_objects();
        let mut retain = |view: super::PinnedView| -> Result<(), StoreError> {
            for key in view.metadata.dependencies(view.id()) {
                keys.insert(match key {
                    LogicalKey::Manifest(id) => ObjectKey::Manifest(id),
                    LogicalKey::Dictionary(id) => ObjectKey::Dictionary(id),
                    LogicalKey::Pack(id) => {
                        ObjectKey::Blob(guard.placement(id)?.preferred().extent.blob())
                    }
                    LogicalKey::Staging(_) => return Err(StoreError::Corrupt(0)),
                });
            }
            Ok(())
        };
        for head in guard.state().all_heads()? {
            if let Some(header) = head.sealed {
                retain(guard.pin(ManifestId::from_bytes(header.parent_physical_digest))?)?;
            }
        }
        for name in guard.root_names()? {
            retain(guard.pin_retained(&guard.read_root(&name)?)?)?;
        }
        let leases = keys
            .iter()
            .map(|key| SharedLock::acquire(&guard.physical_lease_path(*key)))
            .collect::<Result<Vec<_>, _>>()?;
        let root = self
            .storage
            .backend()
            .read_root()?
            .ok_or(StoreError::NoSealedHead)?;
        Ok(Arc::new(Snapshot {
            root,
            keys,
            _leases: leases,
        }))
    }
}

struct Snapshot {
    root: RootRecord,
    keys: BTreeSet<ObjectKey>,
    _leases: Vec<SharedLock>,
}

struct Remote {
    directory: tempfile::TempDir,
    backend: Arc<FaultBackend>,
    storage: Storage,
}

impl Remote {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let backend = Arc::new(FaultBackend::new(Arc::new(MemoryBackend::new()?)));
        let storage = Storage::new(backend.clone(), directory.path().join("coord"))?;
        Ok(Self {
            directory,
            backend,
            storage,
        })
    }

    fn catalog(&self) -> Catalog {
        Catalog::configured(
            self.storage.clone(),
            self.directory.path().join("never-attached.zsqlite"),
        )
    }

    fn collect(&self) -> Result<super::GcReport, StoreError> {
        self.catalog().lock()?.collect(usize::MAX)
    }

    fn revision(&self) -> Result<Option<Revision>, BackendError> {
        Ok(self
            .backend
            .read_root()?
            .map(|root| root.revision().clone()))
    }

    fn fence(&self) -> Result<Revision, BackendError> {
        let root = self.backend.read_root()?.ok_or(BackendError::InvalidData)?;
        let Publication::Applied(revision) = self
            .backend
            .compare_exchange_root(Some(root.revision()), root.bytes())?
        else {
            return Err(BackendError::Stale);
        };
        assert_ne!(
            revision,
            *root.revision(),
            "equal bytes must still get a fresh revision"
        );
        Ok(revision)
    }
}

struct Upload {
    snapshot: Arc<Snapshot>,
    expected: Option<Revision>,
    remote_leases: Vec<SharedLock>,
}

impl Upload {
    fn new(snapshot: Arc<Snapshot>, remote: &Remote) -> Result<Self, BackendError> {
        Ok(Self {
            snapshot,
            expected: remote.revision()?,
            remote_leases: Vec::new(),
        })
    }

    fn copy(&mut self, local: &Local, remote: &Remote, key: ObjectKey) -> Result<(), StoreError> {
        assert!(self.snapshot.keys.contains(&key));
        let bytes = read_all(local.storage.backend(), key, OBJECT_LIMIT)?;
        // Installation and registration are serialized with remote collection.
        // This models ownership; it is not permission to forward local permits.
        let guard = remote.catalog().lock()?;
        put_bytes(remote.backend.as_ref(), key, &bytes)?;
        self.remote_leases
            .push(SharedLock::acquire(&guard.physical_lease_path(key))?);
        Ok(())
    }

    fn copy_all(&mut self, local: &Local, remote: &Remote) -> Result<(), StoreError> {
        for key in self.snapshot.keys.clone() {
            self.copy(local, remote, key)?;
        }
        Ok(())
    }

    fn publish(&self, local: &Local, remote: &Remote) -> Result<Publication, StoreError> {
        let _guard = remote.catalog().lock()?;
        // Successful stat is insufficient: verify the dependency bytes. A real
        // adapter can retain authenticated completion receipts instead.
        for key in &self.snapshot.keys {
            if read_all(remote.backend.as_ref(), *key, OBJECT_LIMIT)?
                != read_all(local.storage.backend(), *key, OBJECT_LIMIT)?
            {
                return Err(StoreError::Corrupt(0));
            }
        }
        Ok(remote
            .backend
            .compare_exchange_root(self.expected.as_ref(), self.snapshot.root.bytes())?)
    }
}

fn applied(publication: Publication) -> Revision {
    match publication {
        Publication::Applied(revision) => revision,
        other => panic!("expected applied publication, got {other:?}"),
    }
}

fn publish(local: &Local, remote: &Remote, snapshot: Arc<Snapshot>) -> TestResult {
    let mut upload = Upload::new(snapshot, remote)?;
    upload.copy_all(local, remote)?;
    applied(upload.publish(local, remote)?);
    Ok(())
}

fn inventory(backend: &dyn StorageBackend) -> Result<BTreeSet<ObjectKey>, BackendError> {
    let mut keys = BTreeSet::new();
    loop {
        let batch = backend.inventory(keys.last().copied(), 4096)?;
        if batch.is_empty() {
            return Ok(keys);
        }
        keys.extend(batch);
    }
}

/// Bootstrap changes the attachment in its backend. Clone the published remote
/// namespace byte for byte so the oracle does not perturb tested CAS revisions.
/// Restore gets no local objects, caches, lease state, or catalog reconstruction.
fn restored(remote: &Remote, expected: &[(&str, &[u8])]) -> TestResult {
    let backend = Arc::new(MemoryBackend::new()?);
    for key in inventory(remote.backend.as_ref())? {
        put_bytes(
            backend.as_ref(),
            key,
            &read_all(remote.backend.as_ref(), key, OBJECT_LIMIT)?,
        )?;
    }
    let root = remote.backend.read_root()?.ok_or("missing remote root")?;
    applied(backend.compare_exchange_root(None, root.bytes())?);
    let directory = tempfile::tempdir()?;
    let storage = Storage::new(backend, directory.path().join("coord"))?;
    for (name, bytes) in expected {
        let database = storage
            .head(name)?
            .bootstrap(directory.path().join(format!("{name}.zsqlite")))?;
        database.verify()?;
        let mut actual = vec![0; bytes.len()];
        assert_eq!(database.read_at(0, &mut actual)?, bytes.len());
        assert_eq!(actual, *bytes, "restored head {name}");
        assert_eq!(database.inspect()?.logical_size, bytes.len() as u64);
    }
    Ok(())
}

#[test]
fn every_missing_dependency_blocks_publication_until_its_upload_completes() -> TestResult {
    let mut local = Local::new()?;
    local.overwrite(10)?;
    let snapshot = local.snapshot()?;
    for omitted in &snapshot.keys {
        let remote = Remote::new()?;
        let mut upload = Upload::new(snapshot.clone(), &remote)?;
        for key in snapshot.keys.iter().rev().filter(|key| *key != omitted) {
            upload.copy(&local, &remote, *key)?;
        }
        assert!(
            upload.publish(&local, &remote).is_err(),
            "omitted {omitted:?}"
        );
        assert!(remote.backend.read_root()?.is_none());
        // A connection can disappear midway through an immutable upload.
        // Dropping the writer must leave no visible partial object.
        let bytes = read_all(local.storage.backend(), *omitted, OBJECT_LIMIT)?;
        let mut interrupted = remote.backend.begin_write()?;
        interrupted.write_all(&bytes[..bytes.len() / 2])?;
        drop(interrupted);
        assert!(remote.backend.stat(*omitted)?.is_none());
        // Negative control: a transport accepts opaque root bytes. Bypassing
        // closure validation must produce an actually unrestorable catalog.
        let broken = Remote::new()?;
        for key in inventory(remote.backend.as_ref())? {
            put_bytes(
                broken.backend.as_ref(),
                key,
                &read_all(remote.backend.as_ref(), key, OBJECT_LIMIT)?,
            )?;
        }
        applied(
            broken
                .backend
                .compare_exchange_root(None, snapshot.root.bytes())?,
        );
        assert!(
            restored(&broken, &[("main", &local.expected)]).is_err(),
            "oracle missed absent {omitted:?}"
        );
        upload.copy(&local, &remote, *omitted)?;
        applied(upload.publish(&local, &remote)?);
        restored(&remote, &[("main", &local.expected)])?;
    }
    Ok(())
}

#[test]
fn every_interrupted_upload_prefix_preserves_old_root_and_can_restart() -> TestResult {
    let mut local = Local::new()?;
    let old = local.snapshot()?;
    let old_bytes = local.expected.clone();
    local.overwrite(20)?;
    let next = local.snapshot()?;
    let keys = next.keys.iter().copied().collect::<Vec<_>>();
    for reverse in [false, true] {
        for prefix in 0..=keys.len() {
            let remote = Remote::new()?;
            publish(&local, &remote, old.clone())?;
            let mut upload = Upload::new(next.clone(), &remote)?;
            let mut order = keys.clone();
            if reverse {
                order.reverse();
            }
            let mut uploaded = BTreeSet::new();
            for key in order.into_iter().take(prefix) {
                upload.copy(&local, &remote, key)?;
                // Retry the same immutable PUT with a lost completion receipt.
                upload.copy(&local, &remote, key)?;
                uploaded.insert(key);
            }
            remote.collect()?;
            for key in uploaded {
                assert!(
                    remote.backend.stat(key)?.is_some(),
                    "collected in-flight upload {key:?}"
                );
            }
            restored(&remote, &[("main", &old_bytes)])?;
            // Worker death abandons only this upload, never the published root.
            drop(upload);
            remote.fence()?;
            remote.collect()?;
            restored(&remote, &[("main", &old_bytes)])?;
            publish(&local, &remote, next.clone())?;
            restored(&remote, &[("main", &local.expected)])?;
        }
    }
    Ok(())
}

#[test]
fn failed_puts_and_unreadable_uploads_cannot_advance_remote_root() -> TestResult {
    let mut local = Local::new()?;
    let old = local.snapshot()?;
    let old_bytes = local.expected.clone();
    let remote = Remote::new()?;
    publish(&local, &remote, old)?;
    local.overwrite(30)?;
    let next = local.snapshot()?;
    let mut upload = Upload::new(next, &remote)?;
    let key = *upload.snapshot.keys.first().unwrap();
    remote.backend.inject(Operation::Put, 1, Fault::Before);
    assert!(upload.copy(&local, &remote, key).is_err());
    restored(&remote, &[("main", &old_bytes)])?;
    upload.copy_all(&local, &remote)?;
    let mut conflicting = read_all(local.storage.backend(), key, OBJECT_LIMIT)?;
    conflicting[0] ^= 1;
    assert!(matches!(
        put_bytes(remote.backend.as_ref(), key, &conflicting),
        Err(StoreError::Backend(BackendError::IdentityMismatch(_)))
    ));
    for fault in [Fault::Before, Fault::ShortRead, Fault::CorruptRead] {
        let before = remote.revision()?;
        remote.backend.inject(Operation::Read, 1, fault);
        assert!(upload.publish(&local, &remote).is_err());
        assert_eq!(remote.revision()?, before);
        restored(&remote, &[("main", &old_bytes)])?;
    }
    applied(upload.publish(&local, &remote)?);
    restored(&remote, &[("main", &local.expected)])?;
    Ok(())
}

#[test]
fn stale_and_duplicate_upload_publications_never_replace_newer_root() -> TestResult {
    let mut local = Local::new()?;
    let remote = Remote::new()?;
    publish(&local, &remote, local.snapshot()?)?;
    local.overwrite(40)?;
    let mut slow = Upload::new(local.snapshot()?, &remote)?;
    slow.copy_all(&local, &remote)?;
    local.overwrite(50)?;
    let mut fast = Upload::new(local.snapshot()?, &remote)?;
    fast.copy_all(&local, &remote)?;
    applied(fast.publish(&local, &remote)?);
    restored(&remote, &[("main", &local.expected)])?;
    assert!(matches!(slow.publish(&local, &remote)?, Publication::Stale));
    assert!(matches!(fast.publish(&local, &remote)?, Publication::Stale));
    drop(slow);
    drop(fast);
    remote.collect()?;
    restored(&remote, &[("main", &local.expected)])?;
    Ok(())
}

#[test]
fn uncertain_publication_retains_dependencies_until_reconciliation() -> TestResult {
    for fault in [Fault::Before, Fault::AfterPublish] {
        let mut local = Local::new()?;
        let remote = Remote::new()?;
        publish(&local, &remote, local.snapshot()?)?;
        let old_bytes = local.expected.clone();
        local.overwrite(60)?;
        let mut upload = Upload::new(local.snapshot()?, &remote)?;
        upload.copy_all(&local, &remote)?;
        remote.backend.inject(Operation::Publish, 1, fault);
        let result = upload.publish(&local, &remote);
        let expected = if matches!(fault, Fault::Before) {
            assert!(result.is_err());
            &old_bytes
        } else {
            assert!(matches!(result?, Publication::Uncertain(_)));
            &local.expected
        };
        // Even reconciliation can fail. Keep candidate ownership while unknown.
        remote.backend.inject(Operation::RootRead, 1, Fault::Before);
        assert!(remote.backend.read_root().is_err());
        remote.collect()?;
        for key in &upload.snapshot.keys {
            assert!(remote.backend.stat(*key)?.is_some());
        }
        restored(&remote, &[("main", expected)])?;
        let actual = remote.backend.read_root()?.unwrap();
        if matches!(fault, Fault::Before) {
            // Collection can itself advance the revision; restart from observation.
            upload.expected = Some(actual.revision().clone());
            applied(upload.publish(&local, &remote)?);
        } else {
            assert_eq!(actual.bytes(), upload.snapshot.root.bytes());
        }
        drop(upload);
        remote.collect()?;
        restored(&remote, &[("main", &local.expected)])?;
    }
    Ok(())
}

#[test]
fn restart_fence_rejects_delayed_old_cas_after_orphan_objects_are_collected() -> TestResult {
    let mut local = Local::new()?;
    let remote = Remote::new()?;
    publish(&local, &remote, local.snapshot()?)?;
    let old_bytes = local.expected.clone();
    let old_root = remote.backend.read_root()?.unwrap();
    local.overwrite(70)?;
    let mut upload = Upload::new(local.snapshot()?, &remote)?;
    upload.copy_all(&local, &remote)?;
    let delayed_root = upload.snapshot.root.bytes().to_vec();
    let delayed_revision = upload.expected.clone();
    let candidates = upload.snapshot.keys.clone();
    // A root read returning old bytes cannot prove this request will not apply.
    assert_eq!(
        remote.backend.read_root()?.unwrap().bytes(),
        old_root.bytes()
    );
    drop(upload);
    // Restart fences surviving requests before collecting abandoned uploads.
    let fence = remote.fence()?;
    assert_ne!(Some(fence), delayed_revision);
    remote.collect()?;
    assert!(candidates.iter().any(
        |key| matches!(key, ObjectKey::Blob(_)) && remote.backend.stat(*key).unwrap().is_none()
    ));
    assert!(matches!(
        remote
            .backend
            .compare_exchange_root(delayed_revision.as_ref(), &delayed_root)?,
        Publication::Stale
    ));
    restored(&remote, &[("main", &old_bytes)])?;
    publish(&local, &remote, local.snapshot()?)?;
    restored(&remote, &[("main", &local.expected)])?;
    Ok(())
}

#[test]
fn abandoned_upload_can_lose_all_local_dependencies_and_restart_from_latest() -> TestResult {
    let mut local = Local::new()?;
    let remote = Remote::new()?;
    publish(&local, &remote, local.snapshot()?)?;
    let remote_bytes = local.expected.clone();
    local.overwrite(160)?;
    let temporary = local
        .store
        .retain_view(RetentionName::new("candidate-only")?, false)?;
    let mut upload = Upload::new(local.snapshot()?, &remote)?;
    upload.copy_all(&local, &remote)?;
    let delayed_root = upload.snapshot.root.bytes().to_vec();
    let delayed_revision = upload.expected.clone();
    let abandoned_keys = upload.snapshot.keys.clone();
    let released = Arc::downgrade(&upload.snapshot);
    local.store.release_view(temporary)?;
    // No outer Arc, saved view, or durable upload pin may survive this worker.
    drop(upload);
    assert!(released.upgrade().is_none());
    let Local {
        directory,
        storage,
        store,
        expected,
    } = local;
    drop(store);
    let store = Store::open_existing(directory.path().join("local.zsqlite"))?;
    let mut local = Local {
        directory,
        storage,
        store,
        expected,
    };
    local.overwrite(170)?;
    assert!(local.store.repack_once()?.repacked_packs > 0);
    // Turn over catalog runs as well as payload packs, proving that retries
    // cannot depend on the old snapshot still happening to exist locally.
    for _ in 0..17 {
        let pin = local
            .store
            .retain_view(RetentionName::new("churn")?, false)?;
        local.store.release_view(pin)?;
    }
    local.store.gc_report(usize::MAX)?;
    let reclaimed = abandoned_keys
        .iter()
        .copied()
        .filter(|key| local.storage.backend().stat(*key).unwrap().is_none())
        .collect::<BTreeSet<_>>();
    assert!(
        reclaimed
            .iter()
            .any(|key| matches!(key, ObjectKey::Blob(_)))
    );
    assert!(
        reclaimed
            .iter()
            .any(|key| matches!(key, ObjectKey::Index(_)))
    );
    local.store.verify()?;
    remote.fence()?;
    remote.collect()?;
    assert!(matches!(
        remote
            .backend
            .compare_exchange_root(delayed_revision.as_ref(), &delayed_root)?,
        Publication::Stale
    ));
    restored(&remote, &[("main", &remote_bytes)])?;
    let latest = local.snapshot()?;
    assert_ne!(latest.root.bytes(), delayed_root);
    assert!(latest.keys.is_disjoint(&reclaimed));
    publish(&local, &remote, latest)?;
    remote.collect()?;
    restored(&remote, &[("main", &local.expected)])?;
    Ok(())
}

#[test]
fn lagging_remote_root_and_remote_readers_survive_local_repack_and_collection() -> TestResult {
    let mut local = Local::new()?;
    let remote = Remote::new()?;
    // Model coordinator ownership spanning tiers: retain source lookup records
    // until the replacement catalog includes the remote reader's dependencies.
    let reader_source = local.snapshot()?;
    publish(&local, &remote, reader_source.clone())?;
    let old_bytes = local.expected.clone();
    let catalog = remote.catalog();
    let reader = {
        let guard = catalog.lock()?;
        let header = guard.state().sealed.unwrap();
        guard.pin(ManifestId::from_bytes(header.parent_physical_digest))?
    };
    let old_blobs = inventory(remote.backend.as_ref())?
        .into_iter()
        .filter(|key| matches!(key, ObjectKey::Blob(_)))
        .collect::<BTreeSet<_>>();
    local.overwrite(80)?;
    assert_eq!(local.store.repack_once()?.repacked_packs, 1);
    local.store.gc_report(usize::MAX)?;
    assert!(
        old_blobs
            .iter()
            .all(|key| local.storage.backend().stat(*key).unwrap().is_some())
    );
    remote.collect()?;
    restored(&remote, &[("main", &old_bytes)])?;
    publish(&local, &remote, local.snapshot()?)?;
    drop(reader_source);
    local.store.gc_report(usize::MAX)?;
    assert!(
        old_blobs
            .iter()
            .all(|key| local.storage.backend().stat(*key).unwrap().is_none())
    );
    remote.collect()?;
    for key in &old_blobs {
        assert!(remote.backend.stat(*key)?.is_some());
    }
    for (index, expected) in old_bytes.chunks_exact(4096).enumerate() {
        assert_eq!(
            reader
                .resolve(PageNumber::new(u32::try_from(index + 1)?)?)?
                .read()?,
            expected
        );
    }
    restored(&remote, &[("main", &local.expected)])?;
    drop(reader);
    remote.collect()?;
    for key in &old_blobs {
        assert!(remote.backend.stat(*key)?.is_none());
    }
    restored(&remote, &[("main", &local.expected)])?;
    Ok(())
}

#[test]
fn exact_upload_snapshot_retains_old_indexes_and_packs_across_local_repack() -> TestResult {
    let mut local = Local::new()?;
    local.overwrite(90)?;
    let pin = local
        .store
        .retain_view(RetentionName::new("capture")?, false)?;
    let snapshot = local.snapshot()?;
    let old_keys = snapshot.keys.clone();
    local.store.release_view(pin)?;
    assert_eq!(local.store.repack_once()?.repacked_packs, 1);
    // Cross an index checkpoint boundary, so current roots stop naming old
    // index runs while the captured upload must continue to own them exactly.
    for _ in 0..17 {
        let pin = local
            .store
            .retain_view(RetentionName::new("churn")?, false)?;
        local.store.release_view(pin)?;
    }
    local.store.gc_report(usize::MAX)?;
    for key in &old_keys {
        assert!(
            local.storage.backend().stat(*key)?.is_some(),
            "lost upload dependency {key:?}"
        );
    }
    let current = local.snapshot()?;
    let exclusive = old_keys
        .difference(&current.keys)
        .copied()
        .collect::<Vec<_>>();
    assert!(
        exclusive
            .iter()
            .any(|key| matches!(key, ObjectKey::Index(_)))
    );
    assert!(
        exclusive
            .iter()
            .any(|key| matches!(key, ObjectKey::Blob(_)))
    );
    let remote = Remote::new()?;
    publish(&local, &remote, snapshot.clone())?;
    restored(&remote, &[("main", &local.expected)])?;
    drop(snapshot);
    local.store.gc_report(usize::MAX)?;
    assert!(
        exclusive
            .iter()
            .any(|key| local.storage.backend().stat(*key).unwrap().is_none())
    );
    remote.collect()?;
    restored(&remote, &[("main", &local.expected)])?;
    Ok(())
}

#[test]
fn snapshot_upload_includes_named_heads_and_retained_manifest_ancestors() -> TestResult {
    let mut local = Local::new()?;
    let old_bytes = local.expected.clone();
    let retention = local
        .store
        .retain_view(RetentionName::new("saved")?, false)?;
    let _fork = local.storage.fork("backup")?;
    local.overwrite(100)?;
    let snapshot = local.snapshot()?;
    assert!(
        snapshot
            .keys
            .iter()
            .filter(|key| matches!(key, ObjectKey::Manifest(_)))
            .count()
            > 1
    );
    let remote = Remote::new()?;
    publish(&local, &remote, snapshot)?;
    restored(
        &remote,
        &[("main", &local.expected), ("backup", &old_bytes)],
    )?;
    let catalog = remote.catalog();
    let guard = catalog.lock()?;
    let retained = guard.pin_retained(&guard.read_root(retention.name())?)?;
    assert_eq!(retained.resolve(PageNumber::new(1)?)?.read()?, page(1));
    drop(guard);
    remote.collect()?;
    restored(
        &remote,
        &[("main", &local.expected), ("backup", &old_bytes)],
    )?;
    Ok(())
}

#[test]
fn blindly_replacing_remote_catalog_with_local_gc_catalog_blocks_collection() -> TestResult {
    let mut local = Local::new()?;
    let remote = Remote::new()?;
    publish(&local, &remote, local.snapshot()?)?;
    let old_bytes = local.expected.clone();
    let reader = {
        let guard = remote.catalog().lock()?;
        let header = guard.state().sealed.unwrap();
        guard.pin(ManifestId::from_bytes(header.parent_physical_digest))?
    };
    local.overwrite(110)?;
    assert_eq!(local.store.repack_once()?.repacked_packs, 1);
    local.store.gc_report(usize::MAX)?;
    // Local deletion cannot authorize removal from the lagging remote tier.
    remote.collect()?;
    restored(&remote, &[("main", &old_bytes)])?;
    publish(&local, &remote, local.snapshot()?)?;
    restored(&remote, &[("main", &local.expected)])?;
    let before = remote.backend.statistics().deletes;
    // Negative control: exact blob leases alone do not preserve the catalog
    // records needed to re-traverse an older reader. This must fail closed.
    assert!(remote.collect().is_err());
    assert_eq!(remote.backend.statistics().deletes, before);
    assert_eq!(reader.resolve(PageNumber::new(1)?)?.read()?, page(1));
    drop(reader);
    remote.collect()?;
    restored(&remote, &[("main", &local.expected)])?;
    Ok(())
}

#[test]
fn collection_fences_delayed_cas_but_blind_retry_with_fresh_revision_is_unsafe() -> TestResult {
    let mut local = Local::new()?;
    let remote = Remote::new()?;
    publish(&local, &remote, local.snapshot()?)?;
    let old_bytes = local.expected.clone();
    local.overwrite(120)?;
    let mut upload = Upload::new(local.snapshot()?, &remote)?;
    upload.copy_all(&local, &remote)?;
    let delayed_root = upload.snapshot.root.bytes().to_vec();
    let delayed_revision = upload.expected.clone();
    drop(upload);
    // Core GC publishes a fresh revision before deleting, implicitly fencing
    // requests from the old worker even without an explicit restart barrier.
    assert!(remote.collect()?.deleted_objects > 0);
    assert!(matches!(
        remote
            .backend
            .compare_exchange_root(delayed_revision.as_ref(), &delayed_root)?,
        Publication::Stale
    ));
    restored(&remote, &[("main", &old_bytes)])?;
    // Negative control: merely reread the revision and resubmit stale catalog
    // bytes without rechecking/reinstalling dependencies. The transport accepts
    // this, but the real restore oracle must detect its now-missing objects.
    let fresh_revision = remote.revision()?;
    applied(
        remote
            .backend
            .compare_exchange_root(fresh_revision.as_ref(), &delayed_root)?,
    );
    assert!(restored(&remote, &[("main", &local.expected)]).is_err());
    Ok(())
}

#[test]
fn logical_retention_alone_does_not_freeze_an_uploads_catalog_indexes() -> TestResult {
    let mut local = Local::new()?;
    let pin = local
        .store
        .retain_view(RetentionName::new("saved")?, false)?;
    let temporary = local
        .store
        .retain_view(RetentionName::new("old-snapshot-only")?, false)?;
    let snapshot = local.snapshot()?;
    let root = snapshot.root.bytes().to_vec();
    let keys = snapshot.keys.clone();
    // Negative control: retain the logical endpoint but omit physical leases.
    drop(snapshot);
    local.store.release_view(temporary)?;
    for _ in 0..17 {
        let temporary = local
            .store
            .retain_view(RetentionName::new("churn")?, false)?;
        local.store.release_view(temporary)?;
    }
    local.store.gc_report(usize::MAX)?;
    assert!(keys.iter().any(|key| matches!(key, ObjectKey::Index(_))
        && local.storage.backend().stat(*key).unwrap().is_none()));
    let catalog = Catalog::configured(
        local.storage.clone(),
        local.directory.path().join("local.zsqlite"),
    );
    let reader = catalog.lock()?.pin_retained(&pin)?;
    assert_eq!(reader.resolve(PageNumber::new(1)?)?.read()?, page(1));
    local.store.verify()?;
    let broken = Remote::new()?;
    for key in keys {
        if local.storage.backend().stat(key)?.is_some() {
            put_bytes(
                broken.backend.as_ref(),
                key,
                &read_all(local.storage.backend(), key, OBJECT_LIMIT)?,
            )?;
        }
    }
    applied(broken.backend.compare_exchange_root(None, &root)?);
    assert!(restored(&broken, &[("main", &local.expected)]).is_err());
    Ok(())
}

#[test]
fn dictionary_encoded_snapshot_requires_uploaded_dictionary_for_restore() -> TestResult {
    let mut local = Local::new()?;
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
    let mut pages = (0..320)
        .map(|_| {
            let mut bytes = common.clone();
            bytes.extend(random(1024));
            bytes
        })
        .collect::<Vec<_>>();
    pages[0][..18].copy_from_slice(&page(0)[..18]);
    local.expected = pages.concat();
    local.store.write_at(0, &local.expected)?;
    local.store.publish(true)?;
    local.store.flush_sidecars()?;
    let overwritten = (0..240).flat_map(|_| page(150)).collect::<Vec<_>>();
    local.store.write_at(0, &overwritten)?;
    local.expected[..overwritten.len()].copy_from_slice(&overwritten);
    local.store.publish(true)?;
    local.store.flush_sidecars()?;
    let maintenance = local.store.repack_once()?;
    assert!(maintenance.repacked_packs > 0);
    assert!(maintenance.copied_frames > 0);
    assert_eq!(maintenance.decoded_input.get(), 0);
    let snapshot = local.snapshot()?;
    assert!(
        snapshot
            .keys
            .iter()
            .any(|key| matches!(key, ObjectKey::Dictionary(_)))
    );
    assert!(
        local
            .store
            .inspect()?
            .frame_distribution
            .iter()
            .any(|bin| bin.dictionary_frames > 0)
    );
    let remote = Remote::new()?;
    let mut upload = Upload::new(snapshot.clone(), &remote)?;
    for key in snapshot
        .keys
        .iter()
        .filter(|key| !matches!(key, ObjectKey::Dictionary(_)))
    {
        upload.copy(&local, &remote, *key)?;
    }
    assert!(upload.publish(&local, &remote).is_err());
    let broken = Remote::new()?;
    for key in inventory(remote.backend.as_ref())? {
        put_bytes(
            broken.backend.as_ref(),
            key,
            &read_all(remote.backend.as_ref(), key, OBJECT_LIMIT)?,
        )?;
    }
    applied(
        broken
            .backend
            .compare_exchange_root(None, snapshot.root.bytes())?,
    );
    assert!(restored(&broken, &[("main", &local.expected)]).is_err());
    upload.copy_all(&local, &remote)?;
    applied(upload.publish(&local, &remote)?);
    restored(&remote, &[("main", &local.expected)])?;
    Ok(())
}
