use super::objects::{CatalogGuard, Dictionary, GcSession, Manifest, ObjectKey, hex, parse_hex};
use super::view::{PinnedView, load_metadata};
use crate::StoreError;
use crate::domain::{DatabaseId, ManifestId, PackId, RetentionId, TransactionId, ViewHash};
use crate::fs::{ExclusiveLock, sync_dir};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

#[cfg(test)]
#[path = "retention_tests.rs"]
mod tests;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RetentionName(String);
impl RetentionName {
    pub fn new(name: impl Into<String>) -> Result<Self, StoreError> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 64
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
        {
            return Err(StoreError::InvalidConfiguration(
                "retention names must be 1–64 ASCII letters, digits, '-' or '_'",
            ));
        }
        Ok(Self(name))
    }
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A durable logical endpoint, not a particular physical segment. Dropping this
/// handle does NOT release retention. Its private token prevents stale release
/// even when a name is recreated or advanced to the same logical endpoint.
#[must_use]
#[derive(Debug)]
pub struct DurablePin {
    name: RetentionName,
    hash: ViewHash,
    txid: TransactionId,
    database: DatabaseId,
    revision: RetentionId,
}
impl DurablePin {
    #[must_use]
    pub fn name(&self) -> &RetentionName {
        &self.name
    }
    #[must_use]
    pub const fn logical_hash(&self) -> ViewHash {
        self.hash
    }
    #[must_use]
    pub const fn txid(&self) -> TransactionId {
        self.txid
    }
    #[must_use]
    pub const fn database(&self) -> DatabaseId {
        self.database
    }
}

#[derive(Clone, Debug, Default)]
pub struct GcReport {
    pub current_view_bytes: u64,
    pub retained_bytes: u64,
    /// Extra bytes retained by named owners, excluding the current view.
    pub fork_retained_bytes: u64,
    /// Extra bytes retained only by readers (not already charged to a fork).
    pub reader_retained_bytes: u64,
    pub collectible_bytes: u64,
    pub partially_obsolete_bytes: u64,
    pub objects: usize,
    pub deleted_objects: usize,
    pub deleted_bytes: u64,
    pub logically_unreachable_packs: usize,
    pub fully_unreachable_blobs: usize,
    pub fully_unreachable_blob_bytes: u64,
    pub dead_extent_bytes: u64,
    pub uncertain_retained_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct MaintenanceReport {
    pub repacked_packs: usize,
    /// Live frames copied byte-for-byte without decompression or compression.
    pub copied_frames: usize,
    /// Payload bytes copied unchanged, excluding frame and pack headers.
    pub copied_bytes: crate::domain::StoredBytes,
    /// Actual decoded input, limited to partially obsolete multi-page frames.
    pub decoded_input: crate::domain::DecodedBytes,
    pub gc: GcReport,
}
impl Default for MaintenanceReport {
    fn default() -> Self {
        Self {
            repacked_packs: 0,
            copied_frames: 0,
            copied_bytes: crate::domain::StoredBytes::new(0),
            decoded_input: crate::domain::DecodedBytes::new(0),
            gc: GcReport::default(),
        }
    }
}

/// Constructed only after a successful traversal of the complete root set.
pub(super) struct ValidatedRoots<'g> {
    guard: &'g CatalogGuard,
    live: BTreeSet<super::adapter::ObjectKey>,
}
impl<'g> ValidatedRoots<'g> {
    pub(super) fn into_parts(self) -> (&'g CatalogGuard, BTreeSet<super::adapter::ObjectKey>) {
        (self.guard, self.live)
    }
}

impl CatalogGuard {
    pub(super) fn retained_packs_except(
        &self,
        current: ManifestId,
    ) -> Result<BTreeSet<PackId>, StoreError> {
        let mut packs = BTreeSet::new();
        let mut retain = |id| -> Result<(), StoreError> {
            packs.extend(
                load_metadata(self, id)?
                    .dependencies(id)
                    .into_iter()
                    .filter_map(|key| match key {
                        ObjectKey::Pack(id) => Some(id),
                        ObjectKey::Manifest(_)
                        | ObjectKey::Dictionary(_)
                        | ObjectKey::Staging(_) => None,
                    }),
            );
            Ok(())
        };
        for name in self.root_names()? {
            let pin = self.read_root(&name)?;
            retain(self.pin_retained(&pin)?.id())?;
        }
        for (name, bytes) in &self.state().heads.entries {
            if name.as_slice() != self.storage().head_name().as_bytes() {
                let head = super::catalog::Head::decode(bytes, self.state().database)?;
                for header in head.sealed.iter().chain(head.pending.iter()) {
                    retain(ManifestId::from_bytes(header.parent_physical_digest))?;
                }
            }
        }
        for entry in std::fs::read_dir(self.root().join("readers"))? {
            let entry = entry?;
            let id = ManifestId::from_bytes(parse_hex(
                &entry
                    .file_name()
                    .into_string()
                    .map_err(|_| StoreError::Corrupt(0))?,
            )?);
            if id == current {
                continue;
            }
            match ExclusiveLock::acquire(&entry.path(), true) {
                Ok(_guard) => {}
                Err(StoreError::Busy) => retain(id)?,
                Err(error) => return Err(error),
            }
        }
        Ok(packs)
    }
    pub(crate) fn pin(&self, id: ManifestId) -> Result<PinnedView, StoreError> {
        PinnedView::open(self, id)
    }

    pub(crate) fn pin_retained(&self, pin: &DurablePin) -> Result<PinnedView, StoreError> {
        let endpoint = super::segment::ParentRef {
            hash: pin.hash,
            txid: pin.txid,
        };
        let id = super::segment::resolve_parent(self, endpoint, &BTreeSet::new())?;
        let view = self.pin(id)?;
        if view.metadata.database != pin.database
            || view.metadata.txid != pin.txid
            || view.metadata.logical_hash() != pin.hash
        {
            return Err(StoreError::IdentityMismatch);
        }
        Ok(view)
    }

    pub(crate) fn retain(
        &self,
        name: RetentionName,
        view: &PinnedView,
        replace: bool,
    ) -> Result<DurablePin, StoreError> {
        if !view.same_catalog(self) {
            return Err(StoreError::IdentityMismatch);
        }
        // Open and validate the replacement before touching its predecessor.
        let validated = self.pin(view.id())?;
        let exists = self
            .state()
            .retentions
            .entries
            .contains_key(name.as_str().as_bytes());
        if !replace && exists {
            return Err(StoreError::RetentionExists(name.as_str().to_owned()));
        }
        if replace && exists {
            let previous = self.read_root(&name)?;
            if previous.database != validated.metadata.database {
                return Err(StoreError::IdentityMismatch);
            }
        }
        let mut nonce = [0; 32];
        getrandom::fill(&mut nonce).map_err(std::io::Error::other)?;
        let pin = DurablePin {
            name,
            hash: validated.metadata.logical_hash(),
            txid: validated.metadata.txid,
            database: validated.metadata.database,
            revision: RetentionId::from_bytes(nonce),
        };
        // Validate what logical lookup will actually select before replacing a
        // predecessor. Matching a filename is not proof of a usable root.
        let _replacement = self.pin_retained(&pin)?;
        self.write_root(&pin)?;
        Ok(pin)
    }

    fn write_root(&self, pin: &DurablePin) -> Result<(), StoreError> {
        let mut bytes = Vec::with_capacity(144);
        bytes.extend(b"ZROOT001");
        bytes.extend(pin.database.as_bytes());
        bytes.extend(pin.hash.as_bytes());
        bytes.extend(pin.txid.get().to_le_bytes());
        bytes.extend(pin.revision.as_bytes());
        bytes.extend(*blake3::hash(&bytes).as_bytes());
        self.state_mut()
            .retentions
            .insert(pin.name.as_str().as_bytes().to_vec(), bytes);
        self.publish_catalog()
    }

    pub(crate) fn read_root(&self, name: &RetentionName) -> Result<DurablePin, StoreError> {
        let state = self.state();
        let bytes = state
            .retentions
            .entries
            .get(name.as_str().as_bytes())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?;
        if bytes.len() != 144 || &bytes[..8] != b"ZROOT001" {
            return Err(StoreError::Corrupt(0));
        }
        let body = bytes.len() - 32;
        if blake3::hash(&bytes[..body]).as_bytes() != &bytes[body..] {
            return Err(StoreError::Corrupt(0));
        }
        Ok(DurablePin {
            name: name.clone(),
            database: DatabaseId::from_bytes(
                bytes[8..40].try_into().map_err(|_| StoreError::Range)?,
            ),
            hash: ViewHash::from_bytes(bytes[40..72].try_into().map_err(|_| StoreError::Range)?),
            txid: TransactionId::new(u64::from_le_bytes(
                bytes[72..80].try_into().map_err(|_| StoreError::Range)?,
            ))?,
            revision: RetentionId::from_bytes(
                bytes[80..112].try_into().map_err(|_| StoreError::Range)?,
            ),
        })
    }

    #[allow(clippy::needless_pass_by_value)] // Explicit release consumes the persistent-pin handle.
    pub(crate) fn release(&self, pin: DurablePin) -> Result<(), StoreError> {
        let current = self.read_root(&pin.name)?;
        if current.hash != pin.hash
            || current.txid != pin.txid
            || current.database != pin.database
            || current.revision != pin.revision
        {
            return Err(StoreError::Busy);
        }
        self.state_mut()
            .retentions
            .remove(pin.name.as_str().as_bytes());
        self.publish_catalog()
    }
    pub(crate) fn destroy(
        &self,
        header: &crate::format::ActiveHeader,
        lifecycle: &ExclusiveLock,
    ) -> Result<(), StoreError> {
        if !self.root_names()?.is_empty() || self.state().heads.entries.len() > 1 {
            return Err(StoreError::Busy);
        }
        if self.state().database.is_some() {
            self.validate_attachment(header)?;
            let mut headers = Vec::new();
            if header.parent_physical_digest != [0; 32] {
                headers.push(*header);
            }
            headers.extend(self.state().sealed);
            headers.extend(self.state().pending);
            for header in headers {
                let view = PinnedView::open_for_bootstrap(
                    self,
                    ManifestId::from_bytes(header.parent_physical_digest),
                    lifecycle,
                )?;
                self.validate_descriptor(&header, &view)?;
            }
        }
        let objects = self.inventory()?;
        let revision = self.state().revision.clone();
        *self.state_mut() = super::catalog::State {
            revision,
            ..super::catalog::State::default()
        };
        self.publish_catalog()?;
        let mut session = GcSession::traced(ValidatedRoots {
            guard: self,
            live: BTreeSet::new(),
        });
        for key in objects {
            session
                .permit(key)
                .ok_or(StoreError::Corrupt(0))?
                .delete()?;
        }
        Ok(())
    }
    pub(crate) fn root_names(&self) -> Result<Vec<RetentionName>, StoreError> {
        self.state()
            .retentions
            .entries
            .keys()
            .map(|bytes| {
                RetentionName::new(std::str::from_utf8(bytes).map_err(|_| StoreError::Corrupt(0))?)
            })
            .collect()
    }
    pub(super) fn reachable_packs(&self) -> Result<BTreeSet<PackId>, StoreError> {
        let (live, _, _) = self.trace_logical()?;
        Ok(live
            .into_iter()
            .filter_map(|key| {
                if let ObjectKey::Pack(pack) = key {
                    Some(pack)
                } else {
                    None
                }
            })
            .collect())
    }
    #[allow(clippy::type_complexity)] // live, active, and retained logical dependency sets
    fn trace_logical(
        &self,
    ) -> Result<
        (
            BTreeSet<ObjectKey>,
            BTreeSet<ObjectKey>,
            BTreeSet<ObjectKey>,
        ),
        StoreError,
    > {
        let mut live = BTreeSet::new();
        let mut current = BTreeSet::new();
        let mut named = BTreeSet::new();
        let mut headers = Vec::new();
        if self.active_path().exists() {
            let active = std::fs::File::open(self.active_path())?;
            let mut bytes = [0; crate::format::ACTIVE_HEADER_SIZE];
            crate::fs::read_exact_at(&active, 0, &mut bytes)?;
            let header = crate::format::ActiveHeader::decode(&bytes)?;
            self.validate_attachment(&header)?;
            if header.parent_physical_digest != [0; 32] {
                let view = self.pin(ManifestId::from_bytes(header.parent_physical_digest))?;
                self.validate_descriptor(&header, &view)?;
                current.extend(view.metadata.dependencies(view.id()));
            }
        }
        let heads = self.state().all_heads()?;
        for head in heads {
            headers.extend(head.sealed);
            headers.extend(head.pending);
        }
        live.extend(current.iter().copied());
        for header in headers {
            let view = self.pin(ManifestId::from_bytes(header.parent_physical_digest))?;
            self.validate_descriptor(&header, &view)?;
            let dependencies = view.metadata.dependencies(view.id());
            if self.state().sealed.is_none_or(|current| {
                current.parent_physical_digest != header.parent_physical_digest
            }) && self.state().pending.is_none_or(|current| {
                current.parent_physical_digest != header.parent_physical_digest
            }) {
                named.extend(dependencies.iter().copied());
            }
            live.extend(dependencies);
        }
        for name in self.root_names()? {
            let view = self.pin_retained(&self.read_root(&name)?)?;
            named.extend(view.metadata.dependencies(view.id()));
        }
        live.extend(named.iter().copied());
        for entry in std::fs::read_dir(self.root().join("readers"))? {
            let entry = entry?;
            let id = ManifestId::from_bytes(parse_hex(
                entry.file_name().to_str().ok_or(StoreError::Corrupt(0))?,
            )?);
            match ExclusiveLock::acquire(&entry.path(), true) {
                Ok(_unused) => {}
                Err(StoreError::Busy) => {
                    let view = self.pin(id)?;
                    live.extend(view.metadata.dependencies(view.id()));
                }
                Err(error) => return Err(error),
            }
        }
        for entry in std::fs::read_dir(self.root().join("object-readers"))? {
            let entry = entry?;
            let key =
                ObjectKey::parse_lease(entry.file_name().to_str().ok_or(StoreError::Corrupt(0))?)?;
            match ExclusiveLock::acquire(&entry.path(), true) {
                Ok(_unused) => {}
                Err(StoreError::Busy) => {
                    live.insert(key);
                }
                Err(error) => return Err(error),
            }
        }
        Ok((live, current, named))
    }
    #[allow(clippy::too_many_lines)]
    pub(crate) fn collect(&mut self, budget: usize) -> Result<GcReport, StoreError> {
        use super::adapter::{BackendError, ObjectKey as Physical};
        if self.state().uncertain {
            return Err(BackendError::Uncertain("reload before collection".into()).into());
        }
        let reloaded =
            super::catalog::State::load_for(self.storage().backend(), self.storage().head_name())?;
        if reloaded.revision != self.state().revision {
            return Err(BackendError::Stale.into());
        }
        *self.state_mut() = reloaded;
        let (logical, current, named) = self.trace_logical()?;
        let uncertain_heads = self.state().has_pending()?;
        let inventory = self.inventory()?;
        // Immutable lengths remain stable under catalog exclusion. Reuse each
        // observation across logical roots, shared representations and reporting
        // instead of issuing another object-store request for the same object.
        let mut lengths = BTreeMap::new();
        let mut stat = |key| -> Result<Option<crate::domain::StoredBytes>, BackendError> {
            if let Some(length) = lengths.get(&key) {
                return Ok(*length);
            }
            let length = self.storage().backend().stat(key)?;
            lengths.insert(key, length);
            Ok(length)
        };
        let mut physical = self.state().index_objects();
        let mut current_physical = BTreeSet::new();
        let mut named_physical = BTreeSet::new();
        let mut leased = BTreeSet::new();
        for entry in std::fs::read_dir(self.root().join("physical-readers"))? {
            let entry = entry?;
            let key = Physical::parse(entry.file_name().to_str().ok_or(StoreError::Corrupt(0))?)?;
            match ExclusiveLock::acquire(&entry.path(), true) {
                Ok(_unused) => {}
                Err(StoreError::Busy) => {
                    leased.insert(key);
                }
                Err(error) => return Err(error),
            }
        }
        physical.extend(leased.iter().copied());
        for key in &logical {
            let object =
                match key {
                    ObjectKey::Manifest(id) => {
                        let _receipt = self.validate::<Manifest>(*id, 512 * 1024 * 1024)?;
                        Physical::Manifest(*id)
                    }
                    ObjectKey::Dictionary(id) => {
                        let _receipt = self.validate::<Dictionary>(
                            *id,
                            u64::from(crate::dictionary::MAX_DICTIONARY_BYTES),
                        )?;
                        Physical::Dictionary(*id)
                    }
                    ObjectKey::Pack(pack) => {
                        let placement = self.placement(*pack)?;
                        let extent = placement.preferred().extent;
                        let object = Physical::Blob(extent.blob());
                        let length = stat(object)?.ok_or(BackendError::Missing(object))?;
                        if length.get() != extent.blob_length().get() {
                            return Err(StoreError::Corrupt(0));
                        }
                        if uncertain_heads {
                            physical.extend(placement.representations.iter().map(
                                |representation| Physical::Blob(representation.extent.blob()),
                            ));
                        }
                        object
                    }
                    ObjectKey::Staging(_) => return Err(StoreError::Corrupt(0)),
                };
            physical.insert(object);
            if current.contains(key) {
                current_physical.insert(object);
            }
            if named.contains(key) {
                named_physical.insert(object);
            }
        }
        // Validate every exact reader dependency before minting any permit.
        for key in &physical {
            if !inventory.contains(key) || stat(*key)?.is_none() {
                return Err(BackendError::Missing(*key).into());
            }
            if let Physical::Index(id) = key
                && leased.contains(key)
            {
                let bytes =
                    super::catalog::read_all(self.storage().backend(), *key, 64 * 1024 * 1024)?;
                if blake3::hash(&bytes).as_bytes() != id.as_bytes() {
                    return Err(StoreError::Corrupt(0));
                }
            }
        }
        let mut report = GcReport {
            objects: inventory.len(),
            ..GcReport::default()
        };
        let placements = self.state().placements.entries.clone();
        let mut dead_extents = BTreeSet::new();
        for (key, bytes) in &placements {
            let pack = PackId::from_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Corrupt(0))?,
            );
            let placement = super::placement::Placements::decode(pack, bytes)?;
            for representation in &placement.representations {
                let object = Physical::Blob(representation.extent.blob());
                if physical.contains(&object)
                    && stat(object)?.is_none_or(|length| {
                        length.get() != representation.extent.blob_length().get()
                    })
                {
                    return Err(StoreError::Corrupt(0));
                }
            }
            if !logical.contains(&ObjectKey::Pack(pack)) {
                report.logically_unreachable_packs += 1;
                for representation in &placement.representations {
                    let extent = representation.extent;
                    if physical.contains(&Physical::Blob(extent.blob()))
                        && dead_extents.insert((extent.blob(), extent.offset(), extent.length()))
                    {
                        report.dead_extent_bytes = report
                            .dead_extent_bytes
                            .saturating_add(extent.length().get());
                    }
                }
            }
        }
        let mut deletable = Vec::new();
        for key in &inventory {
            let length = stat(*key)?.ok_or(BackendError::Missing(*key))?.get();
            if current_physical.contains(key) {
                report.current_view_bytes += length;
            } else if physical.contains(key) {
                report.retained_bytes += length;
                if named_physical.contains(key) {
                    report.fork_retained_bytes += length;
                } else if leased.contains(key) {
                    report.reader_retained_bytes += length;
                } else if uncertain_heads {
                    report.uncertain_retained_bytes += length;
                }
            } else {
                report.collectible_bytes += length;
                if matches!(key, Physical::Blob(_)) {
                    report.fully_unreachable_blobs += 1;
                    report.fully_unreachable_blob_bytes =
                        report.fully_unreachable_blob_bytes.saturating_add(length);
                }
                if deletable.len() < budget {
                    deletable.push((*key, length));
                }
            }
        }
        report.partially_obsolete_bytes = report.dead_extent_bytes;
        if let Some(ObjectKey::Manifest(id)) = current
            .iter()
            .find(|key| matches!(key, ObjectKey::Manifest(_)))
        {
            // The active descriptor selects the exact current manifest, not a
            // sorted ancestor ID.
            let mut encoded = [0; crate::format::ACTIVE_HEADER_SIZE];
            crate::fs::read_exact_at(&std::fs::File::open(self.active_path())?, 0, &mut encoded)?;
            let header = crate::format::ActiveHeader::decode(&encoded)?;
            let id = if header.parent_physical_digest == [0; 32] {
                *id
            } else {
                ManifestId::from_bytes(header.parent_physical_digest)
            };
            let view = self.pin(id)?;
            report.partially_obsolete_bytes += super::repack::estimated_obsolete_bytes(&view)?;
        }
        if deletable.is_empty() {
            return Ok(report);
        }
        let deleting: BTreeSet<_> = deletable.iter().map(|(key, _)| *key).collect();
        // Remove discovery records and retire representations before physical
        // deletion. A failed/uncertain root CAS preserves every candidate object.
        for (key, _) in &deletable {
            if let Physical::Manifest(id) = key {
                self.state_mut().endpoints.remove(id.as_bytes());
            }
        }
        for (key, bytes) in placements {
            let pack = PackId::from_bytes(
                key.as_slice()
                    .try_into()
                    .map_err(|_| StoreError::Corrupt(0))?,
            );
            let mut placement = super::placement::Placements::decode(pack, &bytes)?;
            placement.representations.retain(|representation| {
                !deleting.contains(&Physical::Blob(representation.extent.blob()))
            });
            if placement.representations.is_empty() {
                if logical.contains(&ObjectKey::Pack(pack)) {
                    return Err(StoreError::Corrupt(0));
                }
                self.state_mut().placements.remove(&key);
            } else {
                self.state_mut().placements.insert(key, placement.encode());
            }
        }
        self.publish_catalog()?;
        physical.extend(self.state().index_objects());
        let mut session = GcSession::traced(ValidatedRoots {
            guard: self,
            live: physical,
        });
        for (key, length) in deletable {
            // Publishing a checkpoint can reuse the ID of an older, previously
            // unrooted index. Its new root takes precedence over this inventory.
            if let Some(permit) = session.permit(key) {
                permit.delete()?;
                report.deleted_objects += 1;
                report.deleted_bytes += length;
            }
        }
        Ok(report)
    }
}

pub(super) fn atomic_write(path: &PathBuf, bytes: &[u8]) -> Result<(), StoreError> {
    use std::io::Write;
    let mut nonce = [0_u8; 32];
    getrandom::fill(&mut nonce).map_err(std::io::Error::other)?;
    let temporary = path.with_file_name(format!("{}.pending", hex(nonce)));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        #[cfg(test)]
        super::faults::check(super::faults::Point::RootRenamed)
            .map_err(StoreError::PublicationUncertain)?;
        match sync_dir(path.parent().ok_or(StoreError::Range)?) {
            Ok(()) => {
                #[cfg(test)]
                super::faults::check(super::faults::Point::RootDirectorySynced)
                    .map_err(StoreError::PublicationUncertain)?;
                Ok(())
            }
            Err(StoreError::Io(error)) => Err(StoreError::PublicationUncertain(error)),
            Err(error) => Err(error),
        }
    })();
    let _ = std::fs::remove_file(&temporary);
    result
}
