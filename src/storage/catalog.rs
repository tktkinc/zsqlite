//! Immutable bounded catalog indexes and the single conditional namespace root.
use super::adapter::{BackendError, ObjectKey, ObjectRange, Publication, Revision, StorageBackend};
use super::wire::{Decoder, envelope, open_envelope, u32_bytes};
use crate::StoreError;
use crate::domain::{AttachmentId, DatabaseId, FileOffset, IndexId, StoredBytes, StoredRange};
use std::collections::{BTreeMap, BTreeSet};

const INDEX_LIMIT: usize = 64 * 1024 * 1024;
const MAX_DEPTH: usize = 64;
const CHECKPOINT_DEPTH: usize = 16;

pub(super) fn read_range(
    backend: &dyn StorageBackend,
    key: ObjectKey,
    range: StoredRange,
) -> Result<Vec<u8>, StoreError> {
    if range.length().get() == 0 {
        return Ok(Vec::new());
    }
    let request = ObjectRange::new(key, range)?;
    let mut batch = backend.read_ranges(&[request])?;
    if batch.len() != 1 || batch[0].len() as u64 != range.length().get() {
        return Err(BackendError::InvalidData.into());
    }
    Ok(batch.remove(0))
}
pub(super) fn read_all(
    backend: &dyn StorageBackend,
    key: ObjectKey,
    limit: u64,
) -> Result<Vec<u8>, StoreError> {
    let length = backend.stat(key)?.ok_or(BackendError::Missing(key))?;
    if length.get() == 0 || length.get() > limit {
        return Err(StoreError::Range);
    }
    let mut bytes = Vec::with_capacity(length.as_usize()?);
    let mut offset = 0;
    while offset < length.get() {
        let count = (length.get() - offset).min(super::adapter::MAX_BATCH_BYTES);
        bytes.extend(read_range(
            backend,
            key,
            StoredRange::new(FileOffset::new(offset), StoredBytes::new(count))?,
        )?);
        offset += count;
    }
    Ok(bytes)
}
pub(super) fn put_bytes(
    backend: &dyn StorageBackend,
    key: ObjectKey,
    bytes: &[u8],
) -> Result<(), StoreError> {
    backend.put(
        key,
        StoredBytes::new(bytes.len() as u64),
        &mut std::io::Cursor::new(bytes),
    )?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(super) struct Index {
    pub head: Option<IndexId>,
    pub objects: BTreeSet<IndexId>,
    pub entries: BTreeMap<Vec<u8>, Vec<u8>>,
    dirty: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    depth: usize,
}
impl Index {
    pub fn load(backend: &dyn StorageBackend, head: Option<IndexId>) -> Result<Self, StoreError> {
        let mut index = Self {
            head,
            ..Self::default()
        };
        let mut next = head;
        let mut resolved = BTreeMap::new();
        let mut decoded_bytes = 0;
        while let Some(id) = next {
            if index.depth >= MAX_DEPTH || !index.objects.insert(id) {
                return Err(StoreError::Range);
            }
            index.depth += 1;
            let bytes = read_all(backend, ObjectKey::Index(id), INDEX_LIMIT as u64)?;
            if blake3::hash(&bytes).as_bytes() != id.as_bytes() {
                return Err(StoreError::Corrupt(0));
            }
            let raw = open_envelope(b"ZINDEX01", &bytes, INDEX_LIMIT - decoded_bytes)?;
            decoded_bytes += raw.len();
            let mut wire = Decoder::new(&raw);
            next = read_id(&mut wire)?;
            let count = wire.u32()?;
            if count as usize > raw.len() / 9 {
                return Err(StoreError::Range);
            }
            let mut previous = None;
            for _ in 0..count {
                let key_length = wire.u32()? as usize;
                if key_length == 0 || key_length > 256 {
                    return Err(StoreError::Range);
                }
                let key = wire.take(key_length)?.to_vec();
                if previous.as_ref().is_some_and(|previous| previous >= &key) {
                    return Err(StoreError::Corrupt(0));
                }
                previous = Some(key.clone());
                let value = match wire.u8()? {
                    0 => None,
                    1 => {
                        let length = wire.u32()? as usize;
                        Some(wire.take(length)?.to_vec())
                    }
                    _ => return Err(StoreError::Corrupt(0)),
                };
                resolved.entry(key).or_insert(value);
            }
            wire.finish()?;
        }
        index.entries = resolved
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect();
        Ok(index)
    }
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if self.entries.get(&key) != Some(&value) {
            self.entries.insert(key.clone(), value.clone());
            self.dirty.insert(key, Some(value));
        }
    }
    pub fn remove(&mut self, key: &[u8]) {
        if self.entries.remove(key).is_some() {
            self.dirty.insert(key.to_vec(), None);
        }
    }
    pub fn flush(&mut self, backend: &dyn StorageBackend) -> Result<(), StoreError> {
        if self.dirty.is_empty() && self.depth < CHECKPOINT_DEPTH {
            return Ok(());
        }
        let checkpoint = self.head.is_none() || self.depth >= CHECKPOINT_DEPTH;
        let records = if checkpoint {
            self.entries
                .iter()
                .map(|(key, value)| (key.clone(), Some(value.clone())))
                .collect()
        } else {
            self.dirty.clone()
        };
        let mut raw = Vec::new();
        write_id(&mut raw, if checkpoint { None } else { self.head });
        u32_bytes(
            &mut raw,
            u32::try_from(records.len()).map_err(|_| StoreError::Range)?,
        );
        for (key, value) in records {
            if key.is_empty() || key.len() > 256 {
                return Err(StoreError::Range);
            }
            u32_bytes(
                &mut raw,
                u32::try_from(key.len()).map_err(|_| StoreError::Range)?,
            );
            raw.extend(key);
            if let Some(value) = value {
                raw.push(1);
                u32_bytes(
                    &mut raw,
                    u32::try_from(value.len()).map_err(|_| StoreError::Range)?,
                );
                raw.extend(value);
            } else {
                raw.push(0);
            }
        }
        if raw.len() > INDEX_LIMIT {
            return Err(StoreError::Range);
        }
        let bytes = envelope(b"ZINDEX01", &raw)?;
        let id = IndexId::from_bytes(*blake3::hash(&bytes).as_bytes());
        put_bytes(backend, ObjectKey::Index(id), &bytes)?;
        self.head = Some(id);
        if checkpoint {
            self.objects.clear();
            self.depth = 0;
        }
        self.objects.insert(id);
        self.depth += 1;
        self.dirty.clear();
        Ok(())
    }
}
fn read_id(wire: &mut Decoder<'_>) -> Result<Option<IndexId>, StoreError> {
    match wire.u8()? {
        0 => Ok(None),
        1 => Ok(Some(IndexId::from_bytes(wire.array()?))),
        _ => Err(StoreError::Corrupt(0)),
    }
}
fn write_id(bytes: &mut Vec<u8>, id: Option<IndexId>) {
    if let Some(id) = id {
        bytes.push(1);
        bytes.extend(id.as_bytes());
    } else {
        bytes.push(0);
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct Head {
    pub attachment: Option<AttachmentId>,
    pub sealed: Option<crate::format::ActiveHeader>,
    pub pending: Option<crate::format::ActiveHeader>,
}
impl Head {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = b"ZBRANCH1".to_vec();
        if let Some(id) = self.attachment {
            bytes.push(1);
            bytes.extend(id.as_bytes());
        } else {
            bytes.push(0);
        }
        write_header(&mut bytes, self.sealed.as_ref());
        write_header(&mut bytes, self.pending.as_ref());
        bytes
    }
    pub fn decode(bytes: &[u8], database: Option<DatabaseId>) -> Result<Self, StoreError> {
        let mut wire = Decoder::new(bytes);
        if wire.take(8)? != b"ZBRANCH1" {
            return Err(StoreError::Corrupt(0));
        }
        let attachment = match wire.u8()? {
            0 => None,
            1 => Some(AttachmentId::from_bytes(wire.array()?)),
            _ => return Err(StoreError::Corrupt(0)),
        };
        let sealed = read_header(&mut wire)?;
        let pending = read_header(&mut wire)?;
        wire.finish()?;
        for header in sealed.iter().chain(pending.iter()) {
            if database.as_ref().map(DatabaseId::as_bytes) != Some(&header.database_id)
                || header.parent_physical_digest == [0; 32]
            {
                return Err(StoreError::IdentityMismatch);
            }
        }
        Ok(Self {
            attachment,
            sealed,
            pending,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct State {
    pub revision: Option<Revision>,
    pub database: Option<DatabaseId>,
    pub attachment: Option<AttachmentId>,
    pub sealed: Option<crate::format::ActiveHeader>,
    pub pending: Option<crate::format::ActiveHeader>,
    pub placements: Index,
    pub endpoints: Index,
    pub retentions: Index,
    pub advisory: Index,
    pub uncertain: bool,
    pub heads: Index,
    pub selected: String,
}
impl State {
    pub fn load_for(backend: &dyn StorageBackend, selected: &str) -> Result<Self, StoreError> {
        let Some(root) = backend.read_root()? else {
            return Ok(Self {
                selected: selected.to_owned(),
                ..Self::default()
            });
        };
        let bytes = root.bytes();
        if bytes.len() < 40
            || &bytes[..8] != b"ZCAT0001"
            || blake3::hash(&bytes[..bytes.len() - 32]).as_bytes() != &bytes[bytes.len() - 32..]
        {
            return Err(StoreError::Corrupt(0));
        }
        let mut wire = Decoder::new(&bytes[8..bytes.len() - 32]);
        let database = match wire.u8()? {
            0 => None,
            1 => Some(DatabaseId::from_bytes(wire.array()?)),
            _ => return Err(StoreError::Corrupt(0)),
        };
        let placements = read_id(&mut wire)?;
        let endpoints = read_id(&mut wire)?;
        let retentions = read_id(&mut wire)?;
        let advisory = read_id(&mut wire)?;
        let heads = Index::load(backend, read_id(&mut wire)?)?;
        wire.finish()?;
        for (name, bytes) in &heads.entries {
            super::handle::validate_head(
                std::str::from_utf8(name).map_err(|_| StoreError::Corrupt(0))?,
            )?;
            Head::decode(bytes, database)?;
        }
        let head = heads
            .entries
            .get(selected.as_bytes())
            .map(|bytes| Head::decode(bytes, database))
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            revision: Some(root.revision().clone()),
            database,
            attachment: head.attachment,
            sealed: head.sealed,
            pending: head.pending,
            placements: Index::load(backend, placements)?,
            endpoints: Index::load(backend, endpoints)?,
            retentions: Index::load(backend, retentions)?,
            advisory: Index::load(backend, advisory)?,
            heads,
            selected: selected.to_owned(),
            uncertain: false,
        })
    }
    pub fn all_heads(&self) -> Result<Vec<Head>, StoreError> {
        self.heads
            .entries
            .values()
            .map(|bytes| Head::decode(bytes, self.database))
            .collect()
    }
    pub fn has_pending(&self) -> Result<bool, StoreError> {
        Ok(self.all_heads()?.iter().any(|head| head.pending.is_some()))
    }
    pub fn publish(&mut self, backend: &dyn StorageBackend) -> Result<(), StoreError> {
        if self.uncertain {
            return Err(
                BackendError::Uncertain("reload the catalog before retrying".into()).into(),
            );
        }
        let name = if self.selected.is_empty() {
            "main"
        } else {
            &self.selected
        };
        if self.attachment.is_some() || self.sealed.is_some() || self.pending.is_some() {
            let head = Head {
                attachment: self.attachment,
                sealed: self.sealed,
                pending: self.pending,
            };
            self.heads.insert(name.as_bytes().to_vec(), head.encode());
        } else {
            self.heads.remove(name.as_bytes());
        }
        self.heads.flush(backend)?;
        self.placements.flush(backend)?;
        self.endpoints.flush(backend)?;
        self.retentions.flush(backend)?;
        self.advisory.flush(backend)?;
        let bytes = self.encode_root();
        let publication = match backend.compare_exchange_root(self.revision.as_ref(), &bytes) {
            Ok(publication) => publication,
            Err(error) => {
                self.uncertain = true;
                return Err(error.into());
            }
        };
        match publication {
            Publication::Applied(revision) => {
                self.revision = Some(revision);
                Ok(())
            }
            Publication::Stale => {
                self.uncertain = true;
                Err(BackendError::Stale.into())
            }
            Publication::Uncertain(message) => {
                self.uncertain = true;
                Err(BackendError::Uncertain(message).into())
            }
        }
    }
    pub fn publish_confirmed(&mut self, backend: &dyn StorageBackend) -> Result<(), StoreError> {
        match self.publish(backend) {
            Err(error @ StoreError::Backend(BackendError::Uncertain(_))) => {
                let current = backend.read_root()?;
                if let Some(root) = current
                    && root.bytes() == self.encode_root()
                {
                    self.revision = Some(root.revision().clone());
                    self.uncertain = false;
                    return Ok(());
                }
                Err(error)
            }
            result => result,
        }
    }
    fn encode_root(&self) -> Vec<u8> {
        let mut bytes = b"ZCAT0001".to_vec();
        if let Some(id) = self.database {
            bytes.push(1);
            bytes.extend(id.as_bytes());
        } else {
            bytes.push(0);
        }
        for index in [
            &self.placements,
            &self.endpoints,
            &self.retentions,
            &self.advisory,
            &self.heads,
        ] {
            write_id(&mut bytes, index.head);
        }
        bytes.extend(blake3::hash(&bytes).as_bytes());
        bytes
    }
    pub fn index_objects(&self) -> BTreeSet<ObjectKey> {
        [
            &self.placements,
            &self.endpoints,
            &self.retentions,
            &self.advisory,
            &self.heads,
        ]
        .into_iter()
        .flat_map(|index| index.objects.iter().copied().map(ObjectKey::Index))
        .collect()
    }
}
fn read_header(wire: &mut Decoder<'_>) -> Result<Option<crate::format::ActiveHeader>, StoreError> {
    match wire.u8()? {
        0 => Ok(None),
        1 => Ok(Some(crate::format::ActiveHeader::decode(&wire.array()?)?)),
        _ => Err(StoreError::Corrupt(0)),
    }
}
fn write_header(bytes: &mut Vec<u8>, header: Option<&crate::format::ActiveHeader>) {
    if let Some(header) = header {
        bytes.push(1);
        bytes.extend(header.encode());
    } else {
        bytes.push(0);
    }
}

pub(super) fn pack_key(id: crate::domain::PackId) -> Vec<u8> {
    id.as_bytes().to_vec()
}
pub(super) fn endpoint_value(header: super::segment::Header) -> Vec<u8> {
    header.encode().to_vec()
}
