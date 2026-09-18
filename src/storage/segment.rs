//! Self-describing segment containers. Headers are discovery hints until the
//! header+manifest digest verifies; frame payloads are authenticated separately.
use super::objects::{Manifest, ObjectKind, hex, parse_hex};
use crate::StoreError;
use crate::domain::{ManifestId, SegmentCoverage, TransactionId, TransactionSpan, ViewHash};
use crate::fs::read_exact_at;
use std::fs::File;

pub(super) const HEADER_SIZE: usize = 128;
const LIMIT: u64 = 512 * 1024 * 1024;

pub(super) fn is_container(magic: &[u8]) -> bool {
    magic == b"ZSEG0001"
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Parent {
    Checkpoint,
    Previous(ParentRef),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ParentRef {
    pub hash: ViewHash,
    pub txid: TransactionId,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Header {
    coverage: SegmentCoverage,
    logical_hash: ViewHash,
    parent: Parent,
}
impl Header {
    pub(super) fn new(
        coverage: SegmentCoverage,
        logical_hash: ViewHash,
        parent: Parent,
    ) -> Result<Self, StoreError> {
        let expected = match parent {
            Parent::Checkpoint => coverage.full().begin(),
            Parent::Previous(reference) => {
                if reference.txid < coverage.full().begin()
                    || reference.txid > coverage.full().end()
                {
                    return Err(StoreError::Range);
                }
                if reference.txid == coverage.full().end() {
                    reference.txid
                } else {
                    reference.txid.next()?
                }
            }
        };
        if coverage.represented().begin() != expected {
            return Err(StoreError::Range);
        }
        Ok(Self {
            coverage,
            logical_hash,
            parent,
        })
    }
    pub(super) fn coverage(self) -> SegmentCoverage {
        self.coverage
    }
    pub(super) fn logical_hash(self) -> ViewHash {
        self.logical_hash
    }
    pub(super) fn parent(self) -> Parent {
        self.parent
    }
    pub(super) fn encode(self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0; HEADER_SIZE];
        bytes[..8].copy_from_slice(b"ZSEG0001");
        bytes[16..24].copy_from_slice(&self.coverage.full().begin().get().to_le_bytes());
        bytes[24..32].copy_from_slice(&self.coverage.full().end().get().to_le_bytes());
        bytes[64..72].copy_from_slice(&self.coverage.represented().begin().get().to_le_bytes());
        bytes[80..112].copy_from_slice(self.logical_hash.as_bytes());
        if let Parent::Previous(parent) = self.parent {
            bytes[9] = 1;
            bytes[32..64].copy_from_slice(parent.hash.as_bytes());
            bytes[72..80].copy_from_slice(&parent.txid.get().to_le_bytes());
        }
        bytes
    }
    pub(super) fn decode(bytes: &[u8; HEADER_SIZE]) -> Result<Self, StoreError> {
        if !is_container(&bytes[..8]) || bytes[8] != 0 {
            return Err(StoreError::Corrupt(0));
        }
        if bytes[10..16]
            .iter()
            .chain(&bytes[112..])
            .any(|byte| *byte != 0)
        {
            return Err(StoreError::Corrupt(0));
        }
        let parent = match bytes[9] {
            0 if bytes[32..64]
                .iter()
                .chain(&bytes[72..80])
                .all(|byte| *byte == 0) =>
            {
                Parent::Checkpoint
            }
            1 => Parent::Previous(ParentRef {
                hash: ViewHash::from_bytes(
                    bytes[32..64].try_into().map_err(|_| StoreError::Range)?,
                ),
                txid: TransactionId::new(u64::from_le_bytes(
                    bytes[72..80].try_into().map_err(|_| StoreError::Range)?,
                ))?,
            }),
            _ => return Err(StoreError::Corrupt(0)),
        };
        Self::new(
            SegmentCoverage::new(
                TransactionSpan::new(
                    TransactionId::new(u64::from_le_bytes(
                        bytes[16..24].try_into().map_err(|_| StoreError::Range)?,
                    ))?,
                    TransactionId::new(u64::from_le_bytes(
                        bytes[24..32].try_into().map_err(|_| StoreError::Range)?,
                    ))?,
                )?,
                TransactionId::new(u64::from_le_bytes(
                    bytes[64..72].try_into().map_err(|_| StoreError::Range)?,
                ))?,
            )?,
            ViewHash::from_bytes(bytes[80..112].try_into().map_err(|_| StoreError::Range)?),
            parent,
        )
    }
    pub(super) fn filename(self, id: ManifestId) -> String {
        format!(
            "{:016x}-{:016x}-{}-{}.segment",
            self.coverage.represented().begin().get(),
            self.coverage.full().end().get(),
            hex(*self.logical_hash.as_bytes()),
            hex(*id.as_bytes())
        )
    }
}

pub(super) struct Container {
    pub header: Header,
    pub footer: Vec<u8>,
    pub id: ManifestId,
}
pub(super) fn read(file: &File) -> Result<Container, StoreError> {
    let length = file.metadata()?.len();
    if !(HEADER_SIZE as u64 + 16..=LIMIT).contains(&length) {
        return Err(StoreError::Range);
    }
    let mut encoded = [0; HEADER_SIZE];
    read_exact_at(file, 0, &mut encoded)?;
    let header = Header::decode(&encoded)?;
    let mut tail = [0; 16];
    read_exact_at(file, length - 16, &mut tail)?;
    if &tail[8..] != b"ZEND0001" {
        return Err(StoreError::Corrupt(length - 16));
    }
    let offset = u64::from_le_bytes(tail[..8].try_into().map_err(|_| StoreError::Range)?);
    if offset != HEADER_SIZE as u64 {
        return Err(StoreError::Range);
    }
    let mut footer = vec![0; usize::try_from(length - 16 - offset).map_err(|_| StoreError::Range)?];
    read_exact_at(file, offset, &mut footer)?;
    let mut hash = blake3::Hasher::new();
    hash.update(&encoded);
    hash.update(&footer);
    Ok(Container {
        header,
        footer,
        id: ManifestId::from_bytes(*hash.finalize().as_bytes()),
    })
}

pub(super) fn read_bytes(bytes: &[u8]) -> Result<Container, StoreError> {
    if !((HEADER_SIZE + 16) as u64..=LIMIT).contains(&(bytes.len() as u64)) {
        return Err(StoreError::Range);
    }
    let encoded: &[u8; HEADER_SIZE] = bytes[..HEADER_SIZE]
        .try_into()
        .map_err(|_| StoreError::Range)?;
    let header = Header::decode(encoded)?;
    let tail = &bytes[bytes.len() - 16..];
    if &tail[8..] != b"ZEND0001"
        || u64::from_le_bytes(tail[..8].try_into().map_err(|_| StoreError::Range)?)
            != HEADER_SIZE as u64
    {
        return Err(StoreError::Corrupt(0));
    }
    let footer = bytes[HEADER_SIZE..bytes.len() - 16].to_vec();
    let mut hash = blake3::Hasher::new();
    hash.update(encoded);
    hash.update(&footer);
    Ok(Container {
        header,
        footer,
        id: ManifestId::from_bytes(*hash.finalize().as_bytes()),
    })
}

#[cfg(test)]
pub(super) fn payload_bounds(file: &File) -> Result<(u64, u64), StoreError> {
    let mut magic = [0; 8];
    read_exact_at(file, 0, &mut magic)?;
    match &magic {
        b"ZPACK001" => Ok((HEADER_SIZE as u64, file.metadata()?.len())),
        _ => Err(StoreError::Corrupt(0)),
    }
}

pub(super) fn parse_filename(name: &str) -> Result<ManifestId, StoreError> {
    let bare = name
        .strip_suffix(".segment")
        .ok_or(StoreError::Corrupt(0))?;
    let fields: Vec<_> = bare.split('-').collect();
    if fields.len() != 4 || fields[0].len() != 16 || fields[1].len() != 16 {
        return Err(StoreError::Corrupt(0));
    }
    let span = TransactionSpan::new(
        TransactionId::new(
            u64::from_str_radix(fields[0], 16).map_err(|_| StoreError::Corrupt(0))?,
        )?,
        TransactionId::new(
            u64::from_str_radix(fields[1], 16).map_err(|_| StoreError::Corrupt(0))?,
        )?,
    )?;
    let logical_hash = ViewHash::from_bytes(parse_hex(fields[2])?);
    let physical = ManifestId::from_bytes(parse_hex(fields[3])?);
    let header = Header::new(
        SegmentCoverage::new(span, span.begin())?,
        logical_hash,
        Parent::Checkpoint,
    )?;
    if header.filename(physical) != name {
        return Err(StoreError::Corrupt(0));
    }
    Ok(physical)
}

pub(super) fn resolve_parent(
    guard: &super::objects::CatalogGuard,
    parent: ParentRef,
    seen: &std::collections::BTreeSet<ManifestId>,
) -> Result<ManifestId, StoreError> {
    find_endpoint(guard, parent, seen)?.ok_or(StoreError::Corrupt(0))
}

/// Shared discovery for parent links and durable logical roots. A discovered
/// physical ID still requires full metadata and logical-identity validation.
pub(super) fn find_endpoint(
    guard: &super::objects::CatalogGuard,
    endpoint: ParentRef,
    seen: &std::collections::BTreeSet<ManifestId>,
) -> Result<Option<ManifestId>, StoreError> {
    let mut candidates = Vec::new();
    for (key, encoded) in &guard.state().endpoints.entries {
        let physical = ManifestId::from_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| StoreError::Corrupt(0))?,
        );
        let header = Header::decode(
            encoded
                .as_slice()
                .try_into()
                .map_err(|_| StoreError::Corrupt(0))?,
        )?;
        if header.logical_hash() == endpoint.hash
            && header.coverage().full().end() == endpoint.txid
            && !seen.contains(&physical)
        {
            candidates.push((
                header.coverage().represented().begin(),
                !matches!(header.parent, Parent::Checkpoint),
                physical,
            ));
        }
    }
    candidates.sort_unstable();
    // Filename discovery does not confer authority. The caller validates the
    // physical digest, resolves the selected view, then verifies its logical hash.
    Ok(candidates.first().map(|candidate| candidate.2))
}

pub(super) fn is_manifest<K: ObjectKind>() -> bool {
    K::EXTENSION == Manifest::EXTENSION
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lookup_uses_coverage_not_rewrite_recency() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let catalog = super::super::Catalog::open(directory.path(), true)?;
        let guard = catalog.lock()?;
        let hash = ViewHash::from_bytes([11; 32]);
        let end = TransactionId::new(20)?;
        let add = |begin, last, hash, physical| -> Result<_, StoreError> {
            let span = TransactionSpan::new(TransactionId::new(begin)?, last)?;
            let header = Header::new(
                SegmentCoverage::new(span, span.begin())?,
                hash,
                Parent::Checkpoint,
            )?;
            let id = ManifestId::from_bytes([physical; 32]);
            let bytes = header.encode();
            guard
                .state_mut()
                .endpoints
                .insert(id.as_bytes().to_vec(), bytes.to_vec());
            Ok(id)
        };
        let old = add(10, end, hash, 1)?;
        // A newer file with equal coverage earns no preference.
        let _newer = add(10, end, hash, 2)?;
        let parent = ParentRef { hash, txid: end };
        let mut seen = std::collections::BTreeSet::new();
        assert_eq!(resolve_parent(&guard, parent, &seen)?, old);
        let wide = add(5, end, hash, 3)?;
        assert_eq!(resolve_parent(&guard, parent, &seen)?, wide);
        let _wrong_hash = add(1, end, ViewHash::from_bytes([12; 32]), 4)?;
        let _wrong_end = add(1, end.next()?, hash, 5)?;
        assert_eq!(resolve_parent(&guard, parent, &seen)?, wide);
        seen.insert(wide);
        assert_eq!(resolve_parent(&guard, parent, &seen)?, old);
        // Discovery hints cannot establish validity: these intentionally lack a
        // manifest footer and must never yield durable/validated dependencies.
        assert!(guard.validate::<Manifest>(old, LIMIT).is_err());
        Ok(())
    }

    #[test]
    fn invalid_pack_headers_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let file = tempfile::tempfile()?;
        for magic in [b"BADPACK!", b"PACKBAD!"] {
            crate::fs::write_all_at(&file, 0, magic)?;
            assert!(payload_bounds(&file).is_err());
        }
        Ok(())
    }

    #[test]
    fn names_and_headers_are_validated() -> Result<(), Box<dyn std::error::Error>> {
        let first = TransactionId::new(3)?;
        let last = TransactionId::new(27)?;
        assert!(TransactionSpan::new(last, first).is_err());
        let id = ManifestId::from_bytes([7; 32]);
        let span = TransactionSpan::new(first, last)?;
        let header = Header::new(
            SegmentCoverage::new(span, first.next()?)?,
            ViewHash::from_bytes([11; 32]),
            Parent::Previous(ParentRef {
                hash: ViewHash::from_bytes([7; 32]),
                txid: first,
            }),
        )?;
        assert!(Header::new(header.coverage, header.logical_hash, Parent::Checkpoint).is_err());
        for parent_txid in [TransactionId::new(2)?, TransactionId::new(4)?, last.next()?] {
            assert!(
                Header::new(
                    header.coverage,
                    header.logical_hash,
                    Parent::Previous(ParentRef {
                        hash: header.logical_hash,
                        txid: parent_txid
                    }),
                )
                .is_err()
            );
        }
        assert_eq!(parse_filename(&header.filename(id))?, id);
        let decoded = Header::decode(&header.encode())?;
        assert_eq!(decoded.parent, header.parent);
        assert_eq!(decoded.coverage, header.coverage);
        assert!(!header.filename(id).starts_with('L'));
        assert_eq!(&header.encode()[..8], b"ZSEG0001");
        assert_eq!(header.encode()[8], 0);
        for offset in [0, 8, 9, 10, 112, 127] {
            let mut bytes = header.encode();
            bytes[offset] = 255;
            assert!(Header::decode(&bytes).is_err(), "offset {offset}");
        }
        for invalid in [
            "L00-0000000000000003-000000000000001b",
            "L63-0000000000000003-000000000000001b",
            "000000000000001b-0000000000000003",
            "3-27",
        ] {
            assert!(
                parse_filename(&format!(
                    "{invalid}-{}-{}.segment",
                    hex(*header.logical_hash.as_bytes()),
                    hex(*id.as_bytes())
                ))
                .is_err()
            );
        }
        Ok(())
    }
}
