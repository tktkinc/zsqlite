//! Bounded, ordered batches of independently encoded frames.
use super::frame::{EncodedFrame, FrameEncoder, PageVersion};
use crate::domain::{DictionaryId, PageSize};
use crate::{CompressionOptions, StoreError};

pub(super) const BATCH_BYTES: usize = 8 * 1024 * 1024;
pub(super) type FramePages = Vec<(PageVersion, Vec<u8>)>;

/// Each worker owns its codec contexts. Joining in input order keeps frame and
/// pack layout independent of scheduling. Only one bounded batch is in flight.
pub(super) fn encode_batch(
    mut groups: Vec<FramePages>,
    size: PageSize,
    level: i32,
    dictionary: Option<(DictionaryId, &[u8])>,
    options: CompressionOptions,
) -> Result<Vec<EncodedFrame>, StoreError> {
    if groups.is_empty() {
        return Ok(Vec::new());
    }
    let workers = options.worker_count(groups.len());
    let encode = |groups: &mut [FramePages]| -> Result<Vec<EncodedFrame>, StoreError> {
        options.priority.apply()?;
        let mut encoder = FrameEncoder::new(level, dictionary)?;
        groups
            .iter_mut()
            .map(|pages| encoder.build(size, std::mem::take(pages)))
            .collect()
    };
    // Small seals and explicit single-worker execution need no thread when
    // they inherit priority. Background work always uses a private thread.
    if workers == 1 && options.priority == crate::CompressionPriority::Inherit {
        return encode(&mut groups);
    }
    #[cfg(target_os = "emscripten")]
    {
        encode(&mut groups)
    }
    #[cfg(not(target_os = "emscripten"))]
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        let mut remaining = groups.as_mut_slice();
        for worker in 0..workers {
            let count = remaining.len().div_ceil(workers - worker);
            let (groups, tail) = remaining.split_at_mut(count);
            remaining = tail;
            handles.push(
                std::thread::Builder::new()
                    .name("zsqlite-compression".into())
                    .spawn_scoped(scope, || encode(groups))?,
            );
        }
        let mut frames = Vec::new();
        // Join every worker, including after an error, before returning.
        let mut error = None;
        for handle in handles {
            match handle
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            {
                Ok(encoded) => frames.extend(encoded),
                Err(failure) => {
                    error.get_or_insert(failure);
                }
            }
        }
        error.map_or(Ok(frames), Err)
    })
}

#[cfg(test)]
mod tests {
    use super::super::frame::DecodingDictionary;
    use super::*;
    use crate::domain::{CompressionDictionary, PageNumber, PayloadEncoding, TransactionId};
    use std::collections::BTreeMap;
    use std::num::NonZeroUsize;
    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn parallel_frames_match_serial_encoding_and_authenticate_in_order() -> TestResult {
        let size = PageSize::new(4096)?;
        let dictionary: Vec<_> = (0..128_u32)
            .flat_map(|index| blake3::hash(&index.to_le_bytes()).as_bytes().to_vec())
            .collect();
        let id = DictionaryId::from_bytes(*blake3::hash(&dictionary).as_bytes());
        let decoding = BTreeMap::from([(id, DecodingDictionary::new(dictionary.clone())?)]);
        let mut page = 1;
        let mut groups = Vec::new();
        for index in 0..37 {
            let mut group = Vec::new();
            for _ in 0..=index % 4 {
                let mut raw = dictionary.clone();
                raw[..4].copy_from_slice(&u32::to_le_bytes(page));
                group.push((
                    PageVersion::verified(PageNumber::new(page)?, TransactionId::new(1)?, &raw),
                    raw,
                ));
                page += 1;
            }
            groups.push(group);
        }
        let mut serial = FrameEncoder::new(3, Some((id, &dictionary)))?;
        let expected = groups
            .iter()
            .map(|group| {
                serial
                    .build(size, group.clone())
                    .map(EncodedFrame::into_parts)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let actual = encode_batch(
            groups.clone(),
            size,
            3,
            Some((id, &dictionary)),
            CompressionOptions {
                workers: NonZeroUsize::new(4),
                ..CompressionOptions::default()
            },
        )?;
        assert!(expected.iter().any(|(metadata, _)| metadata.encoding()
            == PayloadEncoding::Zstandard(CompressionDictionary::Shared(id))));
        assert_eq!(actual.len(), groups.len());
        for ((actual, expected), pages) in actual.into_iter().zip(expected).zip(groups) {
            let (metadata, payload) = actual.into_parts();
            assert_eq!((&metadata, &payload), (&expected.0, &expected.1));
            let (_, decoded) = EncodedFrame::verified(metadata, payload, &decoding)?;
            assert_eq!(
                decoded.bytes(),
                pages
                    .into_iter()
                    .flat_map(|(_, bytes)| bytes)
                    .collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    #[test]
    fn a_bad_page_fails_the_whole_parallel_batch() -> TestResult {
        let size = PageSize::new(4096)?;
        let raw = vec![3; size.as_usize()];
        let mut groups = (1..=17)
            .map(|page| {
                Ok(vec![(
                    PageVersion::verified(PageNumber::new(page)?, TransactionId::new(1)?, &raw),
                    raw.clone(),
                )])
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        groups[8][0].1[0] ^= 1;
        assert!(matches!(
            encode_batch(groups, size, 3, None, CompressionOptions::default()),
            Err(StoreError::PageChecksum(9))
        ));
        Ok(())
    }
}
