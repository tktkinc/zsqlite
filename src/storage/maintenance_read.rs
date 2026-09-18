//! Bounded read-ahead for maintenance over immutable, pinned pack locations.
use super::placement::PlacementPin;
use super::view::FrameLocation;
use crate::StoreError;
use crate::domain::{FileOffset, PackId, StoredBytes, StoredRange};

const WINDOW_BYTES: u64 = 8 * 1024 * 1024;
const SINGLE_FRAME_BYTES: u64 = 16 * 1024 * 1024;
const GAP_BYTES: u64 = 64 * 1024;
const READ_AMPLIFICATION: u64 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Span {
    pack: PackId,
    start: u64,
    end: u64,
}
impl Span {
    fn frame(location: &FrameLocation) -> Result<Self, StoreError> {
        if location.payload.length() != location.metadata.payload_bytes() {
            return Err(StoreError::Range);
        }
        let prefix = u64::try_from(location.metadata.encoded_len())
            .map_err(|_| StoreError::Range)?
            .checked_add(36)
            .and_then(|length| length.checked_add(crate::format::FRAME_HEADER_SIZE as u64))
            .ok_or(StoreError::Range)?;
        let start = location
            .payload
            .offset()
            .get()
            .checked_sub(prefix)
            .ok_or(StoreError::Range)?;
        if start < super::segment::HEADER_SIZE as u64 {
            return Err(StoreError::Range);
        }
        let end = location
            .payload
            .offset()
            .get()
            .checked_add(location.payload.length().get())
            .ok_or(StoreError::Range)?;
        Ok(Self {
            pack: location.pack,
            start,
            end,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Window {
    span: Span,
    frame_bytes: u64,
}

fn plan(mut frames: Vec<Span>) -> Result<Vec<Window>, StoreError> {
    frames.sort_unstable_by_key(|frame| (frame.pack, frame.start, frame.end));
    frames.dedup();
    let mut windows: Vec<Window> = Vec::new();
    for frame in frames {
        let frame_bytes = frame
            .end
            .checked_sub(frame.start)
            .filter(|length| *length != 0 && *length <= SINGLE_FRAME_BYTES)
            .ok_or(StoreError::Range)?;
        if let Some(last) = windows.last_mut()
            && last.span.pack == frame.pack
        {
            let gap = frame
                .start
                .checked_sub(last.span.end)
                .ok_or(StoreError::Corrupt(frame.start))?;
            let length = frame.end - last.span.start;
            let included = last
                .frame_bytes
                .checked_add(frame_bytes)
                .ok_or(StoreError::Range)?;
            let amplified = included
                .checked_mul(READ_AMPLIFICATION)
                .ok_or(StoreError::Range)?;
            if gap <= GAP_BYTES && length <= WINDOW_BYTES && length <= amplified {
                last.span.end = frame.end;
                last.frame_bytes = included;
                continue;
            }
        }
        windows.push(Window {
            span: frame,
            frame_bytes,
        });
    }
    Ok(windows)
}

/// Keeps one bounded range in memory. Visit frames in pack/offset order to read
/// each window once. The included metadata bytes only bridge adjacent records;
/// callers still authenticate every returned header and payload against the
/// manifest before using it.
pub(super) struct MaintenanceReads<'pin> {
    placement: &'pin PlacementPin,
    windows: Vec<Window>,
    cached: Option<(usize, Vec<u8>)>,
}
impl<'pin> MaintenanceReads<'pin> {
    pub(super) fn new<'frame>(
        placement: &'pin PlacementPin,
        locations: impl IntoIterator<Item = &'frame FrameLocation>,
    ) -> Result<Self, StoreError> {
        let frames = locations
            .into_iter()
            .map(|location| {
                let frame = Span::frame(location)?;
                if frame.end > placement.length(frame.pack)?.get() {
                    return Err(StoreError::Range);
                }
                Ok(frame)
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        Ok(Self {
            placement,
            windows: plan(frames)?,
            cached: None,
        })
    }

    /// Returns just the serialized frame header and payload, excluding the
    /// metadata prefix that maintenance re-emits from authenticated metadata.
    pub(super) fn read(&mut self, location: &FrameLocation) -> Result<Vec<u8>, StoreError> {
        let frame = Span::frame(location)?;
        let index = self
            .windows
            .partition_point(|window| {
                (window.span.pack, window.span.start) <= (frame.pack, frame.start)
            })
            .checked_sub(1)
            .ok_or(StoreError::Range)?;
        let window = self.windows[index].span;
        if window.pack != frame.pack || frame.end > window.end {
            return Err(StoreError::Range);
        }
        if self.cached.as_ref().is_none_or(|(slot, _)| *slot != index) {
            // Release the previous buffer before asking the backend for more.
            self.cached = None;
            let bytes = self.placement.read(
                window.pack,
                StoredRange::new(
                    FileOffset::new(window.start),
                    StoredBytes::new(window.end - window.start),
                )?,
            )?;
            self.cached = Some((index, bytes));
        }
        let record_start = location
            .payload
            .offset()
            .get()
            .checked_sub(crate::format::FRAME_HEADER_SIZE as u64)
            .and_then(|offset| offset.checked_sub(window.start))
            .ok_or(StoreError::Range)?;
        let start = usize::try_from(record_start).map_err(|_| StoreError::Range)?;
        let end = usize::try_from(frame.end - window.start).map_err(|_| StoreError::Range)?;
        self.cached
            .as_ref()
            .ok_or(StoreError::Range)?
            .1
            .get(start..end)
            .map(<[u8]>::to_vec)
            .ok_or(StoreError::Range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(pack: u8, start: u64, length: u64) -> Span {
        Span {
            pack: PackId::from_bytes([pack; 32]),
            start,
            end: start.checked_add(length).unwrap(),
        }
    }

    #[test]
    fn adjacent_records_share_a_window_in_pack_order() {
        let windows = plan(vec![
            frame(2, 32, 2048),
            frame(1, 2080, 2048),
            frame(1, 32, 2048),
        ])
        .unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].span, frame(1, 32, 4096));
        assert_eq!(windows[0].frame_bytes, 4096);
        assert_eq!(windows[1].span, frame(2, 32, 2048));
    }

    #[test]
    fn sparse_ranges_limit_both_gap_and_amplification() {
        let adjacent = frame(1, 32, 1024);
        // Exactly four times the complete source records is acceptable.
        let near = frame(1, 32 + 7 * 1024, 1024);
        let windows = plan(vec![adjacent, near]).unwrap();
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].span.end - windows[0].span.start, 8 * 1024);
        assert_eq!(
            plan(vec![adjacent, frame(1, near.start + 1, 1024)])
                .unwrap()
                .len(),
            2
        );

        // Large useful records still cannot bridge a gap over 64 KiB.
        let large = frame(1, 32, 1024 * 1024);
        assert_eq!(
            plan(vec![large, frame(1, large.end + GAP_BYTES, 1024 * 1024)])
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            plan(vec![
                large,
                frame(1, large.end + GAP_BYTES + 1, 1024 * 1024)
            ])
            .unwrap()
            .len(),
            2
        );
    }

    #[test]
    fn windows_are_bounded_and_oversized_frames_stay_separate() {
        let first = frame(1, 32, WINDOW_BYTES / 2);
        let second = frame(1, first.end, WINDOW_BYTES / 2);
        let third = frame(1, second.end, 1024);
        let windows = plan(vec![first, second, third]).unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].span.end - windows[0].span.start, WINDOW_BYTES);

        let large = frame(1, 32, SINGLE_FRAME_BYTES);
        assert_eq!(
            plan(vec![large, frame(1, large.end, 1024)]).unwrap().len(),
            2
        );
        assert!(plan(vec![frame(1, 32, SINGLE_FRAME_BYTES + 1)]).is_err());
        assert!(plan(vec![frame(1, 32, 0)]).is_err());
    }

    #[test]
    fn duplicate_frames_do_not_relax_amplification_limit() {
        let first = frame(1, 32, 1024);
        let distant = frame(1, first.end + GAP_BYTES, 1024);
        let mut frames = vec![first; 64];
        frames.push(distant);
        let windows = plan(frames).unwrap();
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].frame_bytes, 1024);
        assert!(plan(vec![first, frame(1, first.start + 1, 1024)]).is_err());
    }

    #[test]
    fn source_span_includes_metadata_and_rejects_invalid_offsets() {
        use crate::domain::{PackOffset, PageNumber, PageSize, TransactionId};
        use crate::storage::frame::{FrameEncoder, PageVersion};
        use crate::storage::placement::PackRange;
        use std::collections::BTreeMap;

        let bytes = vec![7; 512];
        let version = PageVersion::verified(
            PageNumber::new(1).unwrap(),
            TransactionId::new(1).unwrap(),
            &bytes,
        );
        let (metadata, _) = FrameEncoder::new(1, &BTreeMap::new())
            .unwrap()
            .build(PageSize::new(512).unwrap(), vec![(version, bytes)])
            .unwrap()
            .into_parts();
        let prefix = 36 + metadata.encoded_len() as u64 + crate::format::FRAME_HEADER_SIZE as u64;
        let payload_bytes = metadata.payload_bytes();
        let start = super::super::segment::HEADER_SIZE as u64;
        let mut location = FrameLocation {
            pack: PackId::from_bytes([1; 32]),
            payload: PackRange::new(PackOffset::new(start + prefix), payload_bytes).unwrap(),
            metadata,
        };
        assert_eq!(
            Span::frame(&location).unwrap(),
            frame(1, start, prefix + payload_bytes.get())
        );
        location.payload = PackRange::new(PackOffset::new(prefix - 1), payload_bytes).unwrap();
        assert!(Span::frame(&location).is_err());
        location.payload =
            PackRange::new(PackOffset::new(start + prefix - 1), payload_bytes).unwrap();
        assert!(Span::frame(&location).is_err());
        location.payload = PackRange::new(
            PackOffset::new(start + prefix),
            StoredBytes::new(payload_bytes.get() + 1),
        )
        .unwrap();
        assert!(Span::frame(&location).is_err());
    }
}
