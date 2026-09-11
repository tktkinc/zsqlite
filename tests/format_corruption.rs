use std::fmt::Debug;

use zsqlite::format::{
    ACTIVE_STATE_SIZE, ActiveState, Codec, DictionaryPolicyRecord, FormatError, FrameHeader,
    SEGMENT_HEADER_SIZE, SegmentHeader, SegmentTrailer, StoragePolicyRecord,
};

fn assert_all_single_bit_mutations_rejected<const N: usize, T>(
    label: &str,
    encoded: [u8; N],
    expected: &T,
    decode: impl Fn(&[u8; N]) -> Result<T, FormatError>,
) where
    T: Debug + PartialEq,
{
    let decoded = decode(&encoded).expect("valid encoded record must decode");
    assert_eq!(
        &decoded, expected,
        "{label} did not round-trip before corruption"
    );

    // Exhausting every one-bit error covers every byte, including reserved
    // padding and the checksum itself, while being stricter than one fixed
    // whole-byte mutation per position.
    for byte_index in 0..N {
        for bit_index in 0..u8::BITS {
            let mut corrupt = encoded;
            corrupt[byte_index] ^= 1_u8 << bit_index;
            assert!(
                decode(&corrupt).is_err(),
                "{label} accepted corruption at byte {byte_index}, bit {bit_index}"
            );
        }
    }
}

#[test]
fn active_state_rejects_every_single_bit_mutation() {
    let state = ActiveState {
        database_id: [0x41; 32],
        sequence: 7,
        txid: 12,
        logical_size: 32 * 4_096,
        page_size: 4_096,
        history: [0x52; 32],
        commit_unix: 1_700_000_000,
        record_count: 17,
        truncate_pages: Some(12),
    };
    assert_all_single_bit_mutations_rejected(
        "active state",
        state.encode(),
        &state,
        ActiveState::decode,
    );
    assert_eq!(state.encode().len(), ACTIVE_STATE_SIZE);
}

fn storage_policy() -> StoragePolicyRecord {
    StoragePolicyRecord {
        settle_seconds: 300,
        max_stale_seconds: 3_600,
        target_segment_bytes: 64 * 1024 * 1024,
        dictionary: DictionaryPolicyRecord {
            dictionary_bytes: 65_536,
            sample_bytes: 32 * 1024 * 1024,
        },
    }
}

#[test]
fn segment_header_and_trailer_reject_every_single_bit_mutation() {
    let header = SegmentHeader {
        mutable_snapshot: false,
        database_id: [0x51; 32],
        page_size: 4_096,
        start_txid: 7,
        base_history: [0x53; 32],
        parent_physical_digest: [0x54; 32],
        base_logical_size: 6 * 4_096,
        generation: 3,
        policy: storage_policy(),
        dictionary_offset: SEGMENT_HEADER_SIZE as u64,
        dictionary_len: 8_192,
        base_map_offset: SEGMENT_HEADER_SIZE as u64 + 8_192,
        base_map_len: 1_024,
        records_offset: 16_384,
    };
    assert_all_single_bit_mutations_rejected(
        "segment header",
        header.encode(),
        &header,
        SegmentHeader::decode,
    );

    let trailer = SegmentTrailer {
        database_id: header.database_id,
        start_txid: header.start_txid,
        end_txid: 19,
        base_history: header.base_history,
        end_history: [0x61; 32],
        logical_size: 23 * 4_096,
        page_size: header.page_size,
        index_offset: 32_768,
        index_len: 1_200,
        map_offset: 36_864,
        map_len: 2_048,
        content_root: [0x62; 32],
        physical_digest: [0x63; 32],
    };
    assert_all_single_bit_mutations_rejected(
        "segment trailer",
        trailer.encode(true),
        &trailer,
        SegmentTrailer::decode,
    );
}

#[test]
fn raw_and_zstd_frame_headers_reject_every_single_bit_mutation() {
    let raw = FrameHeader {
        page_no: 3,
        codec: Codec::Raw,
        dictionary_index: u16::MAX,
        stored_len: 4_096,
        raw_len: 4_096,
    };
    let zstd = FrameHeader {
        page_no: 44,
        codec: Codec::Zstd,
        dictionary_index: 2,
        stored_len: 1_240,
        raw_len: 4_096,
    };

    for (label, frame) in [("raw frame", raw), ("zstd frame", zstd)] {
        assert_all_single_bit_mutations_rejected(
            label,
            frame.encode(),
            &frame,
            FrameHeader::decode,
        );
    }
}
