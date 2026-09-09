use std::fmt::Debug;

use zsqlite::format::{
    Codec, CommitHeader, DictionaryPolicyRecord, FormatError, FrameHeader, SECTOR_SIZE,
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

fn storage_policy() -> StoragePolicyRecord {
    StoragePolicyRecord {
        settle_seconds: 300,
        max_stale_seconds: 3_600,
        dictionary: DictionaryPolicyRecord {
            dictionary_bytes: 65_536,
            sample_bytes: 32 * 1024 * 1024,
            min_improvement_bps: 500,
            retrain_churn_bps: 2_500,
            promotion_cooldown_seconds: 86_400,
        },
    }
}

#[test]
fn segment_header_and_trailer_reject_every_single_bit_mutation() {
    let header = SegmentHeader {
        database_id: [0x51; 32],
        page_size: 4_096,
        start_txid: 7,
        base_history: [0x53; 32],
        parent_physical_digest: [0x54; 32],
        base_logical_size: 6 * 4_096,
        generation: 3,
        last_dictionary_promotion_unix: 1_800_000_000,
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
fn free_raw_and_zstd_frame_headers_reject_every_single_bit_mutation() {
    let free = FrameHeader {
        free: true,
        page_no: 0,
        txid: 0,
        codec: Codec::Raw,
        dictionary_index: u16::MAX,
        stored_len: 0,
        raw_len: 0,
        capacity: 4_096,
        page_hash: [0; 32],
    };
    let raw = FrameHeader {
        free: false,
        page_no: 3,
        txid: 8,
        codec: Codec::Raw,
        dictionary_index: u16::MAX,
        stored_len: 4_096,
        raw_len: 4_096,
        capacity: 4_096,
        page_hash: [0x71; 32],
    };
    let zstd = FrameHeader {
        free: false,
        page_no: 44,
        txid: 9,
        codec: Codec::Zstd,
        dictionary_index: 2,
        stored_len: 1_240,
        raw_len: 4_096,
        capacity: 2_048,
        page_hash: [0x72; 32],
    };

    for (label, frame) in [
        ("free frame", free),
        ("raw frame", raw),
        ("zstd frame", zstd),
    ] {
        assert_all_single_bit_mutations_rejected(
            label,
            frame.encode(),
            &frame,
            FrameHeader::decode,
        );
    }
}

#[test]
fn commit_headers_with_and_without_truncate_reject_every_single_bit_mutation() {
    let with_truncate = CommitHeader {
        record_len: u32::try_from(SECTOR_SIZE).expect("sector size fits u32"),
        entry_count: 2,
        txid: 13,
        previous_commit: SEGMENT_HEADER_SIZE as u64,
        logical_size: 11 * 4_096,
        page_size: 4_096,
        truncate_pages: Some(4),
        previous_history: [0x81; 32],
        transaction_hash: [0x82; 32],
        resulting_history: [0x83; 32],
        entries_digest: [0x84; 32],
        commit_unix: 1_800_000_100,
    };
    let without_truncate = CommitHeader {
        txid: 14,
        previous_commit: 65_536,
        truncate_pages: None,
        ..with_truncate
    };

    for (label, header) in [
        ("commit header with truncate", with_truncate),
        ("commit header without truncate", without_truncate),
    ] {
        assert_all_single_bit_mutations_rejected(
            label,
            header.encode(),
            &header,
            CommitHeader::decode,
        );
    }
}
