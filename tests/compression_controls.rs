use std::num::NonZeroUsize;
use zsqlite::{CompressionOptions, CompressionPriority};

struct RestoreOptions(CompressionOptions);
impl Drop for RestoreOptions {
    fn drop(&mut self) {
        zsqlite::set_compression_options(self.0).unwrap();
    }
}

/// Exercise the public host controls across multiple batches and layouts, with
/// zero pages on frame/batch boundaries and bit-for-bit exported page checks.
#[test]
fn host_controls_apply_to_conversion_and_preserve_database_bytes()
-> Result<(), Box<dyn std::error::Error>> {
    let _restore = RestoreOptions(zsqlite::compression_options());
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source.sqlite");
    let mut bytes = vec![0; 145 * 65536];
    for (index, page) in bytes.as_chunks_mut::<65536>().0.iter_mut().enumerate() {
        if index % 17 != 0 {
            page.fill(u8::try_from(index % 251 + 1)?);
            page[..8].copy_from_slice(&(index as u64).to_le_bytes());
        }
    }
    bytes[..16].copy_from_slice(b"SQLite format 3\0");
    // SQLite encodes a 65536-byte page size as 1 in this 16-bit field.
    bytes[16..18].copy_from_slice(&1_u16.to_be_bytes());
    bytes[18..20].fill(1);
    bytes[21..24].copy_from_slice(&[64, 32, 32]);
    std::fs::write(&source, &bytes)?;
    for (index, (workers, priority, layout)) in [
        (
            Some(NonZeroUsize::new(1).unwrap()),
            CompressionPriority::Inherit,
            zsqlite::layout::LayoutPolicy::default(),
        ),
        (
            None,
            CompressionPriority::Inherit,
            zsqlite::layout::LayoutPolicy::default(),
        ),
        (
            Some(NonZeroUsize::new(4).unwrap()),
            CompressionPriority::Background,
            zsqlite::layout::LayoutPolicy::default()
                .fixed(zsqlite::domain::DecodedBytes::new(256 * 1024))?,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        if priority == CompressionPriority::Background
            && !cfg!(any(
                target_os = "linux",
                target_os = "android",
                target_os = "macos"
            ))
        {
            continue;
        }
        let options = CompressionOptions { workers, priority };
        zsqlite::set_compression_options(options)?;
        assert_eq!(zsqlite::compression_options(), options);
        let destination = directory.path().join(format!("converted-{index}.db"));
        let policy = zsqlite::StoragePolicy::default()
            .with_layout(layout)
            .with_dictionary(zsqlite::DictionaryPolicy::new(0, 1024 * 1024)?);
        zsqlite::convert_to_zsqlite_with_policy(&source, &destination, policy)?;
        zsqlite::verify(&destination)?;
        let exported = directory.path().join(format!("exported-{index}.sqlite"));
        zsqlite::export_to_sqlite(&destination, &exported)?;
        assert_eq!(std::fs::read(exported)?, bytes);
    }
    assert_eq!(std::fs::read(source)?, bytes);
    Ok(())
}
