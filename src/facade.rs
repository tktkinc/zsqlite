//! Ordinary-SQLite facade files for logical `.db` paths.

use crate::fs::{read_exact_at, sync_parent_dir, write_all_at};
use crate::store::StoreError;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const NOTICE_PAGE_SIZE: usize = 4096;
const NOTICE_PAGE_SIZE_U16: u16 = 4096;
const NOTICE_APPLICATION_ID: [u8; 4] = *b"ZSQL";
const NOTICE_VIEW: &str = "zsqlite_extension_required";
const NOTICE_SQL: &str = "CREATE VIEW zsqlite_extension_required AS SELECT 'This database uses zsqlite storage. Load the zsqlite extension and reopen with vfs=zsqlite.' AS message";
static NOTICE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Maps the supported logical facade name to its active-segment pathname.
pub(crate) fn vfs_storage_path(path: &Path) -> PathBuf {
    if path.extension().is_some_and(|extension| extension == "db") {
        append_suffix(path, ".zsqlite")
    } else {
        path.to_path_buf()
    }
}

/// Resolves a public maintenance path while retaining direct `.zsqlite` use.
pub(crate) fn storage_path(path: &Path) -> PathBuf {
    vfs_storage_path(path)
}

/// Returns the logical `.db` facade belonging to a physical active segment.
pub(crate) fn notice_path(storage: &Path) -> Option<PathBuf> {
    if storage
        .extension()
        .is_none_or(|extension| extension != "zsqlite")
    {
        return None;
    }
    let logical = storage.with_extension("");
    logical
        .extension()
        .is_some_and(|extension| extension == "db")
        .then_some(logical)
}

pub(crate) fn validate_notice(storage: &Path) -> Result<(), StoreError> {
    let Some(path) = notice_path(storage) else {
        return Ok(());
    };
    if !path.exists() || notice_matches(&path)? {
        Ok(())
    } else {
        Err(StoreError::InvalidConfiguration(
            "logical .db path is not the zsqlite notice database",
        ))
    }
}

pub(crate) fn ensure_notice(storage: &Path) -> Result<(), StoreError> {
    let Some(path) = notice_path(storage) else {
        return Ok(());
    };
    match File::open(&path) {
        Ok(file) => {
            if !notice_file_matches(&file)? {
                return Err(StoreError::InvalidConfiguration(
                    "logical .db path is not the zsqlite notice database",
                ));
            }
            make_read_only(&file)?;
            return Ok(());
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let staging = unused_notice_path(&path)?;
    let result = (|| {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&staging)?;
        write_all_at(&file, 0, &notice_database())?;
        file.sync_all()?;
        make_read_only(&file)?;
        file.sync_all()?;
        match std::fs::hard_link(&staging, &path) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                if !notice_matches(&path)? {
                    return Err(StoreError::InvalidConfiguration(
                        "logical .db path is not the zsqlite notice database",
                    ));
                }
            }
            Err(error) => return Err(error.into()),
        }
        sync_parent_dir(&path)?;
        Ok(())
    })();
    let removed = std::fs::remove_file(&staging);
    if result.is_ok() && removed.is_ok() {
        sync_parent_dir(&path)?;
    }
    result
}

pub(crate) fn delete_notice(storage: &Path) -> Result<(), StoreError> {
    let Some(path) = notice_path(storage) else {
        return Ok(());
    };
    match File::open(&path) {
        Ok(file) if notice_file_matches(&file)? => {}
        Ok(_) => {
            return Err(StoreError::InvalidConfiguration(
                "logical .db path is not the zsqlite notice database",
            ));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    match std::fs::remove_file(&path) {
        Ok(()) => sync_parent_dir(&path)?,
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn notice_matches(path: &Path) -> Result<bool, StoreError> {
    match File::open(path) {
        Ok(file) => notice_file_matches(&file),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn notice_file_matches(file: &File) -> Result<bool, StoreError> {
    if file.metadata()?.len() != NOTICE_PAGE_SIZE as u64 {
        return Ok(false);
    }
    let mut bytes = [0; NOTICE_PAGE_SIZE];
    read_exact_at(file, 0, &mut bytes)?;
    Ok(bytes == notice_database())
}

fn make_read_only(file: &File) -> Result<(), StoreError> {
    let mut permissions = file.metadata()?.permissions();
    #[cfg(unix)]
    permissions.set_mode(0o444);
    #[cfg(not(unix))]
    permissions.set_readonly(true);
    file.set_permissions(permissions)?;
    Ok(())
}

fn unused_notice_path(path: &Path) -> Result<PathBuf, StoreError> {
    for _ in 0..1024 {
        let sequence = NOTICE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = path.with_file_name(format!(
            ".{}.notice.{}.{}",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("database.db"),
            std::process::id(),
            sequence
        ));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err(StoreError::Range)
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut output = path.as_os_str().to_os_string();
    output.push(suffix);
    PathBuf::from(output)
}

fn notice_database() -> [u8; NOTICE_PAGE_SIZE] {
    let mut page = [0; NOTICE_PAGE_SIZE];
    page[..16].copy_from_slice(b"SQLite format 3\0");
    page[16..18].copy_from_slice(&NOTICE_PAGE_SIZE_U16.to_be_bytes());
    page[18] = 1;
    page[19] = 1;
    page[21..24].copy_from_slice(&[64, 32, 32]);
    put_u32(&mut page, 24, 1); // File change counter.
    put_u32(&mut page, 28, 1); // One database page.
    put_u32(&mut page, 40, 1); // Schema cookie.
    put_u32(&mut page, 44, 4); // Schema format.
    put_u32(&mut page, 56, 1); // UTF-8.
    put_u32(&mut page, 60, u32::from(crate::format::FORMAT_VERSION));
    page[68..72].copy_from_slice(&NOTICE_APPLICATION_ID);
    put_u32(&mut page, 92, 1); // Version-valid-for matches the change counter.
    put_u32(&mut page, 96, 3_049_000);

    let mut record_header = Vec::new();
    let serial_types = [
        text_serial_type("view"),
        text_serial_type(NOTICE_VIEW),
        text_serial_type(NOTICE_VIEW),
        8, // Integer constant zero: views have no root b-tree page.
        text_serial_type(NOTICE_SQL),
    ];
    let header_len = 1 + serial_types
        .iter()
        .map(|value| varint_len(*value))
        .sum::<usize>();
    put_varint(&mut record_header, header_len as u64);
    for serial_type in serial_types {
        put_varint(&mut record_header, serial_type);
    }

    let mut payload = record_header;
    payload.extend_from_slice(b"view");
    payload.extend_from_slice(NOTICE_VIEW.as_bytes());
    payload.extend_from_slice(NOTICE_VIEW.as_bytes());
    payload.extend_from_slice(NOTICE_SQL.as_bytes());

    let mut cell = Vec::new();
    put_varint(&mut cell, payload.len() as u64);
    put_varint(&mut cell, 1); // sqlite_schema rowid.
    cell.extend_from_slice(&payload);
    let cell_offset = NOTICE_PAGE_SIZE - cell.len();
    let cell_offset_u16 = u16::try_from(cell_offset).expect("notice cell offset fits u16");

    // Page one is the sqlite_schema table's leaf b-tree page. Its page header
    // starts after the 100-byte database header.
    page[100] = 0x0d;
    page[103..105].copy_from_slice(&1_u16.to_be_bytes());
    page[105..107].copy_from_slice(&cell_offset_u16.to_be_bytes());
    page[108..110].copy_from_slice(&cell_offset_u16.to_be_bytes());
    page[cell_offset..].copy_from_slice(&cell);
    page
}

fn text_serial_type(value: &str) -> u64 {
    13 + 2 * value.len() as u64
}

fn varint_len(mut value: u64) -> usize {
    let mut length = 1;
    while value > 0x7f {
        value >>= 7;
        length += 1;
    }
    length
}

fn put_varint(output: &mut Vec<u8>, value: u64) {
    let length = varint_len(value);
    for index in (0..length).rev() {
        let mut byte = ((value >> (index * 7)) & 0x7f) as u8;
        if index != 0 {
            byte |= 0x80;
        }
        output.push(byte);
    }
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_and_storage_names_round_trip() {
        let logical = Path::new("example.db");
        let storage = vfs_storage_path(logical);
        assert_eq!(storage, Path::new("example.db.zsqlite"));
        assert_eq!(notice_path(&storage).as_deref(), Some(logical));
        assert_eq!(
            vfs_storage_path(Path::new("example.zsqlite")),
            Path::new("example.zsqlite")
        );
        assert_eq!(notice_path(Path::new("example.zsqlite")), None);
    }

    #[test]
    fn notice_has_a_valid_sqlite_header() {
        let notice = notice_database();
        assert_eq!(&notice[..16], b"SQLite format 3\0");
        assert_eq!(&notice[68..72], b"ZSQL");
        assert_eq!(u32::from_be_bytes(notice[28..32].try_into().unwrap()), 1);
    }
}
