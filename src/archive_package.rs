pub struct ArchivePackageViewReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> ArchivePackageViewReader<'a> {
    pub fn new(data: &'a [u8]) -> Result<Self, ArchivePackageError> {
        let mut offset = 0;
        read_package_header(data, &mut offset)?;
        Ok(Self { data, offset })
    }

    pub fn read_next(
        &mut self,
    ) -> Result<Option<ArchivePackageEntryView<'a>>, ArchivePackageError> {
        ArchivePackageEntryView::read_from_view(self.data, &mut self.offset)
    }
}

pub fn read_package_header(buf: &[u8], offset: &mut usize) -> Result<(), ArchivePackageError> {
    let end = *offset;

    // NOTE: `end > end + 4` is needed here because it eliminates useless
    // bounds check with panic. It is not even included into result assembly
    if buf.len() < end + 4 || end > end + 4 {
        return Err(ArchivePackageError::UnexpectedArchiveEof);
    }

    if buf[end..end + 4] == ARCHIVE_PREFIX {
        *offset += 4;
        Ok(())
    } else {
        Err(ArchivePackageError::InvalidArchiveHeader)
    }
}

pub struct ArchivePackageEntryView<'a> {
    pub name: &'a str,
    pub data: &'a [u8],
}

impl<'a> ArchivePackageEntryView<'a> {
    fn read_from_view(
        buf: &'a [u8],
        offset: &mut usize,
    ) -> Result<Option<Self>, ArchivePackageError> {
        if buf.len() < *offset + 8 {
            return Ok(None);
        }


        if buf[*offset..*offset + 2] != ARCHIVE_ENTRY_PREFIX {
            println!("offset: {offset}");
            println!("hex: {}", hex::encode(buf[*offset..*offset + 2]));
            return Err(ArchivePackageError::InvalidArchiveEntryHeader);
        }
        *offset += 2;

        let filename_size = u16::from_le_bytes([buf[*offset], buf[*offset + 1]]) as usize;
        *offset += 2;

        let data_size = u32::from_le_bytes([
            buf[*offset],
            buf[*offset + 1],
            buf[*offset + 2],
            buf[*offset + 3],
        ]) as usize;
        *offset += 4;

        if buf.len() < *offset + filename_size + data_size {
            return Err(ArchivePackageError::UnexpectedEntryEof);
        }

        let name = std::str::from_utf8(&buf[*offset..*offset + filename_size])
            .map_err(|_| ArchivePackageError::InvalidArchiveEntryName)?;
        *offset += filename_size;

        let data = &buf[*offset..*offset + data_size];
        *offset += data_size;

        Ok(Some(Self { name, data }))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ArchivePackageError {
    #[error("Invalid archive header")]
    InvalidArchiveHeader,
    #[error("Unexpected archive eof")]
    UnexpectedArchiveEof,
    #[error("Invalid archive entry header")]
    InvalidArchiveEntryHeader,
    #[error("Invalid archive entry name")]
    InvalidArchiveEntryName,
    #[error("Unexpected entry eof")]
    UnexpectedEntryEof,
    #[error("Too small initial batch")]
    TooSmallInitialBatch,
}

const ARCHIVE_PREFIX: [u8; 4] = u32::to_le_bytes(0xae8fdd01);
const ARCHIVE_ENTRY_PREFIX: [u8; 2] = u16::to_le_bytes(0x1e8b);
