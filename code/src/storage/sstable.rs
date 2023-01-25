use super::iterator::TableKeyIterator;
use super::keys::*;
use crate::util::name::sstable_file_name;
use crate::util::types::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};

/// sstable.
/// it's literally an in-memory sstable wrapper which provides read and write interfaces for an on-disk sstable file.
pub struct SSTable {
    /// sstable file number from which the corresponding sstable file could be located.
    file_num: FileNum,
}

impl SSTable {
    pub fn new(file_num: FileNum) -> Self {
        Self { file_num }
    }

    pub fn iter(&self) -> Result<SSTableIterator, ()> {
        match File::open(sstable_file_name(self.file_num)) {
            Ok(file) => {
                return Ok(SSTableIterator {
                    sstable_file_reader: BufReader::new(file),
                    curr_table_key: None,
                });
            }
            Err(err) => {
                log::error!(
                    "Failed to open sstable file {}: {}",
                    &sstable_file_name(self.file_num),
                    err
                );
                return Err(());
            }
        }
    }
}

/// an sstable's iterator.
/// keys in an sstable is clustered into a sequence of chunks.
/// each chunk contains keys with the same user key but with different sequence numbers,
/// and keys with lower sequence numbers are placed first.
pub struct SSTableIterator {
    /// a buffered reader for reading the sstable file.
    sstable_file_reader: BufReader<File>,
    curr_table_key: Option<TableKey>,
}

impl TableKeyIterator for SSTableIterator {
    fn seek(&mut self, lookup_key: &LookupKey) {
        while let Some(table_key) = self.next() {
            if table_key >= lookup_key.as_table_key() {
                break;
            }
        }
    }

    fn next(&mut self) -> Option<TableKey> {
        let mut buf = Vec::new();
        if let Ok(_) = self.sstable_file_reader.read_exact(&mut buf) {
            if let Ok(table_key) = TableKey::decode_from_bytes(&buf) {
                self.curr_table_key = Some(table_key);
                return self.curr_table_key.clone();
            }
        }
        None
    }

    fn curr(&self) -> Option<TableKey> {
        self.curr_table_key.clone()
    }

    fn valid(&self) -> bool {
        self.curr_table_key.is_some()
    }
}
