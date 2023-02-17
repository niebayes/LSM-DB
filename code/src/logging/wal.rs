use std::fs::{remove_file, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};

use crate::storage::keys::{TableKey, TABLE_KEY_SIZE};

pub const LOG_FILE_PATH: &str = "log";

pub struct LogWriter {
    /// file writer.
    writer: BufWriter<File>,
}

impl LogWriter {
    pub fn new() -> Self {
        let file = OpenOptions::new()
            // open the existing file or create a new one if it does not exist.
            .create(true)
            // acquire write permission.
            .write(true)
            .open(LOG_FILE_PATH)
            .unwrap();

        Self {
            writer: BufWriter::new(file),
        }
    }

    pub fn push(&mut self, table_key: &TableKey) {
        self.writer.write(&table_key.encode_to_bytes()).unwrap();
        self.writer.flush().unwrap();
    }

    /// delete the old log file and create a new one.
    pub fn reset(&mut self) {
        remove_file(LOG_FILE_PATH).unwrap();
        *self = LogWriter::new();
    }
}

pub struct LogReader;

impl LogReader {
    pub fn read_all() -> Vec<TableKey> {
        let mut table_keys = Vec::new();

        if let Ok(file) = File::open(LOG_FILE_PATH) {
            let mut reader = BufReader::new(file);
            let mut buf = [0; TABLE_KEY_SIZE];
            while reader.read_exact(&mut buf).is_ok() {
                table_keys.push(TableKey::decode_from_bytes(&buf.to_vec()).unwrap());
            }
        }

        table_keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_writer_reader() {
        let mut writer = LogWriter::new();
        let num_keys = 100;
        for i in 0..num_keys {
            writer.push(&TableKey::identity(i));
        }

        let table_keys = LogReader::read_all();
        for i in 0..num_keys {
            assert_eq!(table_keys.get(i as usize).unwrap(), &TableKey::identity(i));
        }

        writer.reset();
        let table_keys = LogReader::read_all();
        assert_eq!(table_keys.len(), 0);
    }
}
