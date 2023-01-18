use crate::util::types::*;
use integer_encoding::*;

/// database log record.
pub struct DbLogRecord {
    /// memtable log file number.
    pub log_file_num: FileNum,
    /// next sequence number to allocate.
    pub next_seq_num: SeqNum,
    /// next file number to allocate.
    pub next_file_num: FileNum,
    /// the sequence number of the latest persisted kv pair.
    pub latest_persisted_seq_num: SeqNum,
}

impl DbLogRecord {
    pub fn new() -> Self {
        Self {
            log_file_num: FileNum::default(),
            next_seq_num: SeqNum::default(),
            next_file_num: FileNum::default(),
            latest_persisted_seq_num: SeqNum::default(),
        }
    }

    pub fn set_log_file_num(&mut self, log_file_num: FileNum) -> &mut Self {
        self.log_file_num = log_file_num;
        self
    }

    pub fn set_next_seq_num(&mut self, next_seq_num: SeqNum) -> &mut Self {
        self.next_seq_num = next_seq_num;
        self
    }

    pub fn set_next_file_num(&mut self, next_file_num: FileNum) -> &mut Self {
        self.next_file_num = next_file_num;
        self
    }

    pub fn set_latest_persisted_seq_num(&mut self, latest_persisted_seq_num: SeqNum) -> &mut Self {
        self.latest_persisted_seq_num = latest_persisted_seq_num;
        self
    }

    /// the encoded format is: header + data,
    /// where header is an one-byte chunk in which each bit if set to 1 indicates the corresponding data slot has meaningful data.
    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.write_varint(self.log_file_num).unwrap();
        encoded.write_varint(self.next_seq_num).unwrap();
        encoded.write_varint(self.next_file_num).unwrap();
        encoded.write_varint(self.latest_persisted_seq_num).unwrap();
        encoded
    }

    pub fn decode_from_bytes(bytes: Vec<u8>) -> Self {
        let mut reader = bytes.as_slice();
        let mut record = DbLogRecord::new();
        record.set_log_file_num(reader.read_varint().unwrap());
        record.set_next_seq_num(reader.read_varint().unwrap());
        record.set_next_file_num(reader.read_varint().unwrap());
        record.set_latest_persisted_seq_num(reader.read_varint().unwrap());
        record
    }
}
