use crate::util::types::*;
use integer_encoding::*;

/// write log record.
pub struct WriteLogRecord {
    user_key: UserKey,
    seq_num: SeqNum,
    write_type: WriteType,
    user_val: UserValue,
}

impl WriteLogRecord {
    pub fn new() -> Self {
        Self {
            user_key: UserKey::default(),
            seq_num: SeqNum::default(),
            write_type: WriteType::NotSpecified,
            user_val: UserValue::default(),
        }
    }

    pub fn set_user_key(&mut self, user_key: UserKey) -> &mut Self {
        self.user_key = user_key;
        self
    }

    pub fn set_seq_num(&mut self, seq_num: SeqNum) -> &mut Self {
        self.seq_num = seq_num;
        self
    }

    pub fn set_write_type(&mut self, write_type: WriteType) -> &mut Self {
        self.write_type = write_type;
        self
    }

    pub fn set_user_val(&mut self, user_val: UserValue) -> &mut Self {
        self.user_val = user_val;
        self
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.write_varint(self.user_key).unwrap();
        encoded.write_varint(self.seq_num).unwrap();
        encoded.write_varint(self.write_type as u8).unwrap();
        encoded.write_varint(self.user_val).unwrap();
        encoded
    }

    pub fn decode_from_bytes(bytes: Vec<u8>) -> Self {
        let mut reader = bytes.as_slice();
        let mut record = WriteLogRecord::new();
        record.set_user_key(reader.read_varint().unwrap());
        record.set_seq_num(reader.read_varint().unwrap());
        let write_type = match reader.read_varint::<u8>().unwrap() {
            0 => WriteType::Put,
            1 => WriteType::Delete,
            other => panic!("Unexpected write type: {}", other),
        };
        record.set_write_type(write_type);
        record.set_user_val(reader.read_varint().unwrap());
        record
    }
}
