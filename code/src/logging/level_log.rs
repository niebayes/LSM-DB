use crate::util::types::*;
use integer_encoding::*;

pub struct LevelLogRecord {
    update_type: UpdateType,
    sst_file_num: FileNum,
    from_level: LevelNum,
    to_level: LevelNum,
}

impl LevelLogRecord {
    pub fn new() -> Self {
        Self {
            update_type: UpdateType::NotSpecified,
            sst_file_num: FileNum::default(),
            from_level: LevelNum::default(),
            to_level: LevelNum::default(),
        }
    }

    pub fn set_update_type(&mut self, update_type: UpdateType) {
        self.update_type = update_type;
    }

    pub fn set_sst_file_num(&mut self, sst_file_num: FileNum) {
        self.sst_file_num = sst_file_num;
    }

    pub fn set_from_level(&mut self, from_level: LevelNum) {
        self.from_level = from_level;
    }

    pub fn set_to_level(&mut self, to_level: LevelNum) {
        self.to_level = to_level;
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.write_varint(self.update_type as u8).unwrap();
        encoded.write_varint(self.sst_file_num).unwrap();
        encoded.write_varint(self.from_level).unwrap();
        encoded.write_varint(self.to_level).unwrap();
        encoded
    }

    pub fn decode_from_bytes(bytes: Vec<u8>) -> Self {
        let mut reader = bytes.as_slice();
        let mut record = LevelLogRecord::new();
        let update_type = match reader.read_varint::<u8>().unwrap() {
            0 => UpdateType::Add,
            1 => UpdateType::Remove,
            other => panic!("Unexpected update type: {}", other),
        };
        record.set_update_type(update_type);
        record.set_sst_file_num(reader.read_varint().unwrap());
        record.set_from_level(reader.read_varint().unwrap());
        record.set_to_level(reader.read_varint().unwrap());
        record
    }
}
