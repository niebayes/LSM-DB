use crate::util::types::*;
use integer_encoding::*;
use std::cmp::Ordering;
use std::io;
use std::mem;

/// table key type.
/// table key = user key + seq num + write type + user val.
/// user key and sequence number are necessaties for a table key, so they are placed at the front.
/// write type and user value may not be specified for lookup, so they are placed at the tail.
#[derive(Clone)]
pub struct TableKey {
    /// user key.
    pub user_key: UserKey,
    /// sequence number.
    pub seq_num: SeqNum,
    /// write type.
    pub write_type: WriteType,
    /// user value.
    pub user_val: UserValue,
}

impl Default for TableKey {
    fn default() -> Self {
        Self {
            user_key: UserKey::default(),
            seq_num: SeqNum::default(),
            write_type: WriteType::NotSpecified,
            user_val: UserValue::default(),
        }
    }
}

pub const TABLE_KEY_SIZE: usize = mem::size_of::<UserKey>()
    + mem::size_of::<SeqNum>()
    + mem::size_of::<WriteType>()
    + mem::size_of::<UserValue>();

impl TableKey {
    pub fn new(
        user_key: UserKey,
        user_val: UserValue,
        seq_num: SeqNum,
        write_type: WriteType,
    ) -> Self {
        Self {
            user_key,
            user_val,
            seq_num,
            write_type,
        }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.write_varint(self.user_key).unwrap();
        encoded.write_varint(self.seq_num).unwrap();
        encoded.write_varint(self.write_type as u8).unwrap();
        encoded.write_varint(self.user_val).unwrap();
        encoded
    }

    pub fn decode_from_bytes(bytes: &Vec<u8>) -> Result<Self, io::Error> {
        let mut reader = bytes.as_slice();
        let mut table_key = TableKey::default();
        table_key.user_key = reader.read_varint()?;
        table_key.seq_num = reader.read_varint()?;
        let write_type = match reader.read_varint::<u8>()? {
            0 => WriteType::Put,
            1 => WriteType::Delete,
            other => panic!("Unexpected write type: {}", other),
        };
        table_key.write_type = write_type;
        table_key.user_val = reader.read_varint()?;
        Ok(table_key)
    }
}

// implement necessary traits so that TableKey could be compared and sorted.
impl PartialEq for TableKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key.eq(&other.user_key) && self.seq_num.eq(&other.seq_num)
    }
}

impl Eq for TableKey {}

impl PartialOrd for TableKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if let Some(ord) = self.user_key.partial_cmp(&other.user_key) {
            match ord {
                Ordering::Equal => {
                    // keys with higher sequence number are placed first.
                    if let Some(ord) = self.seq_num.partial_cmp(&other.seq_num) {
                        match ord {
                            Ordering::Less => return Some(Ordering::Greater),
                            Ordering::Greater => return Some(Ordering::Less),
                            Ordering::Equal => return Some(Ordering::Equal),
                        }
                    }
                }
                _ => return Some(ord),
            }
        }
        None
    }
}

impl Ord for TableKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ord = self.user_key.cmp(&other.user_key);
        if ord == Ordering::Equal {
            // keys with higher sequence number are placed first.
            match self.seq_num.cmp(&other.seq_num) {
                Ordering::Less => return Ordering::Greater,
                Ordering::Greater => return Ordering::Less,
                Ordering::Equal => return Ordering::Equal,
            }
        }
        ord
    }
}

/// lookup key type.
/// it's literally a table key without user value and write type.
pub struct LookupKey {
    pub user_key: UserKey,
    pub seq_num: SeqNum,
}

impl LookupKey {
    pub fn new(user_key: UserKey, seq_num: SeqNum) -> Self {
        Self { user_key, seq_num }
    }

    pub fn as_table_key(&self) -> TableKey {
        TableKey {
            user_key: self.user_key,
            seq_num: self.seq_num,
            write_type: WriteType::NotSpecified,
            user_val: UserValue::default(),
        }
    }
}
