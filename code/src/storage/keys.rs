use crate::util::types::*;
use integer_encoding::*;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
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
            write_type: WriteType::Empty,
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
        seq_num: SeqNum,
        write_type: WriteType,
        user_val: UserValue,
    ) -> Self {
        Self {
            user_key,
            user_val,
            seq_num,
            write_type,
        }
    }

    /// make a table key with all fields set to i.
    pub fn identity(i: i32) -> Self {
        Self {
            user_key: i,
            seq_num: i as SeqNum,
            write_type: WriteType::Put,
            user_val: i,
        }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        // decoding is based on the assumption that the an encoded table key is of size TABLE_KEY_SIZE.
        // hence, write_fixedint instead of write_varint is used here.
        encoded.write_fixedint(self.user_key).unwrap();
        encoded.write_fixedint(self.seq_num).unwrap();
        encoded.write_fixedint(self.write_type as u8).unwrap();
        encoded.write_fixedint(self.user_val).unwrap();
        encoded
    }

    pub fn decode_from_bytes(bytes: &Vec<u8>) -> Result<Self, io::Error> {
        let mut reader = bytes.as_slice();
        let mut table_key = TableKey::default();
        table_key.user_key = reader.read_fixedint()?;
        table_key.seq_num = reader.read_fixedint()?;
        let write_type = match reader.read_fixedint::<u8>()? {
            1 => WriteType::Put,
            2 => WriteType::Delete,
            other => panic!("Unexpected write type: {}", other),
        };
        table_key.write_type = write_type;
        table_key.user_val = reader.read_fixedint()?;
        Ok(table_key)
    }

    pub fn as_lookup_key(&self) -> LookupKey {
        LookupKey::new(self.user_key, self.seq_num)
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

impl Display for TableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{} | {}]", self.user_key, self.seq_num)
    }
}

impl Debug for TableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{} | {}]", self.user_key, self.seq_num)
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
            write_type: WriteType::Empty,
            user_val: UserValue::default(),
        }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.write_fixedint(self.user_key).unwrap();
        encoded.write_fixedint(self.seq_num).unwrap();
        encoded
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_key_order() {
        let mut a = TableKey::new(0, 1, WriteType::Put, 0);
        let mut b = TableKey::new(1, 1, WriteType::Put, 0);
        assert!(a < b);

        a = TableKey::new(1, 1, WriteType::Put, 0);
        assert_eq!(a, b);

        b = TableKey::new(0, 1, WriteType::Put, 0);
        assert!(a > b);

        b = TableKey::new(1, 1000, WriteType::Put, 0);
        a = TableKey::new(0, 800, WriteType::Put, 0);
        let c = TableKey::new(100, 20, WriteType::Put, 0);
        assert!(a <= b);
        assert!(b <= c);
    }

    #[test]
    fn table_key_encode_decode() {
        let table_key = TableKey::new(1, 2, WriteType::Put, 3);
        let bytes = table_key.encode_to_bytes();
        let decoded_table_key = TableKey::decode_from_bytes(&bytes).unwrap();
        assert_eq!(table_key.user_key, decoded_table_key.user_key);
        assert_eq!(table_key.seq_num, decoded_table_key.seq_num);
        assert_eq!(table_key.write_type, decoded_table_key.write_type);
        assert_eq!(table_key.user_val, decoded_table_key.user_val);
    }
}
