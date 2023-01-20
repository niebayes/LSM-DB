use crate::util::types::*;
use std::cmp::Ordering;
use std::mem;

/// table key type.
/// table key = user key + seq num + write type + user val.
/// user key and sequence number are necessaties for a table key, so they are placed at the front.
/// write type and user value may not be specified for lookup, so they are placed at the tail.
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
