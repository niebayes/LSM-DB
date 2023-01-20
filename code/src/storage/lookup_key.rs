use crate::storage::table_key::TableKey;
use crate::util::types::{SeqNum, UserKey, UserValue, WriteType};

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
