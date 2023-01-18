/// write type.
#[derive(Clone, Copy)]
pub enum WriteType {
    Put,
    Delete,
    NotSpecified,
}

/// level mapping update type.
/// level mapping: which sstable goes to / leave from which level.
#[derive(Clone, Copy)]
pub enum UpdateType {
    /// add sstable x to level y.
    Add,
    // remove sstable x from level y.
    Remove,
    /// not specified.
    NotSpecified,
}

/// sequence number type.
pub type SeqNum = u64;

/// file number type.
pub type FileNum = u64;

/// level number type.
pub type LevelNum = u8;

/// user key type.
pub type UserKey = i32;
/// user value type.
pub type UserValue = i32;
/// user key-value entry type.
pub struct KvEntry {
    pub key: UserKey,
    pub val: UserValue,
}
/// user key-value vector type.
pub type KvVec = Vec<KvEntry>;

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

impl TableKey {
    pub fn new_lookup_key(user_key: UserKey, seq_num: SeqNum) -> Self {
        Self {
            user_key,
            user_val: UserValue::default(),
            seq_num,
            write_type: WriteType::NotSpecified,
        }
    }

    pub fn new_write_key(
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
