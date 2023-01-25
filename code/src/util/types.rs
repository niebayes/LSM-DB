/// write type.
#[derive(Clone, Copy, PartialEq, Eq)]
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
pub struct UserEntry {
    pub key: UserKey,
    pub val: UserValue,
}
