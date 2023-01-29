use std::fmt::Display;

/// write type.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WriteType {
    Put,
    Delete,
    Empty,
}

impl Display for WriteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteType::Put => write!(f, "Put"),
            WriteType::Delete => write!(f, "Delete"),
            WriteType::Empty => write!(f, "Empty"),
        }
    }
}

/// sequence number type.
pub type SeqNum = usize;

/// file number type.
pub type FileNum = usize;

/// level number type.
pub type LevelNum = usize;

/// user key type.
pub type UserKey = i32;
/// user value type.
pub type UserValue = i32;
/// user key-value entry type.
pub struct UserEntry {
    pub key: UserKey,
    pub val: UserValue,
}
