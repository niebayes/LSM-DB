use std::fmt::{Debug, Display};

/// write type.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WriteType {
    Empty,
    Put,
    Delete,
}

impl Display for WriteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteType::Put => write!(f, "P"),
            WriteType::Delete => write!(f, "D"),
            WriteType::Empty => write!(f, "NaN"),
        }
    }
}

impl Debug for WriteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteType::Put => write!(f, "P"),
            WriteType::Delete => write!(f, "D"),
            WriteType::Empty => write!(f, "NaN"),
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
