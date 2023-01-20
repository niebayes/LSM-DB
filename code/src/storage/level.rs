use crate::storage::run::Run;
use crate::util::types::*;

pub struct LevelIterator {}

/// a level in the lsm tree.
pub struct Level {
    /// level number.
    level_num: u32,
    /// sorted runs in this level.
    sorted_runs: Vec<Run>,
    /// max number of sorted runs this level could hold.
    max_num_sorted_runs: i32,
    /// how many bytes or number of sstables this level could hold.
    capacity: u32,
}

impl Level {
    pub fn new(level_num: u32, max_num_sorted_runs: i32, capacity: u32) -> Level {
        Level {
            level_num,
            sorted_runs: Vec::new(),
            max_num_sorted_runs,
            capacity,
        }
    }

    pub fn get(&mut self, key: UserKey) -> Option<UserValue> {
        None
    }
}

pub fn default_two_level() -> Vec<Level> {
    vec![]
}
