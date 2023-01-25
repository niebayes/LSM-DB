use crate::storage::run::{Run, RunIterator};
use crate::util::types::*;
use std::cmp::Ordering;
use std::collections::binary_heap::BinaryHeap;

use super::iterator::TableKeyIterator;
use super::keys::{LookupKey, TableKey};

/// a level in the lsm tree.
pub struct Level {
    /// level number.
    level_num: u32,
    /// min user key stored in the level.
    min_user_key: UserKey,
    /// max user key stored in the level.
    max_user_key: UserKey,
    /// sorted runs in the level.
    runs: Vec<Run>,
    /// max number of sorted runs this level could hold.
    max_num_sorted_runs: u32,
    /// how many bytes or number of sstables this level could hold.
    capacity: u32,
}

impl Level {
    pub fn new(level_num: u32, max_num_sorted_runs: u32, capacity: u32) -> Level {
        Level {
            level_num,
            min_user_key: UserKey::MAX,
            max_user_key: UserKey::MIN,
            runs: Vec::new(),
            max_num_sorted_runs,
            capacity,
        }
    }

    pub fn get(&self, lookup_key: &LookupKey) -> (Option<UserValue>, bool) {
        (None, true)
    }

    pub fn iter(&self) -> Result<LevelIterator, ()> {
        let mut run_iters = BinaryHeap::new();
        for run in self.runs.iter() {
            run_iters.push(run.iter()?);
        }
        Ok(LevelIterator {
            run_iters,
            curr_table_key: None,
        })
    }
}

/// a level's iterator.
pub struct LevelIterator {
    /// iterators of all runs in the level.
    run_iters: BinaryHeap<RunIterator>,
    curr_table_key: Option<TableKey>,
}

impl TableKeyIterator for LevelIterator {
    fn seek(&mut self, lookup_key: &LookupKey) {
        while let Some(table_key) = self.next() {
            if table_key >= lookup_key.as_table_key() {
                break;
            }
        }
    }

    fn next(&mut self) -> Option<TableKey> {
        while let Some(mut run_iter) = self.run_iters.pop() {
            if let Some(table_key) = run_iter.next() {
                self.run_iters.push(run_iter);
                self.curr_table_key = Some(table_key);
                return self.curr_table_key.clone();
            }
        }
        None
    }

    fn curr(&self) -> Option<TableKey> {
        self.curr_table_key.clone()
    }

    fn valid(&self) -> bool {
        self.curr_table_key.is_some()
    }
}

pub fn default_two_level() -> Vec<Level> {
    vec![]
}
