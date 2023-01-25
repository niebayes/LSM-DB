use crate::storage::run::{Run, RunIterator};
use crate::util::types::*;
use std::cmp::Ordering;
use std::collections::binary_heap::BinaryHeap;

use super::iterator::TableKeyIterator;
use super::keys::{LookupKey, TableKey};

/// a level in the lsm tree.
pub struct Level {
    /// level number.
    pub level_num: u32,
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
        if lookup_key.user_key >= self.min_user_key && lookup_key.user_key <= self.max_user_key {
            // collect table keys having the same user key as the lookup key.
            let mut table_keys = Vec::new();
            for run in self.runs.iter() {
                match run.get(lookup_key) {
                    (Some(table_key), _) => table_keys.push(table_key),
                    (None, _) => {}
                }
            }

            if !table_keys.is_empty() {
                // the latest table key will be placed at the beginning.
                table_keys.sort_by(|a, b| a.cmp(b));
                let table_key = table_keys.first().unwrap();

                match table_key.write_type {
                    WriteType::Put => return (Some(table_key.user_val), false),
                    WriteType::Delete => return (Some(table_key.user_val), true),
                    other => panic!("Unexpected write type: {}", other as u8),
                }
            }
        }
        (None, false)
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
    /// currently pointed-to table key.
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
