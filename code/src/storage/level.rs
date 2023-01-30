use crate::storage::run::{Run, RunIterator};
use crate::util::types::*;
use std::cmp;
use std::collections::binary_heap::BinaryHeap;

use super::iterator::TableKeyIterator;
use super::keys::{LookupKey, TableKey};

/// a level in the lsm tree.
pub struct Level {
    /// level number.
    pub level_num: LevelNum,
    /// min table key stored in the level.
    pub min_table_key: Option<TableKey>,
    /// max table key stored in the level.
    pub max_table_key: Option<TableKey>,
    /// sorted runs in the level.
    pub runs: Vec<Run>,
    /// max number of sorted runs this level could hold.
    pub run_capcity: usize,
    /// number of bytes this level could hold.
    pub size_capacity: usize,
}

/// level read implementation.
impl Level {
    pub fn new(level_num: LevelNum, run_capcity: usize, size_capacity: usize) -> Level {
        Level {
            level_num,
            min_table_key: None,
            max_table_key: None,
            runs: Vec::new(),
            run_capcity,
            size_capacity,
        }
    }

    pub fn get(&self, lookup_key: &LookupKey) -> (Option<UserValue>, bool) {
        if self.min_table_key.is_none() {
            return (None, false);
        }

        if &lookup_key.as_table_key() >= self.min_table_key.as_ref().unwrap()
            && &lookup_key.as_table_key() <= self.max_table_key.as_ref().unwrap()
        {
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

pub enum LevelState {
    ExceedSizeCapacity,
    ExceedRunCapacity,
    Normal,
}

impl Level {
    /// add a run into the level.
    pub fn add_run(&mut self, run: Run) {
        self.runs.push(run);
    }

    /// update the key range of the run using the existing sstables.
    pub fn update_key_range(&mut self) {
        if self.runs.is_empty() {
            self.min_table_key = None;
            self.max_table_key = None;
            return;
        }

        let mut min_table_key = self
            .runs
            .first()
            .unwrap()
            .min_table_key
            .as_ref()
            .unwrap()
            .clone();
        let mut max_table_key = self
            .runs
            .first()
            .unwrap()
            .max_table_key
            .as_ref()
            .unwrap()
            .clone();

        for i in 1..self.runs.len() {
            min_table_key = cmp::min(
                min_table_key.clone(),
                self.runs
                    .get(i)
                    .unwrap()
                    .min_table_key
                    .as_ref()
                    .unwrap()
                    .clone(),
            );
            max_table_key = cmp::max(
                max_table_key.clone(),
                self.runs
                    .get(i)
                    .unwrap()
                    .max_table_key
                    .as_ref()
                    .unwrap()
                    .clone(),
            );
        }

        self.min_table_key = Some(min_table_key);
        self.max_table_key = Some(max_table_key);
    }

    /// return true if reached the size_capacity or run limit of this level.
    pub fn state(&self) -> LevelState {
        // it's possible that a level exceeds the size capacity and the run capacity at the same time.
        // in such a case, we prefer a horizontal compaction.
        // TODO: prefer vertical compaction.
        let level_size = self.runs.iter().fold(0, |total, run| total + run.size());
        if self.runs.len() >= self.run_capcity {
            LevelState::ExceedRunCapacity
        } else if level_size >= self.size_capacity {
            LevelState::ExceedSizeCapacity
        } else {
            LevelState::Normal
        }
    }
}
