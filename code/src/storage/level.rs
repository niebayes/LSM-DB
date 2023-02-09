use crate::storage::run::{Run, RunIterator, RunStats};
use crate::util::types::*;
use std::cmp;
use std::collections::binary_heap::BinaryHeap;
use std::fmt::Display;

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
        // to handle the case that this level has no runs because of a major compaction,
        // i.e. all runs are merged into a new run in the next level.
        if self.min_table_key.is_none() {
            return (None, false);
        }

        // warning: cannot simply use <= or >= to compare the min/max table key,
        // as the lookup key has the latest sequence number which must less than the
        // table key with the same user key but having a smaller sequence number.
        if lookup_key.user_key >= self.min_table_key.as_ref().unwrap().user_key
            && lookup_key.user_key <= self.max_table_key.as_ref().unwrap().user_key
        {
            // collect table keys having the same user key as the lookup key.
            // there might be multiple runs having table keys with the same user key as
            // the lookup key.
            // since table keys in different runs has no defined order, we have
            // first collect those table keys having the same user key from all runs,
            // and then apply a sorting to select the latest table key.
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
        self.update_key_range();
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
        // in such a case, we prefer a vertical compaction.
        let level_size = self.runs.iter().fold(0, |total, run| total + run.size());
        if level_size > self.size_capacity {
            LevelState::ExceedSizeCapacity
        } else if self.runs.len() > self.run_capcity {
            LevelState::ExceedRunCapacity
        } else {
            LevelState::Normal
        }
    }
}

pub struct LevelStats {
    run_stats: Vec<RunStats>,
    min_table_key: Option<TableKey>,
    max_table_key: Option<TableKey>,
}

impl Level {
    pub fn stats(&self) -> LevelStats {
        let mut run_stats = Vec::with_capacity(self.runs.len());
        for run in self.runs.iter() {
            run_stats.push(run.stats());
        }

        let mut min_table_key = None;
        let mut max_table_key = None;

        if let Some(table_key) = self.min_table_key.as_ref() {
            min_table_key = Some(table_key.clone());
        }
        if let Some(table_key) = self.max_table_key.as_ref() {
            max_table_key = Some(table_key.clone());
        }

        LevelStats {
            run_stats,
            min_table_key,
            max_table_key,
        }
    }
}

impl Display for LevelStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut stats = String::new();

        if let Some(table_key) = self.min_table_key.as_ref() {
            stats += &format!("min table key: {}\n", table_key);
        } else {
            stats += &format!("min table key: {}\n", "NaN");
        }

        if let Some(table_key) = self.max_table_key.as_ref() {
            stats += &format!("max table key: {}\n", table_key);
        } else {
            stats += &format!("max table key: {}\n", "NaN");
        }

        for (i, run_stats) in self.run_stats.iter().enumerate() {
            stats += &format!("run index: {}\n\t{}", i, run_stats);
        }

        write!(f, "{}", stats)
    }
}
