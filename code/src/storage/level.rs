use crate::logging::manifest::LevelManifest;
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
    pub run_capacity: usize,
    /// number of bytes this level could hold.
    pub size_capacity: usize,
}

/// level read implementation.
impl Level {
    pub fn new(level_num: LevelNum, run_capacity: usize, size_capacity: usize) -> Level {
        Level {
            level_num,
            min_table_key: None,
            max_table_key: None,
            runs: Vec::new(),
            run_capacity,
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
            let mut iter = run.iter()?;
            iter.next();
            run_iters.push(iter);
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
        let mut last_user_key = None;
        while let Some(mut run_iter) = self.run_iters.pop() {
            if run_iter.valid() {
                let table_key = run_iter.curr().unwrap();
                run_iter.next();
                self.run_iters.push(run_iter);

                if last_user_key.is_none() || last_user_key.unwrap() != table_key.user_key {
                    last_user_key = Some(table_key.user_key);

                    // this line is used to suppress the `unused_assignments` warning.
                    // FIXME: find a more elegant solution.
                    let _ = last_user_key.as_ref().unwrap().clone();

                    self.curr_table_key = Some(table_key);
                    return self.curr_table_key.clone();
                }
            }
        }
        self.curr_table_key = None;
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
            println!("level {} exceeds size capacity", self.level_num);
            LevelState::ExceedSizeCapacity
        } else if self.runs.len() > self.run_capacity {
            println!("level {} exceeds run capacity", self.level_num);
            LevelState::ExceedRunCapacity
        } else {
            LevelState::Normal
        }
    }
}

pub struct LevelStats {
    indent: usize,
    run_stats: Vec<RunStats>,
    min_table_key: Option<TableKey>,
    max_table_key: Option<TableKey>,
}

impl Level {
    pub fn stats(&self, indent: usize) -> LevelStats {
        let mut run_stats = Vec::with_capacity(self.runs.len());
        for run in self.runs.iter() {
            run_stats.push(run.stats(indent + 1));
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
            indent,
            run_stats,
            min_table_key,
            max_table_key,
        }
    }
}

impl Display for LevelStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut stats = String::new();

        stats += "  ".repeat(self.indent).as_str();
        if let Some(table_key) = self.min_table_key.as_ref() {
            stats += &format!("Min = {}    ", table_key);
        } else {
            stats += &format!("Min = {}    ", "NaN");
        }

        if let Some(table_key) = self.max_table_key.as_ref() {
            stats += &format!("Max = {}\n", table_key);
        } else {
            stats += &format!("Max = {}\n", "NaN");
        }

        for (i, run_stats) in self.run_stats.iter().enumerate() {
            stats += "  ".repeat(self.indent).as_str();
            stats += &format!("run {}\n{}", i, run_stats);
        }

        write!(f, "{}", stats)
    }
}

impl Level {
    pub fn manifest(&self) -> LevelManifest {
        let mut run_manifests = Vec::new();
        for run in self.runs.iter() {
            run_manifests.push(run.manifest());
        }

        let mut min_table_key = None;
        let mut max_table_key = None;
        if self.min_table_key.is_some() {
            min_table_key = Some(self.min_table_key.as_ref().unwrap().clone());
        }
        if self.max_table_key.is_some() {
            max_table_key = Some(self.max_table_key.as_ref().unwrap().clone());
        }

        LevelManifest {
            level_num: self.level_num,
            run_capacity: self.run_capacity,
            size_capacity: self.size_capacity,
            num_runs: self.runs.len(),
            run_manifests,
            min_table_key,
            max_table_key,
        }
    }

    pub fn from_manifest(level_manifest: &LevelManifest) -> Self {
        let mut min_table_key = None;
        let mut max_table_key = None;
        if level_manifest.min_table_key.is_some() {
            min_table_key = Some(level_manifest.min_table_key.as_ref().unwrap().clone());
            max_table_key = Some(level_manifest.max_table_key.as_ref().unwrap().clone());
        }

        let mut runs = Vec::new();
        for run_manifest in level_manifest.run_manifests.iter() {
            runs.push(Run::from_manifest(run_manifest));
        }

        Self {
            level_num: level_manifest.level_num,
            run_capacity: level_manifest.run_capacity,
            size_capacity: level_manifest.size_capacity,
            min_table_key,
            max_table_key,
            runs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::SSTableWriter;
    use std::fs::{create_dir, remove_dir_all};
    use std::rc::Rc;

    /// insert a sequence of keys into an sstable.
    /// insert another sequence of keys into another sstable but with some delete keys.
    /// create a run to contain the first sstable.
    /// create another run to contain the second sstable.
    /// create a level to contain the two runs.
    /// create a level iterator from the level.
    /// emit all keys and check each key is greater than or equal to the last emitted one.
    /// also check the deleted keys are actually deleted.
    #[test]
    fn level_iterator() {
        let _ = create_dir("./sstables");

        let num_table_keys: i32 = 963;
        let mut level = Level::new(0, 4, 100000);

        // sstable 1.
        let file_num = 42;
        let mut writer = SSTableWriter::new(file_num);

        for i in 0..num_table_keys {
            let table_key = TableKey::new(i, i as usize, WriteType::Put, i);
            writer.push(table_key);
        }
        let sstable = writer.done();
        let run = Run::new(
            vec![Rc::new(sstable)],
            TableKey::identity(0),
            TableKey::identity(num_table_keys - 1),
        );
        level.add_run(run);

        // sstable 2.
        let file_num = file_num + 1;
        let mut writer = SSTableWriter::new(file_num);

        let num_deletes = 200;
        for i in 0..num_deletes {
            let table_key = TableKey::new(i, (i + num_table_keys) as usize, WriteType::Delete, i);
            writer.push(table_key);
        }
        for i in num_deletes..num_table_keys {
            let i = i + num_table_keys;
            let table_key = TableKey::new(i, i as usize, WriteType::Put, i);
            writer.push(table_key);
        }
        let sstable = writer.done();
        let run = Run::new(
            vec![Rc::new(sstable)],
            TableKey::identity(0),
            TableKey::identity(num_table_keys * 2 - 1),
        );
        level.add_run(run);

        let mut visible_cnt = 0;
        let mut last_user_key = None;
        let mut iter = level.iter().unwrap();
        iter.next();
        while iter.valid() {
            let table_key = iter.curr().unwrap();
            if last_user_key.is_none() || last_user_key.unwrap() != table_key.user_key {
                last_user_key = Some(table_key.user_key);
                match table_key.write_type {
                    WriteType::Put => {
                        println!("{}", &table_key);
                        visible_cnt += 1;
                    }
                    _ => {}
                }
            }
            iter.next();
        }

        println!("visible_cnt = {}", visible_cnt);
        assert_eq!(visible_cnt, num_table_keys * 2 - num_deletes * 2);

        let _ = remove_dir_all("./sstables");
    }
}
