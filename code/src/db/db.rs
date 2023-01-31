use rand::Rng;

use crate::storage::iterator::*;
use crate::storage::keys::{LookupKey, TableKey};
use crate::storage::level::{Level, LevelState};
use crate::storage::memtable::MemTable;
use crate::storage::run::Run;
use crate::storage::sstable::*;
use crate::util::types::*;
use std::cmp;
use std::collections::{BinaryHeap, HashSet};
use std::rc::Rc;
use std::vec;

/// database configurations.
pub struct Config {
    /// fanout = current level capacity / previous level capacity.
    pub fanout: usize,
    /// memtable size capacity in bytes
    pub memtable_size_capacity: usize,
    /// sstable size capacity in bytes.
    pub sstable_size_capacity: usize,
    /// run capacity.
    pub run_capacity: usize,
    /// max number of levels.
    pub max_levels: usize,
}

impl Default for Config {
    /// create a default config.
    fn default() -> Self {
        Self {
            fanout: 10,
            memtable_size_capacity: 4 * 1024,
            sstable_size_capacity: 16 * 1024,
            run_capacity: 4,
            max_levels: 4,
        }
    }
}

pub struct Db {
    /// database config.
    pub cfg: Config,
    /// memtable.
    mem: MemTable,
    /// all levels in the lsm tree.
    levels: Vec<Level>,
    /// the next sequence number to allocate for a write.
    next_seq_num: SeqNum,
    /// the next file number to allocate for a file.
    next_file_num: FileNum,
}

impl Db {
    pub fn new(cfg: Config) -> Db {
        // size capacity of the level 0 = run capacity of the level 0 * memtable size capactiy.
        let default_level_0 = Level::new(
            0,
            cfg.run_capacity,
            cfg.run_capacity * cfg.memtable_size_capacity,
        );

        Db {
            cfg,
            mem: MemTable::new(),
            levels: vec![default_level_0],
            next_seq_num: 0,
            next_file_num: 0,
        }
    }

    pub fn alloc_seq_num(&mut self) -> SeqNum {
        let seq_num = self.next_seq_num;
        self.next_seq_num += 1;
        seq_num
    }

    fn latest_seq_num(&self) -> SeqNum {
        if self.next_seq_num > 0 {
            self.next_seq_num - 1
        } else {
            0
        }
    }

    pub fn stats(&self) -> String {
        let mut stats = String::new();

        stats += &format!("next sequence number: {}", self.next_seq_num);
        stats += &format!("next file number: {}", self.next_file_num);

        stats += &format!("{}\n", self.mem.stats());

        for level in self.levels.iter() {
            stats += &format!("{}", level.stats())
        }

        stats
    }
}

/// db write implementation.
impl Db {
    pub fn put(&mut self, user_key: UserKey, user_val: UserValue) {
        self.write(user_key, user_val, WriteType::Put);
    }

    pub fn delete(&mut self, user_key: UserKey) {
        self.write(user_key, UserValue::default(), WriteType::Delete);
    }

    fn write(&mut self, user_key: UserKey, user_val: UserValue, write_type: WriteType) {
        let table_key = TableKey::new(user_key, self.alloc_seq_num(), write_type, user_val);
        self.mem.put(table_key);

        if self.mem.size() >= self.cfg.memtable_size_capacity {
            self.minor_compaction();
            self.check_level_state();
        }
    }
}

/// db read implementation.
impl Db {
    /// point query the associated value in the database.
    pub fn get(&mut self, user_key: UserKey) -> Option<UserValue> {
        let snapshot_seq_num = self.latest_seq_num();
        let lookup_key = LookupKey::new(user_key, snapshot_seq_num);

        // search the key in the memtable.
        match self.mem.get(&lookup_key) {
            // the key exists and is not deleted.
            (Some(user_val), false) => return Some(user_val),
            // the key exists but is deleted.
            (Some(_), true) => return None,
            // the key does not exist, proceed to searching in sstables.
            (None, _) => {}
        };

        // search the key in the lsm tree.
        for level in self.levels.iter() {
            // keys in shallower levels shadow keys having the same user keys in deeper levels,
            // and hence the searching terminates as soon as the key is found.
            match level.get(&lookup_key) {
                // the key exists and is not deleted.
                (Some(user_val), false) => return Some(user_val),
                // the key exists but is deleted.
                (Some(_), true) => return None,
                // the key does not exist, proceed to searching in the next level.
                (None, _) => {}
            };
        }

        // the key does not exist.
        None
    }

    /// range query the values associated with keys in the key range [start_user_key, end_user_key).
    pub fn range(&mut self, start_user_key: UserKey, end_user_key: UserKey) -> Vec<UserEntry> {
        let snapshot_seq_num = self.latest_seq_num();
        let start_lookup_key = LookupKey::new(start_user_key, snapshot_seq_num);
        let end_lookup_key = LookupKey::new(end_user_key, snapshot_seq_num);

        // iterator container to hold iterators from the memtable and all levels of sstables.
        let mut iters: BinaryHeap<TableKeyIteratorType> = BinaryHeap::new();
        iters.push(Box::new(self.mem.iter()));
        for level in self.levels.iter() {
            iters.push(Box::new(level.iter().unwrap()));
        }

        let mut entries = Vec::new();

        let mut last_user_key = None;
        // loop inv: there's at least one iterator in the heap.
        while let Some(mut iter) = iters.pop() {
            // proceed if the iterator is not exhausted.
            if let Some(table_key) = iter.next() {
                // early termination: the current key has a user key equal to or greater than the end user key.
                if table_key.user_key >= end_user_key {
                    break;
                }

                // only the latest visible table key for each user key is collected.
                if last_user_key.is_none() || table_key.user_key != last_user_key.unwrap() {
                    // ensure the table key has a user key within the query range and it's visible to the snapshot.
                    if table_key >= start_lookup_key.as_table_key()
                        && table_key < end_lookup_key.as_table_key()
                    {
                        match table_key.write_type {
                            // only non-deleted keys are collected.
                            WriteType::Put => {
                                entries.push(UserEntry {
                                    key: table_key.user_key,
                                    val: table_key.user_val,
                                });
                                last_user_key = Some(table_key.user_key);
                            }
                            _ => {}
                        }
                    }
                }

                // push back the iterator into the heap.
                iters.push(iter);
            }
        }

        entries
    }
}

/// the context of a major compaction.
struct CompactionContext {
    /// min user key of the current level.
    min_user_key: UserKey,
    /// max user key of the current level.
    max_user_key: UserKey,
    /// compaction inputs, aka. all sstables involved in the compaction.
    inputs: Vec<Rc<SSTable>>,
}

impl CompactionContext {
    fn new(base: Rc<SSTable>) -> Self {
        Self {
            min_user_key: base.min_table_key.user_key,
            max_user_key: base.max_table_key.user_key,
            inputs: vec![base],
        }
    }

    fn get_base(&self) -> &SSTable {
        self.inputs.first().unwrap()
    }

    /// return true if the key range of the given sstable overlaps with the key range of the base sstable.
    fn overlap_with_base(&self, other: &SSTable) -> bool {
        let base = self.get_base();
        let (min, max) = (base.min_table_key.user_key, base.max_table_key.user_key);
        let (other_min, other_max) = (other.min_table_key.user_key, other.max_table_key.user_key);

        (min >= other_min && min <= other_max)
            || (max >= other_min && max <= other_max)
            || (min >= other_min && max <= other_max)
            || (other_min >= min && other_max <= max)
    }

    /// return true if the key range of the given sstable overlaps with the key range of the current the level.
    fn overlap_with_curr_level(&self, other: &SSTable) -> bool {
        let (min, max) = (self.min_user_key, self.max_user_key);
        let (other_min, other_max) = (other.min_table_key.user_key, other.max_table_key.user_key);

        (min >= other_min && min <= other_max)
            || (max >= other_min && max <= other_max)
            || (min >= other_min && max <= other_max)
            || (other_min >= min && other_max <= max)
    }

    fn add_input(&mut self, input: Rc<SSTable>, is_curr_level: bool) {
        // try to extend the key range of the current level.
        if is_curr_level {
            self.min_user_key = cmp::min(self.min_user_key, input.min_table_key.user_key);
            self.max_user_key = cmp::max(self.max_user_key, input.max_table_key.user_key);
        }
        self.inputs.push(input);
    }

    fn iters(&self) -> BinaryHeap<TableKeyIteratorType> {
        let mut iters: BinaryHeap<TableKeyIteratorType> = BinaryHeap::new();
        for input in self.inputs.iter() {
            iters.push(Box::new(input.iter().unwrap()));
        }
        iters
    }
}

/// db compaction implementation.
impl Db {
    /// flush the table keys in memtable to a new sstable.
    fn minor_compaction(&mut self) {
        let mut iter = self.mem.iter();
        let mut sstable_writer_batch = SSTableWriterBatch::new(self.next_file_num);
        while let Some(table_key) = iter.next() {
            sstable_writer_batch.push(table_key);
        }
        let (sstables, next_file_num) = sstable_writer_batch.done();
        self.next_file_num = next_file_num;

        let (min_table_key, max_table_key) = (
            sstables.first().unwrap().min_table_key.clone(),
            sstables.first().unwrap().max_table_key.clone(),
        );
        let run = Run::new(sstables, min_table_key, max_table_key);

        // add this run to level 0.
        self.levels.get_mut(0).unwrap().add_run(run);
    }

    fn check_level_state(&mut self) {
        let mut level_num = 0;
        while level_num < self.cfg.max_levels {
            if let Some(level) = self.levels.get_mut(level_num) {
                if let LevelState::ExceedSizeCapacity | LevelState::ExceedRunCapacity =
                    level.state()
                {
                    self.major_compaction(level_num);
                    // do not increment the level number since a level may exceed
                    // the size capacity and the run capacity at the same time.
                } else {
                    level_num += 1;
                }
            } else {
                break;
            }
        }
    }

    fn select_compaction_base(&self, level_num: LevelNum) -> Rc<SSTable> {
        // randomly select an sstable from a random run in the level.
        let level = self.levels.get(level_num).unwrap();
        let run_idx = rand::thread_rng().gen_range(0..level.runs.len());
        let run = level.runs.get(run_idx).unwrap();
        let sstable_idx = rand::thread_rng().gen_range(0..run.sstables.len());
        let sstable = run.sstables.get(sstable_idx).unwrap();

        sstable.clone()
    }

    fn major_compaction(&mut self, level_num: LevelNum) {
        // select the base sstable in the current level.
        let base = self.select_compaction_base(level_num);
        let curr_level = self.levels.get_mut(level_num).unwrap();
        let mut ctx = CompactionContext::new(base);

        // collect overlapping sstables in the current level.
        for run in curr_level.runs.iter() {
            for sstable in run.sstables.iter() {
                // skip the base sstable itself.
                if ctx.get_base().file_num == sstable.file_num {
                    continue;
                }

                if ctx.overlap_with_base(sstable) {
                    ctx.add_input(sstable.clone(), true);
                }
            }
        }

        if let LevelState::ExceedRunCapacity = curr_level.state() {
            self.horizontal_compaction(&mut ctx, level_num);
        } else {
            self.vertical_compaction(&mut ctx, level_num);
        }

        self.remove_obsolete_sstables(&ctx);
    }

    fn merge(&mut self, iters: &mut BinaryHeap<TableKeyIteratorType>) -> Run {
        let mut sstable_writer_batch = SSTableWriterBatch::new(self.next_file_num.clone());

        let mut last_user_key = None;
        while let Some(mut iter) = iters.pop() {
            if let Some(table_key) = iter.next() {
                if last_user_key.is_none() || last_user_key.unwrap() != table_key.user_key {
                    last_user_key = Some(table_key.user_key);
                    sstable_writer_batch.push(table_key);
                }

                iters.push(iter);
            }
        }

        let (sstables, next_file_num) = sstable_writer_batch.done();
        self.next_file_num = next_file_num;

        Run::new(
            sstables,
            sstable_writer_batch.min_table_key.as_ref().unwrap().clone(),
            sstable_writer_batch.max_table_key.as_ref().unwrap().clone(),
        )
    }

    /// merge inputs into a new run and insert this run into the next level.
    fn vertical_compaction(&mut self, ctx: &mut CompactionContext, curr_level_num: LevelNum) {
        // create the next level if necessary.
        if self.levels.get(curr_level_num + 1).is_none() {
            let curr_level = self.levels.get(curr_level_num).unwrap();
            self.levels.push(Level::new(
                curr_level_num + 1,
                curr_level.run_capcity,
                curr_level.size_capacity * self.cfg.fanout,
            ))
        }
        let next_level = self.levels.get(curr_level_num + 1).unwrap();

        // collect overlapping sstables in the next level.
        for run in next_level.runs.iter() {
            for sstable in run.sstables.iter() {
                if ctx.overlap_with_curr_level(sstable) {
                    ctx.add_input(sstable.clone(), false)
                }
            }
        }

        let run = self.merge(&mut ctx.iters());
        self.levels
            .get_mut(curr_level_num + 1)
            .unwrap()
            .add_run(run);
    }

    fn select_compaction_run(&mut self, curr_level_num: LevelNum) -> Run {
        // randomly select a run in the current level.
        let curr_level = self.levels.get_mut(curr_level_num).unwrap();
        let run_idx = rand::thread_rng().gen_range(0..curr_level.runs.len());
        curr_level.runs.remove(run_idx)
    }

    /// merge inputs into a new run and merge this run with another run in the current level.
    fn horizontal_compaction(&mut self, ctx: &mut CompactionContext, curr_level_num: LevelNum) {
        let mut iters: BinaryHeap<TableKeyIteratorType> = BinaryHeap::new();

        // collect file nums of the sstables involved in the compaction.
        let mut obsolete_file_nums = HashSet::new();
        for sstable in ctx.inputs.iter() {
            obsolete_file_nums.insert(sstable.file_num);
        }

        let old_run = self.select_compaction_run(curr_level_num);
        for sstable in old_run.sstables.iter() {
            // only sstables not involved in the compaction are merged with the new run.
            if !obsolete_file_nums.contains(&sstable.file_num) {
                iters.push(Box::new(sstable.iter().unwrap()));
            }
        }

        let new_run = self.merge(&mut ctx.iters());
        iters.push(Box::new(new_run.iter().unwrap()));

        let merged_run = self.merge(&mut iters);

        // add the merged run into the current level.
        self.levels
            .get_mut(curr_level_num)
            .unwrap()
            .add_run(merged_run);
    }

    fn remove_obsolete_sstables(&mut self, ctx: &CompactionContext) {
        // remove sstables involved in the compaction from the runs they belong to.
        let mut obsolete_file_nums = HashSet::new();
        for sstable in ctx.inputs.iter() {
            obsolete_file_nums.insert(sstable.file_num);
        }

        for level in self.levels.iter_mut() {
            let mut new_runs = Vec::new();
            for run in level.runs.iter_mut() {
                let mut new_sstables = Vec::new();
                for sstable in run.sstables.iter() {
                    // obsolete sstables won't be moved into the new sstables.
                    if !obsolete_file_nums.contains(&sstable.file_num) {
                        new_sstables.push(sstable.clone());
                    }
                }
                run.sstables = new_sstables;

                // empty runs won't be moved into the new runs.
                if !run.sstables.is_empty() {
                    run.update_key_range();
                    new_runs.push(run.clone());
                }
            }
            level.runs = new_runs;
            level.update_key_range();
        }
    }
}
