use rand::Rng;

use crate::storage::block::BLOCK_SIZE;
use crate::storage::iterator::*;
use crate::storage::keys::{LookupKey, TableKey, TABLE_KEY_SIZE};
use crate::storage::level::{Level, LevelState};
use crate::storage::memtable::MemTable;
use crate::storage::run::Run;
use crate::storage::sstable::*;
use crate::util::types::*;
use std::cmp;
use std::collections::{BinaryHeap, HashSet};
use std::fs::{create_dir, remove_dir_all, remove_file};
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

/// database default configuration.
impl Default for Config {
    /// create a default config.
    // warning:
    // currently, a block is of size 4096 bytes and hence could only store at most 240 table keys.
    // on the other hand, each sstable only allocates one block as the index block which stores fence pointers.
    // this means an sstable could at most store 240 data blocks which are of size 240 * 4096 = 983040 bytes,
    // which is 57825 table keys.
    // this limitation could be easily resolved by increasing the size of one block or let each sstable could allocate
    // more than one blocks to be used as the index blocks.
    // in summary, the default configuration does not work currently.
    fn default() -> Self {
        Self {
            fanout: 10,
            memtable_size_capacity: 4 * 1024 * 1024, // 4MB.
            sstable_size_capacity: 16 * 1024 * 1024, // 16MB.
            run_capacity: 4,
            max_levels: 4,
        }
    }
}

/// database test configuration.
impl Config {
    pub fn test() -> Self {
        Self {
            fanout: 2,
            memtable_size_capacity: 16 * 1024, // 16KB.
            sstable_size_capacity: 64 * 1024,  // 64KB.
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

impl Drop for Db {
    // fields in Db would still be dropped.
    fn drop(&mut self) {
        // remove the sstables directory.
        let _ = remove_dir_all("./sstables");
    }
}

impl Db {
    pub fn new(cfg: Config) -> Db {
        // create a new sstables directory.
        let _ = create_dir("./sstables");

        // size capacity of the level 0 = run capacity of the level 0 * (memtable size capacity + 3 * BLOCK_SIZE).
        // where the 3 * BLOCK_SIZE corresponds to the filter, index and footer blocks inherently stored in
        // one sstable file.
        let default_level_0 = Level::new(
            0,
            cfg.run_capacity,
            cfg.run_capacity * (cfg.memtable_size_capacity + 3 * BLOCK_SIZE),
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

    fn snapshot_seq_num(&self) -> SeqNum {
        self.next_seq_num
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

        if self.mem.size() > self.cfg.memtable_size_capacity - TABLE_KEY_SIZE {
            self.minor_compaction();
            self.check_level_state();
            self.mem = MemTable::new();
        }
    }
}

/// db read implementation.
impl Db {
    /// point query the associated value in the database.
    pub fn get(&mut self, user_key: UserKey) -> Option<UserValue> {
        let snapshot_seq_num = self.snapshot_seq_num();
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
        let snapshot_seq_num = self.snapshot_seq_num();
        let start_lookup_key = LookupKey::new(start_user_key, snapshot_seq_num);
        let end_lookup_key = LookupKey::new(end_user_key, snapshot_seq_num);

        // iterator container to hold iterators from the memtable and all levels of sstables.
        let mut iters: BinaryHeap<TableKeyIteratorType> = BinaryHeap::new();
        let mut mem_iter = Box::new(self.mem.iter());
        mem_iter.next();
        iters.push(mem_iter);

        for level in self.levels.iter() {
            let mut level_iter = Box::new(level.iter().unwrap());
            level_iter.next();
            iters.push(level_iter);
        }

        let mut entries = Vec::new();

        let mut last_user_key = None;
        // loop inv: there's at least one iterator in the heap.
        while let Some(mut iter) = iters.pop() {
            // proceed if the iterator is not exhausted.
            if let Some(table_key) = iter.curr() {
                // early termination: the current key has a user key equal to or greater than the end user key.
                if table_key.user_key >= end_user_key {
                    break;
                }

                // ensure the table key is in the query range and is visible.
                if table_key >= start_lookup_key.as_table_key()
                    && table_key < end_lookup_key.as_table_key()
                {
                    // only the latest visible table key for each user key is collected.
                    if last_user_key.is_none() || table_key.user_key != last_user_key.unwrap() {
                        last_user_key = Some(table_key.user_key);
                        match table_key.write_type {
                            // only non-deleted keys are collected.
                            WriteType::Put => {
                                entries.push(UserEntry {
                                    key: table_key.user_key,
                                    val: table_key.user_val,
                                });
                            }
                            _ => {}
                        }
                    }
                }

                // push back the iterator into the heap.
                iter.next();
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

    fn overlap(me: &SSTable, other: &SSTable) -> bool {
        let (min, max) = (me.min_table_key.user_key, me.max_table_key.user_key);
        let (other_min, other_max) = (other.min_table_key.user_key, other.max_table_key.user_key);

        (min >= other_min && min <= other_max)
            || (max >= other_min && max <= other_max)
            || (min >= other_min && max <= other_max)
            || (other_min >= min && other_max <= max)
    }

    /// return true if the key range of the given sstable overlaps with the key range of the base sstable.
    fn overlap_with_base(&self, other: &SSTable) -> bool {
        let base = self.get_base();
        CompactionContext::overlap(base, other)
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
            let mut iter = Box::new(input.iter().unwrap());
            iter.next();
            iters.push(iter);
        }
        iters
    }
}

/// db compaction implementation.
impl Db {
    /// flush the table keys in memtable to a new sstable.
    fn minor_compaction(&mut self) {
        let mut sstable_writer_batch =
            SSTableWriterBatch::new(self.next_file_num, self.cfg.sstable_size_capacity);

        println!("minor compacting...");

        // compact table keys having the same user keys
        let mut last_user_key = None;
        let mut iter = self.mem.iter();
        while let Some(table_key) = iter.next() {
            if last_user_key.is_none() || last_user_key.unwrap() != table_key.user_key {
                last_user_key = Some(table_key.user_key);
                sstable_writer_batch.push(table_key);
            }
        }

        // complete the write.
        let (sstables, next_file_num) = sstable_writer_batch.done();
        // sync the next_file_num.
        self.next_file_num = next_file_num;

        let run = Run::new(
            sstables,
            sstable_writer_batch.min_table_key.as_ref().unwrap().clone(),
            sstable_writer_batch.max_table_key.as_ref().unwrap().clone(),
        );

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
                    print!("\nBefore compaction:\n{}\n", self.stats());
                    self.major_compaction(level_num);
                    print!("\nAfter compaction:\n{}\n", self.stats());
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

        println!(
            "select sstable {} at run {} as the base",
            sstable.file_num, run_idx
        );

        sstable.clone()
    }

    fn major_compaction(&mut self, level_num: LevelNum) {
        // select the base sstable in the current level.
        let base = self.select_compaction_base(level_num);
        println!("major compacting level {}", level_num);
        println!(
            "base = sstable {},  Min = {}  Max = {}",
            base.file_num,
            base.min_table_key.clone(),
            base.max_table_key.clone()
        );

        let curr_level = self.levels.get_mut(level_num).unwrap();
        let mut ctx = CompactionContext::new(base);

        // collect overlapping sstables in the current level.
        println!("collecting sstables at level {}", level_num);
        for run in curr_level.runs.iter() {
            for sstable in run.sstables.iter() {
                // skip the base sstable itself.
                if ctx.get_base().file_num == sstable.file_num {
                    continue;
                }

                if ctx.overlap_with_base(sstable) {
                    ctx.add_input(sstable.clone(), true);
                    println!(
                        "collect sstable {}, Min = {}  Max = {}",
                        sstable.file_num,
                        sstable.min_table_key.clone(),
                        sstable.max_table_key.clone()
                    );
                }
            }
        }

        println!(
            "current level key range: Min = {}  Max = {}",
            ctx.min_user_key, ctx.max_user_key
        );

        if let LevelState::ExceedRunCapacity = curr_level.state() {
            self.horizontal_compaction(&mut ctx, level_num);
        } else {
            self.vertical_compaction(&mut ctx, level_num);
        }

        self.remove_obsolete_sstables(&ctx);
    }

    fn merge(&mut self, iters: &mut BinaryHeap<TableKeyIteratorType>) -> Run {
        let mut sstable_writer_batch =
            SSTableWriterBatch::new(self.next_file_num, self.cfg.sstable_size_capacity);

        let mut num_input_keys = 0;
        let mut num_merged_keys = 0;
        let mut num_output_keys = 0;

        let mut last_user_key = None;
        while let Some(mut iter) = iters.pop() {
            if let Some(table_key) = iter.curr() {
                num_input_keys += 1;
                if last_user_key.is_none() || last_user_key.unwrap() != table_key.user_key {
                    last_user_key = Some(table_key.user_key);
                    // println!("push key {} to writer", table_key);
                    sstable_writer_batch.push(table_key);
                    num_output_keys += 1;
                } else {
                    num_merged_keys += 1;
                }

                iter.next();
                iters.push(iter);
            }
        }

        let (sstables, next_file_num) = sstable_writer_batch.done();
        self.next_file_num = next_file_num;

        assert_eq!(num_input_keys, num_merged_keys + num_output_keys);
        println!(
            "num_input_keys = {}  num_merged_keys = {}  num_output_keys = {}",
            num_input_keys, num_merged_keys, num_output_keys
        );

        println!("output sstables:");
        for sstable in sstables.iter() {
            println!(
                "output sstable {}, Min = {}  Max = {}",
                sstable.file_num,
                sstable.min_table_key.clone(),
                sstable.max_table_key.clone()
            );
        }

        // assert all sstables in this run has no overlapping keys.
        for i in 0..sstables.len() {
            for j in 0..sstables.len() {
                if i == j {
                    continue;
                }
                assert!(!CompactionContext::overlap(
                    sstables.get(i).unwrap(),
                    sstables.get(j).unwrap()
                ));
            }
        }

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
        println!("collecting sstables at level {}", next_level.level_num);
        for run in next_level.runs.iter() {
            for sstable in run.sstables.iter() {
                if ctx.overlap_with_curr_level(sstable) {
                    ctx.add_input(sstable.clone(), false);
                    println!(
                        "collect sstable {}, Min = {}  Max = {}",
                        sstable.file_num,
                        sstable.min_table_key.clone(),
                        sstable.max_table_key.clone()
                    );
                }
            }
        }

        // cannot skip merging even if there's only one input sstables.
        // that's because an sstable cannot be modified anyway which means even
        // its file name cannot be renamed.
        // hence we must do merging to move keys from the old sstable file to the new sstable file.
        let run = self.merge(&mut ctx.iters());

        self.levels
            .get_mut(curr_level_num + 1)
            .unwrap()
            .add_run(run);
    }

    fn select_compaction_run(&mut self, curr_level_num: LevelNum, base_file_num: FileNum) -> Run {
        // randomly select a run in the current level.
        // this run cannot be the run containing the base sstable.
        let curr_level = self.levels.get_mut(curr_level_num).unwrap();
        assert!(curr_level.runs.len() > 1);

        let mut run_idx;
        loop {
            run_idx = rand::thread_rng().gen_range(0..curr_level.runs.len());
            let run = curr_level.runs.get(run_idx).unwrap();

            let mut ok = true;
            for sstable in run.sstables.iter() {
                if sstable.file_num == base_file_num {
                    ok = false;
                    break;
                }
            }
            if ok {
                break;
            }
        }

        println!(
            "select run {} as the compaction run. Min = {}  Max = {}",
            run_idx,
            curr_level
                .runs
                .get(run_idx)
                .unwrap()
                .min_table_key
                .as_ref()
                .unwrap(),
            curr_level
                .runs
                .get(run_idx)
                .unwrap()
                .max_table_key
                .as_ref()
                .unwrap(),
        );

        // the key range is lazily updated.
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

        let old_run = self.select_compaction_run(curr_level_num, ctx.get_base().file_num);
        for sstable in old_run.sstables.iter() {
            // only sstables not involved in the compaction are merged with the new run.
            if !obsolete_file_nums.contains(&sstable.file_num) {
                let mut sstable_iter = Box::new(sstable.iter().unwrap());
                sstable_iter.next();
                iters.push(sstable_iter);
            }
        }

        let new_run = self.merge(&mut ctx.iters());
        println!(
            "merged the inputs into a new run. Min = {}  Max = {}",
            new_run.min_table_key.clone().unwrap(),
            new_run.max_table_key.clone().unwrap(),
        );

        let mut new_run_iter = Box::new(new_run.iter().unwrap());
        new_run_iter.next();
        iters.push(new_run_iter);

        let merged_run = self.merge(&mut iters);
        println!(
            "merged the new run and the old run into a merged run. Min = {}  Max = {}",
            merged_run.min_table_key.clone().unwrap(),
            merged_run.max_table_key.clone().unwrap(),
        );

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

        // delete obsolete sstable files.
        for file_num in obsolete_file_nums.iter() {
            remove_file(sstable_file_name(*file_num)).unwrap();
        }
    }
}

impl Db {
    pub fn stats(&self) -> String {
        let mut stats = String::new();
        for level in self.levels.iter() {
            stats += &format!("level {}\n{}", level.level_num, level.stats(1))
        }
        stats
    }
}

// `cfg(test)` on the tests module tells Rust to compile and run the test code only when you run cargo test
#[cfg(test)]
mod tests {
    // import all names from the being-tested module.
    use super::*;

    /// write a sequence of user keys in the range [0, max_user_key), and check all these keys are
    /// inserted successfully.
    fn check_sequential_keys(db: &mut Db, num_table_keys: usize) {
        for user_key in 0..num_table_keys {
            db.put(user_key as i32, 0);
        }

        assert_eq!(num_table_keys, db.next_seq_num);

        for user_key in 0..num_table_keys {
            assert_eq!(db.get(user_key as i32).unwrap(), 0);
        }
    }

    /// configures the #writes such that only the in-memory memtable is involved, i.e. no compaction.
    // `test` macro turns the function into a unit test.
    #[test]
    fn mem_only_sequential() {
        let mut db = Db::new(Config::test());
        check_sequential_keys(&mut db, 100);
    }

    /// configures the #writes such that a minor compaction is triggered.
    #[test]
    fn minor_sequential() {
        let mut db = Db::new(Config::test());
        check_sequential_keys(&mut db, 1000);
    }

    #[test]
    fn merge() {
        let mut db = Db::new(Config::test());

        // write [0, 962], [5778, 6740] to an sstable.
        let mut sstable_writer_batch =
            SSTableWriterBatch::new(db.next_file_num, db.cfg.sstable_size_capacity);
        let num_table_keys = 963;
        for i in 0..num_table_keys {
            let table_key = TableKey::identity(i);
            sstable_writer_batch.push(table_key)
        }
        for i in 0..num_table_keys {
            let table_key = TableKey::identity(i + 5778);
            sstable_writer_batch.push(table_key)
        }
        let (sstables, next_file_num) = sstable_writer_batch.done();
        db.next_file_num = next_file_num;
        let a = sstables.first().unwrap();

        // write [963, 1925] to an sstable.
        sstable_writer_batch =
            SSTableWriterBatch::new(db.next_file_num, db.cfg.sstable_size_capacity);
        for i in 0..num_table_keys {
            let table_key = TableKey::identity(i + 963);
            sstable_writer_batch.push(table_key)
        }
        let (sstables, next_file_num) = sstable_writer_batch.done();
        db.next_file_num = next_file_num;
        let b = sstables.first().unwrap();

        // write [2889, 4814] to an sstable.
        sstable_writer_batch =
            SSTableWriterBatch::new(db.next_file_num, db.cfg.sstable_size_capacity);
        for i in 0..num_table_keys * 2 {
            let table_key = TableKey::identity(i + 2889);
            sstable_writer_batch.push(table_key)
        }
        let (sstables, next_file_num) = sstable_writer_batch.done();
        db.next_file_num = next_file_num;
        let c = sstables.first().unwrap();

        // merge sstables a, b, c.
        let mut iters: BinaryHeap<TableKeyIteratorType> = BinaryHeap::new();

        // init iterators.
        let mut a_iter = Box::new(a.iter().unwrap());
        a_iter.next();
        let mut b_iter = Box::new(b.iter().unwrap());
        b_iter.next();
        let mut c_iter = Box::new(c.iter().unwrap());
        c_iter.next();

        iters.push(a_iter);
        iters.push(b_iter);
        iters.push(c_iter);

        println!("merging...");
        // assertion is done inside `merge`.
        db.merge(&mut iters);
    }

    /// configures the #writes such that a major compaction is triggered.
    #[test]
    fn major_sequential() {
        let mut db = Db::new(Config::test());
        check_sequential_keys(&mut db, 10000);
    }

    #[test]
    fn mem_only_range_no_delete() {
        let mut db = Db::new(Config::test());
        let num_table_keys = 100;
        for i in 0..num_table_keys {
            db.put(i, i);
        }

        let entries = db.range(0, num_table_keys);
        assert_eq!(entries.len(), num_table_keys as usize);

        for i in 0..num_table_keys {
            assert_eq!(entries[i as usize].val, i);
        }
    }

    #[test]
    fn mem_only_range_with_delete() {
        let mut db = Db::new(Config::test());
        let num_table_keys = 100;
        for i in 0..num_table_keys {
            db.put(i, i);
        }

        let max_num_deletes = 20;
        let mut deleted_keys = HashSet::with_capacity(max_num_deletes);
        let mut rng = rand::thread_rng();
        for _ in 0..max_num_deletes {
            let i = rng.gen_range(0..num_table_keys);
            if !deleted_keys.contains(&i) {
                db.delete(i);
                deleted_keys.insert(i);
            }
        }

        let seq_num = num_table_keys as usize + deleted_keys.len();
        assert_eq!(seq_num, db.next_seq_num);

        let entries = db.range(0, num_table_keys);
        assert_eq!(entries.len(), num_table_keys as usize - deleted_keys.len());

        for i in 0..num_table_keys {
            let mut exist = false;
            let mut val = None;
            for entry in entries.iter() {
                if entry.key == i {
                    exist = true;
                    val = Some(entry.val);
                    break;
                }
            }

            if deleted_keys.contains(&i) {
                assert_eq!(exist, false);
            } else {
                assert_eq!(exist, true);
                assert_eq!(val.unwrap(), i);
            }
        }
    }

    // TODO: add unit testing for on-disk range.

    /// put a sequence of keys.
    /// randomly select some keys to be deleted.
    /// delete these keys.
    /// randomly select some keys not deleted.
    /// update these keys.
    /// put another sequence of keys inorder to push all former keys into the disk.
    /// check the deleted keys are deleted.
    /// check the updated keys are updated.
    /// check all other keys still exist and their values are correct.
    /// the number of keys are configured such that a set of major compactions will be incurred.
    // FIXME: seems there're still bugs in the implementation
    #[test]
    fn compaction() {
        let mut db = Db::new(Config::test());
        let num_puts = 10000;
        for i in 0..num_puts {
            db.put(i, i);
        }

        let max_num_deletes = 2000;
        let mut deleted_keys = HashSet::with_capacity(max_num_deletes);
        let mut rng = rand::thread_rng();
        for _ in 0..max_num_deletes {
            let i = rng.gen_range(0..num_puts);
            if !deleted_keys.contains(&i) {
                db.delete(i);
                deleted_keys.insert(i);
            }
        }

        let max_num_updates = 2000;
        let mut updated_keys = HashSet::with_capacity(max_num_updates);
        for _ in 0..max_num_updates {
            let i = rng.gen_range(0..num_puts);
            // do not update keys that were deleted.
            if !deleted_keys.contains(&i) && !updated_keys.contains(&i) {
                db.put(i, i + num_puts);
                updated_keys.insert(i);
            }
        }

        let num_puts_2 = 2000;
        for i in num_puts..num_puts + num_puts_2 {
            db.put(i, i);
        }

        let seq_num =
            num_puts as usize + deleted_keys.len() + updated_keys.len() + num_puts_2 as usize;
        assert_eq!(seq_num, db.next_seq_num);

        for i in 0..num_puts {
            if deleted_keys.contains(&i) {
                // TODO: wrap all debug println to debug_print.
                println!("key {} is deleted", i);
            } else if updated_keys.contains(&i) {
                println!("key {} is updated", i);
            } else {
                println!("key {} is not changed", i);
            }

            let val = db.get(i);
            if deleted_keys.contains(&i) {
                assert!(val.is_none());
            } else if updated_keys.contains(&i) {
                assert_eq!(val.unwrap(), i + num_puts);
            } else {
                assert_eq!(val.unwrap(), i);
            }
        }
    }
}
