use crate::config::config::Config;
use crate::logging::db_log::DbLogRecord;
use crate::logging::write_log::WriteLogRecord;
use crate::storage::iterator::*;
use crate::storage::keys::{LookupKey, TableKey};
use crate::storage::level::{default_two_level, Level};
use crate::storage::memtable::MemTable;
use crate::util::name::*;
use crate::util::types::*;
use std::collections::{BinaryHeap, HashMap, LinkedList};
use std::fs::OpenOptions;
use std::io::Write;
use std::vec;

pub struct Db {
    /// database config.
    cfg: Config,
    /// memtable.
    mem: MemTable,
    /// all levels in the lsm tree.
    levels: Vec<Level>,
    /// next sequence number to allocate for a write.
    next_seq_num: SeqNum,
    /// next file number to allocate for a file.
    next_file_num: FileNum,
    /// file number of the write log file.
    write_log_file_num: Option<FileNum>,
    /// file number of the database log file.
    db_log_file_num: Option<FileNum>,
    /// file number of the level mapping log file.
    level_log_file_num: Option<FileNum>,
}

/// db miscs implementation.
impl Db {
    pub fn new(cfg: Config) -> Db {
        let mut db = Db {
            cfg,
            mem: MemTable::new(),
            levels: default_two_level(),
            next_seq_num: SeqNum::default(),
            next_file_num: FileNum::default(),
            write_log_file_num: None,
            db_log_file_num: None,
            level_log_file_num: None,
        };

        // crash recovery.
        db.recover();

        // allocate a file number for the write log file if not allocated so far.
        if db.write_log_file_num.is_none() {
            db.write_log_file_num = Some(db.alloc_file_num());
        }

        // allocate a file number for the db log file if not allocated so far.
        if db.db_log_file_num.is_none() {
            db.db_log_file_num = Some(db.alloc_file_num());
        }

        // allocate a file number for the db log file if not allocated so far.
        if db.level_log_file_num.is_none() {
            db.level_log_file_num = Some(db.alloc_file_num());
        }

        // TODO: add more necessary fields to the DbLogRecord struct.
        // TODO: write a db log record.

        db
    }

    pub fn print_stats(&mut self) {}

    fn alloc_seq_num(&mut self) -> SeqNum {
        let seq_num = self.next_seq_num;
        self.next_seq_num += 1;
        seq_num
    }

    fn latest_seq_num(&self) -> SeqNum {
        return self.next_seq_num - 1;
    }

    fn alloc_file_num(&mut self) -> SeqNum {
        let file_num = self.next_file_num;
        self.next_file_num += 1;
        file_num
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
        // allocate a new sequence number for the write.
        let seq_num = self.alloc_seq_num();

        // write a db log record.
        let db_log_record = DbLogRecord::new()
            .set_next_seq_num(self.next_seq_num)
            .encode_to_bytes();

        let mut db_log_file = OpenOptions::new()
            .append(true)
            .open(db_log_file_name(self.db_log_file_num.unwrap()))
            .unwrap();

        db_log_file.write(db_log_record.as_slice()).unwrap();

        // write a write log record.
        let write_log_record = WriteLogRecord::new()
            .set_user_key(user_key)
            .set_user_val(user_val)
            .set_write_type(write_type)
            .set_seq_num(seq_num)
            .encode_to_bytes();

        let mut write_log_file = OpenOptions::new()
            .append(true)
            .open(write_log_file_name(self.write_log_file_num.unwrap()))
            .unwrap();

        write_log_file.write(write_log_record.as_slice()).unwrap();

        // construct a table key and write it into the memtable.
        let table_key = TableKey::new(user_key, user_val, seq_num, write_type);
        self.mem.put(table_key);

        if self.mem.size() >= self.cfg.memtable_capacity {
            self.minor_compaction();
        }
    }
}

/// db read implementation.
impl Db {
    /// point query the associated value in the database.
    pub fn get(&mut self, user_key: UserKey) -> Option<UserValue> {
        // each query is on a snapshot of the database where a snapshot contains all keys with sequence numbers
        // less than or equal to the snapshot sequence number.
        let lookup_key = LookupKey::new(user_key, self.latest_seq_num());

        // search the key in the memtable.
        match self.mem.get(&lookup_key) {
            // the key exists and is not deleted.
            (Some(user_val), false) => return Some(user_val),
            // the key exists but is deleted.
            (Some(_), true) => return None,
            // the key does not exist, proceed to searching in sstables.
            (None, _) => {}
        };

        // search the key in the sstables.
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

    pub fn range(&mut self, start_user_key: UserKey, end_user_key: UserKey) -> Vec<UserEntry> {
        let snapshot_seq_num = self.latest_seq_num();

        // entry container to hold all visible entries found within the key range.
        let mut entries = Vec::new();

        // iterator container to hold iterators from the memtable and all levels of sstables.
        let mut iters: BinaryHeap<TableKeyIteratorType> = BinaryHeap::new();

        iters.push(Box::new(self.mem.iter()));

        for level in self.levels.iter() {
            if let Ok(iter) = level.iter() {
                iters.push(Box::new(iter));
            } else {
                log::error!(
                    "Failed to construct an iterator of level {}",
                    level.level_num
                );
            }
        }

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
                    if table_key.user_key >= start_user_key
                        && table_key.user_key < end_user_key
                        && table_key.seq_num <= snapshot_seq_num
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
                            // skip deleted keys.
                            WriteType::Delete => {}
                            other => panic!("Unexpected write type: {}", other as u8),
                        }
                    }
                }
                iters.push(iter);
            }
        }

        entries
    }
}
/// db compaction implementation.
impl Db {
    fn minor_compaction(&mut self) {}

    fn major_compaction(&mut self) {}
}

/// db recover implementation.
impl Db {
    fn recover(&mut self) {}
}
