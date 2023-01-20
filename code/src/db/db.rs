use crate::config::config::Config;
use crate::logging::db_log::DbLogRecord;
use crate::logging::write_log::WriteLogRecord;
use crate::storage::level::{default_two_level, Level};
use crate::storage::lookup_key::LookupKey;
use crate::storage::memtable::MemTable;
use crate::storage::table_key::TableKey;
use crate::util::name::*;
use crate::util::types::*;
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
    }
}

/// db read implementation.
impl Db {
    pub fn get(&mut self, user_key: UserKey) -> Option<UserValue> {
        // construct the corresponding table key.
        // the sequence number is used to construct a snapshot of the db wherein all keys considered
        // are keys with sequence number <= the latest sequence number.
        let lookup_key = LookupKey::new(user_key, self.latest_seq_num());

        // search the key in the memtable.
        if let Some(val) = self.mem.get(&lookup_key) {
            return Some(val);
        }

        // search the key in the sstables.
        for level in self.levels.iter_mut() {
            if let Some(val) = level.get(user_key) {
                return Some(val);
            }
        }

        // the key does not exist.
        None
    }

    pub fn range(&mut self, start_user_key: UserKey, end_user_key: UserKey) -> Vec<UserEntry> {
        vec![]
    }
}

/// db compaction implementation.
impl Db {}

/// db recover implementation.
impl Db {
    fn recover(&mut self) {}
}
