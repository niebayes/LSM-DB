use std::vec;

use crate::util::types::{KvEntry, KvVec, TableKey, UserKey, UserValue};

/// memtable.
pub trait MemTable {
    fn put(&mut self, table_key: TableKey);
    fn get(&self, table_key: &TableKey) -> Option<UserValue>;
    fn range(&self, start_table_key: &TableKey, end_table_key: &TableKey) -> Vec<UserValue>;
}

impl MemTable for KvVec {
    fn put(&mut self, table_key: TableKey) {}

    fn get(&self, table_key: &TableKey) -> Option<UserValue> {
        None
    }

    fn range(&self, start_table_key: &TableKey, end_table_key: &TableKey) -> Vec<UserValue> {
        vec![]
    }
}
