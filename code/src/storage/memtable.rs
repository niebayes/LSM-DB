use super::iterator::TableKeyIterator;
use super::keys::*;
use crate::util::types::*;
use std::collections::BTreeSet;
use std::fmt::Display;

/// memtable.
/// keys are written into the memtable buffer before being flushed to the sstables.
pub struct MemTable {
    /// the set maintains table keys in a specified order.
    set: BTreeSet<TableKey>,
}

/// a memtable iterator is simply an wrapper of the underlying set's iterator.
pub struct MemTableIterator<'a> {
    /// the iterator of the underlying set structure.
    set_iter: Box<dyn Iterator<Item = &'a TableKey> + 'a>,
    curr_table_key: Option<TableKey>,
}

impl<'a> TableKeyIterator for MemTableIterator<'a> {
    fn seek(&mut self, lookup_key: &LookupKey) {
        while let Some(table_key) = self.next() {
            if table_key >= lookup_key.as_table_key() {
                break;
            }
        }
    }

    fn next(&mut self) -> Option<TableKey> {
        if let Some(table_key) = self.set_iter.next() {
            self.curr_table_key = Some(table_key.clone());
            return self.curr_table_key.clone();
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

impl MemTable {
    pub fn new() -> Self {
        Self {
            set: BTreeSet::new(),
        }
    }

    pub fn iter(&self) -> MemTableIterator {
        MemTableIterator {
            set_iter: Box::new(self.set.iter()),
            curr_table_key: None,
        }
    }

    /// write a table key into the memtable.
    pub fn put(&mut self, table_key: TableKey) {
        // such an insertion must succeed since the sequence number for each write key is unique.
        assert_eq!(self.set.insert(table_key), true);
    }

    /// point query the value associated of the given key.
    /// the iterator gives us a flatten view of the keys stored in the memtable:
    /// keys with the same user key are clustered together and form a chunk.
    /// each chunk contains keys with different sequence numbers and keys with lower
    /// sequence numbers are iterated first.
    /// for point query, we first locate the chunk having the same user key as the lookup key,
    /// and then we inspect keys in the chunk from left to right.
    /// the latest key visible to the snapshot is the target key.
    pub fn get(&self, lookup_key: &LookupKey) -> (Option<UserValue>, bool) {
        let mut iter = self.iter();
        iter.seek(lookup_key);
        if iter.valid() {
            let table_key = iter.curr().unwrap();
            match table_key.write_type {
                WriteType::Put => return (Some(table_key.user_val), false),
                WriteType::Delete => return (Some(table_key.user_val), true),
                other => panic!("Unexpected write type {}", other as u8),
            }
        }
        (None, false)
    }

    /// return the total size in bytes of the table keys stored in the memtable.
    pub fn size(&self) -> usize {
        self.set.len() * TABLE_KEY_SIZE
    }

    pub fn stats(&self) -> MemTableStats {
        let mut all_table_keys = Vec::new();
        let mut visible_table_keys = Vec::new();

        let mut iter = self.iter();
        let mut last_user_key = None;
        while let Some(table_key) = iter.next() {
            if last_user_key.is_none() || last_user_key.unwrap() == table_key.user_key {
                last_user_key = Some(table_key.user_key);
                visible_table_keys.push(format!("{}", table_key));
            }
            all_table_keys.push(format!("{}", table_key));
        }

        MemTableStats {
            all_table_keys,
            visible_table_keys,
        }
    }
}

pub struct MemTableStats {
    /// all table keys in the memtable.
    all_table_keys: Vec<String>,
    /// keys with higher sequence numbers shadows keys with lower sequence numbers.
    visible_table_keys: Vec<String>,
}

impl Display for MemTableStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut stats = String::new();

        stats += &format!("all table keys:\n\tcount: {}", self.all_table_keys.len());
        for table_key in self.all_table_keys.iter() {
            stats += &format!("\n\t{}", table_key);
        }

        stats += &format!(
            "visible table keys:\n\tcount: {}",
            self.visible_table_keys.len()
        );
        for table_key in self.visible_table_keys.iter() {
            stats += &format!("\n\t{}", table_key);
        }

        write!(f, "{}", stats)
    }
}
