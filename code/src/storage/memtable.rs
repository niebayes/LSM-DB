// use std::borrow::Borrow;
use super::lookup_key::LookupKey;
use super::table_key::{TableKey, TABLE_KEY_SIZE};
use crate::util::types::*;
use std::collections::BTreeSet;

/// memtable.
pub struct MemTable {
    set: BTreeSet<TableKey>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            set: BTreeSet::new(),
        }
    }

    /// write a table key into the memtable.
    pub fn put(&mut self, table_key: TableKey) {
        // such an insertion must succeed since the sequence number for each write key is unique.
        assert_eq!(self.set.insert(table_key), true);
    }

    /// point query the val associated of the given key.
    pub fn get(&self, lookup_key: &LookupKey) -> Option<UserValue> {
        // any key with an equal or less sequence number are considered in the snapshot.
        let snapshot_seq_num = lookup_key.seq_num;

        let mut iter = self.set.iter();

        // seek a table key containg an equal or greater user key.
        while let Some(mut table_key) = iter.next() {
            if table_key.user_key >= lookup_key.user_key {
                // fetch the latest table key containing an equal or less sequence number.
                while table_key.seq_num > snapshot_seq_num {
                    if let Some(next_table_key) = iter.next() {
                        table_key = next_table_key;
                    } else {
                        // break if the iterator is exhausted.
                        break;
                    }
                }

                if table_key.seq_num <= lookup_key.seq_num {
                    // found a visible key.
                    return Some(table_key.user_val);
                }
                return None;
            }
        }
        None
    }

    /// range query the values associated with keys in the given range.
    /// the iterator gives us a flatten view of the keys stored in the memtable:
    /// keys with the same user key are clustered together and form a chunk.
    /// each chunk contain keys with different sequence numbers and keys with lower
    /// sequence numbers are iterated first.
    /// for range query, we first collect all chunks within the range. And for each
    /// collected chunk, the latest key visible to the snapshot is collected.
    pub fn range(
        &self,
        start_lookup_key: &LookupKey,
        end_lookup_key: &LookupKey,
    ) -> Vec<UserEntry> {
        let mut entries = Vec::new();
        let snapshot_seq_num = start_lookup_key.seq_num;

        let mut iter = self.set.iter();
        while let Some(mut table_key) = iter.next() {
            if table_key.user_key >= start_lookup_key.user_key {
                // true if the iterator is exhausted.
                let mut exhausted = false;

                // inspect each chunk.
                while !exhausted && table_key.user_key < end_lookup_key.user_key {
                    // the user key of the current chunk.
                    let curr_user_key = table_key.user_key;

                    // inspect keys in the current chunk, but skip keys not visible to the snapshot.
                    while table_key.user_key == curr_user_key
                        && table_key.seq_num > snapshot_seq_num
                    {
                        if let Some(next_table_key) = iter.next() {
                            table_key = next_table_key;
                        } else {
                            exhausted = true;
                            break;
                        }
                    }

                    if !exhausted {
                        // collect the latest visible key in the current chunk.
                        if table_key.user_key == curr_user_key
                            && table_key.seq_num <= snapshot_seq_num
                        {
                            entries.push(UserEntry {
                                key: table_key.user_key,
                                val: table_key.user_val,
                            });
                        }

                        // consume remaining keys in the current chunk if any.
                        while !exhausted && table_key.user_key == curr_user_key {
                            if let Some(next_table_key) = iter.next() {
                                table_key = next_table_key;
                            } else {
                                exhausted = true;
                            }
                        }
                    }
                }
                // all keys beyond the range are not iterated over.
                break;
            }
        }

        entries
    }

    /// return the total size in bytes of the table keys stored in the memtable.
    pub fn size(&self) -> usize {
        self.set.len() * TABLE_KEY_SIZE
    }
}
