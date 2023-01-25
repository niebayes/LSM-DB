use crate::storage::sstable::{SSTable, SSTableIterator};
use crate::util::types::{UserKey, UserValue};
use std::cmp::Ordering;

use super::iterator::TableKeyIterator;
use super::keys::*;

/// sorted run.
/// two properties a sorted run must have:
/// (1) keys are sorted.
/// (2) keys are non-overlapping.
pub struct Run {
    /// min user key stored in the run.
    pub min_user_key: UserKey,
    /// max user key stored in the run.
    pub max_user_key: UserKey,
    /// sstables in the run.
    /// the sstables are sorted by the max user key, i.e. sstables with lower max user keys are placed first.
    sstables: Vec<SSTable>,
}

impl Run {
    pub fn new() -> Self {
        Self {
            min_user_key: UserKey::MAX,
            max_user_key: UserKey::MIN,
            sstables: Vec::new(),
        }
    }

    pub fn get(&self, lookup_key: &LookupKey) -> (Option<TableKey>, bool) {
        if lookup_key.user_key >= self.min_user_key && lookup_key.user_key <= self.max_user_key {
            if let Some(sstable) = self.binary_search(lookup_key) {
                return sstable.get(lookup_key);
            }
        }
        (None, false)
    }

    fn binary_search(&self, lookup_key: &LookupKey) -> Option<&SSTable> {
        match self
            .sstables
            .binary_search_by(|sstable| sstable.max_user_key.cmp(&lookup_key.user_key))
        {
            Ok(i) => return self.sstables.get(i),
            Err(i) => return self.sstables.get(i - 1),
        }
    }

    pub fn iter(&self) -> Result<RunIterator, ()> {
        let mut sstable_iters = Vec::new();
        for sstable in self.sstables.iter() {
            sstable_iters.push(sstable.iter()?);
        }
        Ok(RunIterator {
            sstable_iters,
            curr_table_key: None,
            curr_sstable_idx: 0,
        })
    }
}

pub struct RunIterator {
    sstable_iters: Vec<SSTableIterator>,
    curr_table_key: Option<TableKey>,
    curr_sstable_idx: usize,
}

impl TableKeyIterator for RunIterator {
    fn seek(&mut self, lookup_key: &LookupKey) {
        while let Some(table_key) = self.next() {
            if table_key >= lookup_key.as_table_key() {
                break;
            }
        }
    }

    fn next(&mut self) -> Option<TableKey> {
        while self.curr_sstable_idx < self.sstable_iters.len() {
            if let Some(table_key) = self.sstable_iters[self.curr_sstable_idx].next() {
                self.curr_table_key = Some(table_key);
                return self.curr_table_key.clone();
            } else {
                self.curr_sstable_idx += 1;
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

impl PartialEq for RunIterator {
    fn eq(&self, other: &Self) -> bool {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => return head == other_head,
            (Some(_), None) | (None, Some(_)) => return false,
            (None, None) => return true,
        }
    }
}

impl Eq for RunIterator {}

impl<'a> PartialOrd for RunIterator {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => return head.partial_cmp(&other_head),
            (Some(_), None) => return Some(Ordering::Less),
            (None, Some(_)) => return Some(Ordering::Greater),
            (None, None) => return Some(Ordering::Equal),
        }
    }
}

impl Ord for RunIterator {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => return head.cmp(&other_head),
            (Some(_), None) => return Ordering::Less,
            (None, Some(_)) => return Ordering::Greater,
            (None, None) => return Ordering::Equal,
        }
    }
}
