use crate::storage::sstable::{SSTable, SSTableIterator};
use std::cmp::{self, Ordering};
use std::rc::Rc;

use super::iterator::TableKeyIterator;
use super::keys::*;

/// sorted run.
/// two properties a sorted run must have:
/// (1) keys are sorted.
/// (2) keys are non-overlapping.
#[derive(Clone)]
pub struct Run {
    /// sstables in the run.
    /// the sstables are sorted by the max user key, i.e. sstables with lower max user keys are placed first.
    pub sstables: Vec<Rc<SSTable>>,
    /// min table key stored in the run.
    pub min_table_key: Option<TableKey>,
    /// max table key stored in the run.
    pub max_table_key: Option<TableKey>,
}

impl Run {
    pub fn new(
        sstables: Vec<Rc<SSTable>>,
        min_table_key: TableKey,
        max_table_key: TableKey,
    ) -> Self {
        Self {
            sstables,
            min_table_key: Some(min_table_key),
            max_table_key: Some(max_table_key),
        }
    }

    pub fn get(&self, lookup_key: &LookupKey) -> (Option<TableKey>, bool) {
        if &lookup_key.as_table_key() >= self.min_table_key.as_ref().unwrap()
            && &lookup_key.as_table_key() <= self.max_table_key.as_ref().unwrap()
        {
            if let Some(sstable) = self.binary_search(lookup_key) {
                return sstable.get(lookup_key);
            }
        }
        (None, false)
    }

    fn binary_search(&self, lookup_key: &LookupKey) -> Option<Rc<SSTable>> {
        match self
            .sstables
            .binary_search_by(|sstable| sstable.max_table_key.cmp(&lookup_key.as_table_key()))
        {
            Ok(i) => {
                if let Some(sstable) = self.sstables.get(i) {
                    Some(sstable.clone())
                } else {
                    None
                }
            }
            Err(i) => {
                if let Some(sstable) = self.sstables.get(i - 1) {
                    Some(sstable.clone())
                } else {
                    None
                }
            }
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

/// run write implementation.
impl Run {
    /// return the total size in bytes of all sstables stored in the run.
    pub fn size(&self) -> usize {
        let mut total = 0;
        for sstable in self.sstables.iter() {
            total += sstable.file_size;
        }
        total
    }

    /// update the key range of the run using the existing sstables.
    pub fn update_key_range(&mut self) {
        if self.sstables.is_empty() {
            self.min_table_key = None;
            self.max_table_key = None;
            return;
        }

        let mut min_table_key = self.sstables.first().unwrap().min_table_key.clone();
        let mut max_table_key = self.sstables.first().unwrap().max_table_key.clone();

        for i in 1..self.sstables.len() {
            min_table_key = cmp::min(
                min_table_key.clone(),
                self.sstables.get(i).unwrap().min_table_key.clone(),
            );
            max_table_key = cmp::max(
                max_table_key.clone(),
                self.sstables.get(i).unwrap().max_table_key.clone(),
            );
        }

        self.min_table_key = Some(min_table_key);
        self.max_table_key = Some(max_table_key);
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
