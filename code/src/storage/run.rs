use crate::storage::sstable::{SSTable, SSTableIterator, SSTableStats};
use std::cmp::{self, Ordering};
use std::fmt::Display;
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
        if lookup_key.user_key >= self.min_table_key.as_ref().unwrap().user_key
            && lookup_key.user_key <= self.max_table_key.as_ref().unwrap().user_key
        {
            if let Some(sstable) = self.binary_search(lookup_key) {
                return sstable.get(lookup_key);
            }
        }
        (None, false)
    }

    // binary search the first sstable that has a greater max user key than the lookup key's user key.
    fn binary_search(&self, lookup_key: &LookupKey) -> Option<Rc<SSTable>> {
        let mut lo = 0; // start of the search space.
        let mut len = self.sstables.len(); // search space length.

        // loop inv: the search space is not empty.
        while len > 0 {
            let half = len / 2; // the length of the left half of the search space.
            let mid = lo + half;
            let sstable = self.sstables.get(mid).unwrap();

            // if adjacent sstables contain the same user key, only the left sstable might be target sstable.
            // so the lower-bound binary searching is applied here.
            if sstable.max_table_key.user_key < lookup_key.user_key {
                // proceed searching in the right half.
                lo = mid + 1;
                len -= half + 1;
            } else {
                // proceed searching in the left half.
                len = half;
            }
        }

        // further check that this sstable maybe contain the target key.
        let sstable = self.sstables.get(lo).unwrap();
        if sstable.min_table_key.user_key <= lookup_key.user_key
            && sstable.max_table_key.user_key >= lookup_key.user_key
        {
            Some(sstable.clone())
        } else {
            None
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

impl PartialOrd for RunIterator {
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

pub struct RunStats {
    indent: usize,
    sstable_stats: Vec<SSTableStats>,
    min_table_key: TableKey,
    max_table_key: TableKey,
}

impl Run {
    pub fn stats(&self, indent: usize) -> RunStats {
        let mut sstable_stats = Vec::new();
        for sstable in self.sstables.iter() {
            sstable_stats.push(sstable.stats(indent + 1));
        }
        RunStats {
            indent,
            sstable_stats,
            min_table_key: self.min_table_key.as_ref().unwrap().clone(),
            max_table_key: self.max_table_key.as_ref().unwrap().clone(),
        }
    }
}

impl Display for RunStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut stats = String::new();

        stats += "  ".repeat(self.indent).as_str();
        stats += &format!("Min = {}    ", self.min_table_key);
        stats += &format!("Max = {}\n", self.max_table_key);

        for sstable_stats in self.sstable_stats.iter() {
            stats += "  ".repeat(self.indent).as_str();
            stats += &format!("sstable {}\n{}", sstable_stats.file_num, sstable_stats);
        }

        write!(f, "{}", stats)
    }
}
