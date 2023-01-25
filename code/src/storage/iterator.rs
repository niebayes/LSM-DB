use std::cmp::Ordering;

use super::keys::{LookupKey, TableKey};

pub trait TableKeyIterator {
    fn seek(&mut self, lookup_key: &LookupKey);
    fn next(&mut self) -> Option<TableKey>;
    fn curr(&self) -> Option<TableKey>;
    fn valid(&self) -> bool;
}

pub type TableKeyIteratorType = Box<dyn TableKeyIterator>;

impl PartialEq for TableKeyIteratorType {
    fn eq(&self, other: &Self) -> bool {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => return head == other_head,
            (Some(_), None) | (None, Some(_)) => return false,
            (None, None) => return true,
        }
    }
}

impl Eq for TableKeyIteratorType {}

impl PartialOrd for TableKeyIteratorType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => return head.partial_cmp(&other_head),
            (Some(_), None) => return Some(Ordering::Less),
            (None, Some(_)) => return Some(Ordering::Greater),
            (None, None) => return Some(Ordering::Equal),
        }
    }
}

impl Ord for TableKeyIteratorType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => return head.cmp(&other_head),
            (Some(_), None) => return Ordering::Less,
            (None, Some(_)) => return Ordering::Greater,
            (None, None) => return Ordering::Equal,
        }
    }
}
