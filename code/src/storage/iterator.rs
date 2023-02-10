use std::{cmp::Ordering, collections::BinaryHeap};

use super::keys::{LookupKey, TableKey};

pub trait TableKeyIterator {
    /// `seek` will move the cursor to point to the greater than or equal table key.
    /// a greater table key could be a table key with a higher user key or with the identical
    /// user key but having a higher sequence number.
    fn seek(&mut self, lookup_key: &LookupKey);
    fn next(&mut self) -> Option<TableKey>;
    fn curr(&self) -> Option<TableKey>;
    fn valid(&self) -> bool;
}

pub type TableKeyIteratorType<'a> = Box<dyn TableKeyIterator + 'a>;

impl<'a> PartialEq for TableKeyIteratorType<'a> {
    fn eq(&self, other: &Self) -> bool {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => return head == other_head,
            (Some(_), None) | (None, Some(_)) => return false,
            (None, None) => return true,
        }
    }
}

impl<'a> Eq for TableKeyIteratorType<'a> {}

impl<'a> PartialOrd for TableKeyIteratorType<'a> {
    // binary heap in rust is a max-heap, and hence this method is defined such that greater values swim up.
    // note that the returned order is the reversed table key order.
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // FIXME: found the bugs: initially, all iterators point to None since the sstable iterator is not initialized.
        // might be solved by pre-init each iterator.
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => match head.partial_cmp(&other_head).unwrap() {
                Ordering::Less => return Some(Ordering::Greater),
                Ordering::Equal => return Some(Ordering::Equal),
                Ordering::Greater => return Some(Ordering::Less),
            },
            (Some(_), None) => return Some(Ordering::Greater),
            (None, Some(_)) => return Some(Ordering::Less),
            (None, None) => return Some(Ordering::Equal),
        }
    }
}

impl<'a> Ord for TableKeyIteratorType<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => return head.cmp(&other_head),
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => return Ordering::Equal,
        }
    }
}
