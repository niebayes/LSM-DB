use std::cmp::Ordering;

use super::keys::{LookupKey, TableKey};

// note, to use a table key iterator, you must first call `next` once to init the iterator.
// then you must use the iterator with such pattern:
// if iter.valid() {
//     let table_key = iter.curr().unwrap();
//     do something ...
//     iter.next();
// }
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
        match (self.curr(), other.curr()) {
            // for two keys [0,0], [0, 100], partial_cmp of TableKey will return Ordering::Greater since
            // we want to keys with higher sequence numbers to be placed first.
            // however, the binary heap in rust is a max-heap which makes the key [0,0] swims up since the comparison result is
            // Ordering::Greater.
            // hence, we need to reverse the order so that the key [0,100] swims up.
            (Some(head), Some(other_head)) => match head.partial_cmp(&other_head).unwrap() {
                Ordering::Less => Some(Ordering::Greater),
                Ordering::Equal => Some(Ordering::Equal),
                Ordering::Greater => Some(Ordering::Less),
            },
            (Some(_), None) => Some(Ordering::Greater),
            (None, Some(_)) => Some(Ordering::Less),
            (None, None) => Some(Ordering::Equal),
        }
    }
}

impl<'a> Ord for TableKeyIteratorType<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.curr(), other.curr()) {
            (Some(head), Some(other_head)) => match head.cmp(&other_head) {
                Ordering::Less => Ordering::Greater,
                Ordering::Equal => Ordering::Equal,
                Ordering::Greater => Ordering::Less,
            },
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sstable::SSTableWriter;
    use crate::util::types::*;
    use std::collections::BinaryHeap;
    use std::fs::{create_dir, remove_dir_all};

    /// insert a sequence of keys into an sstable.
    /// insert another sequence of keys into another sstable but with some delete keys.
    /// create a binary heap to maintain the iterators of the two sstables.
    /// emit all keys and check each key is greater than or equal to the last emitted one.
    #[test]
    fn heap_property() {
        let _ = create_dir("./sstables");

        let num_table_keys: i32 = 963;
        let mut heap: BinaryHeap<TableKeyIteratorType> =
            BinaryHeap::with_capacity((num_table_keys * 2) as usize);

        // sstable 1.
        let file_num = 42;
        let mut writer = SSTableWriter::new(file_num);

        for i in 0..num_table_keys {
            let table_key = TableKey::new(i, i as usize, WriteType::Put, i);
            writer.push(table_key);
        }
        let sstable = writer.done();

        let mut iter = Box::new(sstable.iter().unwrap());
        iter.next();
        heap.push(iter);

        // sstable 2.
        let file_num = file_num + 1;
        let mut writer = SSTableWriter::new(file_num);

        let num_deletes = 200;
        for i in 0..num_deletes {
            let table_key = TableKey::new(i, (i + num_table_keys) as usize, WriteType::Delete, i);
            writer.push(table_key);
        }
        for i in num_deletes..num_table_keys {
            let i = i + num_table_keys;
            let table_key = TableKey::new(i, i as usize, WriteType::Put, i);
            writer.push(table_key);
        }
        let sstable = writer.done();

        let mut iter = Box::new(sstable.iter().unwrap());
        iter.next();
        heap.push(iter);

        let mut visible_cnt = 0;
        let mut last_user_key = None;
        while let Some(mut iter) = heap.pop() {
            if iter.valid() {
                let table_key = iter.curr().unwrap();
                if last_user_key.is_none() || last_user_key.unwrap() != table_key.user_key {
                    last_user_key = Some(table_key.user_key);
                    match table_key.write_type {
                        WriteType::Put => {
                            println!("{}", &table_key);
                            visible_cnt += 1;
                        }
                        _ => {}
                    }
                }

                iter.next();
                heap.push(iter);
            }
        }

        println!("visible_cnt = {}", visible_cnt);
        assert_eq!(visible_cnt, num_table_keys * 2 - num_deletes * 2);

        let _ = remove_dir_all("./sstables");
    }
}
