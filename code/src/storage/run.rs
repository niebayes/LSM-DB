use crate::storage::sstable::SSTable;

pub struct RunIterator {}

/// sorted run.
pub struct Run {
    sstables: Vec<SSTable>,
}
