use crate::storage::sstable::SSTable;

/// sorted run.
pub struct Run {
    sstables: Vec<SSTable>,
}
