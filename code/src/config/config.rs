const MB: u32 = 4096;

/// in-memory indexing data structure.
pub enum InMemoryIndex {
    /// skip list.
    SkipList,
    /// b+ tree.
    BPlusTree,
    /// adaptive radix tree.
    AdaptiveRadixTree,
}

/// major compaction strategies.
/// major compaction strategy is defined by the max number of sorted runs in a level.
/// if the max number of sorted runs in a level is limited to 1, then this level adopts
/// the leveled compaction.
/// if otherwise the max number of sorted runs in a level is greater than 1, then this level
/// adopts the tiered compaction.
/// despite the compaction strategy is associated with one level not the entire lsm tree,
/// this enum describes the overall major compaction strategy of all levels.
pub enum MajorCompactionStrategy {
    /// all levels adopt the leveled compaction strategy.
    Leveled,
    /// all levels adopt the tiered compaction strategy.
    Tiered,
    /// some levels adopt the leveld compaction strategy while others adopt the tiered compaction strategy.
    Hybrid,
}

/// database configurations.
pub struct Config {
    /// max lsm tree level.
    max_lsm_tree_level: i32,
    /// fanout = current level capacity / previous level capacity.
    fanout: i32,
    /// memtable capacity in bytes.
    memtable_capacity: u32,
    /// sstable capacity in bytes.
    sstable_capacity: u32,
    /// level 0 capacity in number of sstables.
    level_zero_capacity: u32,
    /// in-memory indexing data structure.
    in_mem_index: InMemoryIndex,
    /// major compaction strategy.
    major_compaction_strategy: MajorCompactionStrategy,
    /// batch writes in memory before written to memtable if true.
    batch_write: bool,
}

impl Default for Config {
    /// create a default config.
    fn default() -> Self {
        Self {
            max_lsm_tree_level: 2,
            fanout: 10,
            memtable_capacity: 4 * MB,
            sstable_capacity: 16 * MB,
            level_zero_capacity: 4,
            in_mem_index: InMemoryIndex::SkipList,
            major_compaction_strategy: MajorCompactionStrategy::Leveled,
            batch_write: false,
        }
    }
}
