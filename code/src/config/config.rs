const MB: u32 = 4096;

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
    pub max_lsm_tree_level: usize,
    /// fanout = current level capacity / previous level capacity.
    pub fanout: usize,
    /// memtable capacity in bytes.
    pub memtable_capacity: usize,
    /// sstable capacity in bytes.
    pub sstable_capacity: usize,
    /// level 0 capacity in number of sstables.
    pub level_zero_capacity: usize,
    /// major compaction strategy.
    pub major_compaction_strategy: MajorCompactionStrategy,
    /// batch writes in memory before written to memtable if true.
    pub batch_write: bool,
}

impl Default for Config {
    /// create a default config.
    fn default() -> Self {
        Self {
            max_lsm_tree_level: 2,
            fanout: 10,
            memtable_capacity: 4 * MB as usize,
            sstable_capacity: 16 * MB as usize,
            level_zero_capacity: 4,
            major_compaction_strategy: MajorCompactionStrategy::Leveled,
            batch_write: false,
        }
    }
}
