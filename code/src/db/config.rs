pub struct Config {
    memtable_level: i32, // number of memtable levels.
    batch_write: bool,   // batch writes in memory if true.
}

impl Config {
    pub fn new() -> Config {
        Config {
            memtable_level: 1,
            batch_write: false,
        }
    }
}
