use crate::db::cmd::Command;
use crate::db::config::Config;

pub struct Db {
    /// database config.
    cfg: Config,
}

impl Db {
    pub fn new(cfg: Config) -> Db {
        Db { cfg: cfg }
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Put(key, val) => {}
            Command::Get(key) => {}
            Command::Range(start_key, end_key) => {}
            Command::Delete(key) => {}
            Command::Load(cmd_batch_file) => {}
            Command::PrintStats => {}
            _ => {}
        }
    }
}
