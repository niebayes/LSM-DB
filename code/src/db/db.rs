mod db;
use config::Config;

pub struct DB {
    cfg: Config, // DB config.
}

impl DB {
    pub fn new(cfg: Config) -> DB {
        DB { cfg: cfg }
    }

    pub fn run(&mut self) {
        // repeatedly read user commands from the terminal.
        loop {
            match self.get_next_cmd() {
                Command::Put(key, val) => {}
                Command::Get(key) => {}
                Command::Range(start_key, end_key) => {}
                Command::Delete(key) => {}
                Command::Load(cmd_file) => {}
                Command::PrintStats => {}
            }
        }
    }

    fn get_next_cmd(&mut self) -> Command {}
}
