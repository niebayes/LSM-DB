use crate::db::db::Db;
use crate::server::cmd::{print_help, Command};
use crate::util::types::{UserEntry, UserKey, UserValue};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::fs::File;
use std::io::{BufReader, Read};
use std::mem;
use std::path::Path;

/// key-value server.
pub struct Server {
    /// key-value database.
    db: Db,
    /// readline editor.
    editor: Editor<()>,
    /// history file path.
    history_path: String,
}

impl Server {
    pub fn new(db: Db) -> Server {
        // set a cmd_history file to store history commands.
        let history_path = format!(
            "{}/.cmd_history",
            std::env::current_dir().unwrap().display().to_string()
        );
        // create an editor for reading lines.
        let mut editor = Editor::<()>::new().unwrap();
        // attempt to load history from file ./.cmd_history if it exists.
        let _ = editor.load_history(&history_path);
        Server {
            db,
            editor,
            history_path,
        }
    }

    pub fn run(&mut self) {
        // print help options.
        print_help();

        // repeatedly read commands from the terminal.
        loop {
            let cmd = self.get_next_cmd();
            match cmd {
                // terminate the server if typed in quit command.
                Command::Quit => break,
                // print help options if requested.
                Command::Help => print_help(),
                // forward other commands to the database.
                _ => self.handle_cmd(cmd),
            }
        }
    }

    fn get_next_cmd(&mut self) -> Command {
        loop {
            static PROMPT: &str = "(lsm_db) ";
            match self.editor.readline(PROMPT) {
                Ok(line) => {
                    // skip empty lines.
                    if line.trim().len() == 0 {
                        continue;
                    }

                    // save the command as a history entry.
                    self.editor.add_history_entry(line.as_str());
                    self.editor.save_history(&self.history_path).unwrap();

                    // split the command line into tokens.
                    let tokens: Vec<&str> = line.split_whitespace().collect();
                    // construct a commmand from tokens.
                    if let Some(cmd) = Command::from_tokens(&tokens) {
                        return cmd;
                    } else {
                        println!("Unrecognized command");
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    // user pressed ctrl-c, prompt the user to type `quit` inorder to quit.
                    println!("Hint: type \"q\" or \"quit\" to exit");
                }
                Err(ReadlineError::Eof) => {
                    // user pressed ctrl-d, which is the equivalence of "quit" for our purposes
                    return Command::Quit;
                }
                Err(err) => {
                    panic!("Unexpected error: {:?}", err);
                }
            }
        }
    }

    fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Put(key, val) => {
                self.db.put(key, val);
            }
            Command::Get(key) => {
                if let Some(val) = self.db.get(key) {
                    // print the value.
                    println!("{}", val);
                }
            }
            Command::Range(start_key, end_key) => {
                let entries = self.db.range(start_key, end_key);
                if entries.is_empty() {
                    // print an empty line.
                    println!();
                } else {
                    // print each kv entry in the format key:value.
                    for entry in entries.iter() {
                        println!("{}:{}", entry.key, entry.val);
                    }
                }
            }
            Command::Delete(key) => {
                self.db.delete(key);
            }
            Command::Load(cmd_batch_file) => {
                // open the file.
                let file = File::open(Path::new(&cmd_batch_file)).unwrap();

                // read all file content into the buffer.
                let mut reader = BufReader::new(file);
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).unwrap();

                // read kv entries entry by entry.
                static ENTRY_SIZE: usize = mem::size_of::<UserEntry>();
                static KEY_SIZE: usize = mem::size_of::<UserKey>();
                assert_eq!(buf.len() % ENTRY_SIZE, 0);

                for i in (0..buf.len()).step_by(ENTRY_SIZE) {
                    let key_bytes = &buf[i..i + KEY_SIZE];
                    let val_bytes = &buf[i + KEY_SIZE..i + ENTRY_SIZE];
                    let key = unsafe { *(key_bytes.as_ptr()) } as UserKey;
                    let val = unsafe { *(val_bytes.as_ptr()) } as UserValue;

                    // insert the kv entry into the db.
                    self.db.put(key, val);
                }
            }
            Command::PrintStats => {
                println!("{}", self.db.stats());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::db::Config;

    // TODO: document each unit tests. Rename them properly.
    #[test]
    fn put_get_sequential() {
        let db = Db::new(Config::test());
        let mut server = Server::new(db);
        let num_table_keys = 1000;
        for i in 0..num_table_keys {
            let put = Command::Put(i, i);
            server.handle_cmd(put);
            server.handle_cmd(Command::PrintStats);
        }

        for i in 0..num_table_keys {
            let get = Command::Get(i);
            server.handle_cmd(get);
        }
    }
}
