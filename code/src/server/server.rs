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
            db: db,
            editor: editor,
            history_path: history_path,
        }
    }

    pub fn run(&mut self) {
        // print help options.
        print_help();
        // repeatedly read commands from the terminal and forward it to the db.
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
                    // skip empty line.
                    if line.trim().len() == 0 {
                        continue;
                    }

                    // save the command as a history entry.
                    self.editor.add_history_entry(line.as_str());
                    if let Err(_) = self.editor.save_history(&self.history_path) {
                        log::warn!("Failed to save history file");
                    }

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
                    log::error!("Unexpected error: {:?}", err);
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

                // read all bytes into the buffer.
                let mut reader = BufReader::new(file);
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).unwrap();

                // read kv entries entry by entry.
                static ENTRY_SIZE: usize = mem::size_of::<UserEntry>();
                static KEY_SIZE: usize = mem::size_of::<UserKey>();
                assert!(buf.len() % ENTRY_SIZE == 0);

                for i in (0..buf.len()).step_by(ENTRY_SIZE) {
                    let key_bytes = &buf[i..i + KEY_SIZE];
                    let val_bytes = &buf[i + KEY_SIZE..i + ENTRY_SIZE];
                    // FIXME: use converter module to do this conversion.
                    let key = key_bytes.as_ptr() as UserKey;
                    let val = val_bytes.as_ptr() as UserValue;

                    // insert the kv entry into the db.
                    self.db.put(key, val);
                }
            }
            Command::PrintStats => {
                self.db.print_stats();
            }
            _ => {}
        }
    }
}
