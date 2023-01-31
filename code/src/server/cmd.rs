use crate::util::types::{UserKey, UserValue};
use std::path::Path;

// commands provided by the server.
pub enum Command {
    Put(UserKey, UserValue), // upsert a kv pair to the db.
    Get(UserKey),            // fetch the associated value of the given key if the key exists.
    Range(UserKey, UserKey), // fetch values in the key range [start_key, end_key).
    Delete(UserKey),         // remove the kv pair associated with the given key if the key exists.
    Load(String),            // upsert kv pairs stored in the file to the db.
    PrintStats,              // print the key range in all levels of the lsm tree.
    Quit,                    // terminate the session.
    Help,                    // print help options.
}

impl Command {
    // construct a cmd from tokens parsed from command line input.
    pub fn from_tokens(tokens: &Vec<&str>) -> Option<Command> {
        match tokens[0] {
            "p" | "put" => {
                if tokens.len() == 3 && is_valid_key(tokens[1]) && is_valid_value(tokens[2]) {
                    return Some(Command::Put(
                        tokens[1].parse().unwrap(),
                        tokens[2].parse().unwrap(),
                    ));
                }
                None
            }
            "g" | "get" => {
                if tokens.len() == 2 && is_valid_key(tokens[1]) {
                    return Some(Command::Get(tokens[1].parse().unwrap()));
                }
                None
            }
            "r" | "range" => {
                if tokens.len() == 3 && is_valid_key(tokens[1]) && is_valid_key(tokens[2]) {
                    let start_key = tokens[1].parse().unwrap();
                    let end_key = tokens[2].parse().unwrap();
                    // ensure the range is valid.
                    if start_key <= end_key {
                        return Some(Command::Range(start_key, end_key));
                    }
                }
                None
            }
            "d" | "delete" => {
                if tokens.len() == 2 && is_valid_key(tokens[1]) {
                    return Some(Command::Get(tokens[1].parse().unwrap()));
                }
                None
            }
            "l" | "load" => {
                if tokens.len() == 2 && Path::new(tokens[1]).is_file() {
                    return Some(Command::Load(tokens[1].to_owned()));
                }
                None
            }
            "s" | "print" => {
                if tokens.len() == 1 {
                    return Some(Command::PrintStats);
                }
                None
            }
            "q" | "quit" => {
                if tokens.len() == 1 {
                    return Some(Command::Quit);
                }
                None
            }
            "h" | "help" => {
                if tokens.len() == 1 {
                    return Some(Command::Help);
                }
                None
            }
            _ => None,
        }
    }
}

/// return true if the int_str str can be casted to the key type without error.
fn is_valid_key(int_str: &str) -> bool {
    int_str.parse::<UserKey>().is_ok()
}

/// return true if the int_str str can be casted to the value type without error.
fn is_valid_value(int_str: &str) -> bool {
    int_str.parse::<UserValue>().is_ok()
}

/// print help options.
pub fn print_help() {
    static PUT: &str = "p | put <key> <value>";
    static GET: &str = "g | get <key>";
    static RANGE: &str = "r | range <start_key> <end_key>";
    static DELETE: &str = "d | delete <key>";
    static LOAD: &str = "l | load <command_batch_file>";
    static PRINT_STATS: &str = "s | print";
    static QUIT: &str = "q | quit";
    static HELP: &str = "h | help";

    print!(
        "  Usage:\n\t{:<35}{}\n\t{:<35}{}\n\t{:<35}{}\n\t{:<35}{}\n\t{:<35}{}\n\t{:<35}{}\n\t{:<35}{}\n\t{:<35}{}\n",
        PUT, "upsert a key-value pair to the database",
        GET, "fetch the associated value of the given key",
        RANGE, "fetch values associated with the keys in the key range [start_key, end_key)",
        DELETE, "delete the key-value pair associated with the given key",
        LOAD, "upsert a sequence of key-value pairs stored in the file to the database",
        PRINT_STATS, "print the current state of the database",
        QUIT, "terminate the session",
        HELP, "print this help message"
    );
}
