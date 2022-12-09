mod db;

use types::{KeyType, ValueType};

// commands provided by the domain specific language.
pub enum Command {
    Put(KeyType, ValueType), // insert a kv pair into the db. Old value will be replaced.
    Get(KeyType),            // fetch the corresponding value of the given key if the key exists.
    Range(KeyType, KeyType), // fetch values corresponding to the given range of keys [start_key, end_key).
    Delete(KeyType),         // remove the kv pair with the key equal to the given key.
    Load(String),            // load commands from a file from the given file path.
    PrintStats, // print the current state of the db including the in-mem states and the on-disk states.
}

impl Command {
    // construct a cmd from tokens parsed from command line input.
    pub fn from_cli_tokens(tokens: &Vec<&str>) -> Command {}
}
