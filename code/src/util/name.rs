/// directory consts.
const WRITE_LOG_DIR: &str = "write_log";
const DB_LOG_DIR: &str = "db_log";
const LEVEL_LOG_DIR: &str = "level_log";
const SSTABLE_DIR: &str = "sstable";
const MANIFEST_DIR: &str = "manifest";

/// name prefixes consts.
const WRITE_LOG_FILE_NAME_PREFIX: &str = "write_log_file_";
const DB_LOG_FILE_NAME_PREFIX: &str = "db_log_file_";
const LEVEL_LOG_FILE_NAME_PREFIX: &str = "level_log_file_";
const SSTABLE_FILE_NAME_PREFIX: &str = "sstable_file_";
const MANIFEST_FILE_NAME_PREFIX: &str = "manifest_file_";

/// functions to construct file names for different file types.
pub fn write_log_file_name(file_num: u64) -> String {
    return WRITE_LOG_DIR.to_owned() + WRITE_LOG_FILE_NAME_PREFIX + file_num.to_string().as_str();
}

pub fn db_log_file_name(file_num: u64) -> String {
    return DB_LOG_DIR.to_owned() + DB_LOG_FILE_NAME_PREFIX + file_num.to_string().as_str();
}

pub fn level_log_file_name(file_num: u64) -> String {
    return LEVEL_LOG_DIR.to_owned() + LEVEL_LOG_FILE_NAME_PREFIX + file_num.to_string().as_str();
}

pub fn sstable_file_name(file_num: u64) -> String {
    return SSTABLE_DIR.to_owned() + SSTABLE_FILE_NAME_PREFIX + file_num.to_string().as_str();
}

pub fn manifest_file_name(file_num: u64) -> String {
    return MANIFEST_DIR.to_owned() + MANIFEST_FILE_NAME_PREFIX + file_num.to_string().as_str();
}
