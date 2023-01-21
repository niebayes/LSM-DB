use crate::storage::keys::TableKey;
use crate::util::types::*;

pub trait Iterator {
    fn seek(&mut self, table_key: &TableKey) -> Option<&TableKey>;
    fn next(&mut self) -> Option<&TableKey>;
}
