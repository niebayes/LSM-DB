use super::bloom_filter::BloomFilter;
use super::keys::{TableKey, TABLE_KEY_SIZE};
use integer_encoding::*;
use std::cmp;
use std::mem;

pub const BLOCK_SIZE: usize = 4 * 1024;

fn maybe_pad(bytes: &mut Vec<u8>) {
    if bytes.len() < BLOCK_SIZE {
        bytes.append(&mut vec![0; BLOCK_SIZE - bytes.len()]);
    }
}

pub struct DataBlock {
    table_keys: Vec<TableKey>,
    max_table_key: Option<TableKey>,
}

impl DataBlock {
    pub fn new() -> Self {
        Self {
            table_keys: Vec::new(),
            max_table_key: None,
        }
    }

    pub fn add(&mut self, table_key: TableKey) {
        self.max_table_key = Some(cmp::max(self.max_table_key.unwrap(), table_key.clone()));
        self.table_keys.push(table_key);
    }

    pub fn fence_pointer(&self) -> TableKey {
        self.max_table_key.unwrap().clone()
    }

    pub fn size(&self) -> usize {
        self.table_keys.len() * TABLE_KEY_SIZE
    }

    pub fn reset(&mut self) {
        self.table_keys.clear();
        self.max_table_key = None;
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for table_key in self.table_keys {
            bytes.append(&mut table_key.encode_to_bytes());
        }
        maybe_pad(&mut bytes);
        bytes
    }
}

pub struct FilterBlock {
    bloom_filters: Vec<BloomFilter>,
}
// TODO: replace with the correct size.
const BLOOM_FILTER_SIZE: usize = 0;

impl FilterBlock {
    pub fn new() -> Self {
        Self {
            bloom_filters: Vec::new(),
        }
    }

    pub fn add(&mut self, bloom_filter: BloomFilter) {
        self.bloom_filters.push(bloom_filter);
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        // TODO.
        let mut bytes = Vec::new();
        maybe_pad(&mut bytes);
        bytes
    }
}

pub struct IndexBlock {
    fence_pointers: Vec<TableKey>,
}
const FENCE_POINTER_SIZE: usize = TABLE_KEY_SIZE;

impl IndexBlock {
    pub fn new() -> Self {
        Self {
            fence_pointers: Vec::new(),
        }
    }

    pub fn add(&mut self, fence_pointer: TableKey) {
        self.fence_pointers.push(fence_pointer);
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for fence_pointer in self.fence_pointers {
            bytes.append(&mut fence_pointer.encode_to_bytes());
        }
        maybe_pad(&mut bytes);
        bytes
    }
}

pub struct Footer {
    num_table_keys: usize,
    filter_block_offset: usize,
    index_block_offset: usize,
    min_table_key: TableKey,
    max_table_key: TableKey,
}
const FOOTER_SIZE: usize = mem::size_of::<usize>() + 2 * TABLE_KEY_SIZE;

impl Footer {
    pub fn new(
        num_table_keys: usize,
        filter_block_offset: usize,
        index_block_offset: usize,
        min_table_key: TableKey,
        max_table_key: TableKey,
    ) -> Self {
        Self {
            num_table_keys,
            filter_block_offset,
            index_block_offset,
            min_table_key,
            max_table_key,
        }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.write_varint(self.num_table_keys).unwrap();
        bytes.write_varint(self.filter_block_offset).unwrap();
        bytes.write_varint(self.index_block_offset).unwrap();
        bytes.append(&mut self.min_table_key.encode_to_bytes());
        bytes.append(&mut self.max_table_key.encode_to_bytes());
        maybe_pad(&mut bytes);
        bytes
    }
}
