use super::bloom_filter::BloomFilter;
use super::iterator::TableKeyIterator;
use super::keys::{LookupKey, TableKey, TABLE_KEY_SIZE};
use crate::util::types::*;
use integer_encoding::*;
use std::mem;
use std::{cmp, io};

pub const BLOCK_SIZE: usize = 4 * 1024; // 4KB.
pub const KEYS_PER_BLOCK: usize = BLOCK_SIZE / TABLE_KEY_SIZE;

fn maybe_pad(bytes: &mut Vec<u8>) {
    if bytes.len() < BLOCK_SIZE {
        bytes.append(&mut vec![0; BLOCK_SIZE - bytes.len()]);
    }
}

pub fn table_keys_to_blocks(num_table_keys: usize) -> usize {
    let num_data_blocks = (num_table_keys + KEYS_PER_BLOCK) / KEYS_PER_BLOCK;
    num_data_blocks
}

pub struct DataBlock {
    pub table_keys: Vec<TableKey>,
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
        if self.max_table_key.is_none() {
            self.max_table_key = Some(table_key.clone());
        } else {
            self.max_table_key = Some(cmp::max(
                self.max_table_key.as_ref().unwrap().clone(),
                table_key.clone(),
            ));
        }
        self.table_keys.push(table_key);
    }

    pub fn fence_pointer(&self) -> TableKey {
        self.max_table_key.as_ref().unwrap().clone()
    }

    pub fn size(&self) -> usize {
        self.table_keys.len() * TABLE_KEY_SIZE
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(BLOCK_SIZE);
        for table_key in self.table_keys.iter() {
            bytes.append(&mut table_key.encode_to_bytes());
        }
        maybe_pad(&mut bytes);
        assert_eq!(bytes.len(), BLOCK_SIZE);
        bytes
    }

    // num_table_keys: #table keys in the data block.
    pub fn decode_from_bytes(bytes: &Vec<u8>, num_table_keys: usize) -> Result<Self, io::Error> {
        let mut data_block = DataBlock::new();
        for i in 0..num_table_keys {
            let offset = i * TABLE_KEY_SIZE;
            let table_key =
                TableKey::decode_from_bytes(&bytes[offset..offset + TABLE_KEY_SIZE].to_owned())?;
            data_block.add(table_key);
        }
        Ok(data_block)
    }

    pub fn iter(&self) -> DataBlockIterator {
        DataBlockIterator {
            table_keys: self.table_keys.clone(),
            cursor: -1,
        }
    }
}

pub struct DataBlockIterator {
    table_keys: Vec<TableKey>,
    cursor: isize,
}

impl TableKeyIterator for DataBlockIterator {
    fn seek(&mut self, lookup_key: &super::keys::LookupKey) {
        while let Some(table_key) = self.next() {
            if table_key >= lookup_key.as_table_key() {
                break;
            }
        }
    }

    fn next(&mut self) -> Option<TableKey> {
        self.cursor += 1;
        self.curr()
    }

    fn valid(&self) -> bool {
        self.cursor >= 0 && (self.cursor as usize) < self.table_keys.len()
    }

    fn curr(&self) -> Option<TableKey> {
        // warning: tolerate integer underflow.
        if let Some(table_key) = self.table_keys.get(self.cursor as usize) {
            Some(table_key.clone())
        } else {
            None
        }
    }
}

pub struct FilterBlock {
    bloom_filter: BloomFilter,
}

impl FilterBlock {
    pub fn new() -> Self {
        Self {
            bloom_filter: BloomFilter::new(),
        }
    }

    pub fn insert(&mut self, table_key: &TableKey) {
        self.bloom_filter.insert(table_key);
    }

    pub fn maybe_contain(&self, lookup_key: &LookupKey) -> bool {
        self.bloom_filter.maybe_contain(lookup_key)
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.bloom_filter.encode_to_bytes();
        maybe_pad(&mut bytes);
        assert_eq!(bytes.len(), BLOCK_SIZE);
        bytes
    }

    pub fn decode_from_bytes(bytes: &Vec<u8>) -> Self {
        Self {
            bloom_filter: BloomFilter::decode_from_bytes(bytes),
        }
    }
}

pub struct IndexBlock {
    fence_pointers: Vec<TableKey>,
}

impl IndexBlock {
    pub fn new() -> Self {
        Self {
            fence_pointers: Vec::new(),
        }
    }

    pub fn add(&mut self, fence_pointer: TableKey) {
        self.fence_pointers.push(fence_pointer);
    }

    /// returns Some(i) if the key might exist in the sstable.
    pub fn binary_search(&self, lookup_key: &LookupKey) -> Option<usize> {
        let mut lo = 0; // start of the search space.
        let mut len = self.fence_pointers.len(); // search space length.

        // loop inv: the search space is not empty.
        while len > 0 {
            let half = len / 2; // the length of the left half of the search space.
            let mid = lo + half;
            let fence_pointer = self.fence_pointers.get(mid).unwrap();

            // if adjacent sstables contain the same user key, only the left sstable might be target sstable.
            // so the lower-bound binary searching is applied here.
            if fence_pointer.user_key < lookup_key.user_key {
                // proceed searching in the right half.
                lo = mid + 1;
                len -= half + 1;
            } else {
                // proceed searching in the left half.
                len = half;
            }
        }

        // further check that this sstable maybe contain the target key.
        let fence_pointer = self.fence_pointers.get(lo).unwrap();
        if fence_pointer.user_key >= lookup_key.user_key {
            Some(lo)
        } else {
            None
        }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for fence_pointer in self.fence_pointers.iter() {
            bytes.append(&mut fence_pointer.encode_to_bytes());
        }
        maybe_pad(&mut bytes);
        assert_eq!(bytes.len(), BLOCK_SIZE);
        bytes
    }

    pub fn decode_from_bytes(bytes: &Vec<u8>, num_data_blocks: usize) -> Result<Self, io::Error> {
        let mut index_block = IndexBlock::new();
        for i in 0..num_data_blocks {
            let offset = i * TABLE_KEY_SIZE;
            // a fence pointer is literally the max table key of a data block.
            let fence_pointer =
                TableKey::decode_from_bytes(&bytes[offset..offset + TABLE_KEY_SIZE].to_owned())?;
            index_block.add(fence_pointer);
        }
        Ok(index_block)
    }
}

pub struct Footer {
    pub num_table_keys: usize,
    pub filter_block_offset: usize,
    pub index_block_offset: usize,
    pub min_table_key: TableKey,
    pub max_table_key: TableKey,
}

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
        bytes.write_fixedint(self.num_table_keys).unwrap();
        bytes.write_fixedint(self.filter_block_offset).unwrap();
        bytes.write_fixedint(self.index_block_offset).unwrap();
        bytes.append(&mut self.min_table_key.encode_to_bytes());
        bytes.append(&mut self.max_table_key.encode_to_bytes());
        maybe_pad(&mut bytes);
        assert_eq!(bytes.len(), BLOCK_SIZE);
        bytes
    }

    pub fn decode_from_bytes(bytes: &Vec<u8>) -> Result<Self, io::Error> {
        let mut reader = bytes.as_slice();

        let num_table_keys = reader.read_fixedint()?;
        let filter_block_offset = reader.read_fixedint()?;
        let index_block_offset = reader.read_fixedint()?;
        let offset = 3 * mem::size_of::<usize>();
        let min_table_key =
            TableKey::decode_from_bytes(&bytes[offset..offset + TABLE_KEY_SIZE].to_owned())?;
        let max_table_key = TableKey::decode_from_bytes(
            &bytes[offset + TABLE_KEY_SIZE..offset + 2 * TABLE_KEY_SIZE].to_owned(),
        )?;

        Ok(Self {
            num_table_keys,
            filter_block_offset,
            index_block_offset,
            min_table_key,
            max_table_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_block_encode_decode() {
        let mut data_block = DataBlock::new();
        let num_table_keys = 200;
        for i in 0..num_table_keys {
            let table_key = TableKey::identity(i);
            data_block.add(table_key);
        }

        let bytes = data_block.encode_to_bytes();
        let decoded_data_block =
            DataBlock::decode_from_bytes(&bytes, num_table_keys as usize).unwrap();

        assert_eq!(
            data_block.max_table_key.as_ref().unwrap().clone(),
            decoded_data_block.max_table_key.as_ref().unwrap().clone()
        );

        for i in 0..num_table_keys {
            assert_eq!(
                data_block.table_keys[i as usize],
                decoded_data_block.table_keys[i as usize]
            );
        }
    }

    #[test]
    fn filter_block_encode_decode() {
        let mut filter_block = FilterBlock::new();
        let num_table_keys = 500;
        for i in 0..num_table_keys {
            let table_key = TableKey::identity(i);
            filter_block.insert(&table_key);
        }

        let bytes1 = filter_block.encode_to_bytes();
        let bytes2 = FilterBlock::decode_from_bytes(&bytes1).encode_to_bytes();

        for i in 0..BLOCK_SIZE {
            assert_eq!(bytes1[i], bytes2[i]);
        }
    }

    #[test]
    fn index_block_encode_decode() {
        let mut index_block = IndexBlock::new();
        let num_table_keys: usize = 10;
        for i in 0..num_table_keys {
            index_block.add(TableKey::new(
                i as UserKey,
                i,
                WriteType::Put,
                i as UserValue,
            ));
        }
        let bytes = index_block.encode_to_bytes();
        let decoded_index_block = IndexBlock::decode_from_bytes(&bytes, num_table_keys).unwrap();

        assert_eq!(
            index_block.fence_pointers.len(),
            decoded_index_block.fence_pointers.len()
        );

        for i in 0..num_table_keys {
            assert_eq!(
                index_block.fence_pointers[i],
                decoded_index_block.fence_pointers[i]
            );
        }
    }
}
