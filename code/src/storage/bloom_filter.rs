use super::keys::{LookupKey, TableKey};
use murmur3::murmur3_x86_128;
use std::collections::HashSet;
use xxhash_rust::xxh3::xxh3_128;

const M: usize = 10000;
const BYTE_VEC_SIZE: usize = M / 8;
const K: usize = 7;
const SEED: u32 = 0; // static seed.

pub struct BloomFilter {
    /// byte array of fixed-size floor(M/8).
    /// the M is chosen such that there's no remainder and hence it's not necessary to apply flooring.
    byte_vec: [u8; BYTE_VEC_SIZE],
}

impl BloomFilter {
    pub fn new() -> Self {
        Self {
            byte_vec: [0; BYTE_VEC_SIZE],
        }
    }

    /// hash the key using double-hashing:
    /// h(key) = (h1(key) + k * h2(key)) % M.
    /// where h1 is murmur3, h2 is xxhash, and 0 <= k < K.
    fn hash(key: &[u8], k: usize) -> usize {
        ((murmur3_x86_128(&mut key.clone(), SEED).unwrap() + k as u128 * xxh3_128(key)) as usize)
            % M
    }

    pub fn insert(&mut self, table_key: &TableKey) {
        // hashed indexes of all hash functions.
        let mut bit_indexes = HashSet::new();
        let key = table_key.as_lookup_key().encode_to_bytes();
        let key = key.as_slice();
        for k in 0..K {
            bit_indexes.insert(BloomFilter::hash(key, k));
        }

        // set those bits to 1.
        for bit_index in bit_indexes.iter() {
            let byte_index = bit_index / 8;
            // bit offset within a byte.
            let offset = bit_index % 8;
            let byte = self.byte_vec.get_mut(byte_index).unwrap();
            *byte |= 1 << offset;
        }
    }

    /// return true if the filter maybe contain the key.
    /// return false if the filter definitely does not contain the key.
    pub fn maybe_contain(&self, lookup_key: &LookupKey) -> bool {
        // hashed indexes of all hash functions.
        let mut bit_indexes = HashSet::new();
        let key = lookup_key.encode_to_bytes();
        let key = key.as_slice();
        for k in 0..K {
            bit_indexes.insert(BloomFilter::hash(key, k));
        }

        // check if all those bits are 1.
        for bit_index in bit_indexes.iter() {
            let byte_index = bit_index / 8;
            // bit offset within a byte.
            let offset = bit_index % 8;
            let byte = self.byte_vec.get(byte_index).unwrap();
            if (*byte & (1 << offset)) == 0 {
                return false;
            }
        }

        true
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        self.byte_vec.to_vec()
    }

    pub fn decode_from_bytes(bytes: &Vec<u8>) -> Self {
        let mut byte_vec = [0; BYTE_VEC_SIZE];
        for i in 0..BYTE_VEC_SIZE {
            byte_vec[i] = *bytes.get(i).unwrap();
        }
        Self { byte_vec }
    }
}

// TODO: add unit testing for bloom filter.
