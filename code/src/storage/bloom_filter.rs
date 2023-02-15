use crate::util::types::UserKey;

use integer_encoding::*;
use murmur3::murmur3_x86_128;
use std::collections::HashSet;
use xxhash_rust::xxh3::xxh3_128;

const M: usize = 10000;
const BYTE_VEC_SIZE: usize = M / 8;
const K: usize = 7;
const SEED: u32 = 0; // static seed.

fn user_key_to_bytes(user_key: UserKey) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.write_fixedint(user_key).unwrap();
    encoded
}

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
        let h1 = murmur3_x86_128(&mut key.clone(), SEED).unwrap() as usize % M;
        let h2 = xxh3_128(key) as usize % M;
        (h1 + k * h2) % M
    }

    pub fn insert(&mut self, user_key: UserKey) {
        // hashed indexes of all hash functions.
        let mut bit_indexes = HashSet::with_capacity(K);
        let key = user_key_to_bytes(user_key);
        let key = key.as_slice();
        for k in 0..K {
            bit_indexes.insert(BloomFilter::hash(key, k));
        }

        // set those bits to 1.
        for bit_index in bit_indexes.iter() {
            let byte_index = bit_index / 8;
            let byte = self.byte_vec.get_mut(byte_index).unwrap();
            // bit offset within a byte.
            let offset = bit_index % 8;
            // note, endianness is defined on the bytes not on the bits.
            // as long as the bit setting and bit checking follow the unified paradigm,
            // the bit manipulation is correct.
            *byte |= 1 << offset;
        }
    }

    /// return true if the filter maybe contain the key.
    /// return false if the filter definitely does not contain the key.
    pub fn maybe_contain(&self, user_key: UserKey) -> bool {
        // hashed indexes of all hash functions.
        let mut bit_indexes = HashSet::new();
        let key = user_key_to_bytes(user_key);
        let key = key.as_slice();
        for k in 0..K {
            bit_indexes.insert(BloomFilter::hash(key, k));
        }

        // check if all those bits are 1.
        for bit_index in bit_indexes.iter() {
            let byte_index = bit_index / 8;
            let byte = self.byte_vec.get(byte_index).unwrap();
            // bit offset within a byte.
            let offset = bit_index % 8;
            if (byte >> offset) & 0b1 == 0b0 {
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

#[cfg(test)]
mod tests {
    use crate::util::types::*;

    use super::*;

    /// randomly generate a set of keys to be inserted into the bloom filter.
    /// check `maybe_contain` returns true for these keys.
    #[test]
    fn insert_contain() {
        let mut filter = BloomFilter::new();

        let mut inserted = Vec::new();
        let num_keys = 1000;
        for _ in 0..num_keys {
            let rand_key = rand::random::<UserKey>();
            filter.insert(rand_key);
            inserted.push(rand_key);
        }

        for key in inserted.iter() {
            assert_eq!(filter.maybe_contain(*key), true);
        }
    }

    /// randomly generate a set of keys to be inserted into the bloom filter.
    /// compute the variance of the bits in the bit vector.
    /// check the variance is smaller than a specified tolerance.
    #[test]
    fn hash_randomness() {
        let mut filter = BloomFilter::new();

        let num_keys = 10000;
        for _ in 0..num_keys {
            let rand_key = rand::random::<UserKey>();
            filter.insert(rand_key);
        }

        // sum of all bits.
        let mut sum: u32 = 0;
        for byte_idx in 0..BYTE_VEC_SIZE {
            let byte = &filter.byte_vec[byte_idx];
            for bit_idx in 0..8 {
                let bit = (byte >> bit_idx) & 0b1;
                sum += bit as u32;
            }
        }
        let num_bits = (BYTE_VEC_SIZE * 8) as f32;
        let avg = sum as f32 / num_bits;

        let mut diff_square_sum = 0 as f32;
        for byte_idx in 0..BYTE_VEC_SIZE {
            let byte = &filter.byte_vec[byte_idx];
            for bit_idx in 0..8 {
                let bit = (byte >> bit_idx) & 0b1;
                let diff = bit as f32 - avg;
                diff_square_sum += diff * diff;
            }
        }
        let variance = diff_square_sum / num_bits;
        println!("variance = {}", variance);

        let tolerance = 0.001;
        assert!(variance <= tolerance);
    }

    /// randomly generate a set of keys A to be inserted into the bloom filter.
    /// randomly generate another set of keys B not in the bloom filter.
    /// count #trues returned by `maybe_contain` for these keys.
    /// check that #trues / B.len() = expected false positive rate.
    fn false_positive_rate_one() -> f32 {
        let mut filter = BloomFilter::new();

        let mut inserted = Vec::new();
        let num_keys = 1000;
        for _ in 0..num_keys {
            let rand_key = rand::random::<UserKey>();
            filter.insert(rand_key);
            inserted.push(rand_key);
        }

        let mut keys = Vec::new();
        while keys.len() < num_keys {
            let rand_key = rand::random::<UserKey>();
            let mut is_inserted = false;
            for key in inserted.iter() {
                if *key == rand_key {
                    is_inserted = true;
                }
            }

            let mut is_dup = false;
            for key in keys.iter() {
                if *key == rand_key {
                    is_dup = true;
                }
            }
            if !is_inserted && !is_dup {
                keys.push(rand_key);
            }
        }

        let mut true_count = 0;
        for key in keys.iter() {
            if filter.maybe_contain(*key) {
                true_count += 1;
            }
        }

        let false_positive_rate = true_count as f32 / keys.len() as f32;
        false_positive_rate
    }

    #[test]
    fn false_positive_rate_avg() {
        let num_tests = 100;
        let mut rate_sum = 0 as f32;
        for _ in 0..num_tests {
            rate_sum += false_positive_rate_one();
        }
        let rate_avg = rate_sum / num_tests as f32;
        let expected_rate = 0.01;
        let abs_diff = (rate_avg - expected_rate).abs();
        println!(
            "rate_avg = {}, expected_rate = {}, abs_diff = {}",
            rate_avg, expected_rate, abs_diff
        );

        let tolerance = 0.01;
        assert!(abs_diff <= tolerance);
    }
}
