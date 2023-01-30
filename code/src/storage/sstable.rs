use super::block::*;
use super::bloom_filter::BloomFilter;
use super::iterator::TableKeyIterator;
use super::keys::*;
use crate::util::types::*;
use std::cmp;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::panic;
use std::rc::Rc;

/// in-memory sstable metadata.
pub struct SSTable {
    /// sstable file number from which the corresponding sstable file could be located.
    pub file_num: FileNum,
    /// sstable file size.
    pub file_size: usize,
    /// min table key stored in the sstable.
    pub min_table_key: TableKey,
    /// max table key stored in the sstable.
    pub max_table_key: TableKey,
}

impl SSTable {
    pub fn new(
        file_num: FileNum,
        file_size: usize,
        min_table_key: TableKey,
        max_table_key: TableKey,
    ) -> Self {
        Self {
            file_num,
            file_size,
            min_table_key,
            max_table_key,
        }
    }

    pub fn get(&self, lookup_key: &LookupKey) -> (Option<TableKey>, bool) {
        // TODO: binary search by fence pointers which are constructed from index block.
        if lookup_key.as_table_key() >= self.min_table_key
            && lookup_key.as_table_key() <= self.max_table_key
        {
            if let Ok(mut iter) = self.iter() {
                iter.seek(lookup_key);
                if iter.valid() {
                    let table_key = iter.curr().unwrap();
                    if table_key.user_key == lookup_key.user_key {
                        match table_key.write_type {
                            WriteType::Put => return (Some(table_key), false),
                            WriteType::Delete => return (Some(table_key), true),
                            other => panic!("Unexpected write type: {}", other as u8),
                        }
                    }
                }
            }
        }
        (None, false)
    }

    pub fn iter(&self) -> Result<SSTableIterator, ()> {
        match File::open(sstable_file_name(self.file_num)) {
            Ok(file) => {
                return Ok(SSTableIterator {
                    sstable_file_reader: BufReader::new(file),
                    curr_table_key: None,
                });
            }
            Err(err) => {
                log::error!(
                    "Failed to open sstable file {}: {}",
                    &sstable_file_name(self.file_num),
                    err
                );
                return Err(());
            }
        }
    }
}

/// an sstable's iterator.
/// keys in an sstable is clustered into a sequence of chunks.
/// each chunk contains keys with the same user key but with different sequence numbers,
/// and keys with lower sequence numbers are placed first.
pub struct SSTableIterator {
    /// a buffered reader for reading the sstable file.
    sstable_file_reader: BufReader<File>,
    curr_table_key: Option<TableKey>,
}

impl TableKeyIterator for SSTableIterator {
    fn seek(&mut self, lookup_key: &LookupKey) {
        while let Some(table_key) = self.next() {
            if table_key >= lookup_key.as_table_key() {
                break;
            }
        }
    }

    fn next(&mut self) -> Option<TableKey> {
        let mut buf = vec![0; TABLE_KEY_SIZE];
        if let Ok(_) = self.sstable_file_reader.read_exact(&mut buf) {
            if let Ok(table_key) = TableKey::decode_from_bytes(&buf) {
                self.curr_table_key = Some(table_key);
                return self.curr_table_key.clone();
            }
        }
        None
    }

    fn curr(&self) -> Option<TableKey> {
        self.curr_table_key.clone()
    }

    fn valid(&self) -> bool {
        self.curr_table_key.is_some()
    }
}

/// a writer for writing table keys into a sstable file.
struct SSTableWriter {
    file_num: FileNum,
    writer: BufWriter<File>,
    data_block: DataBlock,
    filter_block: FilterBlock,
    index_block: IndexBlock,
    num_table_keys: usize,
    min_table_key: Option<TableKey>,
    max_table_key: Option<TableKey>,
}

impl SSTableWriter {
    fn new(file_num: FileNum) -> Self {
        let file = File::open(sstable_file_name(file_num)).unwrap();
        SSTableWriter {
            file_num,
            writer: BufWriter::new(file),
            data_block: DataBlock::new(),
            filter_block: FilterBlock::new(),
            index_block: IndexBlock::new(),
            num_table_keys: 0,
            min_table_key: None,
            max_table_key: None,
        }
    }

    pub fn push(&mut self, table_key: TableKey) {
        self.min_table_key = Some(cmp::min(
            self.min_table_key.as_ref().unwrap().clone(),
            table_key.clone(),
        ));
        self.max_table_key = Some(cmp::max(
            self.max_table_key.as_ref().unwrap().clone(),
            table_key.clone(),
        ));

        self.data_block.add(table_key);
        self.num_table_keys += 1;

        if self.data_block.size() >= BLOCK_SIZE {
            self.flush_data_block();
        }
    }

    fn flush_data_block(&mut self) {
        self.writer
            .write(&self.data_block.encode_to_bytes())
            .unwrap();

        // add a bloom filter for the data block.
        self.filter_block
            .add(BloomFilter::from_data_block(&self.data_block));

        // add a fence pointer for the data block.
        self.index_block.add(self.data_block.fence_pointer());

        self.data_block.reset();
    }

    pub fn done(&mut self) -> SSTable {
        // flush other blocks.
        self.writer
            .write(&self.filter_block.encode_to_bytes())
            .unwrap();

        self.writer
            .write(&self.index_block.encode_to_bytes())
            .unwrap();

        let num_data_blocks = ((self.num_table_keys * TABLE_KEY_SIZE) + BLOCK_SIZE) / BLOCK_SIZE;
        let filter_block_offset = num_data_blocks * BLOCK_SIZE;
        let index_block_offset = filter_block_offset + BLOCK_SIZE;
        let footer = Footer::new(
            self.num_table_keys,
            filter_block_offset,
            index_block_offset,
            self.min_table_key.as_ref().unwrap().clone(),
            self.max_table_key.as_ref().unwrap().clone(),
        );
        self.writer.write(&footer.encode_to_bytes()).unwrap();

        // create an in-memory sstable filemeta.
        SSTable::new(
            self.file_num,
            self.file_size(),
            self.min_table_key.as_ref().unwrap().clone(),
            self.max_table_key.as_ref().unwrap().clone(),
        )
    }

    pub fn file_size(&self) -> usize {
        // file_size = #blocks * block size = (#data blocks + #filter block + #index block + #footer) * block size.
        let num_data_blocks = ((self.num_table_keys * TABLE_KEY_SIZE) + BLOCK_SIZE) / BLOCK_SIZE;
        (num_data_blocks + 3) * BLOCK_SIZE
    }
}

pub struct SSTableWriterBatch {
    sstable_writer: Option<SSTableWriter>,
    next_file_num: FileNum,
    outputs: Vec<Rc<SSTable>>,
    pub min_table_key: Option<TableKey>,
    pub max_table_key: Option<TableKey>,
}

/// receives table keys and write them into a batch of sstable files.
impl SSTableWriterBatch {
    pub fn new(next_file_num: FileNum) -> Self {
        Self {
            sstable_writer: None,
            next_file_num,
            outputs: Vec::new(),
            min_table_key: None,
            max_table_key: None,
        }
    }

    fn alloc_file_num(&mut self) -> FileNum {
        let file_num = self.next_file_num;
        self.next_file_num += 1;
        file_num
    }

    /// push a table key into the active sstable writer.
    pub fn push(&mut self, table_key: TableKey) {
        if self.sstable_writer.is_none() {
            let file_num = self.alloc_file_num();
            self.sstable_writer = Some(SSTableWriter::new(file_num));
        }

        self.sstable_writer.as_mut().unwrap().push(table_key);
    }

    /// harness an sstable.
    // TODO: rewrite harness logic in db.
    pub fn harness(&mut self) {
        self.outputs
            .push(Rc::new(self.sstable_writer.as_mut().unwrap().done()));
        // reset.
        self.sstable_writer = None;
    }

    pub fn done(&mut self) -> (Vec<Rc<SSTable>>, FileNum) {
        if self.sstable_writer.is_some() {
            self.harness();
        }

        let mut min_table_key = self.outputs.first().unwrap().min_table_key.clone();
        let mut max_table_key = self.outputs.first().unwrap().max_table_key.clone();

        for i in 1..self.outputs.len() {
            min_table_key = cmp::min(
                min_table_key.clone(),
                self.outputs.get(i).unwrap().min_table_key.clone(),
            );
            max_table_key = cmp::max(
                max_table_key.clone(),
                self.outputs.get(i).unwrap().max_table_key.clone(),
            );
        }

        self.min_table_key = Some(min_table_key);
        self.max_table_key = Some(max_table_key);

        (self.outputs.clone(), self.next_file_num)
    }
}

pub struct SSTableStats {
    pub file_num: FileNum,
    all_table_keys: Vec<String>,
    visible_table_keys: Vec<String>,
    min_table_key: TableKey,
    max_table_key: TableKey,
}

impl Display for SSTableStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut stats = String::new();

        stats += &format!("min table key: {}\n", self.min_table_key);
        stats += &format!("max table key: {}\n", self.max_table_key);

        stats += &format!("all table keys:\n\tcount: {}", self.all_table_keys.len());
        for table_key in self.all_table_keys.iter() {
            stats += &format!("\n\t{}", table_key);
        }

        stats += &format!(
            "visible table keys:\n\tcount: {}",
            self.visible_table_keys.len()
        );
        for table_key in self.visible_table_keys.iter() {
            stats += &format!("\n\t{}", table_key);
        }

        write!(f, "{}", stats)
    }
}

impl SSTable {
    pub fn stats(&self) -> SSTableStats {
        let mut all_table_keys = Vec::new();
        let mut visible_table_keys = Vec::new();

        let mut iter = self.iter().unwrap();
        let mut last_user_key = None;
        while let Some(table_key) = iter.next() {
            if last_user_key.is_none() || last_user_key.unwrap() == table_key.user_key {
                last_user_key = Some(table_key.user_key);
                visible_table_keys.push(format!("{}", table_key));
            }
            all_table_keys.push(format!("{}", table_key));
        }

        SSTableStats {
            file_num: self.file_num,
            all_table_keys,
            visible_table_keys,
            min_table_key: self.min_table_key.clone(),
            max_table_key: self.max_table_key.clone(),
        }
    }
}

fn sstable_file_name(file_num: FileNum) -> String {
    format!("sstables/sstable_file_{}", file_num)
}
