use super::block::*;
use super::bloom_filter::BloomFilter;
use super::iterator::TableKeyIterator;
use super::keys::*;
use crate::db::db::FileNumDispatcher;
use crate::util::types::*;
use std::cmp;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::panic;
use std::rc::Rc;

/// sstable.
/// it's literally an in-memory sstable wrapper which provides read and write interfaces for an on-disk sstable file.
/// it also stores file metadata.
pub struct SSTable {
    /// sstable file number from which the corresponding sstable file could be located.
    file_num: FileNum,
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
        self.min_table_key = Some(cmp::min(self.min_table_key.unwrap(), table_key.clone()));
        self.max_table_key = Some(cmp::max(self.max_table_key.unwrap(), table_key.clone()));

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

    pub fn done(&self) -> SSTable {
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
            self.min_table_key.unwrap(),
            self.max_table_key.unwrap(),
        );

        self.writer.write(&footer.encode_to_bytes()).unwrap();

        // create an in-memory sstable filemeta.
        SSTable::new(
            self.file_num,
            self.file_size(),
            self.min_table_key.unwrap(),
            self.max_table_key.unwrap(),
        )
    }

    pub fn file_size(&self) -> usize {
        // file_size = #blocks * block size = (#data blocks + #filter block + #index block + #footer) * block size.
        let num_data_blocks = ((self.num_table_keys * TABLE_KEY_SIZE) + BLOCK_SIZE) / BLOCK_SIZE;
        (num_data_blocks + 3) * BLOCK_SIZE
    }
}

pub struct SSTableWriterBatch {
    file_num_dispatcher: Rc<FileNumDispatcher>,
    pub sstable_writer: Option<SSTableWriter>,
    outputs: Vec<Rc<SSTable>>,
}

/// receives table keys and write them into a batch of sstable files.
impl SSTableWriterBatch {
    pub fn new(file_num_dispatcher: Rc<FileNumDispatcher>) -> Self {
        Self {
            file_num_dispatcher,
            sstable_writer: None,
            outputs: Vec::new(),
        }
    }

    /// push a table key into the active sstable writer.
    pub fn push(&mut self, table_key: TableKey) {
        if self.sstable_writer.is_none() {
            let file_num = self.file_num_dispatcher.alloc_file_num();
            self.sstable_writer = Some(SSTableWriter::new(file_num));
        }

        self.sstable_writer.unwrap().push(table_key);
    }

    /// harness an sstable.
    // TODO: rewrite harness logic in db.
    pub fn harness(&mut self) {
        self.outputs
            .push(Rc::new(self.sstable_writer.unwrap().done()));
        // reset.
        self.sstable_writer = None;
    }

    pub fn done(&mut self) -> Vec<Rc<SSTable>> {
        if self.sstable_writer.is_some() {
            self.harness();
        }
        self.outputs
    }
}

fn sstable_file_name(file_num: FileNum) -> String {
    format!("sstables/sstable_file_{}", file_num)
}
