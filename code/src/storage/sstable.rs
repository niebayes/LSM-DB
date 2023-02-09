use super::block::*;
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
        if lookup_key.user_key >= self.min_table_key.user_key
            && lookup_key.user_key <= self.max_table_key.user_key
        {
            let mut iter = self.iter().unwrap();
            iter.seek(lookup_key);
            if let Some(table_key) = iter.curr() {
                if table_key.user_key == lookup_key.user_key {
                    match table_key.write_type {
                        WriteType::Put => return (Some(table_key), false),
                        WriteType::Delete => return (Some(table_key), true),
                        other => panic!("Unexpected write type: {}", other as u8),
                    }
                }
            }
        }
        (None, false)
    }

    pub fn iter(&self) -> Result<SSTableIterator, ()> {
        let reader = SSTableReader::new(self.file_num);
        Ok(SSTableIterator {
            reader,
            data_block_iter: None,
        })
    }
}

pub struct SSTableIterator {
    reader: SSTableReader,
    data_block_iter: Option<DataBlockIterator>,
}

impl TableKeyIterator for SSTableIterator {
    fn seek(&mut self, lookup_key: &LookupKey) {
        // if the key definitely not in the sstable, terminates searching.
        // FIXME: correct the bloom filter implementation.
        // if !self.reader.filter_block.maybe_contain(lookup_key) {
        //     return;
        // }

        // binary search the lookup key by fence pointers.
        if let Some(data_block_idx) = self.reader.index_block.binary_search(lookup_key) {
            self.reader.advance_to(data_block_idx);
            self.reader.next();
            self.data_block_iter = Some(self.reader.data_block.as_ref().unwrap().iter());
            self.data_block_iter.as_mut().unwrap().seek(lookup_key);
        }
    }

    fn next(&mut self) -> Option<TableKey> {
        if self.data_block_iter.is_some() {
            if let Some(table_key) = self.data_block_iter.as_mut().unwrap().next() {
                // this data block is not exhausted.
                return Some(table_key);
            }
        }
        // reach here if either the data block iter is some but exhausted,
        // or the data block iter is none which could only happen on the init.

        self.reader.next();
        if self.reader.done() {
            // all data blocks are read over.
            return None;
        }
        // successfully read the next data block.
        self.data_block_iter = Some(self.reader.data_block.as_ref().unwrap().iter());
        // this next must succeed, i.e. some table key must be returned.
        self.data_block_iter.as_mut().unwrap().next()
    }

    fn curr(&self) -> Option<TableKey> {
        if self.data_block_iter.is_some() {
            self.data_block_iter.as_ref().unwrap().curr()
        } else {
            None
        }
    }

    fn valid(&self) -> bool {
        self.data_block_iter.is_some() && self.data_block_iter.as_ref().unwrap().valid()
    }
}

/// a reader for reading an sstable file.
struct SSTableReader {
    reader: BufReader<File>,
    data_block: Option<DataBlock>,
    filter_block: FilterBlock,
    index_block: IndexBlock,
    total_num_table_keys: usize,
    next_data_block_idx: usize,
    num_data_blocks: usize,
}

impl SSTableReader {
    pub fn new(file_num: FileNum) -> Self {
        let file = File::open(sstable_file_name(file_num)).unwrap();
        let file_size = file.metadata().unwrap().len();
        assert_eq!(file_size as usize % BLOCK_SIZE, 0);

        let mut reader = BufReader::new(file);

        // read the footer.
        let footer_offset = file_size as usize - BLOCK_SIZE;
        reader.seek_relative(footer_offset as i64).unwrap();
        let mut buf = make_block_buf();
        reader.read_exact(&mut buf).unwrap();
        let footer = Footer::decode_from_bytes(&buf).unwrap();

        // reset the seek cursor and read the filter block.
        reader
            .seek_relative(-((footer_offset + BLOCK_SIZE) as i64))
            .unwrap();
        buf = make_block_buf();
        reader
            .seek_relative(footer.filter_block_offset as i64)
            .unwrap();
        reader.read_exact(&mut buf).unwrap();
        let filter_block = FilterBlock::decode_from_bytes(&buf);

        // reset the seek cursor and read the index block.
        reader
            .seek_relative(-((footer.filter_block_offset + BLOCK_SIZE) as i64))
            .unwrap();
        buf = make_block_buf();
        reader
            .seek_relative(footer.index_block_offset as i64)
            .unwrap();
        reader.read_exact(&mut buf).unwrap();
        let num_data_blocks = table_keys_to_blocks(footer.num_table_keys);
        let index_block = IndexBlock::decode_from_bytes(&buf, num_data_blocks).unwrap();

        // reset the seek cursor to prepare for reading data blocks.
        reader
            .seek_relative(-((footer.index_block_offset + BLOCK_SIZE) as i64))
            .unwrap();

        Self {
            reader,
            data_block: None,
            filter_block,
            index_block,
            total_num_table_keys: footer.num_table_keys,
            next_data_block_idx: 0,
            num_data_blocks,
        }
    }

    /// advance to the next data block if any.
    /// return true if the advancing is successful.
    fn next(&mut self) {
        if self.done() {
            return;
        }

        // read the next data block into the buffer.
        let block_offset = self.next_data_block_idx * BLOCK_SIZE;
        self.reader.seek_relative(block_offset as i64).unwrap();
        let mut buf = make_block_buf();
        self.reader.read_exact(&mut buf).unwrap();
        // reset cursor to the file start.
        self.reader
            .seek_relative(-((block_offset + BLOCK_SIZE) as i64))
            .unwrap();

        // #table keys in the next data block.
        let remaining_num_table_keys =
            self.total_num_table_keys - KEYS_PER_BLOCK * self.next_data_block_idx;
        let num_table_keys = cmp::min(KEYS_PER_BLOCK, remaining_num_table_keys);

        let data_block = DataBlock::decode_from_bytes(&buf, num_table_keys).unwrap();
        self.data_block = Some(data_block);

        self.next_data_block_idx += 1;
    }

    /// advance the cursor to the start of the data block with index data_block_idx.
    fn advance_to(&mut self, data_block_idx: usize) {
        while self.next_data_block_idx < data_block_idx {
            self.next();
        }
    }

    fn done(&self) -> bool {
        self.next_data_block_idx >= self.num_data_blocks
    }
}

/// a writer for writing table keys into a sstable file.
struct SSTableWriter {
    file_num: FileNum,
    writer: BufWriter<File>,
    data_block: Option<DataBlock>,
    filter_block: FilterBlock,
    index_block: IndexBlock,
    num_table_keys: usize,
    min_table_key: Option<TableKey>,
    max_table_key: Option<TableKey>,
}

impl SSTableWriter {
    fn new(file_num: FileNum) -> Self {
        let file = File::create(sstable_file_name(file_num)).unwrap();
        SSTableWriter {
            file_num,
            writer: BufWriter::new(file),
            data_block: None,
            filter_block: FilterBlock::new(),
            index_block: IndexBlock::new(),
            num_table_keys: 0,
            min_table_key: None,
            max_table_key: None,
        }
    }

    pub fn push(&mut self, table_key: TableKey) {
        if self.min_table_key.is_none() {
            self.min_table_key = Some(table_key.clone());
        } else {
            self.min_table_key = Some(cmp::min(
                self.min_table_key.as_ref().unwrap().clone(),
                table_key.clone(),
            ));
        }

        if self.max_table_key.is_none() {
            self.max_table_key = Some(table_key.clone());
        } else {
            self.max_table_key = Some(cmp::max(
                self.max_table_key.as_ref().unwrap().clone(),
                table_key.clone(),
            ));
        }

        self.filter_block.insert(&table_key);
        if self.data_block.is_none() {
            self.data_block = Some(DataBlock::new());
        }
        self.data_block.as_mut().unwrap().add(table_key);
        self.num_table_keys += 1;

        // ensure the flushing happens before the data block is full.
        if self.data_block.as_ref().unwrap().size() > BLOCK_SIZE - TABLE_KEY_SIZE {
            self.flush_data_block();
        }
    }

    fn flush_data_block(&mut self) {
        self.writer
            .write(&self.data_block.as_ref().unwrap().encode_to_bytes())
            .unwrap();
        self.writer.flush().unwrap();

        // add a fence pointer for the data block.
        self.index_block
            .add(self.data_block.as_ref().unwrap().fence_pointer());

        self.data_block = None;
    }

    pub fn done(&mut self) -> SSTable {
        if self.data_block.is_some() {
            self.flush_data_block();
        }

        // flush other blocks.
        // padding is done inside `encode_to_bytes`.
        self.writer
            .write(&self.filter_block.encode_to_bytes())
            .unwrap();

        self.writer
            .write(&self.index_block.encode_to_bytes())
            .unwrap();

        let num_data_blocks = table_keys_to_blocks(self.num_table_keys);
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
        self.writer.flush().unwrap();

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
    sstable_size_capacity: usize,
    outputs: Vec<Rc<SSTable>>,
    pub min_table_key: Option<TableKey>,
    pub max_table_key: Option<TableKey>,
}

/// receives table keys and write them into a batch of sstable files.
impl SSTableWriterBatch {
    pub fn new(next_file_num: FileNum, sstable_size_capacity: usize) -> Self {
        Self {
            sstable_writer: None,
            next_file_num,
            sstable_size_capacity,
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
        if self.sstable_writer.as_ref().unwrap().file_size()
            > self.sstable_size_capacity - TABLE_KEY_SIZE
        {
            self.harness();
        }
    }

    /// harness an sstable.
    pub fn harness(&mut self) {
        self.outputs
            .push(Rc::new(self.sstable_writer.as_mut().unwrap().done()));
        // reset.
        self.sstable_writer = None;
    }

    pub fn done(&mut self) -> (Vec<Rc<SSTable>>, FileNum) {
        // `done` could be called when all data blocks are flushed or there's one pending-to-be-flushed data block.
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
            "\nvisible table keys:\n\tcount: {}",
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
            if last_user_key.is_none() || last_user_key.unwrap() != table_key.user_key {
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

pub fn sstable_file_name(file_num: FileNum) -> String {
    format!("sstables/sstable_file_{}", file_num)
}

fn make_block_buf() -> Vec<u8> {
    vec![0; BLOCK_SIZE]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_reader() {
        let file_num = 42;
        let mut writer = SSTableWriter::new(file_num);

        let num_table_keys = 200;
        for i in 0..num_table_keys {
            let table_key = TableKey::new(i, i as usize, WriteType::Put, i);
            writer.push(table_key);
        }
        writer.done();

        let reader = SSTableReader::new(file_num);

        // check num_table_keys.
        assert_eq!(writer.num_table_keys, reader.total_num_table_keys);

        // check filter block.
        {
            let writer_bytes = writer.filter_block.encode_to_bytes();
            let reader_bytes = reader.filter_block.encode_to_bytes();
            for i in 0..BLOCK_SIZE {
                assert_eq!(writer_bytes[i], reader_bytes[i]);
            }
        }

        // check index block.
        {
            let writer_bytes = writer.index_block.encode_to_bytes();
            let reader_bytes = reader.index_block.encode_to_bytes();
            for i in 0..BLOCK_SIZE {
                assert_eq!(writer_bytes[i], reader_bytes[i]);
            }
        }
    }
}
