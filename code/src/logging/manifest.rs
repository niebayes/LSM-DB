use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};

use integer_encoding::*;

use crate::storage::keys::*;
use crate::util::types::*;

pub const MANIFEST_FILE_PATH: &str = "manifest";

fn read_min_max_table_keys(reader: &mut &[u8]) -> (TableKey, TableKey) {
    let mut buf = make_table_key_buf();

    reader.read_exact(&mut buf).unwrap();
    let min_table_key = TableKey::decode_from_bytes(&buf).unwrap();
    reader.read_exact(&mut buf).unwrap();
    let max_table_key = TableKey::decode_from_bytes(&buf).unwrap();

    (min_table_key, max_table_key)
}

pub struct LevelManifest {
    /// level number.
    pub level_num: LevelNum,
    /// max number of sorted runs this level could hold.
    // could be computed out and hence not necessarily to be persisted.
    pub run_capacity: usize,
    /// number of bytes this level could hold.
    pub size_capacity: usize,
    /// number of runs in this level.
    pub num_runs: usize,
    /// min table key stored in the level.
    pub min_table_key: Option<TableKey>,
    /// max table key stored in the level.
    pub max_table_key: Option<TableKey>,
    /// sorted runs in the level.
    pub run_manifests: Vec<RunManifest>,
}

impl Default for LevelManifest {
    fn default() -> Self {
        Self {
            level_num: LevelNum::default(),
            run_capacity: usize::default(),
            size_capacity: usize::default(),
            num_runs: usize::default(),
            min_table_key: None,
            max_table_key: None,
            run_manifests: Vec::new(),
        }
    }
}

impl LevelManifest {
    fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.write_fixedint(self.level_num).unwrap();
        encoded.write_fixedint(self.run_capacity).unwrap();
        encoded.write_fixedint(self.size_capacity).unwrap();
        encoded.write_fixedint(self.num_runs).unwrap();
        if self.min_table_key.is_some() {
            encoded.append(&mut self.min_table_key.as_ref().unwrap().encode_to_bytes());
            encoded.append(&mut self.max_table_key.as_ref().unwrap().encode_to_bytes());
        }
        for run_manifest in self.run_manifests.iter() {
            encoded.append(&mut run_manifest.encode_to_bytes());
        }
        encoded
    }
}

pub struct RunManifest {
    /// min table key stored in the run.
    pub min_table_key: TableKey,
    /// max table key stored in the run.
    pub max_table_key: TableKey,
    /// number of sstables in this run.
    pub num_sstables: usize,
    /// sstables in the run.
    /// the sstables are sorted by the max user key, i.e. sstables with lower max user keys are placed first.
    pub sstable_manifests: Vec<SSTableManifest>,
}

impl Default for RunManifest {
    fn default() -> Self {
        Self {
            min_table_key: TableKey::default(),
            max_table_key: TableKey::default(),
            num_sstables: usize::default(),
            sstable_manifests: Vec::new(),
        }
    }
}

impl RunManifest {
    fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.append(&mut self.min_table_key.encode_to_bytes());
        encoded.append(&mut self.max_table_key.encode_to_bytes());
        encoded.write_fixedint(self.num_sstables).unwrap();
        for sstable_manifest in self.sstable_manifests.iter() {
            encoded.append(&mut sstable_manifest.encode_to_bytes());
        }
        encoded
    }
}

pub struct SSTableManifest {
    /// sstable file number from which the corresponding sstable file could be located.
    pub file_num: FileNum,
    /// sstable file size.
    pub file_size: usize,
    /// min table key stored in the sstable.
    pub min_table_key: TableKey,
    /// max table key stored in the sstable.
    pub max_table_key: TableKey,
}

impl Default for SSTableManifest {
    fn default() -> Self {
        Self {
            file_num: FileNum::default(),
            file_size: usize::default(),
            min_table_key: TableKey::default(),
            max_table_key: TableKey::default(),
        }
    }
}

impl SSTableManifest {
    fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.write_fixedint(self.file_num).unwrap();
        encoded.write_fixedint(self.file_size).unwrap();
        encoded.append(&mut self.min_table_key.encode_to_bytes());
        encoded.append(&mut self.max_table_key.encode_to_bytes());
        encoded
    }
}

/// database manifest.
/// each manifest corresponds to one version of the database.
pub struct Manifest {
    /// the next sequence number to allocate for a write.
    pub next_seq_num: SeqNum,
    /// the next file number to allocate for a file.
    pub next_file_num: FileNum,
    /// number of levels.
    pub num_levels: usize,
    /// level manifests.
    pub level_manifests: Vec<LevelManifest>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            next_seq_num: SeqNum::default(),
            next_file_num: FileNum::default(),
            num_levels: usize::default(),
            level_manifests: Vec::new(),
        }
    }
}

impl Manifest {
    // replace the old manifest (if any) with the latest manifest.
    pub fn set(manifest: Manifest) {
        let file = OpenOptions::new()
            // open the existing file or create a new one if it does not exist.
            .create(true)
            // discard the old manifest if any.
            .truncate(true)
            // acquire write permission.
            .write(true)
            .open(MANIFEST_FILE_PATH)
            .unwrap();

        let mut writer = BufWriter::new(file);
        writer.write(&manifest.encode_to_bytes()).unwrap();
        writer.flush().unwrap();
    }

    // read the manifest file and decode the latest manifest if any.
    // note, the manifest file always stores the latest manifest if any.
    pub fn get() -> Option<Self> {
        if let Ok(file) = File::open(MANIFEST_FILE_PATH) {
            // read all bytes into the buffer.
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();
            let file_size = reader.read_to_end(&mut buf).unwrap();
            if file_size == 0 {
                return None;
            }

            // read the db manifest.
            let mut manifest = Manifest::default();
            let mut reader = buf.as_slice();
            manifest.next_seq_num = reader.read_fixedint().unwrap();
            manifest.next_file_num = reader.read_fixedint().unwrap();
            manifest.num_levels = reader.read_fixedint().unwrap();

            // read level manifests.
            for _ in 0..manifest.num_levels {
                let mut level_manifest = LevelManifest::default();
                level_manifest.level_num = reader.read_fixedint().unwrap();
                level_manifest.run_capacity = reader.read_fixedint().unwrap();
                level_manifest.size_capacity = reader.read_fixedint().unwrap();
                level_manifest.num_runs = reader.read_fixedint().unwrap();

                if level_manifest.num_runs > 0 {
                    let (min_table_key, max_table_key) = read_min_max_table_keys(&mut reader);
                    level_manifest.min_table_key = Some(min_table_key);
                    level_manifest.max_table_key = Some(max_table_key);
                }

                // read run manifests.
                for _ in 0..level_manifest.num_runs {
                    let mut run_manifest = RunManifest::default();
                    (run_manifest.min_table_key, run_manifest.max_table_key) =
                        read_min_max_table_keys(&mut reader);
                    run_manifest.num_sstables = reader.read_fixedint().unwrap();

                    // read sstable manifests.
                    for _ in 0..run_manifest.num_sstables {
                        let mut sstable_manifest = SSTableManifest::default();
                        sstable_manifest.file_num = reader.read_fixedint().unwrap();
                        sstable_manifest.file_size = reader.read_fixedint().unwrap();
                        (
                            sstable_manifest.min_table_key,
                            sstable_manifest.max_table_key,
                        ) = read_min_max_table_keys(&mut reader);

                        run_manifest.sstable_manifests.push(sstable_manifest);
                    }

                    level_manifest.run_manifests.push(run_manifest);
                }

                manifest.level_manifests.push(level_manifest);
            }

            Some(manifest)
        } else {
            None
        }
    }

    pub fn encode_to_bytes(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.write_fixedint(self.next_seq_num).unwrap();
        encoded.write_fixedint(self.next_file_num).unwrap();
        encoded.write_fixedint(self.num_levels).unwrap();
        for level_manifest in self.level_manifests.iter() {
            encoded.append(&mut level_manifest.encode_to_bytes())
        }
        encoded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::level::Level;
    use crate::storage::run::Run;
    use crate::storage::sstable::SSTable;
    use std::rc::Rc;

    fn make_identity_sstable(i: i32) -> SSTable {
        SSTable::new(
            i as FileNum,
            i as usize,
            TableKey::identity(i as i32),
            TableKey::identity(i as i32),
        )
    }

    #[test]
    fn to_from_sstable_manifest() {
        let sstable = SSTable::new(0, 100, TableKey::identity(3), TableKey::identity(4));
        let manifest = sstable.manifest();
        let sstable2 = SSTable::from_manifest(&manifest);

        assert_eq!(sstable.file_num, sstable2.file_num);
        assert_eq!(sstable.file_size, sstable2.file_size);
        assert_eq!(sstable.min_table_key, sstable2.min_table_key);
        assert_eq!(sstable.max_table_key, sstable2.max_table_key);
    }

    #[test]
    fn to_from_run_manifest() {
        let mut sstables = Vec::new();
        let num_sstables = 3;
        for i in 0..num_sstables {
            sstables.push(Rc::new(make_identity_sstable(i)));
        }
        let run = Run::new(
            sstables,
            TableKey::identity(0),
            TableKey::identity((num_sstables - 1) as i32),
        );

        let manifest = run.manifest();
        let run2 = Run::from_manifest(&manifest);

        assert_eq!(run.min_table_key, run2.min_table_key);
        assert_eq!(run.max_table_key, run2.max_table_key);

        for i in 0..num_sstables {
            let sstable = run.sstables.get(i as usize).unwrap();
            let sstable2 = run2.sstables.get(i as usize).unwrap();

            assert_eq!(sstable.file_num, sstable2.file_num);
            assert_eq!(sstable.file_size, sstable2.file_size);
            assert_eq!(sstable.min_table_key, sstable2.min_table_key);
            assert_eq!(sstable.max_table_key, sstable2.max_table_key);
        }
    }

    #[test]
    fn to_from_level_manifest() {
        let mut level = Level::new(1, 2, 3);
        let num_runs = 3;
        for i in 0..num_runs {
            level.runs.push(Run::new(
                vec![Rc::new(make_identity_sstable(i))],
                TableKey::identity(i),
                TableKey::identity(i),
            ));
        }
        level.min_table_key = Some(TableKey::identity(0));
        level.max_table_key = Some(TableKey::identity(num_runs - 1));

        let manifest = level.manifest();
        let level2 = Level::from_manifest(&manifest);

        assert_eq!(level.level_num, level2.level_num);
        assert_eq!(level.run_capacity, level2.run_capacity);
        assert_eq!(level.size_capacity, level2.size_capacity);
        assert_eq!(level.min_table_key.unwrap(), level2.min_table_key.unwrap());
        assert_eq!(level.max_table_key.unwrap(), level2.max_table_key.unwrap());

        for i in 0..num_runs {
            let run = level.runs.get(i as usize).unwrap();
            let run2 = level2.runs.get(i as usize).unwrap();
            assert_eq!(run.min_table_key, run2.min_table_key);
            assert_eq!(run.max_table_key, run2.max_table_key);
            assert_eq!(run.sstables.len(), run2.sstables.len());

            for i in 0..run.sstables.len() {
                let sstable = run.sstables.get(i as usize).unwrap();
                let sstable2 = run2.sstables.get(i as usize).unwrap();

                assert_eq!(sstable.file_num, sstable2.file_num);
                assert_eq!(sstable.file_size, sstable2.file_size);
                assert_eq!(sstable.min_table_key, sstable2.min_table_key);
                assert_eq!(sstable.max_table_key, sstable2.max_table_key);
            }
        }
    }
}
