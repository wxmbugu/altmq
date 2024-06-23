#![allow(dead_code)]
use crate::Result;
use memmap2::MmapMut;
use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    path::PathBuf,
    u32,
};
#[derive(Debug)]
pub enum StorageError {
    NoSpaceLeft,
    IoError(io::Error),
    SegmentNotFound,
}
const SEGMENT_SIZE: usize = 1024;
const START_OFFSET: usize = 0;
#[derive(Debug)]
struct Storage {
    segments: Vec<Segment>,
    name: String,
}

#[derive(Debug)]
pub struct Segment {
    log: Log,
    path: String,
    base_offset: u32,
    current_offset: u32,
    segment_size: u32,
    closed: bool,
}
#[derive(Debug)]
pub struct Log {
    file: File,
    index: Index,
}
#[derive(Debug)]
pub struct Index {
    file: File,
    mmap: MmapMut,
    offset: usize,
    max_size: usize,
}
#[derive(Debug)]
pub struct Entry {
    offset: u32,
    size: u32,
}
impl Segment {
    fn new(dir: &str, log_name: u32, segment_size: u32) -> Segment {
        Segment {
            log: Log::new(dir, log_name).expect("creating file"),
            path: dir.to_string(),
            base_offset: 0,
            current_offset: 0,
            segment_size,
            closed: false,
        }
    }
    fn append_data(&mut self, data: &[u8]) -> Result<(), StorageError> {
        match self.check_split(data.len() as u32) {
            Ok(_) => {
                let entry = Entry::new(self.current_offset, data.len() as u32);
                self.log.write(data, &entry);
                self.offset(entry.size);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    fn offset(&mut self, offset: u32) {
        self.current_offset += offset;
    }
    fn check_split(&self, entry_size: u32) -> Result<bool, StorageError> {
        match (self.current_offset + entry_size) > self.segment_size {
            true => Err(StorageError::NoSpaceLeft),
            false => Ok(true),
        }
    }
    // fn seek_at(&self, entry: Entry) {}
}
impl Entry {
    fn new(offset: u32, size: u32) -> Entry {
        Entry { offset, size }
    }
    fn as_bytes(&self) -> Vec<u8> {
        let mut payload: Vec<u8> = Vec::with_capacity(8);
        payload.extend(self.offset.to_be_bytes());
        payload.extend(self.size.to_be_bytes());
        payload
    }
    fn from_bytes(data: &[u8]) -> Self {
        Entry {
            offset: u32::from_be_bytes(data[..4].try_into().unwrap()),
            size: u32::from_be_bytes(data[4..8].try_into().unwrap()),
        }
    }
}
impl Index {
    fn new(dir: &str, offset: u32, max_size: usize) -> Index {
        let idx_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(PathBuf::from(&format!("{dir}/{offset}.idx")))
            .unwrap();
        idx_file.set_len(max_size as u64).unwrap(); //TODO: fix this for setting log file len(maybe a cacluate how long the index file should be)
        let mmap = unsafe { MmapMut::map_mut(&idx_file).expect("failed to map file") };
        Index {
            file: idx_file,
            mmap,
            offset: 0,
            max_size,
        }
    }
    fn write(&mut self, entry: &Entry) {
        let size = (&mut self.mmap[self.offset..])
            .write(&entry.as_bytes())
            .unwrap();
        self.offset(size);
    }
    fn offset(&mut self, size: usize) {
        self.offset += size;
    }
    fn seek_at(&self, size: usize, offset: usize) -> Entry {
        Entry::from_bytes(&self.mmap[offset..(offset + size)])
    }
}
impl Log {
    fn new(dir: &str, offset: u32) -> Result<Log, StorageError> {
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .truncate(true)
            .open(PathBuf::from(&format!("{dir}/{offset}.log")))
        {
            Ok(file) => Ok(Log {
                file,
                index: Index::new(dir, offset, 1032), //TODO Index: max_size should be a setting
            }),
            Err(err) => Err(StorageError::IoError(err)),
        }
    }
    fn write(&mut self, data: &[u8], entry: &Entry) {
        self.index.write(entry);
        self.file.write_all(data).unwrap();
    }
}
mod test {
    #![allow(unused_imports)]

    use crate::internal::log::{Entry, SEGMENT_SIZE};

    use super::Segment;
    use std::fs;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use std::{
        env::temp_dir,
        fs::File,
        os::{self},
    };

    #[test]
    fn test_create_log() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let mut seg = Segment::new("test_data", offset, SEGMENT_SIZE as u32);
        let data = b"Hello World!";
        seg.append_data(data).unwrap();
        assert_eq!(seg.current_offset, data.len() as u32);
    }
    #[test]
    fn test_no_space_left() {
        let segment_size = 12;
        let data = b"Hello World!";
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let mut seg = Segment::new("test_data", offset, segment_size);
        seg.append_data(data).unwrap();
        assert!(seg.append_data(data).is_err());
    }
    #[test]
    fn test_index_offsets() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let mut seg = Segment::new("test_data", offset, SEGMENT_SIZE as u32);
        let data = b"Hello World!";
        seg.append_data(data).unwrap();
        seg.append_data(data).unwrap();
        seg.append_data(data).unwrap();
        let mmap = &seg.log.index.mmap;
        let expected_entries = vec![
            Entry::new(0, data.len() as u32).as_bytes(),
            Entry::new(data.len() as u32, data.len() as u32).as_bytes(),
            Entry::new((data.len() as u32) * 2, data.len() as u32).as_bytes(),
        ];
        let mut offset = 0;
        for expected in expected_entries {
            let entry_bytes = &mmap[offset..offset + expected.len()];
            assert_eq!(entry_bytes, expected);
            offset += expected.len();
        }
    }
}
