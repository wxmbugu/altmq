//TODO: UNIQUE_ID for segment
// TODO: crc digest to test data consistency
// TODO: check if data iwill fit next segment on storage save instead on segment save
// TODO: Log Replication and Log Compaction
// FIX: Too many files open, should we maybe use a custom bufferwriter and bufferreader
// FIX:  storage directory to match queue name/entry
#![allow(dead_code)]
use crate::Result;
use core::str;
use memmap2::{MmapMut, MmapOptions, RemapOptions};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
    u32, u64, u8,
};

#[derive(Debug)]
pub enum StorageError {
    NoSpaceLeft,
    IoError(io::Error),
    SegmentNotFound,
    DirEmpty,
}
const SEGMENT_SIZE: usize = 5 * 1024 * 1204;
const START_OFFSET: usize = 0;
const DIR_PATH: &str = "storage/queue/";
#[derive(Debug)]
pub struct Storage {
    pub segments: HashMap<String, Vec<Segment>>,
    segment_size: u32,
    dir_path: PathBuf, //TODO: should be a setting
}

#[derive(Debug)]
pub struct Segment {
    log: Log,
    path: PathBuf,
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
impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> Self {
        StorageError::IoError(error)
    }
}
impl Storage {
    pub fn new(queue_name: &str, segment_size: u32, dir_path: String) -> Self {
        let mut segments = HashMap::new();
        let mut vec_segments: Vec<Segment> = Vec::new();
        // fs::create_dir_all(&dir_path).unwrap();
        let segment = Segment::new(&dir_path, 0, segment_size);
        vec_segments.push(segment);
        segments.insert(queue_name.to_string(), vec_segments);
        Storage {
            segments,
            segment_size,
            dir_path: PathBuf::from(dir_path),
        }
    }
    pub fn save_to_disk(&mut self, queue_name: &str, data: &[u8]) {
        if data.len() > self.segment_size as usize {
            //TODO: UNIQUE_ID for segment
            // we split the data into chunks that will fit each segment size
            // todo give a unique id to trunk the every chunk or data so that means any entry will
            // have an id so that we can use it where the message might be larger than the segment_size
            let mut cursor: u32 = 0;
            let end_pos = data.len();
            loop {
                let final_segment_size = end_pos % self.segment_size as usize;
                let final_chunk_pos = end_pos - final_segment_size;
                let next_cursor = cursor + self.segment_size;
                let chunk = &data[cursor as usize..next_cursor as usize];
                self.save_data_to_segment(queue_name, chunk);
                cursor = next_cursor;
                if cursor as usize == final_chunk_pos {
                    if final_segment_size > 0 {
                        let final_chunk = &data[cursor as usize..];
                        self.save_data_to_segment(queue_name, final_chunk);
                        break;
                    }
                    break;
                }
            }
        } else {
            self.save_data_to_segment(queue_name, data);
        }
    }
    fn save_data_to_segment(&mut self, queue_name: &str, data: &[u8]) {
        let segments = self
            .segments
            .get_mut(queue_name)
            .expect("expect vec of queue messages");
        let len_segments = segments.len();
        let segment = &mut segments[len_segments - 1];
        match segment.append_data(data) {
            Ok(_) => (),
            Err(e) => match e {
                StorageError::NoSpaceLeft => {
                    segment.log.index.resize();
                    segment.closed = true;
                    let mut new_segment = Segment::new(
                        self.dir_path.as_os_str().to_str().expect("queue path"),
                        (len_segments) as u32,
                        self.segment_size,
                    );
                    new_segment.append_data(data).unwrap();
                    segments.push(new_segment);
                }
                _ => {
                    eprintln!("{:?}", e);
                }
            },
        }
    }
    fn restore_from_disk(&mut self) -> Result<(), StorageError> {
        if self.dir_path.read_dir()?.next().is_none() {
            Err(StorageError::DirEmpty)
        } else {
            for entry in fs::read_dir(&self.dir_path).unwrap() {
                let entry = entry.expect("dir entry");
                let path = entry.path();
                if path.is_dir() {
                    let sub_dir = path.strip_prefix(DIR_PATH).unwrap();
                    let vec_segments = load_segments_from_disk(
                        self.dir_path.to_str().expect("storage path").to_string(),
                    );
                    self.segments
                        .insert(sub_dir.to_str().unwrap().to_string(), vec_segments);
                }
            }
            Ok(())
        }
    }
}

pub fn load_segments_from_disk(path: String) -> Vec<Segment> {
    let mut log_file: Vec<u32> = Vec::new();
    let mut segments: Vec<Segment> = Vec::new();
    for entry in fs::read_dir(&path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_dir() && path.extension().unwrap().to_str().unwrap() == "log" {
            let log_id = path
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
                .parse::<u32>()
                .unwrap();
            log_file.push(log_id);
        }
    }
    log_file.sort();
    let last_log = log_file[log_file.len() - 1];
    for log in log_file {
        let log_path = format!("{path}/{log:0>12}.log");
        let idx_path = format!("{path}/{log:0>12}.idx");
        if log == last_log {
            let segment = load_segment(&path, log_path, idx_path, log, SEGMENT_SIZE as u64)
                .expect("a segment");
            segments.push(segment)
        } else {
            let idx_f = File::open(&idx_path).unwrap();
            let metadata = idx_f.metadata().unwrap();
            let segment =
                load_segment(&path, log_path, idx_path, log, metadata.len()).expect("a segment");
            segments.push(segment)
        }
    }
    segments
}

fn load_segment(
    dir_path: &str,
    log_path: String,
    index_path: String,
    log_id: u32,
    index_len: u64,
) -> Result<Segment, StorageError> {
    let idx_f = File::open(index_path)?;
    let log_f = File::open(log_path)?;
    let log_md = log_f.metadata()?;
    let idx_md = idx_f.metadata()?;
    let last_entry_offset = idx_md.len();
    let segment = Segment::load(
        dir_path,
        log_id,
        log_md.len() as u32,
        SEGMENT_SIZE as u32,
        last_entry_offset as u32,
        false,
        index_len,
    );
    Ok(segment)
}

impl Segment {
    pub fn new(dir: &str, log_name: u32, segment_size: u32) -> Segment {
        let path = PathBuf::from(dir);
        Segment {
            log: Log::new(&path, log_name).expect("creating file"),
            path,
            base_offset: 0,
            current_offset: 0,
            segment_size,
            closed: false,
        }
    }
    // load existing segment
    fn load(
        dir: &str,
        log_name: u32,
        current_offset: u32,
        segment_size: u32,
        index_offset: u32,
        closed: bool,
        index_len: u64,
    ) -> Segment {
        let path = PathBuf::from(dir);
        Segment {
            log: Log::load(&path, log_name, index_offset as u64, index_len).expect("creating file"),
            path,
            base_offset: 0,
            current_offset,
            segment_size,
            closed,
        }
    }
    pub fn append_data(&mut self, data: &[u8]) -> Result<(), StorageError> {
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
    fn read(&mut self, entry: Entry) -> Vec<u8> {
        self.log.seek(entry.size as usize, entry.offset)
    }
    fn close(&mut self) {
        self.closed = true
    }
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
    #[allow(clippy::ptr_arg)]
    fn new(dir: &PathBuf, pos: u32, max_size: usize) -> Index {
        let idx_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(dir.join(format!("{pos:0>12}.idx")))
            .unwrap();
        idx_file.set_len(max_size as u64).unwrap(); //TODO: fix this for setting log file len(maybe a calculate how long the index file should be)
        let mmap = unsafe { MmapMut::map_mut(&idx_file).expect("failed to map file") };
        Index {
            file: idx_file,
            mmap,
            offset: 0,
            max_size,
        }
    }
    fn load(dir: PathBuf, pos: u32, offset: u64, max_size: usize) -> Index {
        let idx_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.join(format!("{pos:0>12}.idx")))
            .unwrap();
        idx_file.set_len(max_size as u64).unwrap(); //TODO: fix this for setting log file len(maybe a calculate how long the index file should be)
        let mmap = unsafe {
            MmapOptions::new()
                .offset(0)
                .map_mut(&idx_file)
                .expect("failed to map file")
        };
        Index {
            file: idx_file,
            mmap,
            offset: offset as usize,
            max_size,
        }
    }
    fn write(&mut self, entry: &Entry) {
        let _size = (&mut self.mmap[self.offset..])
            .write(&entry.as_bytes())
            .unwrap();
        self.mmap.flush().unwrap();
        self.offset(entry.as_bytes().len());
    }
    fn offset(&mut self, size: usize) {
        self.offset += size;
    }
    fn seek_at(&self, size: usize, offset: usize) -> Entry {
        Entry::from_bytes(&self.mmap[offset..(offset + size)])
    }
    fn resize(&mut self) {
        let options = RemapOptions::new();
        unsafe {
            self.mmap.remap(self.offset, options).unwrap();
        };
        self.file.set_len(self.offset as u64).unwrap();
    }
}

impl Log {
    fn new(dir: &PathBuf, pos: u32) -> Result<Log, StorageError> {
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(dir.join(format!("{pos:0>12}.log")))
        {
            Ok(file) => Ok(Log {
                file,
                index: Index::new(dir, pos, SEGMENT_SIZE), //TODO Index: max_size should be a setting
            }),
            Err(err) => Err(StorageError::IoError(err)),
        }
    }
    fn load(dir: &PathBuf, pos: u32, offset: u64, index_len: u64) -> Result<Log, StorageError> {
        match OpenOptions::new()
            .write(true)
            .read(true)
            .open(dir.join(format!("{pos:0>12}.log")))
        {
            Ok(file) => Ok(Log {
                file,
                index: Index::load(dir.to_owned(), pos, offset, index_len as usize),
            }),
            Err(err) => Err(StorageError::IoError(err)),
        }
    }
    fn write(&mut self, data: &[u8], entry: &Entry) {
        self.index.write(entry);
        self.file.write_all(data).unwrap();
    }
    fn seek(&mut self, size: usize, offset: u32) -> Vec<u8> {
        let mut buf = [0u8; 4096];
        self.file
            .read_at(&mut buf[..], (offset - size as u32) as u64)
            .unwrap();
        buf[..size].to_vec()
    }
    // TODO: Tes This when too man files are created and not closed
    // fn close(&self) {
    //     drop(self.file)
    // }
}

#[cfg(test)]
mod test {
    #![allow(unused_imports)]

    use crate::internal::log::{load_segments_from_disk, Entry, Storage, SEGMENT_SIZE};
    use crate::internal::queue;

    use super::{Segment, StorageError};
    use std::cell::RefCell;
    use std::ops::Div;
    use std::panic;
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::Once;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use std::{
        env::temp_dir,
        fs::File,
        os::{self},
    };
    use std::{fs, path};
    const PATH: &str = "test_data";
    use std::{sync::Mutex, thread::sleep};
    #[test]
    fn test_create_log() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u32);
        let data = b"Hello World!";
        seg.append_data(data).unwrap();
        assert_eq!(seg.current_offset, data.len() as u32)
    }

    #[test]
    #[should_panic]
    fn test_no_space_left() {
        let segment_size = 12;
        let data = b"Hello World!";
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let mut seg = Segment::new(PATH, offset, segment_size);
        seg.append_data(data).unwrap();
        seg.append_data(data).unwrap();
    }
    #[test]
    fn test_index_offsets() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u32);
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
    #[test]
    fn test_log_read_at() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();

        let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u32);
        let data = b"Hello World!";
        seg.append_data(data).unwrap();
        let entry = Entry::new(data.len() as u32, seg.current_offset);
        let read_data = seg.read(entry);
        assert_eq!(read_data, data);
    }
}
