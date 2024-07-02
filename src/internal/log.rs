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
    u32, u64,
};

#[derive(Debug)]
pub enum StorageError {
    NoSpaceLeft,
    IoError(io::Error),
    SegmentNotFound,
    DirEmpty,
}
const SEGMENT_SIZE: usize = 5 * 1024 * 1024;
const START_OFFSET: usize = 0;
const DIR_PATH: &str = "storage/queue/";
#[derive(Debug)]
struct Storage {
    segments: HashMap<String, Vec<Segment>>,
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
    fn new(queue_name: &str, segment_size: u32, dir_path: String) -> Self {
        let mut segments = HashMap::new();
        let mut vec_segments: Vec<Segment> = Vec::new();
        let segment = Segment::new(&dir_path, 0, segment_size);
        vec_segments.push(segment);
        segments.insert(queue_name.to_string(), vec_segments);
        Storage {
            segments,
            segment_size,
            dir_path: PathBuf::from(dir_path),
        }
    }
    fn load_exitisting(&mut self) -> Result<(), StorageError> {
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

fn load_segments_from_disk(path: String) -> Vec<Segment> {
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
    fn new(dir: &str, log_name: u32, segment_size: u32) -> Segment {
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
    fn seek_at(&mut self, entry: Entry) -> Vec<u8> {
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

    use crate::internal::log::{load_segments_from_disk, Entry, SEGMENT_SIZE};

    use super::{Segment, StorageError};
    use std::cell::RefCell;
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
    static SETUP_ONCE: Once = Once::new();
    static TEARDOWN_ONCE: Once = Once::new();
    fn setup() {
        SETUP_ONCE.call_once(|| {
            fs::create_dir_all(format!("{PATH}/segments").as_str()).unwrap();
        });
    }

    fn teardown() {
        TEARDOWN_ONCE.call_once(|| {
            fs::remove_dir_all(PATH).unwrap();
        });
    }
    fn run_test<T>(test: T)
    where
        T: FnOnce() + panic::UnwindSafe,
    {
        setup();
        let result = panic::catch_unwind(|| test);
        teardown();
        assert!(result.is_ok())
    }

    fn run_test_panic<T>(test: T)
    where
        T: FnOnce() + panic::UnwindSafe,
    {
        setup();
        let result = panic::catch_unwind(|| test);
        teardown();
        assert!(result.is_err())
    }
    #[test]
    fn test_create_log() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        run_test(|| {
            let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u32);
            let data = b"Hello World!";
            seg.append_data(data).unwrap();
            assert_eq!(seg.current_offset, data.len() as u32);
        });
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
        run_test_panic(|| {
            let mut seg = Segment::new(PATH, offset, segment_size);
            seg.append_data(data).unwrap();
            seg.append_data(data).unwrap();
        });
    }
    #[test]
    fn test_index_offsets() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        run_test(|| {
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
        });
    }
    #[test]
    fn test_read_at() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();

        run_test(|| {
            let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u32);
            let data = b"Hello World!";
            seg.append_data(data).unwrap();
            let entry = Entry::new(data.len() as u32, seg.current_offset);
            let read_data = seg.seek_at(entry);
            assert_eq!(read_data, data);
        });
    }

    #[test]
    fn test_load_segments() {
        let path_str = format!("{PATH}/segments");
        let path = PathBuf::from(&path_str);
        let num_segments = 6;

        run_test(|| {
            create_segments(&path_str, num_segments);
            let segments = load_segments_from_disk(path.to_str().unwrap().to_string());
            assert_eq!(segments.len(), num_segments as usize);
        });
    }
    fn create_segments(path: &str, no_segments: u32) {
        let mut vec_seg: Vec<Rc<RefCell<Segment>>> = Vec::new();
        let mut _count = 0;
        let mut next_seg = 0;
        let mut new_segments: Vec<Rc<RefCell<Segment>>> = Vec::new();
        let segment = Segment::new(path, next_seg, 1024);
        vec_seg.push(Rc::new(RefCell::new(segment)));
        loop {
            if next_seg == no_segments {
                let log_path = format!("{path}/{next_seg:0>12}.log");
                let idx_path = format!("{path}/{next_seg:0>12}.idx");
                fs::remove_file(log_path).unwrap();
                fs::remove_file(idx_path).unwrap();
                break;
            }
            let mut remove_indices = Vec::new();
            for (index, segment) in vec_seg.iter().enumerate() {
                let mut segment_mut = segment.borrow_mut();
                let data = b"Hello World!";
                match segment_mut.append_data(data) {
                    Ok(_) => {
                        _count += 1;
                    }
                    Err(e) => match e {
                        StorageError::NoSpaceLeft => {
                            println!("{}", segment_mut.log.index.offset);
                            segment_mut.log.index.resize();
                            next_seg += 1;
                            segment_mut.closed = true;
                            let new_seg = Segment::new(path, next_seg, 1024);
                            new_segments.push(Rc::new(RefCell::new(new_seg)));
                            remove_indices.push(index);
                        }
                        _ => {
                            eprintln!("{:?}", e);
                        }
                    },
                }
            }
            vec_seg.extend(new_segments.iter().cloned());
            new_segments.clear();
            for &index in remove_indices.iter().rev() {
                vec_seg.remove(index);
            }
        }
    }
}
