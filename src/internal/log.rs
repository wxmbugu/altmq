// TODO: crc implementation data consistency
// TODO: Log Replication and Log Compaction
#![allow(dead_code)]
#![allow(
    clippy::needless_question_mark,
    clippy::seek_from_current,
    clippy::unused_io_amount
)]
use crate::Result;
use core::str;
use memmap2::{MmapMut, MmapOptions, RemapOptions};
use std::{
    borrow::BorrowMut,
    fmt::{Debug, Display},
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
};

#[derive(Debug)]
pub enum StorageError {
    NoSpaceLeft,
    IoError(io::Error),
    SegmentNotFound,
    DirEmpty,
    InvalidSeek,
    LogIndexOutofBound,
}

impl Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}
pub const SEGMENT_SIZE: usize = 1024 * 1024; //TODO: should be a setting
const START_OFFSET: usize = 0;
// represents the size of our entry/idx byte size
const ENTRY_SIZE: usize = 8;
const DIR_PATH: &str = "storage/queue/";

pub struct CommitLog {
    pub name: String,
    pub segments: Vec<Segment>,
    pub dir_path: PathBuf,
    segment_size: u64,
    // track where the last read from the client to queue was from
    pub queue_pos: File,
    // keep track of the last segment to be read
    rposition: u32,
    // keep track of the offset that was  last read by client
    rindex_offset: u32,
    // keep track of the last segment to be written
    wposition: u32,
    // keep track of the offset that was  last write by client
    windex_offset: u32,
}

pub struct Segment {
    pub log: Log,
    path: PathBuf,
    base_offset: u64,
    pub current_offset: u64,
    segment_size: u64,
    closed: bool,
}

pub struct Log {
    writer: CursorWriter<File>,
    reader: CursorReader<File>,
    pub index: Index,
}

#[derive(Debug)]
pub struct Index {
    file: File,
    mmap: MmapMut,
    offset: usize,
    pub roffset: usize,
    max_size: usize,
}

//will be used to track the position of commit log and save to disk
#[derive(Debug)]
pub struct Tracker {
    // which file it was reading at before closing
    position: u32,
    //offset in specific file
    offset: u32,
    last_write_offset: u32,
}

impl Tracker {
    pub fn to_bytes(position: u32, offset: u32, lwoffset: u32) -> Vec<u8> {
        let mut payload: Vec<u8> = Vec::with_capacity(
            offset.to_be_bytes().len()
                + position.to_be_bytes().len()
                + lwoffset.to_be_bytes().len(),
        );
        payload.extend(position.to_be_bytes());
        payload.extend(offset.to_be_bytes());
        payload.extend(lwoffset.to_be_bytes());
        payload
    }
    pub fn from_bytes(buf: &[u8]) -> Self {
        let position = u32::from_be_bytes(buf[..4].try_into().unwrap());
        let offset = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        let lwoffset = u32::from_be_bytes(buf[8..12].try_into().unwrap());

        Self {
            position,
            offset,
            last_write_offset: lwoffset,
        }
    }
}

#[derive(Debug)]
pub struct Entry {
    offset: u32,
    size: u32,
}
pub struct CursorWriter<T: Write + Seek> {
    writer: BufWriter<T>,
    position: u64,
}
pub struct CursorReader<T: Read + Seek> {
    reader: BufReader<T>,
    position: u64,
}
impl<T: Read + Seek + Write> CursorWriter<T> {
    pub fn new(mut inner: T, pos: SeekFrom) -> Result<Self, StorageError> {
        let position = inner.seek(pos)?;
        Ok(CursorWriter {
            writer: BufWriter::new(inner),
            position,
        })
    }
}

impl<T: Read + Seek + Write> CursorReader<T> {
    pub fn new(mut inner: T) -> Result<Self, StorageError> {
        let position = inner.seek(io::SeekFrom::Current(0))?;
        Ok(CursorReader {
            reader: BufReader::new(inner),
            position,
        })
    }
    // Read data from the specified offset
    pub fn read_at(&mut self, buf: &mut [u8], offset: u64) -> Result<usize, StorageError> {
        self.reader.seek(SeekFrom::Start(offset))?;
        let size = self.read(buf)?;
        Ok(size)
    }
}
impl<T: Write + Seek> Write for CursorWriter<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let size = self.writer.write(buf)?;
        self.position += size as u64;
        Ok(size)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}
impl<T: Write + Seek> Seek for CursorWriter<T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.position = self.writer.seek(pos)?;
        Ok(self.position)
    }
}
impl From<io::Error> for StorageError {
    fn from(error: io::Error) -> Self {
        StorageError::IoError(error)
    }
}
impl<T: Read + Seek> Read for CursorReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.position += len as u64;
        Ok(len)
    }
}

impl<T: Read + Seek> Seek for CursorReader<T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.position = self.reader.seek(pos)?;
        Ok(self.position)
    }
}
impl Iterator for CommitLog {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut segment_idx = 0;
        while segment_idx < self.segments.len() {
            let segment = self.segments[segment_idx].borrow_mut();
            match segment.read_from_start() {
                Ok(data) => {
                    if segment.log.reader.position + data.len() as u64
                        == segment.log.writer.position
                    {
                        segment_idx += 1;
                        continue;
                    }

                    return Some(data);
                }
                Err(_) => {
                    return None;
                }
            }
        }
        None
    }
}

impl CommitLog {
    pub fn new(queue_name: &str, segment_size: u64, dir_path: &str) -> Self {
        let dir_path = format!("{}{}", dir_path, queue_name);
        let mut segments: Vec<Segment> = Vec::new();
        let offsets_path = format!("{}/{}", dir_path, "offsets");
        fs::create_dir_all(&offsets_path).unwrap();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(format!("{offsets_path}/{}", queue_name))
            .unwrap();
        let segment = Segment::new(&dir_path, 0, segment_size);
        segments.push(segment);
        CommitLog {
            name: queue_name.to_owned(),
            segments,
            segment_size,
            dir_path: PathBuf::from(dir_path),
            queue_pos: file,
            rposition: 0,
            rindex_offset: 0,
            windex_offset: 0,
            wposition: 0,
        }
    }
    /// Save data to disk by appending to the current segment
    pub fn save_to_disk(&mut self, data: &[u8]) -> Result<(), StorageError> {
        self.save_data_to_segment(data)
    }
    /// Append data to the current segment, or create a new segment if necessary
    fn save_data_to_segment(&mut self, data: &[u8]) -> Result<(), StorageError> {
        let len_segments = self.segments.len();

        self.windex_offset = self.segments[self.wposition as usize].log.index.offset as u32;
        let segment = &mut self.segments[len_segments - 1];
        match segment.append_data(data) {
            Ok(_) => {
                self.save_queue_offset();
                Ok(())
            }
            Err(e) => match e {
                StorageError::NoSpaceLeft => {
                    segment.log.index.resize();
                    segment.closed = true;
                    // Handle segment full scenario by closing the current segment and creating a new one
                    let mut new_segment = self.create_new_segment(len_segments as u32);
                    new_segment.append_data(data)?;
                    self.windex_offset = new_segment.log.index.offset as u32;
                    self.segments.push(new_segment);
                    self.wposition += 1;
                    self.save_queue_offset();
                    Ok(())
                }
                _ => Err(e),
            },
        }
    }
    // helper method to create  new segments the segments are created in ascening order from 0
    fn create_new_segment(&mut self, pos: u32) -> Segment {
        Segment::new(
            self.dir_path.as_os_str().to_str().expect("queue path"),
            pos,
            self.segment_size,
        )
    }

    /// Restore data from disk by loading all segments from the directory
    pub fn restore_from_disk(segment_size: u64, dir_path: &str) -> Result<Vec<Self>, StorageError> {
        let path = PathBuf::from(&dir_path);
        let mut _queue_name = String::new();
        let mut logs = Vec::new();
        if path.read_dir()?.next().is_none() {
            Err(StorageError::DirEmpty)
        } else {
            for entry in fs::read_dir(&path).unwrap() {
                let mut vec_segments = Vec::new();
                let entry = entry.expect("dir entry");
                let path = entry.path();
                if path.is_dir() {
                    let sub_dir = path.strip_prefix(dir_path).unwrap();
                    _queue_name = sub_dir.to_owned().to_str().unwrap().to_string();
                    let offsets_path = format!("{}{_queue_name}/{}", dir_path, "offsets");
                    let mut file = OpenOptions::new()
                        .create(true)
                        .read(true)
                        .write(true)
                        .open(format!("{offsets_path}/{}", _queue_name))
                        .unwrap();
                    let mut buf = [0; 12];
                    file.read(&mut buf).unwrap();
                    let tracker = Tracker::from_bytes(&buf);
                    let segments = load_segments_from_disk(
                        path.to_str().expect("storage path").to_string(),
                        tracker.last_write_offset as usize,
                    );
                    vec_segments.extend(segments);
                    let total_segments = vec_segments.len();
                    let log = CommitLog {
                        name: _queue_name.to_string(),
                        segments: vec_segments,
                        segment_size,
                        dir_path: path,
                        rposition: tracker.position,
                        queue_pos: file,
                        rindex_offset: tracker.offset,
                        wposition: (total_segments - 1) as u32,
                        windex_offset: tracker.last_write_offset,
                    };
                    logs.push(log);
                }
            }
            Ok(logs)
        }
    }

    pub fn read(&mut self) -> Result<Vec<u8>, StorageError> {
        let len = self.segments.len() - 1;
        let segment_offset = self.segments[self.rposition as usize].log.index.offset as u32;
        if self.rindex_offset >= segment_offset && self.rposition == len as u32 {
            self.rindex_offset = segment_offset;
            return Err(StorageError::LogIndexOutofBound);
        }
        if self.rindex_offset >= segment_offset && self.rposition != len as u32 {
            self.rposition += 1;
            self.rindex_offset = 0;
        }
        self.rindex_offset += ENTRY_SIZE as u32;
        self.save_queue_offset();
        let segment = &mut self.segments[self.rposition as usize];
        Ok(segment.read_at(self.rindex_offset as usize)?)
    }
    fn save_queue_offset(&mut self) {
        let payload = Tracker::to_bytes(self.rposition, self.rindex_offset, self.windex_offset);
        self.queue_pos.write_at(&payload, 0).unwrap();
    }
    /// reads data stored from a given offset
    pub fn read_at_from_disk(&mut self, position: usize) -> Result<Vec<u8>, StorageError> {
        let len_segments = self.segments.len();
        let segment = &mut self.segments[len_segments - 1];
        Ok(segment.read_at(position)?)
    }

    /// reads data stored from a given offset
    pub fn read_from_start(&mut self) -> Result<Vec<u8>, StorageError> {
        let len_segments = self.segments.len();
        let segment = &mut self.segments[len_segments - 1];
        Ok(segment.read_from_start()?)
    }
    /// reads next data stored from a given offset
    pub fn read_next_from_disk(&mut self, position: usize) -> Result<Vec<u8>, StorageError> {
        let len_segments = self.segments.len();
        let segment = &mut self.segments[len_segments - 1];
        Ok(segment.read_next(position)?)
    }
}

/// Reads the entire data stored on disk and loads it as a vector of segments
pub fn load_segments_from_disk(path: String, l_offset: usize) -> Vec<Segment> {
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
        let idx_f = File::open(&idx_path).unwrap();
        let metadata = idx_f.metadata().unwrap();
        let mut segment_size = metadata.len();
        let mut last_entry_offset = None;

        if log == last_log {
            segment_size = SEGMENT_SIZE as u64;
            last_entry_offset = Some(l_offset as u32);
        }
        let segment = load_segment(
            &path,
            log_path,
            idx_path,
            log,
            segment_size,
            last_entry_offset,
        )
        .expect("a segment");
        segments.push(segment)
    }
    segments
}

fn load_segment(
    dir_path: &str,
    log_path: String,
    index_path: String,
    log_id: u32,
    index_len: u64,
    mut last_entry_offset: Option<u32>,
) -> Result<Segment, StorageError> {
    let idx_f = File::open(index_path)?;
    let log_f = File::open(log_path)?;
    let log_md = log_f.metadata()?;
    let idx_md = idx_f.metadata()?;
    if last_entry_offset.is_none() {
        last_entry_offset = Some(idx_md.len() as u32);
    }
    let segment = Segment::load(
        dir_path,
        log_id,
        log_md.len(),
        SEGMENT_SIZE as u64,
        last_entry_offset.unwrap() as u64,
        false,
        index_len,
    );
    Ok(segment)
}

impl Segment {
    pub fn new(dir: &str, log_name: u32, segment_size: u64) -> Segment {
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
        current_offset: u64,
        segment_size: u64,
        index_offset: u64,
        closed: bool,
        index_len: u64,
    ) -> Segment {
        let path = PathBuf::from(dir);
        Segment {
            log: Log::load(&path, log_name, index_offset, index_len).expect("creating file"),
            path,
            base_offset: 0,
            current_offset,
            segment_size,
            closed,
        }
    }
    pub fn append_data(&mut self, data: &[u8]) -> Result<(), StorageError> {
        match self.check_split(data.len() as u64) {
            Ok(_) => {
                let entry = Entry::new(self.current_offset as u32, data.len() as u32);
                self.log.write(data, &entry)?;
                self.current_offset();
                self.flush()?;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
    fn flush(&mut self) -> Result<(), StorageError> {
        self.log.flush()?;
        Ok(())
    }
    fn current_offset(&mut self) {
        self.current_offset = self.log.writer.position;
    }
    fn check_split(&self, entry_size: u64) -> Result<bool, StorageError> {
        match (self.current_offset + entry_size) > self.segment_size {
            true => Err(StorageError::NoSpaceLeft),
            false => Ok(true),
        }
    }
    /// read the  message or data from the given index position
    fn read_at(&mut self, position: usize) -> Result<Vec<u8>, StorageError> {
        let data = self.log.seek_at(position)?;
        Ok(data)
    }
    /// read the next message or data from the index position
    fn read_next(&mut self, position: usize) -> Result<Vec<u8>, StorageError> {
        let data = self.log.seek_next(position)?;
        Ok(data)
    }
    /// reads the message at the current read offset position
    fn read_from_start(&mut self) -> Result<Vec<u8>, StorageError> {
        let data = self.log.seek_from_start()?;
        Ok(data)
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
            roffset: 0,
            max_size,
        }
    }
    fn load(dir: PathBuf, pos: u32, offset: u64, max_size: usize) -> Index {
        let idx_file = OpenOptions::new()
            .read(true)
            .append(true)
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
            roffset: 0,
            max_size,
        }
    }
    fn write(&mut self, entry: &Entry) -> Result<usize, StorageError> {
        let size = (&mut self.mmap[self.offset..]).write(&entry.as_bytes())?;
        self.offset(size);
        Ok(size)
    }
    fn offset(&mut self, size: usize) {
        self.offset += size;
    }
    fn flush(&mut self) -> Result<(), StorageError> {
        self.mmap.flush()?;
        Ok(())
    }

    // seek the entry data at a specific offset
    fn seek_at(&mut self, offset: usize) -> Result<Entry, StorageError> {
        if offset.overflowing_sub(ENTRY_SIZE).1 || offset > self.offset {
            return Err(StorageError::LogIndexOutofBound);
        }
        Ok(Entry::from_bytes(&self.mmap[(offset - ENTRY_SIZE)..offset]))
    }

    // seek the entry data at a specific offset
    fn seek_from_start(&mut self) -> Result<Entry, StorageError> {
        if self.roffset >= self.offset {
            Err(StorageError::LogIndexOutofBound)
        } else {
            self.roffset += 8;
            Ok(Entry::from_bytes(
                &self.mmap[(self.roffset - ENTRY_SIZE)..self.roffset],
            ))
        }
    }
    // seek next entry data after a specific offseet
    fn seek_after(&self, offset: usize) -> Result<Entry, StorageError> {
        Ok(Entry::from_bytes(&self.mmap[offset..(offset + ENTRY_SIZE)]))
    }
    // seek the entry data at the current writer cursor position / offset
    fn seek_current(&self) -> Entry {
        Entry::from_bytes(&self.mmap[self.offset - ENTRY_SIZE..(self.offset)])
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
        let path = dir.join(format!("{pos:0>12}.log"));
        let offsets_path = format!("{}/{}", dir.as_path().to_str().unwrap(), "offsets");
        fs::create_dir_all(offsets_path).unwrap();
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&path)
        {
            Ok(file) => {
                let read_file = OpenOptions::new().read(true).open(&path)?;
                Ok(Log {
                    writer: CursorWriter::new(file, SeekFrom::Start(0))?,
                    reader: CursorReader::new(read_file)?,
                    index: Index::new(dir, pos, SEGMENT_SIZE), //TODO Index: max_size should be a setting
                })
            }
            Err(err) => Err(StorageError::IoError(err)),
        }
    }
    fn load(dir: &PathBuf, pos: u32, offset: u64, index_len: u64) -> Result<Log, StorageError> {
        let path = dir.join(format!("{pos:0>12}.log"));
        match OpenOptions::new().append(true).read(true).open(&path) {
            Ok(file) => {
                let read_file = OpenOptions::new().read(true).open(&path)?;
                Ok(Log {
                    writer: CursorWriter::new(file, SeekFrom::End(0))?,
                    reader: CursorReader::new(read_file)?,
                    index: Index::load(dir.to_owned(), pos, offset, index_len as usize),
                })
            }
            Err(err) => Err(StorageError::IoError(err)),
        }
    }
    fn write(&mut self, data: &[u8], entry: &Entry) -> Result<(), StorageError> {
        self.index.write(entry)?;
        self.writer.write(data)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.writer.flush()?;
        self.index.flush()?;
        Ok(())
    }
    fn seek_at(&mut self, offset: usize) -> Result<Vec<u8>, StorageError> {
        let entry = self.index.seek_at(offset)?;
        let mut buf = vec![0u8; entry.size as usize];
        self.reader.read_at(&mut buf[..], (entry.offset) as u64)?;
        Ok(buf[..entry.size as usize].to_vec())
    }

    fn seek_from_start(&mut self) -> Result<Vec<u8>, StorageError> {
        let entry = self.index.seek_from_start()?;
        let mut buf = vec![0u8; entry.size as usize];
        self.reader.read_at(&mut buf[..], (entry.offset) as u64)?;
        Ok(buf.to_vec())
    }
    fn seek_next(&mut self, offset: usize) -> Result<Vec<u8>, StorageError> {
        let entry = self.index.seek_after(offset)?;
        let mut buf = vec![0u8; entry.size as usize];
        self.reader.read_at(&mut buf[..], (entry.offset) as u64)?;
        Ok(buf.to_vec())
    }

    // TODO: Test This when too man files are created and not closed
    // fn close(&self) {
    //     drop(self.file)
    // }
}

#[cfg(test)]
mod test {
    #![allow(unused_imports)]

    use crate::internal::log::{load_segments_from_disk, CursorReader, Entry, SEGMENT_SIZE};

    use super::{Segment, StorageError};
    use std::cell::RefCell;
    use std::io::Read;
    use std::ops::Div;
    use std::panic;
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::Once;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use std::{
        env::temp_dir,
        os::{self},
    };
    use std::{fs::File, path};
    const PATH: &str = "test_data";
    use std::{sync::Mutex, thread::sleep};
    #[test]
    fn test_create_log() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u64);
        let data = b"Hello World!";
        seg.append_data(data).unwrap();
        assert_eq!(seg.current_offset, data.len() as u64)
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
        let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u64);
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

        let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u64);
        let data = b"Hello World!";
        let data2 = b"Hello World!1";
        seg.append_data(data).unwrap();
        seg.append_data(data2).unwrap();
        let read_data = seg.read_at(8).unwrap();
        let read_current_data = seg.read_at(16).unwrap();
        assert_eq!(read_data, data);
        assert_eq!(read_current_data, data2);
    }
    #[test]
    #[should_panic]
    fn test_log_read_at_panic() {
        let offset = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();

        let mut seg = Segment::new(PATH, offset, SEGMENT_SIZE as u64);
        let data = b"Hello World!";
        seg.append_data(data).unwrap();
        seg.read_at(0).unwrap();
    }
}
