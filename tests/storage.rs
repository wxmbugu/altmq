use mq::internal::log::{load_segments_from_disk, CommitLog};
use std::path::PathBuf;

const PATH: &str = "test_data";

#[test]
fn test_load_from_disk() {
    let path_str = format!("{PATH}/segments");
    let path = PathBuf::from(&path_str);
    let num_segments = 6;
    let queue_name = "test";
    let data = b"Hello World!";
    let segment_size = data.len() as u64;
    let storage = CommitLog::new(queue_name, segment_size, path_str.to_owned());
    create_segments(num_segments, storage, data);
    let segments = load_segments_from_disk(path.to_str().unwrap().to_string());

    assert_eq!(segments.len(), num_segments as usize);
}
fn create_segments(no_segments: u32, mut store: CommitLog, data: &[u8]) {
    let mut next_seg = 0;

    loop {
        if next_seg == no_segments {
            break;
        }
        store.save_to_disk(data).unwrap();
        next_seg += 1;
    }
}
