use mq::internal::log::{load_segments_from_disk, Storage};
use std::path::PathBuf;

const PATH: &str = "test_data";
#[test]
fn test_data_larger_than_segment_size() {
    let path_str = format!("{PATH}/storage");
    let queue_name = "test";
    let segment_size = 3;
    let mut storage = Storage::new(queue_name, segment_size, path_str);
    let data = b"Hello World!";
    storage.save_to_disk(queue_name, data);
    let sm = storage.segments.get_mut(queue_name).unwrap();
    let num_segments = data.len().div_ceil(segment_size as usize);
    assert_eq!(sm.len(), num_segments);
}

#[test]
fn test_load_from_disk() {
    let path_str = format!("{PATH}/segments");
    let path = PathBuf::from(&path_str);
    let num_segments = 6;
    let queue_name = "test";
    let segment_size = 12;
    let storage = Storage::new(queue_name, segment_size, path_str.to_owned());
    create_segments(num_segments, storage, queue_name);
    let segments = load_segments_from_disk(path.to_str().unwrap().to_string());
    assert_eq!(segments.len(), num_segments as usize);
}
fn create_segments(no_segments: u32, mut store: Storage, queue_name: &str) {
    let mut next_seg = 0;

    loop {
        if next_seg == no_segments {
            break;
        }
        let data = b"Hello World!";
        store.save_to_disk(queue_name, data);
        next_seg += 1;
    }
}
