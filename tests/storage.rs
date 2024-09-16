use mq::{internal::log::CommitLog, Topic};

const PATH: &str = "test_data";

#[test]
fn test_load_from_disk() {
    let path_str = format!("{PATH}/segments/");
    let num_segments = 6;
    let queue_name = "test";
    let data = b"Hello World!";
    let segment_size = data.len() as u64;
    let storage = CommitLog::new(queue_name, segment_size, &path_str);
    create_segments(num_segments, storage, data);
    let log = CommitLog::restore_from_disk(segment_size, &path_str).unwrap();
    // assert_eq!(log.segments.len(), num_segments as usize);
    // assert_eq!(log.name, queue_name);
}
#[test]
fn test_load_from_storage() {
    let path_str = "storage/queue/";
    let segment_size = 10 * 1024 * 1024;
    let storage = CommitLog::restore_from_disk(segment_size, path_str).unwrap();
    // println!("info ===> {:?}", storage);
    for store in storage {
        println!("{:?}", store.name);
    }
    // for log in storage {
    //     let topic = Topic::from_bytes(&log);
    //     println!(
    //         "{:?},{:?}",
    //         topic,
    //         String::from_utf8(topic.message.to_vec())
    //     );
    // }
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
