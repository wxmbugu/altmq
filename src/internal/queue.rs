use std::collections::HashSet;

#[derive(Default, Debug)]
pub struct Queue {
    pub message: HashSet<Vec<u8>>,
}

impl Queue {
    pub fn new() -> Queue {
        Queue {
            message: HashSet::new(),
        }
    }
}
