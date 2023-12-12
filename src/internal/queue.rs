use std::collections::HashSet;

#[derive(Default, Debug)]
pub struct Queue {
    pub name: String,
    pub message: HashSet<Vec<u8>>,
}

impl Queue {
    pub fn new(name: String) -> Queue {
        Queue {
            name,
            message: HashSet::new(),
        }
    }
}
