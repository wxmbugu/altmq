use std::{collections::HashSet, net::TcpStream};

#[derive(Default, Debug)]
pub struct Queue {
    pub name: String,
    // pub client: Option<Client>,
    pub message: HashSet<Vec<u8>>,
}
pub struct Client {
    pub stream: TcpStream,
}

impl Queue {
    pub fn new(name: String) -> Queue {
        Queue {
            name,
            // client: None,
            message: HashSet::new(),
        }
    }
    pub fn publish() {}
    pub fn subscribe() {}
}
impl Client {
    pub fn new(stream: TcpStream) -> Client {
        Client { stream }
    }
}
