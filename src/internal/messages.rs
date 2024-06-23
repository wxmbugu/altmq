use std::fmt::Display;

use serde_derive::{Deserialize, Serialize};
#[derive(Debug)]
#[repr(u32)]
pub enum Commands {
    QUIT = 0,
    SUBSCRIBE = 1,
    PUBLISH = 2,
    PING = 3,
    UNKNOWN(String),
}

impl Display for Commands {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let command_str = match self {
            Commands::UNKNOWN(s) => s.clone(),
            _ => format!("{:?}", self).to_lowercase(),
        };
        write!(f, "{}", command_str)
    }
}
impl Commands {
    pub fn new() -> Option<Commands> {
        None
    }

    pub fn from_u8(value: u32) -> Commands {
        match value {
            0 => Commands::QUIT,
            1 => Commands::SUBSCRIBE,
            2 => Commands::PUBLISH,
            _ => Commands::UNKNOWN(format!("Unknown command: {}", value)),
        }
    }
}

#[derive(Default, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub command: Option<u8>,
    pub queue: Option<String>,
    pub message: Option<Vec<u8>>,
}

impl Message {
    pub fn new(command: u8, queue: String, message: Vec<u8>) -> Message {
        Message {
            command: Some(command),
            queue: Some(queue),
            message: Some(message),
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

pub fn decode(bytes: Vec<u8>) -> Message {
    bincode::deserialize(&bytes).unwrap()
}

mod test {
    #![allow(unused_imports)]
    use crate::internal::messages::{decode, Message};
    #[test]
    fn byte_encode_decode() {
        let bytes = vec![1, 2, 3];
        let message = Message::new(1, "random".to_string(), bytes);
        let buffer = message.encode();
        let output = decode(buffer.to_vec());
        assert_eq!(message, output);
    }
}
