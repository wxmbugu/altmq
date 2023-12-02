#![allow(dead_code)]
use minicbor::{Decode, Encode};
use std::fmt::Display;

#[derive(Debug)]
#[repr(u8)]
pub enum Commands {
    SUBSCRIBE = 1,
    PUBLISH = 2,
    ACK = 3,
    NACK = 4,
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

    pub fn from_u8(value: u8) -> Commands {
        match value {
            1 => Commands::SUBSCRIBE,
            2 => Commands::PUBLISH,
            _ => Commands::UNKNOWN(format!("Unknown command: {}", value)),
        }
    }
}

#[derive(Debug, Default, Encode, Decode, PartialEq, Clone)]
pub struct Message<'a> {
    #[n(0)]
    pub command: Option<u8>,
    #[n(1)]
    pub queue: Option<&'a str>,
    #[cbor(n(2), with = "minicbor::bytes")]
    pub message: Option<&'a [u8]>,
}
impl<'a> Message<'a> {
    pub fn new(command: u8, queue: &'a str, message: &'a [u8]) -> Message<'a> {
        Message {
            command: Some(command),
            queue: Some(queue),
            message: Some(message),
        }
    }
    pub fn encode(&self, buffer: &mut [u8; 1024]) {
        minicbor::encode(self, buffer.as_mut()).unwrap();
    }
}

pub fn decode(buffer: &[u8; 1024]) -> Message<'_> {
    let output: Message = minicbor::decode(buffer).unwrap();
    output.to_owned()
}
mod test {
    #![allow(unused_imports)]
    use crate::{decode, Message};
    #[test]
    fn cbor_encode_decode() {
        let bytes = vec![1, 2, 3];
        let message = Message::new(1, "random", bytes.as_slice());
        let mut buffer = [0; 1024];
        message.encode(&mut buffer);
        let output = decode(&buffer);
        assert_eq!(message, output);
    }
}
