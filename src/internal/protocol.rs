use std::u32;

#[derive(PartialEq, Debug, Clone)]
pub struct BinaryHeader {
    pub command: u32,
    pub length: u32,
    pub payload: Vec<u8>,
}
#[derive(Debug, PartialEq)]
pub struct Topic {
    pub id: u32,
    pub length: u32,
    pub timestamp: u64,
    pub topic_name: String,
    pub message: Vec<u8>,
}

impl BinaryHeader {
    pub fn new(command: u32, payload: Vec<u8>) -> BinaryHeader {
        BinaryHeader {
            command,
            length: 8 + payload.len() as u32,
            payload,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut binary: Vec<u8> = Vec::new();
        let command = self.command.to_be_bytes();
        let length = self.length.to_be_bytes();
        let mut payload = self.payload.to_owned();
        binary.append(&mut command.to_vec());
        binary.append(&mut length.to_vec());
        binary.append(&mut payload);
        binary
    }
    pub fn from_bytes(data: Vec<u8>) -> BinaryHeader {
        let command = u32::from_be_bytes(data[..4].try_into().unwrap());
        let length = u32::from_be_bytes(data[4..8].try_into().unwrap());
        BinaryHeader {
            command,
            length,
            payload: data[8..].to_vec(),
        }
    }
}

impl Topic {
    pub fn new(id: u32, timestamp: u64, message: Vec<u8>, topic_name: String) -> Topic {
        Topic {
            id,
            length: message.len() as u32,
            timestamp,
            topic_name,
            message,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut payload: Vec<u8> = Vec::with_capacity(
            self.id.to_be_bytes().len()
                + self.length.to_be_bytes().len()
                + self.timestamp.to_be_bytes().len()
                + self.topic_name.len()
                + self.message.len(),
        );

        payload.extend(&self.id.to_be_bytes());
        payload.extend(&self.length.to_be_bytes());
        payload.extend(&self.timestamp.to_be_bytes());
        payload.extend(self.topic_name.as_bytes());
        payload.extend(&self.message);

        payload
    }
    pub fn from_bytes(data: Vec<u8>) -> Topic {
        let id = u32::from_be_bytes(data[..4].try_into().unwrap());
        let length = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let timestamp = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let topic_length = data.len() - length as usize;
        let topic_name = String::from_utf8(data[16..topic_length].to_vec()).unwrap();
        let message: Vec<u8> = data[topic_length..].to_vec();
        Topic {
            id,
            length,
            timestamp,
            topic_name,
            message,
        }
    }
}

mod test {
    #![allow(unused_imports)]
    use crate::internal::protocol::*;
    #[test]
    fn tcp_header_byte_test() {
        let message = BinaryHeader::new(1, b"random byte data".to_vec());
        assert_eq!(message, BinaryHeader::from_bytes(message.to_bytes()));
    }
    #[test]
    fn payload_byte_test() {
        let topic = Topic::new(
            1,
            1718709072,
            b"Hello World!".to_vec(),
            "topic_name".to_string(),
        );
        assert_eq!(topic, Topic::from_bytes(topic.to_bytes()));
    }
}
