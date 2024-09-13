use std::fmt::Display;

#[derive(PartialEq, Debug, Clone)]
pub struct BinaryHeader {
    pub command: u32,
    pub length: u32,
    pub payload_length: u32,
    pub queue_name: Option<String>,
    pub payload: Option<Vec<u8>>,
}
#[derive(Debug, PartialEq)]
pub struct Topic {
    pub id: u32,
    pub length: u32,
    pub timestamp: u64,
    pub message: Vec<u8>,
}

impl BinaryHeader {
    pub fn new(command: u32, queue_name: Option<String>, payload: Option<Vec<u8>>) -> BinaryHeader {
        let payload_length = payload.clone().map(|s| s.len()).unwrap_or(0) as u32;
        let queue_name_len = queue_name.clone().map(|s| s.len()).unwrap_or(0) as u32;

        BinaryHeader {
            command,
            length: 12 + payload_length + queue_name_len,
            payload,
            queue_name,
            payload_length,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut binary: Vec<u8> = Vec::new();
        let command = self.command.to_be_bytes();
        let length = self.length.to_be_bytes();
        let payload_length = self.payload_length.to_be_bytes();
        binary.append(&mut command.to_vec());
        binary.append(&mut length.to_vec());
        binary.append(&mut payload_length.to_vec());
        if let Some(queue_name) = &self.queue_name {
            let data = queue_name.as_bytes();
            binary.append(&mut data.to_vec())
        };

        if let Some(data) = &self.payload {
            binary.extend(data);
        }
        binary
    }
    pub fn from_bytes(data: &[u8]) -> BinaryHeader {
        let command = u32::from_be_bytes(data[..4].try_into().unwrap());
        let length = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let payload_length = u32::from_be_bytes(data[8..12].try_into().unwrap());
        let queue_name_end_pos: usize = (length - payload_length) as usize;
        let mut _queue_name = None;
        let mut payload = None;
        if payload_length > queue_name_end_pos as u32 {
            _queue_name = Some(String::from_utf8(data[12..queue_name_end_pos].to_vec()).unwrap());
            payload = Some(data[queue_name_end_pos..].to_vec());
        } else {
            _queue_name = Some(String::from_utf8(data[12..queue_name_end_pos].to_vec()).unwrap());
        }
        BinaryHeader {
            command,
            length,
            payload_length,
            queue_name: _queue_name,
            payload,
        }
    }
}

impl Topic {
    pub fn new(id: u32, timestamp: u64, message: Vec<u8>) -> Topic {
        Topic {
            id,
            length: 16 + message.len() as u32,
            timestamp,
            message,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut payload: Vec<u8> = Vec::with_capacity(
            self.id.to_be_bytes().len()
                + self.length.to_be_bytes().len()
                + self.timestamp.to_be_bytes().len()
                + self.message.len(),
        );

        payload.extend(&self.id.to_be_bytes());
        payload.extend(&self.length.to_be_bytes());
        payload.extend(&self.timestamp.to_be_bytes());
        payload.extend(&self.message);

        payload
    }

    pub fn from_bytes(data: &[u8]) -> Topic {
        let id = u32::from_be_bytes(data[..4].try_into().unwrap());
        let length = u32::from_be_bytes(data[4..8].try_into().unwrap());
        let timestamp = u64::from_be_bytes(data[8..16].try_into().unwrap());
        let message: Vec<u8> = data[16..length as usize].to_vec();
        Topic {
            id,
            length,
            timestamp,
            message,
        }
    }
}

#[derive(Debug)]
#[repr(u16)]
pub enum ResponseCode {
    Ok = 0,
    Err = 1,
}
#[derive(Debug)]
pub enum ResponseMessage {
    // no body to attach to response header
    EmptyResponse = 0,
    // no new messages from queue
    NoNewMessages = 1,
    // responseheader with Message(Topic) body
    ResponseWithBody = 2,
    // responseheader with string message body
    ResponseWithMessage = 3,
    // responseheader with string error body
    ErrorResponse = 4,
    // error specifying queue_name is required
    QueueNameRequired = 5,

    // error specifying queue_name is required
    MessageBodyRequired = 6,
    UNKNOWN,
}

impl Display for ResponseMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[derive(PartialEq, Debug)]
pub struct Response {
    pub response_code: u16,
    pub response_message: u16,
    pub response_length: u32,
    pub response_data: Option<Vec<u8>>,
}

impl Response {
    pub fn new(
        response_code: ResponseCode,
        response_message: ResponseMessage,
        response_data: Option<Vec<u8>>,
    ) -> Self {
        let len_data = response_data.clone().map(|s| s.len()).unwrap_or(0);

        Self {
            response_code: response_code as u16,
            response_message: response_message as u16,
            response_length: 8 + len_data as u32,
            response_data,
        }
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let len_data = self.response_data.clone().map(|s| s.len());
        let mut payload: Vec<u8> = Vec::with_capacity(
            self.response_code.to_be_bytes().len()
                + self.response_message.to_be_bytes().len()
                + self.response_length.to_be_bytes().len()
                + len_data.unwrap_or(0),
        );
        payload.extend(&self.response_code.to_be_bytes());
        payload.extend(&self.response_message.to_be_bytes());
        payload.extend(&self.response_length.to_be_bytes());
        if let Some(data) = &self.response_data {
            payload.extend(data);
        }
        payload
    }
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut response_data = None;
        let response_code = u16::from_be_bytes(data[..2].try_into().unwrap());
        let response_message = u16::from_be_bytes(data[2..4].try_into().unwrap());
        let response_length = u32::from_be_bytes(data[4..8].try_into().unwrap());
        if response_length > 8 {
            response_data = Some(data[8..response_length as usize].to_vec());
        }
        Self {
            response_code,
            response_message,
            response_length,
            response_data,
        }
    }
}

impl ResponseMessage {
    pub fn new() -> Option<Self> {
        None
    }

    pub fn from_u16(value: u16) -> ResponseMessage {
        match value {
            1 => ResponseMessage::NoNewMessages,
            _ => ResponseMessage::UNKNOWN,
        }
    }
}

mod test {
    #![allow(unused_imports)]
    use crate::internal::protocol::*;
    #[test]
    fn tcp_header_byte_test() {
        let message = BinaryHeader::new(
            1,
            Some("new".to_string()),
            Some(b"random byte data".to_vec()),
        );
        assert_eq!(message, BinaryHeader::from_bytes(&message.to_bytes()));
    }
    #[test]
    fn tcp_header_byte_test_without_data() {
        let message = BinaryHeader::new(1, Some("new".to_string()), None);
        assert_eq!(message, BinaryHeader::from_bytes(&message.to_bytes()));
    }

    #[test]
    fn payload_byte_test() {
        let topic = Topic::new(1, 1718709072, b"Hello World!".to_vec());
        assert_eq!(topic, Topic::from_bytes(&topic.to_bytes()));
    }
    #[test]
    fn response_byte_test_with_data() {
        let message = Response::new(
            ResponseCode::Ok,
            ResponseMessage::NoNewMessages,
            Some(b"No new messages".to_vec()),
        );
        assert_eq!(message, Response::from_bytes(&message.to_bytes()));
    }
    #[test]
    fn response_byte_test_without_data() {
        let message = Response::new(ResponseCode::Ok, ResponseMessage::NoNewMessages, None);
        assert_eq!(message, Response::from_bytes(&message.to_bytes()));
    }
}
