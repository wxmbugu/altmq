use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{io, result, u8};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub mod internal;

pub use crate::internal::messages::*;
pub use crate::internal::protocol::*;
pub use crate::internal::queue::*;

pub type Result<T, E> = result::Result<T, E>;

#[derive(Debug)]
pub struct Server {
    pub stream: Arc<TcpStream>,
    // pub client: HashMap<i32, Client>,
    pub jobs: Arc<RwLock<HashMap<String, HashSet<Vec<u8>>>>>,
}

#[derive(Debug)]
pub struct Client {
    pub ip: String,
}
impl Server {
    pub fn new(
        stream: Arc<TcpStream>,
        jobs: Arc<RwLock<HashMap<String, HashSet<Vec<u8>>>>>,
    ) -> Server {
        Server {
            stream,
            jobs,
            // client: HashMap::new(),
        }
    }
    pub fn decode_buffer(&mut self, buffer: Vec<u8>) {
        let tcp_header = BinaryHeader::from_bytes(buffer);
        self.handle_client_command(tcp_header.command, tcp_header.payload)
    }
    pub fn handle_client_command(&mut self, command: u32, payload: Vec<u8>) {
        let payload = Topic::from_bytes(payload);
        match Commands::from_u8(command) {
            Commands::QUIT => {}
            Commands::SUBSCRIBE => {
                let name = payload.topic_name;
                loop {
                    if let Some(mut queue) = self.jobs.write().unwrap().remove(&name) {
                        for messages in &queue {
                            let mut data: Vec<u8> = vec![];
                            data.push(messages.len() as u8);
                            data.append(&mut messages.to_owned());
                            self.stream.try_write(&data).unwrap();
                            println!("INFO: RECEIVED MESSAGE TO TOPIC: {}", name);
                        }
                        queue.clear();
                    }
                }
            }
            Commands::PUBLISH => {
                let name = payload.topic_name;
                self.jobs
                    .write()
                    .unwrap()
                    .entry(name.to_string())
                    .or_default()
                    .insert(payload.message);
                println!("INFO: PUBLISHED MESSAGE TO TOPIC:{name}");
            }
            _ => {
                eprintln!("NO SUCH COMMAND");
            }
        }
    }
    pub fn reset(&mut self) {
        self.jobs = Arc::new(RwLock::new(HashMap::new()));
    }
}

pub struct MessageQueueClient {
    stream: Arc<Mutex<TcpStream>>,
}

impl MessageQueueClient {
    pub async fn dial(server_address: &str) -> Result<MessageQueueClient, io::Error> {
        let stream = Arc::new(Mutex::new(TcpStream::connect(server_address).await?));
        Ok(Self { stream })
    }

    pub async fn publish(&mut self, queue_name: &str, message: &[u8]) -> Result<(), io::Error> {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let topic = Topic::new(id, 1718709072, message.to_vec(), queue_name.to_string());
        let payload = BinaryHeader::new(2, topic.to_bytes());
        self.send_message(payload.to_bytes()).await
    }
    pub async fn subscribe(&mut self, queue_name: &str) -> Result<(), std::io::Error> {
        let stream = self.stream.clone();
        let mut buffer = [0; 1024];
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let topic = Topic::new(id, 1718709072, b"Random".to_vec(), queue_name.to_string());
        let payload = BinaryHeader::new(1, topic.to_bytes());

        match self.send_message(payload.to_bytes()).await {
            Ok(()) => {
                loop {
                    let mut stream_lock = stream.lock().await;
                    match stream_lock.try_read(&mut buffer[..1]) {
                        Ok(0) => {
                            println!("INFO: Connection closed by the server");
                            break;
                        }
                        Ok(_bytes_read) => {
                            let length = buffer[0] as usize;
                            match read(&mut stream_lock, &mut buffer[..length]).await {
                                Ok(message_length) => {
                                    let received_data =
                                        String::from_utf8_lossy(&buffer[..message_length]);
                                    println!("Received data: {}", received_data);
                                }
                                Err(err) => {
                                    if err.kind() == std::io::ErrorKind::WouldBlock {
                                        continue;
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            if err.kind() == std::io::ErrorKind::WouldBlock {
                                continue;
                            } else {
                                eprintln!("ERROR: An unexpected error occurred: {:?}", err);
                                break;
                            }
                        }
                    }
                }

                Ok(())
            }
            Err(err) => Err(err),
        }
    }
    async fn send_message(&self, payload: Vec<u8>) -> Result<(), std::io::Error> {
        match self.stream.lock().await.try_write(&payload) {
            Ok(_n) => Ok(()),
            Err(err) => {
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    Err(err)
                } else {
                    eprintln!("ERROR: Failed to write to stream: {:?}", err);
                    Err(err)
                }
            }
        }
    }
}
pub async fn read(stream: &mut TcpStream, buffer: &mut [u8]) -> Result<usize, io::Error> {
    let read_bytes = stream.read_exact(buffer).await?;
    Ok(read_bytes)
}
