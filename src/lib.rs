use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::{io, result, u8};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub mod internal;

pub use crate::internal::messages::*;
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
        let message = decode(buffer);
        self.handle_client_command(message);
    }
    pub fn handle_client_command(&mut self, message: Message) {
        match Commands::from_u8(message.command.unwrap()) {
            Commands::QUIT => {}
            Commands::SUBSCRIBE => {
                if let Some(name) = message.queue {
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
                if let Some(name) = message.queue {
                    self.jobs
                        .write()
                        .unwrap()
                        .entry(name.to_string())
                        .or_default()
                        .insert(message.message.unwrap().to_vec());
                    println!("INFO: PUBLISHED MESSAGE TO TOPIC:{name}");
                }
            }
            Commands::ACK => {
                println!("{:?}", message.queue);
            }
            Commands::NACK => {
                println!("{:?}", message.queue);
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
        let message = Message::new(2, queue_name.to_string(), message.to_vec());
        self.send_message(message).await
    }
    pub async fn subscribe(&mut self, queue_name: &str) -> Result<(), std::io::Error> {
        let stream = self.stream.clone();
        let message = Message::new(1, queue_name.to_string(), [].to_vec());
        let mut buffer = [0; 1024];
        match self.send_message(message).await {
            Ok(()) => {
                tokio::spawn(async move {
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
                });

                Ok(())
            }
            Err(err) => Err(err),
        }
    }
    async fn send_message(&self, message: Message) -> Result<(), std::io::Error> {
        let message_buffer = construct_message(message);

        match self.stream.lock().await.try_write(&message_buffer) {
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
fn construct_message(message: Message) -> Vec<u8> {
    message.encode().to_vec()
}
