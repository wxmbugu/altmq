#![allow(dead_code)]
use minicbor::decode::Error;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::{io, result, u8};
use tokio::net::TcpStream;
use tokio::time::Duration;
pub mod internal;

pub use crate::internal::messages::*;
pub use crate::internal::queue::*;

pub type Result<T, E> = result::Result<T, E>;

#[derive(Debug)]
pub struct Server {
    pub stream: Arc<TcpStream>,
    pub client: HashMap<i32, Client>,
    pub jobs: Arc<RwLock<HashMap<String, HashSet<Vec<u8>>>>>,
}

#[derive(Debug)]
pub struct Client {
    pub ip: String,
}
unsafe fn any_as_u8_slice<T: Sized>(p: &T) -> &[u8] {
    ::core::slice::from_raw_parts((p as *const T) as *const u8, ::core::mem::size_of::<T>())
}
impl Server {
    pub fn new(
        stream: Arc<TcpStream>,
        jobs: Arc<RwLock<HashMap<String, HashSet<Vec<u8>>>>>,
    ) -> Server {
        Server {
            stream,
            jobs,
            client: HashMap::new(),
        }
    }
    pub fn decode_buffer(&mut self, buffer: &mut [u8]) -> Result<(), Error> {
        let message = decode(buffer);
        match message {
            Ok(message) => {
                self.handle_client_command(message);
                Ok(())
            }
            Err(e) => {
                eprintln!("ERROR: WRONG MESSAGE FORMAT {:?}", e);
                Err(e)
            }
        }
    }
    pub fn handle_client_command(&mut self, message: Message) {
        match Commands::from_u8(message.command.unwrap()) {
            Commands::QUIT => {}
            Commands::SUBSCRIBE => {
                if let Some(name) = message.queue {
                    if let Some(queue) = self.jobs.read().unwrap().get(name) {
                        for messages in queue {
                            let mut data: Vec<u8> = vec![];
                            data.push(messages.len() as u8);
                            data.append(&mut messages.to_owned());
                            self.stream.try_write(&data).unwrap();
                            println!("INFO: RECEIVED MESSAGE TO TOPIC:{name}");
                        }
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
}

pub struct MessageQueueClient {
    stream: Arc<TcpStream>,
}

impl MessageQueueClient {
    pub async fn dial(server_address: &str) -> Result<MessageQueueClient, io::Error> {
        let stream = Arc::new(TcpStream::connect(server_address).await?);
        Ok(Self { stream })
    }

    pub fn publish(&mut self, queue_name: &str, message: &[u8]) -> Result<(), io::Error> {
        let message = Message::new(2, queue_name, message);
        let mut buffer = [0; 1024];
        self.send_message(message, &mut buffer)
    }
    pub async fn subscribe(&mut self, queue_name: &str) -> Result<(), std::io::Error> {
        let stream = self.stream.clone();
        let message = Message::new(1, queue_name, &[]);
        let mut buffer = [0; 1024];

        match self.send_message(message, &mut buffer) {
            Ok(()) => {
                tokio::spawn(async move {
                    loop {
                        match stream.try_read(&mut buffer[..1]) {
                            Ok(0) => {
                                println!("INFO: Connection closed by the server");
                                break;
                            }
                            Ok(_bytes_read) => {
                                let length = buffer[0] as usize;
                                match stream.try_read(&mut buffer[..length]) {
                                    Ok(message_length) => {
                                        let received_data =
                                            String::from_utf8_lossy(&buffer[..message_length]);
                                        println!("Received data: {}", received_data);
                                    }
                                    Err(err) => {
                                        if err.kind() == std::io::ErrorKind::WouldBlock {
                                            tokio::time::sleep(Duration::from_secs(5)).await;
                                            continue;
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                if err.kind() == std::io::ErrorKind::WouldBlock {
                                    tokio::time::sleep(Duration::from_secs(5)).await;
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
    // async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
    //     let result = self.stream.read_exact(buf).await;
    //     if let Err(error) = result {
    //         return Err(error);
    //     }
    //
    //     Ok(result.unwrap())
    // }
    fn send_message(&self, message: Message<'_>, buffer: &mut [u8]) -> Result<(), std::io::Error> {
        let message_buffer = construct_message(message.to_owned(), buffer);
        match self.stream.try_write(message_buffer) {
            Ok(_n) => Ok(()),
            Err(err) => {
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    // Handle WouldBlock error here
                    Err(err)
                } else {
                    eprintln!("ERROR: Failed to write to stream: {:?}", err);
                    Err(err)
                }
            }
        }
    }
}
fn construct_message<'a>(message: Message<'a>, buffer: &'a mut [u8]) -> &'a mut [u8] {
    message.encode(buffer);
    buffer
}
