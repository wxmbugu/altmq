#![allow(unused_imports)]
use core::time::Duration;
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::result;
use std::sync::{Arc, Mutex, RwLock};
pub mod internal;

pub use crate::internal::messages::*;
pub use crate::internal::queue::*;

pub type Result<T> = result::Result<T, ()>;

#[derive(Debug)]
pub struct Server {
    pub stream: TcpStream,
    pub caches: Arc<RwLock<HashMap<String, Queue>>>,
}

impl Server {
    pub fn new(stream: TcpStream, caches: Arc<RwLock<HashMap<String, Queue>>>) -> Server {
        Server { stream, caches }
    }
    pub fn run(&mut self, buffer: &mut [u8]) {
        loop {
            let bytes_read = match self.stream.read(buffer) {
                Ok(0) => {
                    println!("INFO: Connection closed by the client");
                    break;
                }
                Ok(n) => n,
                Err(err) => match err.kind() {
                    ErrorKind::WouldBlock => {
                        eprintln!("EROR: would have blocked");
                        break;
                    }
                    _ => panic!("Got an error: {}", err),
                },
            };
            let message = decode(&buffer[..bytes_read]).unwrap();
            self.match_command(message);
        }
    }
    pub fn match_command(&mut self, message: Message) {
        match Commands::from_u8(message.command.unwrap()) {
            Commands::QUIT => {
                self.stream
                    .shutdown(std::net::Shutdown::Both)
                    .expect("shutdown call failed");
            }
            Commands::SUBSCRIBE => {
                if let Some(name) = message.queue {
                    if let Some(queue) = self.caches.try_read().unwrap().get(name) {
                        for message in &queue.message {
                            self.stream.write_all(message).unwrap();
                        }
                    }
                }
            }
            Commands::PUBLISH => {
                if let Some(name) = message.queue {
                    let mut queue = Queue::new(name.to_string());
                    if let Some(message) = message.message {
                        queue.message.insert(message.to_vec());
                        self.caches
                            .try_write()
                            .unwrap()
                            .insert(name.to_string(), queue);
                    }
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
