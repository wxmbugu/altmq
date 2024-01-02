use mq::{Result, Server};
use std::collections::{HashMap, HashSet};
use std::io::{self, ErrorKind};
use std::sync::{Arc, RwLock};
use tokio::net::{TcpListener, TcpStream};

const PORT: u32 = 8000;
const BUFFER: usize = 1024;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT)).await?;
    println!("INFO: listening on port:{}", PORT);
    let caches = Arc::new(RwLock::new(HashMap::new()));
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let caches_clone = Arc::clone(&caches);
                tokio::spawn(async move {
                    handle_incoming_connection(Arc::new(stream), caches_clone);
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
}

fn handle_incoming_connection(
    stream: Arc<TcpStream>,
    caches: Arc<RwLock<HashMap<String, HashSet<Vec<u8>>>>>,
) {
    let mut buffer = [0; BUFFER];
    loop {
        match stream.try_read(&mut buffer) {
            Ok(0) => {
                println!("INFO: Connection closed by the client");
                break;
            }
            Ok(bytes_read) => {
                let mut server = Server::new(stream.clone(), caches.clone());
                match server.decode_buffer(&mut buffer[..bytes_read]) {
                    Ok(_) => {
                        continue;
                    }
                    Err(e) => {
                        eprintln!("ERROR: Decode error occured: {:?},{:?}", e, buffer);
                        break;
                    }
                }
            }
            Err(err) => match err.kind() {
                ErrorKind::WouldBlock => {
                    continue;
                }
                _ => {
                    eprintln!("ERROR: Got an unexpected error: {:?}", err);
                    break;
                }
            },
        };
    }
}
