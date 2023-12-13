use mq::{Result, Server};
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};

const PORT: u32 = 8000;
const BUFFER: usize = 1024;
fn main() -> Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT)).unwrap();

    println!("INFO: listening on port:{}", PORT);
    let caches = Arc::new(RwLock::new(HashMap::new()));
    let buffer = Arc::new(Mutex::new([0; BUFFER]));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let caches_clone = Arc::clone(&caches);
                let mut server = Server::new(stream, caches_clone);
                let buffer_clone = Arc::clone(&buffer);
                std::thread::spawn(move || {
                    server.run(&mut *buffer_clone.lock().unwrap());
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {}", e);
            }
        }
    }
    Ok(())
}
