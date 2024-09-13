use mq::internal::log::CommitLog;
use mq::{BinaryHeader, Result, Server};
use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{error, info};

const PORT: u32 = 9000;
const BUFFER: usize = 1024;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::new())
        .expect("setting default subscriber failed");
    let listener = TcpListener::bind(format!("127.0.0.1:{}", PORT)).await?;
    info!("INFO: listening on port:{}", PORT);
    let messages = Arc::new(RwLock::new(HashMap::new()));
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let messages_clone = Arc::clone(&messages);
                tokio::spawn(async move {
                    handle_incoming_connection(Arc::new(stream), messages_clone).await;
                });
            }
            Err(e) => {
                error!("Error accepting connection: {}", e);
            }
        }
    }
}

async fn handle_incoming_connection(
    stream: Arc<TcpStream>,
    messages: Arc<RwLock<HashMap<String, CommitLog>>>,
) {
    loop {
        let mut buffer = [0; BUFFER];
        match stream.try_read(&mut buffer) {
            Ok(bytes_read) => {
                if bytes_read < 1 {
                    break;
                } else {
                    let mut server = Server::new(stream.clone(), messages.clone());
                    let length = buffer[7];
                    let tcp_header = BinaryHeader::from_bytes(&buffer[..length as usize]);
                    server
                        .decode_buffer(
                            tcp_header.command,
                            tcp_header.payload,
                            tcp_header.queue_name,
                        )
                        .await;
                }
            }
            Err(err) => match err.kind() {
                ErrorKind::WouldBlock => {
                    continue;
                }
                _ => {
                    error!("ERROR: Got an unexpected error: {:?}", err);
                    return;
                }
            },
        };
    }
}
