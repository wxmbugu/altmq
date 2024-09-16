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
    Server::restore_from_disk(messages.clone()).await;
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
    let mut leftover_data: Vec<u8> = Vec::new();
    let mut server = Server::new(stream.clone(), messages.clone());

    loop {
        let mut buffer = vec![0u8; BUFFER];
        let mut all_requests = Vec::new();

        match stream.try_read(&mut buffer) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    break;
                }
                all_requests.extend_from_slice(&leftover_data);
                all_requests.extend_from_slice(&buffer[..bytes_read]);
                let mut offset = 0;
                while offset < all_requests.len() {
                    if all_requests.len() - offset < 8 {
                        break;
                    }
                    // length of request
                    let length =
                        u32::from_be_bytes(all_requests[4..8].try_into().unwrap()) as usize;
                    if offset + length > all_requests.len() {
                        break;
                    }
                    let message_data = &all_requests[offset..offset + length];
                    let tcp_header = BinaryHeader::from_bytes(message_data);
                    server
                        .decode_buffer(
                            tcp_header.command,
                            tcp_header.payload,
                            tcp_header.queue_name,
                        )
                        .await;

                    offset += tcp_header.length as usize;
                }
                leftover_data.clear();
                leftover_data = all_requests[offset..].to_vec();
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
