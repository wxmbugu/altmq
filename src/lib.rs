use internal::log::{CommitLog, StorageError, SEGMENT_SIZE};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::sync::{Arc, PoisonError};
use std::{io, result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{error, info};

pub mod internal;

pub use crate::internal::commands::*;
pub use crate::internal::protocol::*;

pub type Result<T, E> = result::Result<T, E>;
const DIR_PATH: &str = "storage/queue/";
pub struct Server {
    pub stream: Arc<TcpStream>,
    messages: Arc<RwLock<HashMap<String, CommitLog>>>,
}

#[derive(Debug)]
pub struct Client {
    pub ip: String,
    pub read_position: u64,
}
#[derive(Debug)]
pub enum ServerError {
    PoisonError,
    Error(io::Error),
    NoNewMessages,
}
impl<T> From<PoisonError<T>> for ServerError {
    fn from(_err: PoisonError<T>) -> Self {
        Self::PoisonError
    }
}

impl From<io::Error> for ServerError {
    fn from(error: io::Error) -> Self {
        ServerError::Error(error)
    }
}

async fn send_response_ok(
    stream: &mut Arc<TcpStream>,
    resp_message: ResponseMessage,
    data: Option<Vec<u8>>,
) -> Result<usize, ServerError> {
    let resp = Response::new(ResponseCode::Ok, resp_message, data);
    let usize = stream.try_write(&resp.to_bytes())?;
    Ok(usize)
}

async fn send_response_err(
    stream: &mut Arc<TcpStream>,
    resp_message: ResponseMessage,
    data: Option<Vec<u8>>,
) -> Result<usize, ServerError> {
    let resp = Response::new(ResponseCode::Err, resp_message, data);
    let usize = stream.try_write(&resp.to_bytes())?;
    Ok(usize)
}
impl Server {
    pub fn new(
        stream: Arc<TcpStream>,
        messages: Arc<RwLock<HashMap<String, CommitLog>>>,
    ) -> Server {
        Server { stream, messages }
    }
    pub async fn decode_buffer(
        &mut self,
        command: u32,
        payload: Option<Vec<u8>>,
        queue_name: Option<String>,
    ) {
        self.handle_client_command(command, payload, queue_name)
            .await
    }
    pub async fn handle_client_command(
        &mut self,
        command: u32,
        data: Option<Vec<u8>>,
        queue_name: Option<String>,
    ) {
        if queue_name.is_none() {
            if let Err(e) =
                send_response_err(&mut self.stream, ResponseMessage::QueueNameRequired, None).await
            {
                error!("ERROR: Failed to write response to stream: {:?}", e);
                return;
            }
        }
        match Commands::from_u32(command) {
            Commands::QUIT => {}
            Commands::SUBSCRIBE => {
                let name = queue_name.unwrap();
                let mut message_map = self.messages.write().await;
                if let Some(commit_log) = message_map.get_mut(&name) {
                    let data = match commit_log.read() {
                        Ok(data) => Some(data),
                        Err(_) => None,
                    };
                    let mut message = ResponseMessage::ResponseWithBody;
                    if data.is_none() || data.clone().unwrap().is_empty() {
                        message = ResponseMessage::NoNewMessages;
                    }
                    if let Err(e) = send_response_ok(&mut self.stream, message, data).await {
                        error!("ERROR: Failed to write response to stream: {:?}", e);
                        return;
                    }
                    info!("INFO: SUBSCRIBED TO TOPIC: {name}");
                } else {
                    info!("WARN: No commit log found for topic: {name}");
                }
            }
            Commands::PUBLISH => {
                if data.is_none() {
                    if let Err(e) = send_response_err(
                        &mut self.stream,
                        ResponseMessage::MessageBodyRequired,
                        None,
                    )
                    .await
                    {
                        error!("ERROR: Failed to write response to stream: {:?}", e);
                        return;
                    }
                }
                let name = queue_name.unwrap();
                let queue_exists = self.messages.read().await.contains_key(&name);
                match queue_exists {
                    true => {
                        self.save_to_queue(&name, &data.unwrap()).await;
                        info!("INFO: PUBLISHED MESSAGE TO TOPIC:{name}");
                    }
                    false => {
                        match self.create_new_queue(&name, &data.unwrap()).await {
                            Ok(_) => info!("INFO: PUBLISHED MESSAGE TO NEW TOPIC:{name}"),
                            Err(e) => error!("{:?}", e),
                        };
                    }
                }
                if let Err(e) =
                    send_response_ok(&mut self.stream, ResponseMessage::EmptyResponse, None).await
                {
                    error!("ERROR: Failed to write response to stream: {:?}", e);
                }
            }
            _ => {
                error!("NO SUCH COMMAND");
            }
        }
    }
    async fn save_to_queue(&mut self, queue: &str, data: &[u8]) {
        let mut messages = self.messages.write().await;
        if let Some(topic) = messages.get_mut(queue) {
            topic.save_to_disk(data).unwrap();
        }
    }
    async fn create_new_queue(
        &mut self,
        queue_name: &str,
        payload: &[u8],
    ) -> Result<(), ServerError> {
        let mut log = CommitLog::new(queue_name, SEGMENT_SIZE as u64, DIR_PATH);
        log.save_to_disk(payload).unwrap();
        self.messages
            .write()
            .await
            .insert(queue_name.to_owned(), log);
        Ok(())
    }
    pub async fn restore_from_disk(mut messages: Arc<RwLock<HashMap<String, CommitLog>>>) {
        match CommitLog::restore_from_disk(SEGMENT_SIZE as u64, DIR_PATH) {
            Ok(logs) => {
                for log in logs {
                    let mut messages = messages.borrow_mut().write().await;
                    info!("loading topic  name:{}, path:{:?}", &log.name, log.dir_path);
                    messages.insert(log.name.to_owned(), log);
                }
            }
            Err(e) => {
                if e.to_string() == StorageError::IoError(io::Error::last_os_error()).to_string() {
                    info!("No topic created: {e:?}");
                } else {
                    error!("an error occurred:{e}")
                }
            }
        }
    }
}

pub struct MessageQueueClient {
    stream: TcpStream,
}

impl MessageQueueClient {
    pub async fn dial(server_address: &str) -> Result<MessageQueueClient, io::Error> {
        let stream = TcpStream::connect(server_address).await.unwrap();
        Ok(Self { stream })
    }

    pub async fn publish(&mut self, queue_name: &str, message: &[u8]) -> Result<(), io::Error> {
        let topic = Topic::new(1, 1718709072, message.to_vec());
        let payload = BinaryHeader::new(2, Some(queue_name.to_string()), Some(topic.to_bytes()));
        self.send_message(payload.to_bytes()).await?;
        info!("Payload sent: {payload:?}");
        Ok(())
    }

    pub async fn subscribe(&mut self, queue_name: &str) -> Result<(), std::io::Error> {
        let (mut rx, mut tx) = self.stream.split();
        let payload = BinaryHeader::new(1, Some(queue_name.to_string()), None);
        match tx.write(&payload.to_bytes()).await {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                error!("Connection closed by server: {:?}", e);
            }
            Err(e) => return Err(e),
        }
        let mut buffer = vec![0; 1024];
        match rx.read(&mut buffer).await {
            Ok(bytes_read) if bytes_read > 0 => {
                let resp = Response::from_bytes(&buffer[..bytes_read]);
                info!("{:?}", resp);
            }
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::BrokenPipe => {
                error!("Connection closed by server: {:?}", e);
            }
            Err(e) => return Err(e),
        }
        Ok(())
    }

    async fn send_message(&mut self, payload: Vec<u8>) -> Result<(), std::io::Error> {
        match self.stream.write(&payload).await {
            Ok(_n) => Ok(()),
            Err(err) => {
                if err.kind() == std::io::ErrorKind::WouldBlock {
                    Err(err)
                } else {
                    error!("ERROR: Failed to write to stream: {:?}", err);
                    Err(err)
                }
            }
        }
    }
}
