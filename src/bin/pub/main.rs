#![allow(dead_code)]
use mq::MessageQueueClient;
use mq::Result;
use std::io;

static ADDR: &str = "127.0.0.1:8000";

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let mut queue = MessageQueueClient::dial(ADDR).await?;
    queue.publish("queue", b"Hello World!").await.unwrap();
    Ok(())
}
