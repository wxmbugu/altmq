#![allow(dead_code)]
use mq::MessageQueueClient;
use mq::Result;
use std::io;

static ADDR: &str = "127.0.0.1:8000";

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let mut queue = MessageQueueClient::dial(ADDR).await?;
    if let Err(err) = queue.subscribe("queue").await {
        eprintln!("ERROR: Failed to subscribe: {:?}", err);
    }
    Ok(())
}
