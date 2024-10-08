#![allow(dead_code)]
use mq::MessageQueueClient;
use mq::Result;
use std::io;
use tokio::time::sleep;
use tokio::time::Duration;

static ADDR: &str = "127.0.0.1:9000";

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::new())
        .expect("setting default subscriber failed");

    let mut queue = MessageQueueClient::dial(ADDR).await?;
    loop {
        queue.subscribe("new").await?;
        sleep(Duration::from_millis(100)).await;
    }
}
