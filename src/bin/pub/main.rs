#![allow(dead_code)]
use mq::MessageQueueClient;
use mq::Result;
use std::io;
use std::time::Duration;
use tokio::time::sleep;

static ADDR: &str = "127.0.0.1:8000";

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let mut id = 1;
    loop {
        let mut queue = MessageQueueClient::dial(ADDR).await?;

        queue
            .publish("queue", format!("Hello World!{id}").as_bytes())
            .await
            .unwrap();
        id += 1;
        sleep(Duration::from_secs(1)).await;
    }
}
