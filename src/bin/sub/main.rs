#![allow(dead_code)]
use mq::MessageQueueClient;
use mq::Result;
use std::io;
use tokio::sync::mpsc;

static ADDR: &str = "127.0.0.1:8000";

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let mut queue = MessageQueueClient::dial(ADDR).await?;

    let (tx, mut rx) = mpsc::channel::<()>(1);

    tokio::spawn(async move {
        loop {
            if tx.send(()).await.is_err() {
                eprintln!("ERROR: Failed to send notification");
            }
        }
    });

    while rx.recv().await.is_some() {
        if let Err(err) = queue.subscribe("queue").await {
            eprintln!("ERROR: Failed to subscribe: {:?}", err);
        }
    }

    Ok(())
}
