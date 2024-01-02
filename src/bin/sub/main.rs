#![allow(dead_code)]
use mq::MessageQueueClient;
use mq::Result;
use std::io;
use std::time::Duration;
use tokio::sync::mpsc;

static ADDR: &str = "127.0.0.1:8000";

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let mut queue = MessageQueueClient::dial(ADDR).await?;

    // Create a channel for receiving notifications
    let (tx, mut rx) = mpsc::channel::<()>(1);

    // Spawn a task to handle incoming messages
    tokio::spawn(async move {
        loop {
            // Simulate receiving a message
            // Replace this with your actual logic to check for new messages
            tokio::time::sleep(Duration::from_millis(10)).await;

            // Notify the main task that there is new data
            if tx.send(()).await.is_err() {
                // Handle channel send error
                eprintln!("ERROR: Failed to send notification");
            }
        }
    });

    // Main loop to wait for notifications
    while rx.recv().await.is_some() {
        // Perform processing when a notification is received
        if let Err(err) = queue.subscribe("queue").await {
            eprintln!("ERROR: Failed to subscribe: {:?}", err);
        }
    }

    Ok(())
}
