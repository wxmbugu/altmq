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

    let mut id = 0;
    let mut queue = MessageQueueClient::dial(ADDR).await?;
    loop {
        queue
            .publish("new", format!("Hello World-{id}").as_bytes())
            .await
            .unwrap();

        id += 1;
        sleep(Duration::from_millis(100)).await;

        // if id == 10 {
        // break;
        // }
    }
    // Ok(())
}
