#![allow(dead_code)]
use mq::MessageQueueClient;
use mq::Result;
use std::io;

static ADDR: &str = "127.0.0.1:9000";

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    tracing::subscriber::set_global_default(tracing_subscriber::FmtSubscriber::new())
        .expect("setting default subscriber failed");

    let mut id = 0;
    let mut queue = MessageQueueClient::dial(ADDR).await?;
    loop {
        queue
            .publish("adventure", format!("Hello World-{id}").as_bytes())
            .await
            .unwrap();

        id += 1;
        // if id == 10 {
        //     break;
        // }
    }
    // Ok(())
}
