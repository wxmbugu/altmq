#![allow(dead_code)]
use core::time::Duration;
use mq::Message;
use std::io::prelude::*;
use std::net::TcpStream;

fn construct_message<'a>(message: Message<'a>, buffer: &'a mut [u8]) -> &'a mut [u8] {
    message.encode(buffer);
    buffer
}
fn send_messages(stream: &mut TcpStream) -> std::io::Result<()> {
    let message = Message {
        command: Some(2),
        queue: Some("dsfkaslf"),
        message: Some("Hello from the other world! This is mars dumbshit".as_bytes()),
        // message:None,
    };
    let mut buffer = [0; 512];
    let message_buffer = construct_message(message, &mut buffer);

    stream.write_all(message_buffer)?;
    Ok(())
}

fn read_messages(stream: &mut TcpStream) -> std::io::Result<()> {
    let message = Message {
        command: Some(1),
        queue: Some("dsfkaslf"),
        message: None,
    };
    let mut buffer = [0; 1024];

    let message_buffer = construct_message(message, &mut buffer);
    stream.write_all(message_buffer)?;

    let result = stream.read(&mut buffer)?;
    println!("{:?},{:?}", result.to_string(), buffer);
    Ok(())
}
static ADDR: &str = "127.0.0.1:8000";

fn main() {
    let mut stream = TcpStream::connect(ADDR).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(1000)))
        .unwrap();

    // let mut i = 0;
    // loop {
    send_messages(&mut stream).unwrap();
    // read_messages(&mut stream).unwrap();

    // std::thread::sleep(std::time::Duration::from_millis(10));
    // if i == 1 {
    // break;
    // }
    // i += 1;
    // }
}
