use core::time::Duration;
use mq::Message;
use std::io::prelude::*;
use std::net::TcpStream;

fn construct_message<'a>(message: Message<'a>, buffer: &'a mut [u8]) -> &'a mut [u8] {
    message.encode(buffer);
    buffer
}

fn read_messages(stream: &mut TcpStream) -> std::io::Result<()> {
    let message = Message {
        command: Some(1),
        queue: Some("dsfkaslf"),
        message: None,
    };
    let mut buffer = [0; 1024];

    let message_buffer = construct_message(message, &mut buffer);
    match stream.write_all(message_buffer) {
        Ok(_) => {
            match stream.read(&mut buffer) {
                Ok(result) => {
                    println!("{:?},{:?}", buffer, result);
                    Ok(())
                }
                Err(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    // Handle WouldBlock error here
                    println!("WouldBlock error. Sleeping or retrying...");
                    Ok(())
                }
                Err(err) => Err(err),
            }
        }
        Err(err) => Err(err),
    }
}
static ADDR: &str = "127.0.0.1:8000";

fn main() {
    let mut stream = TcpStream::connect(ADDR).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(1000)))
        .unwrap();

    // loop {
    read_messages(&mut stream).unwrap();
    // std::thread::sleep(std::time::Duration::from_millis(10));
    // }
}
