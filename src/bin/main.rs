#![allow(unused_imports)]
#![allow(dead_code)]
use mq::Queue;
use mq::{decode, Message};
use std::io;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};

fn handle_client(mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop {
        let bytes_read = match stream.read(&mut buffer) {
            Ok(0) => {
                println!("Connection closed by the client");
                break;
            }
            Ok(n) => n,
            Err(err) => {
                eprintln!("Error reading from stream: {:?}", err);
                break;
            }
        };

        let received_data = String::from_utf8_lossy(&buffer[..bytes_read]);
        println!("Received data: {}", received_data);

        if received_data.trim() == "quit" {
            println!("Received exit command. Closing the connection.");
            break;
        }
        let command = sanitize_commands(&received_data);
        println!("Sanitized command: {}", command);

        let cmd = sanitize_numeric_command(&received_data);
        println!("Sanitized numeric command: {}", cmd);

        if let Err(err) = stream.write_all(&buffer[..bytes_read]) {
            eprintln!("Error writing to stream: {:?}", err);
            break;
        }
    }
}
fn sanitize_commands(command: &str) -> Commands {
    match command.trim().to_lowercase().as_str() {
        "subscribe" => Commands::SUBSCRIBE,
        "send" => Commands::SEND,
        // "topic" => Commands::TOPIC,
        // "message" => Commands::MESSAGE,
        _ => Commands::UNKNOWN(command.to_string()),
    }
}
fn sanitize_numeric_command(received_data: &str) -> Commands {
    match received_data.trim().parse::<u8>() {
        Ok(num) => Commands::from_u8(num),
        Err(_) => Commands::UNKNOWN(format!("Invalid numeric command: {}", received_data)),
    }
}
static PORT: &str = "127.0.0.1:8000";

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(PORT)?;
    println!("listening on: {}", PORT);
    for stream in listener.incoming() {
        let stream = stream?;
        std::thread::spawn(|| handle_client(stream));
    }
    Ok(())
}
