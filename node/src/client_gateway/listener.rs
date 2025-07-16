use std::{io, thread};
use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use std::thread::JoinHandle;
use bincode::{config, decode_from_slice};
use crate::client_gateway::client_transaction::ClientTransaction;

pub fn create_listener(s_addr : SocketAddrV4) -> io::Result<TcpListener> {
    let listener = TcpListener::bind(s_addr)?;
    Ok(listener)
}

pub fn create_listener_thread(listener: TcpListener) -> JoinHandle<()> {
    return thread::spawn(move || {
        println!("Listening on {}", listener.local_addr().unwrap());
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("New connection: {}", stream.peer_addr().unwrap());
                    handle_client(stream);
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
            }
        }
        println!("Listener closed.");
    });
}

fn handle_client(p0: TcpStream) {
    thread::spawn(move || {
        let mut stream = p0;
        let mut buffer = [0; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => {
                    println!("Connection closed by client: {}", stream.peer_addr().unwrap());
                    break;
                }
                Ok(n) => {
                    let (transaction, _): (ClientTransaction, _) = decode_from_slice(&buffer[..n], config::standard())
                        .expect("Failed to deserialize");
                    add_transaction(transaction);
                }
                Err(e) => {
                    eprintln!("Failed to read from stream: {}", e);
                    break;
                }
            }
        }
    });
}

fn add_transaction(transaction: ClientTransaction) {
    println!("Received transaction: {:?}", transaction);
}