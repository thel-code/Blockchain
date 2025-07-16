use std::{io, thread};
use std::io::{Read};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
use bincode::{config, decode_from_slice};
use crate::client_gateway::client_transaction::ClientTransaction;
use crate::handle_socket;

pub fn create(s_addr : SocketAddrV4) -> io::Result<TcpListener> {
    let listener = TcpListener::bind(s_addr)?;
    Ok(listener)
}

pub fn run_thread(listener: TcpListener)  {
    thread::spawn(move || {
        println!("Client Listener started on {}", listener.local_addr().unwrap());
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("New connection: {}", stream.peer_addr().unwrap());
                    handle_socket!(stream, client_handler)
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
            }
        }
        println!("Listener closed.");
    });
}


fn client_handler(buffer: &[u8]) {
    match decode_from_slice::<ClientTransaction, _>(buffer, config::standard()) {
        Ok((transaction, _)) => {
            println!("Received transaction: {:?}", transaction);
        }

        Err(e) => {
            eprintln!("Error deserializing transaction: {}", e);
        }
    }
}