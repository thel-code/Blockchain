use std::net::{SocketAddrV4, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;
use crate::gossip::network::Network;

pub fn start(sock_addr: SocketAddrV4, network: Arc<Mutex<Network>>) {
    let tcp_listener =  TcpListener::bind(sock_addr).unwrap();
    println!("Gossip listener started on {}", sock_addr);

    thread::spawn(move || {
        for stream in tcp_listener.incoming() {
            match stream {
                Ok(stream) => {
                    network.lock().unwrap().add_stream(stream);
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
            }
        }
    });

}