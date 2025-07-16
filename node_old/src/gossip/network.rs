use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpStream};
use std::thread;
use crate::handle_socket;

pub struct  Network {
    pub peers: Vec<TcpStream>,
}
impl Network {
    pub fn new(peers: Vec<SocketAddrV4>) -> Self {
        let mut streams = Vec::new();
        for peer in peers {
            if let Ok(stream) = TcpStream::connect(peer) {
                println!("Connected to peer: {}", peer);
                streams.push(stream.try_clone().unwrap());
                handle_socket!(stream, gossip_handler);
            } else {
                eprintln!("Failed to connect to peer: {}", peer);
            }
        }

        Network { peers: streams  }
    }
    pub fn add_stream(&mut self, stream: TcpStream) {
        println!("New peer incoming: {}", stream.peer_addr().unwrap());
        self.peers.push(stream.try_clone().unwrap());
        handle_socket!(stream, gossip_handler);
    }

    pub fn broadcast(&self, message: &[u8]) {
        for mut peer in &self.peers {
            if let Err(e) = peer.write_all(message) {
                eprintln!("Failed to send message to {}: {}", peer.peer_addr().unwrap(), e);
            }
        }
    }
}

fn gossip_handler(buffer: &[u8]) {
    match str::from_utf8(buffer) {
        Ok(gossip_message) => {
            println!("Gossip message: {}", gossip_message);
        }
        Err(e) => {
            eprintln!("Received invalid UTF-8 gossip message: {}", e);
        }
    }
}
