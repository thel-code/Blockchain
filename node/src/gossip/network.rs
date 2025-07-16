use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpStream};
use std::thread;
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
                add_thread(stream);
            } else {
                eprintln!("Failed to connect to peer: {}", peer);
            }
        }

        Network { peers: streams  }
    }
    pub fn add_stream(&mut self, stream: TcpStream) {
        println!("New peer incoming: {}", stream.peer_addr().unwrap());
        self.peers.push(stream.try_clone().unwrap());
        add_thread(stream);
    }



    pub fn broadcast(&self, message: &[u8]) {
        for mut peer in &self.peers {
            if let Err(e) = peer.write_all(message) {
                eprintln!("Failed to send message to {}: {}", peer.peer_addr().unwrap(), e);
            }
        }
    }
}

pub fn add_thread(mut stream: TcpStream) {
    thread::spawn(move || {
        let mut buffer = [0; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => {
                    println!("Connection closed by peer: {}", stream.peer_addr().unwrap());
                    break;
                }
                Ok(n) => {
                    println!("Received {} bytes from peer: {}", n, stream.peer_addr().unwrap());
                    // Process the data received from the peer
                }
                Err(e) => {
                    eprintln!("Failed to read from stream: {}", e);
                    break;
                }
            }
        }
    });
}


