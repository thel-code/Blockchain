use std::string::String;
use std::io::Write;
use std::net::SocketAddrV4;
use std::str::FromStr;
use bincode::{config::standard, encode_to_vec};

mod transaction;
mod node_gateway;

fn main() {
    // Initialize the node gateway with a specific address
    let node_gateway_socket_addr = SocketAddrV4::new(
        std::net::Ipv4Addr::new(127, 0, 0, 1),
        8080,
    );

    let mut socket = node_gateway::create_stream(node_gateway_socket_addr).unwrap();

        let transaction = transaction::Transaction {
            from: String::from("Test1"),
            to: String::from("Test2"),
            amount: 100,
        };
        /*socket.write_all(&encode_to_vec(&transaction, standard())
            .expect("Failed to serialize transaction"))
            .expect("Failed to send transaction");*/
    loop {
        // do nothing here for now
        std::thread::sleep(std::time::Duration::from_secs(1));
    }



}
