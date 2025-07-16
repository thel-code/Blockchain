use std::net::SocketAddrV4;
use std::str::FromStr;
use crate::client_gateway;

pub mod listener;
mod client_transaction;

pub fn init(socket_addr: String) {
    let client_gateway_socket_addr = SocketAddrV4::from_str(&*socket_addr)
        .expect("Invalid client gateway address format");

    let client_gateway_listener = listener::create_listener(client_gateway_socket_addr)
        .expect("Failed to create client gateway listener");

    listener::create_listener_thread(client_gateway_listener)
        .join().expect("Failed to join client gateway thread"); 
}