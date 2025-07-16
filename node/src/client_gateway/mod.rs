use std::net::SocketAddrV4;
use std::str::FromStr;
use crate::client_gateway;

pub mod listener;
mod client_transaction;

pub fn init(addr: SocketAddrV4)  {
    let listener = listener::create(addr).unwrap();
    listener::run_thread(listener);
}