mod network;
mod listener;

use std::net::{SocketAddrV4, };
use std::sync::{Arc, Mutex};
use crate::gossip::network::Network;

pub fn init(peers: Vec<SocketAddrV4>, gossip_listeners_address : SocketAddrV4) -> Arc<Mutex<Network>> {
    let network = Arc::new(Mutex::new(Network::new(peers)));
    listener::start(gossip_listeners_address, network.clone());
    network
}
