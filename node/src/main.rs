use std::net::SocketAddrV4;
use std::str::FromStr;

mod client_gateway;
mod gossip_gateway;
mod config;

fn main() {
    let config = config::load();
    
    gossip_gateway::init(config.network.seed_addresses);
    
    client_gateway::init(config.network.client_gateway_address);
}
