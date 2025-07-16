use std::str::FromStr;

mod client_gateway;
mod gossip;
mod config;
mod macros;

fn main() {
    let config = config::load();

    let network = gossip::init(config.network.peers_addresses, config.network.gossip_listener_address);

    client_gateway::init(config.network.client_gateway_address);

    network.lock().unwrap().broadcast(b"Hello, world!");

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
