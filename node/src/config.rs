use std::net::SocketAddrV4;
use config::Config;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub network: NetworkConfig,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub peers_addresses: Vec<SocketAddrV4>,
    pub client_gateway_address: SocketAddrV4,
    pub gossip_listener_address: SocketAddrV4,
}

pub fn load() -> AppConfig {
    let config = Config::builder()
        .add_source(config::File::with_name("Config"))
        .build()
        .unwrap()
        .try_deserialize::<AppConfig>()
        .unwrap();
    config
}
