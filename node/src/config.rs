use std::net::SocketAddrV4;
use config::Config;
use serde::Deserialize;
use tokio::time;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub network: NetworkConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
pub struct LoggingConfig {
    pub level: String
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub peers_addresses: Vec<SocketAddrV4>,
    pub client_gateway_address: SocketAddrV4,
    pub node_listener_address: SocketAddrV4,
    #[serde(with = "humantime_serde")]
    pub heartbeat_interval: time::Duration,
    #[serde(with = "humantime_serde")]
    pub heartbeat_timeout: time::Duration,
}

pub fn load() -> Result<AppConfig, config::ConfigError> {
     Config::builder()
        .add_source(config::File::with_name("Config"))
        .build()?
        .try_deserialize::<AppConfig>()
}
