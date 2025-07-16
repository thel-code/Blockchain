use config::Config;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub network: NetworkConfig,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    pub seed_addresses: Vec<String>,
    pub client_gateway_address: String,
}

pub fn load() -> AppConfig {
    let config = Config::builder()
        .add_source(config::File::with_name("Config"))
        .build()
        .unwrap()
        .try_deserialize::<AppConfig>()
        .unwrap();
    println!("Loaded configuration: {:?}", config);
    config
}
