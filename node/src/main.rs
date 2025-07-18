mod config;
mod common;
mod network;
mod transaction;
mod client_gateway;
mod messaging;

use std::str::FromStr;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, Level};
use tracing_subscriber::FmtSubscriber;
use common::types::Message;
use common::types::NodeEvent;
#[tokio::main]
async fn main() {
    let config = config::load().expect("Failed to load configuration");

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::from_str(&*config.logging.level).unwrap())
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    info!("Application starting up!");
    info!("Logging level: {}", config.logging.level);

    let (node_event_sender, node_event_receiver) = mpsc::channel::<NodeEvent>(100);
    let (incoming_message_sender, incoming_message_receiver) = mpsc::channel::<Message>(100);
    let (network_state_sender, network_state_receiver) = broadcast::channel(100);

    let network_manager = network::init(
        node_event_receiver,
        node_event_sender.clone(),
        incoming_message_sender.clone(),
        network_state_sender,
        config.network,
    );

    let mut message_manager = messaging::manager::MessageManager::new(
        incoming_message_receiver,
        network_manager.clone(),
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    tokio::spawn(async move {
        message_manager.broadcast().await;
    });

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}