use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex};
use crate::common::types::{NetworkEvent};
use crate::config::NetworkConfig;
use crate::common::types::{Message, NetworkChangeEvent};
use crate::network::manager::NetworkManager;

pub mod manager;
mod listener;
mod connector;
mod peer_handler;

pub fn init(
    network_channel_receiver: mpsc::Receiver<NetworkEvent>,
    network_channel_sender: mpsc::Sender<NetworkEvent>,
    incoming_message_sender: mpsc::Sender<Message>,
    network_state_sender: broadcast::Sender<NetworkChangeEvent>,
    network_config: NetworkConfig,
) {
    let mut network_manager = manager::NetworkManager::new(
        network_channel_receiver,
        network_channel_sender.clone(),
        incoming_message_sender,
        network_state_sender,
        network_config.heartbeat_interval,
        network_config.heartbeat_timeout,
    );



    let listener = listener::Listener::new(
        network_config.node_listener_address,
        network_channel_sender.clone(),
    );

    tokio::spawn(async move {
        network_manager.run().await;
    });

    tokio::spawn(async move {
        connector::connect(network_config.peers_addresses, network_channel_sender.clone()).await;
    });

    tokio::spawn(async move {
        listener.run().await.expect("Could not run listener");
    });
}