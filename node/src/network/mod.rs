use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use manager::NodeEvent;
use crate::config::NetworkConfig;
use crate::messaging::message::Message;
use crate::network::manager::NetworkManager;

pub mod manager;
mod listener;
mod connector;
mod peer_handler;

pub fn init(
    node_event_receiver: mpsc::Receiver<NodeEvent>,
    node_event_sender: mpsc::Sender<NodeEvent>,
    incoming_message_sender: mpsc::Sender<Message>,
    network_config: NetworkConfig,
) -> Arc<Mutex<NetworkManager>> {
    let network_manager = manager::NetworkManager::new(
        node_event_receiver,
        node_event_sender.clone(),
        incoming_message_sender,
        network_config.heartbeat_interval,
        network_config.heartbeat_timeout,
    );
    let arc_mut_network_manager = Arc::new(Mutex::new(network_manager));
    let arc_mut_clone_network_manager = arc_mut_network_manager.clone();
    
    

    let listener = listener::Listener::new(
        network_config.node_listener_address,
        node_event_sender.clone(),
    );

    tokio::spawn(async move {
        arc_mut_clone_network_manager.lock().await.run().await;
    });

    tokio::spawn(async move {
        connector::connect(network_config.peers_addresses, node_event_sender.clone()).await;
    });

    tokio::spawn(async move {
        listener.run().await.expect("Could not run listener");
    });
   arc_mut_network_manager 
}