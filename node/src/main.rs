mod config;
mod common;
mod network;
mod transaction;
mod client_gateway;
mod messaging;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::str::FromStr;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;
use common::types::Message;
use crate::common::types::{NetworkEvent, NodeInfo, PeerCommand, Transaction};

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

    let (network_channel_sender, network_channel_receiver) = mpsc::channel::<NetworkEvent>(100);
    let (incoming_message_sender, incoming_message_receiver) = mpsc::channel::<Message>(100);
    let (network_state_sender, network_state_receiver) = broadcast::channel(100);

    network::init(
        network_channel_receiver,
        network_channel_sender.clone(),
        incoming_message_sender.clone(),
        network_state_sender,
        config.network,
    );

     messaging::manager::MessageManager::new(
        incoming_message_receiver,
    );

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    let (channel_sender, channel_receiver) = oneshot::channel::<HashMap<SocketAddrV4, NodeInfo>>();

    network_channel_sender.send(NetworkEvent::GetNodes(channel_sender))
        .await
        .expect("Failed to send GetNodes event");

    match channel_receiver.await {
        Ok(nodes) => {
            info!("Current nodes in the network:");
            for (addr, node_info) in nodes.iter() {
                // send a transaction to each node
                let transaction = Transaction {
                    from: "sender_address".to_string(),
                    to: "receiver_address".to_string(),
                    amount: 100,
                };
                for node in nodes.values() {
                    let sender = node.sender.clone();
                    let message = Message::Transaction(transaction.clone());
                    if let Err(e) = sender.send(PeerCommand::SendMessage(message)).await {
                        error!("Failed to send transaction to node {}: {:?}", addr, e);
                    } else {
                        info!("Transaction sent to node: {}", addr);
                    }
                }

            }
        }
        Err(e) => {
            error!("Failed to receive nodes: {:?}", e);
        }
    }



    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}