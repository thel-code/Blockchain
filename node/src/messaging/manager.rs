use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;
use tracing::info;
use crate::common::types::Transaction;
use crate::common::types::Message;
use crate::network::manager::NetworkManager;
use crate::common::types::PeerCommand;

pub struct MessageManager {
    pub receiver: tokio::sync::mpsc::Receiver<Message>,
    pub network_manager: Arc<Mutex<NetworkManager>> ,
    pub message_cache: HashMap<u64, Message>,
}

impl MessageManager {
    pub fn new(
        receiver: tokio::sync::mpsc::Receiver<Message>,
        network_manager: Arc<Mutex<NetworkManager>>,
    ) -> Self {
        MessageManager {
            receiver,
            network_manager,
            message_cache: HashMap::new(),
        }
    }
    pub async fn broadcast(&mut self) {
        info!("Broadcasting messages to all peers...");
        let peer_senders = self.network_manager.lock().await.get_peer_senders();
        info!("Sending {} messages to all peers...", peer_senders.len());
        for sender in peer_senders {
            info!("Broadcasting to peer: {:?}", sender.0);
            let message = Message::Transaction(Transaction {
                from : "node1".to_string(),
                to: "node2".to_string(),
                amount: 100,
            });
            sender.1.send(PeerCommand::SendMessage(message.clone())).await.unwrap();
        }

    }
}