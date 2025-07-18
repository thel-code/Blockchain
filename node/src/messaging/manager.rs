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
    pub message_cache: HashMap<u64, Message>,
}

impl MessageManager {
    pub fn new(
        receiver: tokio::sync::mpsc::Receiver<Message>,
    ) -> Self {
        MessageManager {
            receiver,
            message_cache: HashMap::new(),
        }
    }

}