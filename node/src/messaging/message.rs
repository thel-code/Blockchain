use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use bincode::{Decode, Encode};
use tokio::sync::Mutex;
use crate::common::types::Transaction;
use crate::network::manager::NetworkManager;

#[derive(Debug, Serialize, Deserialize, Encode , Decode, Clone)]
pub enum Message {
    Heartbeat,
    Transaction(Transaction),
}

pub struct MessageManager {
    pub receiver: tokio::sync::mpsc::Receiver<Message>,
    pub network_manager: Arc<Mutex<NetworkManager>> ,
    pub message_cache: HashMap<u64, Message>,
}
