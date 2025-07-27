use std::collections::{HashMap, HashSet};
use std::net::SocketAddrV4;
use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio::net::TcpStream;

#[derive(Debug, Serialize, Deserialize, Encode, Decode, Clone)]
pub struct Transaction {
    pub from: String,
    pub to: String,
    pub amount: u64,
}

pub enum PeerCommand {
    SendMessage(Message),
}

#[derive(Debug, Serialize, Deserialize, Encode , Decode, Clone)]
pub enum Message {
    Heartbeat,
    Transaction(Transaction),
}


#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub enum Recipient {
    All,
    Specific(HashSet<SocketAddrV4>),
}

#[derive(Debug, Serialize, Deserialize, Encode, Decode, Clone)]
pub struct OutgoingMessage {
    pub recipient: Recipient,
    pub message: Message,
}

#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub sender: mpsc::Sender<PeerCommand>,
    pub last_heartbeat: time::Instant,
}

#[derive(Debug, Clone)]
pub enum NetworkChangeEvent {
    NodeAdded(SocketAddrV4),
    NodeRemoved(SocketAddrV4),
}

#[derive(Debug)]
pub enum NetworkEvent {
    ConnectNode(SocketAddrV4, TcpStream),
    DisconnectNode(SocketAddrV4),
    ReceiveHeartbeat(SocketAddrV4),
    SendMessage(OutgoingMessage),
}