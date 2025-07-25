use std::collections::HashMap;
use std::net::SocketAddrV4;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tracing::{debug, error};
use crate::common::types::{NetworkEvent, NodeInfo, OutgoingMessage, PeerCommand};
use crate::common::types::Recipient::Specific;

pub async fn start(mut outgoing_message_receiver: Receiver<OutgoingMessage>, network_channel_sender: Sender<NetworkEvent>) {
    tokio::spawn(async move {
        while let Some(message) = outgoing_message_receiver.recv().await {
            let (peer_sender, peer_receiver) = oneshot::channel::<HashMap<SocketAddrV4, NodeInfo>>();
            let recipients = message.recipients.clone();

           
        }
    });
}