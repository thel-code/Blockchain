use tokio::sync::{broadcast, mpsc};
use crate::common::types::{Message, NetworkEvent, OutgoingMessage};


pub fn init(
    incoming_message_receiver: mpsc::Receiver<Message>,
    network_channel_sender: mpsc::Sender<NetworkEvent>,
    internal_message_sender: broadcast::Sender<Message>,
    internal_message_receiver: broadcast::Receiver<Message>,
) {
    
    
}
