mod internal;
mod sender;

use tokio::sync::{broadcast, mpsc};
use crate::common::types::{Message, NetworkEvent, OutgoingMessage};


pub fn init(
    incoming_message_receiver: mpsc::Receiver<Message>,
    network_channel_sender: mpsc::Sender<NetworkEvent>,
    internal_message_sender: broadcast::Sender<Message>,
    internal_message_receiver: broadcast::Receiver<Message>,
) {
    
    let (outgoing_message_sender, outgoing_message_receiver) = mpsc::channel::<OutgoingMessage>(100);
    
    tokio::spawn(async move {
    internal::start(incoming_message_receiver, internal_message_sender.clone()).await;
    });
    
    tokio::spawn(async move {
        sender::start(
            outgoing_message_receiver,
            network_channel_sender.clone(),
        ).await;
    });
    
    
    
}
