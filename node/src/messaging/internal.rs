use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error};
use crate::common::types::Message;

pub async fn start(
    mut incoming_message_receiver: mpsc::Receiver<Message>,
    internal_message_sender: broadcast::Sender<Message>,
){
    tokio::spawn(async move {
        while let Some(message) = incoming_message_receiver.recv().await {
            if internal_message_sender.send(message).is_err() {
                error!("Failed to broadcast message");
            }
        }
    });
}