use std::net::SocketAddrV4;
use tokio::sync::mpsc;
use tracing::{error, info};
use crate::common::types::{NetworkEvent};

pub async fn connect(nodes: Vec<SocketAddrV4>, node_event_sender: mpsc::Sender<NetworkEvent>) {
    info!("Starting connection attempts to {} nodes.", nodes.len());
    for node in nodes {
        info!("Attempting to connect to node: {}", node);
        match tokio::net::TcpStream::connect(node).await {
            Ok(stream) => {
                info!("Successfully connected to node: {}", node);
                if let Err(e) = node_event_sender.send(NetworkEvent::ConnectNode(node, stream)).await {
                    error!("Failed to send AddNode event for {}: {:?}", node, e);
                }
            }
            Err(e) => {
                error!("Failed to connect to node {}: {:?}", node, e);
            }
        }
    }
    info!("Finished connection attempts.");
}