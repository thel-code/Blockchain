use std::net::SocketAddrV4;
use tokio::sync::mpsc;
use crate::ensure_ipv4_addr; // Assuming this macro is defined elsewhere
use anyhow::{Result, Context};
use tracing::{info, error, warn};
use crate::common::types::NetworkEvent;

pub async fn start(
    socket_addr: SocketAddrV4,
    network_channel_sender: mpsc::Sender<NetworkEvent>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(socket_addr)
        .await
        .with_context(|| format!("Failed to bind TCP listener to {}", socket_addr))?;

    info!("Listening for incoming connections on {}", socket_addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                handle_accepted_connection(stream, peer_addr, &network_channel_sender).await;
            }
            Err(e) => {
                error!("Failed to accept connection: {:?}", e);
            }
        }
    }
}

async fn handle_accepted_connection(
    stream: tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
    network_channel_sender: &mpsc::Sender<NetworkEvent>,
) {
    info!("Accepted connection from {}", peer_addr);

    let addr = match ensure_ipv4_addr!(peer_addr) {
        Ok(ipv4_addr) => ipv4_addr,
        Err(e) => {
            warn!("Received non-IPv4 connection from {}. Skipping. Error: {}", peer_addr, e);
            return; 
        }
    };

    if let Err(e) = network_channel_sender.send(NetworkEvent::ConnectNode(addr, stream)).await {
        error!("Failed to send ConnectNode event for {}: {:?}", addr, e);
    }
}