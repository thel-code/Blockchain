use std::net::SocketAddrV4;
use tokio::sync::mpsc;
use crate::ensure_ipv4_addr;
use anyhow::{Result, Context};
use tracing::{info, error, warn};
use crate::common::types::NetworkEvent;

pub struct Listener {
    socket_addr: SocketAddrV4,
    network_channel_sender: mpsc::Sender<NetworkEvent>,
}

impl Listener {
    pub fn new(socket_addr: SocketAddrV4, network_channel_sender: mpsc::Sender<NetworkEvent>) -> Self {
        Listener {
            socket_addr,
            network_channel_sender,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(self.socket_addr)
            .await
            .with_context(|| format!("Failed to bind TCP listener to {}", self.socket_addr))?;

        info!("Listening for incoming connections on {}", self.socket_addr);
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("Accepted connection from {}", peer_addr);

                    let addr = match ensure_ipv4_addr!(peer_addr) {
                        Ok(ipv4_addr) => ipv4_addr,
                        Err(e) => {
                            warn!("Received non-IPv4 connection from {}. Skipping. Error: {}", peer_addr, e);
                            continue;
                        }
                    };

                    if let Err(e) = self.network_channel_sender.send(NetworkEvent::ConnectNode(addr, stream)).await {
                        error!("Failed to send AddNode event for {}: {:?}", addr, e);
                    }
                }
                Err(e) => {
                    error!("Failed to accept connection: {:?}", e);
                }
            }
        }
    }
}