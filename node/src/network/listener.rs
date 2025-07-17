use std::net::SocketAddrV4;
use tokio::sync::mpsc;
use crate::network::manager::NodeEvent;
use crate::ensure_ipv4_addr;
use anyhow::{Result, Context};
use tracing::{info, error, warn};

pub struct Listener {
    socket_addr: SocketAddrV4,
    node_event_sender: mpsc::Sender<NodeEvent>,
}

impl Listener {
    pub fn new(socket_addr: SocketAddrV4, node_event_sender: mpsc::Sender<NodeEvent>) -> Self {
        Listener {
            socket_addr,
            node_event_sender,
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

                    if let Err(e) = self.node_event_sender.send(NodeEvent::AddNode(addr, stream)).await {
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