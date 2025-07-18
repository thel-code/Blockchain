// src/network/manager.rs

use tokio::time;
use crate::network::peer_handler;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};
use tokio::net::TcpStream;
use crate::common::types::{Message, NetworkChangeEvent, NetworkEvent , NodeInfo};
use crate::common::types::PeerCommand;

pub struct NetworkManager {
    nodes: HashMap<SocketAddrV4, NodeInfo>,
    network_channel_receiver: mpsc::Receiver<NetworkEvent>,
    network_channel_sender: mpsc::Sender<NetworkEvent>,
    incoming_message_sender: mpsc::Sender<Message>,
    network_state_sender: broadcast::Sender<NetworkChangeEvent>,
    heartbeat_interval: time::Duration,
    heartbeat_timeout: time::Duration,
}

impl NetworkManager {
    pub fn new(
        network_channel_receiver: mpsc::Receiver<NetworkEvent>,
        network_channel_sender: mpsc::Sender<NetworkEvent>,
        incoming_message_sender: mpsc::Sender<Message>,
        network_state_sender: broadcast::Sender<NetworkChangeEvent>,
        heartbeat_interval: time::Duration,
        heartbeat_timeout: time::Duration,
    ) -> Self {
        NetworkManager {
            nodes: HashMap::new(),
            network_channel_receiver,
            network_channel_sender,
            incoming_message_sender,
            network_state_sender,
            heartbeat_interval,
            heartbeat_timeout,
        }
    }

    pub async fn run(&mut self) {
        info!("Network Manager started.");

        let mut heartbeat_timer = time::interval(self.heartbeat_interval);
        heartbeat_timer.tick().await;

        loop {
            tokio::select! {
                event = self.network_channel_receiver.recv() => {
                    match event {
                        Some(event) => {
                            match event {
                                NetworkEvent::ConnectNode(addr, stream) => {
                                    if self.nodes.contains_key(&addr) {
                                        debug!("Node {} already exists. Skipping addition.", addr);
                                        continue;
                                    }
                                    info!("Adding new node: {}", addr);

                                    let (peer_command_sender, peer_command_receiver) = mpsc::channel::<PeerCommand>(100);

                                    let node_info = NodeInfo {
                                        sender: peer_command_sender,
                                        last_heartbeat: time::Instant::now(),
                                    };

                                    self.nodes.insert(addr, node_info);


                                    let network_channel_sender_clone = self.network_channel_sender.clone();
                                    let message_event_sender_clone = self.incoming_message_sender.clone();
                                    if let Err(e) = self.network_state_sender.send(NetworkChangeEvent::NodeAdded(addr)) {
                                        error!("Failed to send NodeAdded event for {}: {}", addr, e);
                                    }
                                    tokio::spawn(async move {
                                        if let Err(e) = peer_handler::handle(
                                            stream,
                                            network_channel_sender_clone,
                                            message_event_sender_clone,
                                            peer_command_receiver,
                                        ).await {
                                            error!("Peer handler for {} exited with error: {:?}", addr, e);
                                        }
                                    });
                                }
                                NetworkEvent::DisconnectNode(addr) => {
                                    info!("Node disconnected: {}", addr);
                                    // TODO: add possibility to disconnect gracefully
                                    if self.nodes.remove(&addr).is_some() {
                                        debug!("Successfully removed node: {}", addr);
                                        if let Err(e) = self.network_state_sender.send(NetworkChangeEvent::NodeRemoved(addr)) {
                                            error!("Failed to send NodeRemoved event for {}: {}", addr, e);
                                        }
                                    } else {
                                        debug!("Attempted to disconnect unknown node: {}", addr);
                                    }
                                }
                                NetworkEvent::ReceiveHeartbeat(addr) => {
                                    if let Some(node_info) = self.nodes.get_mut(&addr) {
                                        node_info.last_heartbeat = time::Instant::now();
                                        debug!("Received heartbeat from node: {}", addr);
                                    } else {
                                        warn!("Received heartbeat from unknown node: {}. Ignoring.", addr);
                                    }
                                }
                                NetworkEvent::GetNodes(channel) => {
                                    debug!("Received request for current nodes. Sending response.");
                                    let nodes = self.nodes.clone();
                                    // send nodes to the requesting channel
                                    if let Err(e) = channel.send(nodes) {
                                        error!("Failed to send current nodes to channel");
                                    } else {
                                        debug!("Successfully sent current nodes.");
                                    }

                                }
                            }
                        },
                        None => {
                            info!("Network Manager's node event receiver closed. Shutting down.");
                            break;
                        }
                    }
                },

                heartbeat = heartbeat_timer.tick() => {
                    debug!("Heartbeat tick: Checking peers and sending heartbeats.");
                    let mut dead_peers = Vec::new();

                    let current_nodes: Vec<_> = self.nodes.iter().map(|(addr, info)| (*addr, info.sender.clone(), info.last_heartbeat)).collect();

                    for (addr, sender, last_heartbeat) in current_nodes {
                        let heartbeat_msg = Message::Heartbeat;
                        if let Err(e) = sender.send(PeerCommand::SendMessage(heartbeat_msg)).await {
                            warn!("Failed to send heartbeat to {} (channel closed?): {}", addr, e);
                            dead_peers.push(addr);
                            continue;
                        }

                        if last_heartbeat.elapsed() > self.heartbeat_timeout {
                            warn!("Peer {} timed out (no heartbeat for {:?}). Marking for disconnection.", addr, last_heartbeat.elapsed());
                            dead_peers.push(addr);
                        }
                    }

                    for addr in dead_peers {
                        if let Err(e) = self.network_channel_sender.send(NetworkEvent::DisconnectNode(addr)).await {
                            error!("Failed to self-send DisconnectNode event for timed out peer {}: {}", addr, e);
                        }
                    }
                }
            }
        }
        info!("Network Manager shutting down.");
    }

}
