// src/network/manager.rs

use tokio::time;
use crate::network::peer_handler;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};
use tokio::net::TcpStream;
use crate::messaging::message::Message;
use crate::network::peer_handler::PeerCommand;

#[derive(Debug)]
pub struct NodeInfo {
    pub sender: mpsc::Sender<PeerCommand>,
    pub last_heartbeat: time::Instant,
}

#[derive(Debug)]
pub enum NodeEvent {
    AddNode(SocketAddrV4, TcpStream),
    DisconnectNode(SocketAddrV4),
    ReceiveHeartbeat(SocketAddrV4),
}

pub struct NetworkManager {
    nodes: HashMap<SocketAddrV4, NodeInfo>,
    node_event_receiver: mpsc::Receiver<NodeEvent>,
    node_event_sender: mpsc::Sender<NodeEvent>,
    incoming_message_sender: mpsc::Sender<Message>,
    heartbeat_interval: time::Duration,
    heartbeat_timeout: time::Duration,
}

impl NetworkManager {
    pub fn new(
        node_event_receiver: mpsc::Receiver<NodeEvent>,
        node_event_sender: mpsc::Sender<NodeEvent>,
        incoming_message_sender: mpsc::Sender<Message>,
        heartbeat_interval: time::Duration,
        heartbeat_timeout: time::Duration,
    ) -> Self {
        NetworkManager {
            nodes: HashMap::new(),
            node_event_receiver,
            node_event_sender,
            incoming_message_sender,
            heartbeat_interval,
            heartbeat_timeout,
        }
    }

    pub async fn run(&mut self) {
        info!("Network Manager started.");

        let mut heartbeat_timer = time::interval(self.heartbeat_interval);
        heartbeat_timer.tick().await; // Initial tick to make it ready

        loop {
            tokio::select! {
                event = self.node_event_receiver.recv() => {
                    match event {
                        Some(event) => { // Matched an actual NodeEvent
                            match event {
                                NodeEvent::AddNode(addr, stream) => {
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


                                    let node_event_sender_clone = self.node_event_sender.clone();
                                    let message_event_sender_clone = self.incoming_message_sender.clone(); 

                                    tokio::spawn(async move {
                                        if let Err(e) = peer_handler::handle(
                                            stream,
                                            node_event_sender_clone,
                                            message_event_sender_clone, 
                                            peer_command_receiver,
                                        ).await {
                                            error!("Peer handler for {} exited with error: {:?}", addr, e);
                                        }
                                    });
                                }
                                NodeEvent::DisconnectNode(addr) => {
                                    info!("Node disconnected: {}", addr);
                                    // TODO: add possibility to disconnect gracefully
                                    if self.nodes.remove(&addr).is_some() {
                                    } else {
                                        debug!("Attempted to disconnect unknown node: {}", addr);
                                    }
                                }
                                NodeEvent::ReceiveHeartbeat(addr) => {
                                    if let Some(node_info) = self.nodes.get_mut(&addr) {
                                        node_info.last_heartbeat = time::Instant::now();
                                        debug!("Received heartbeat from node: {}", addr);
                                    } else {
                                        warn!("Received heartbeat from unknown node: {}. Ignoring.", addr);
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
                        if let Err(e) = self.node_event_sender.send(NodeEvent::DisconnectNode(addr)).await {
                            error!("Failed to self-send DisconnectNode event for timed out peer {}: {}", addr, e);
                        }
                    }
                }
            }
        }
        info!("Network Manager shutting down.");
    }
}
