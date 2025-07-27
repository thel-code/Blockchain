use tokio::time::{self, Instant, Duration};
use crate::network::peer_handler;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};
use crate::common::types::{Message, NetworkChangeEvent, NetworkEvent, NodeInfo, PeerCommand, Recipient};

pub async fn start(
    mut network_channel_receiver: mpsc::Receiver<NetworkEvent>,
    network_channel_sender: mpsc::Sender<NetworkEvent>,
    incoming_message_sender: mpsc::Sender<Message>,
    network_state_sender: broadcast::Sender<NetworkChangeEvent>,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) {
    info!("Network Manager started.");

    let mut nodes: HashMap<SocketAddrV4, NodeInfo> = HashMap::new();
    let mut heartbeat_timer = time::interval(heartbeat_interval);
    heartbeat_timer.tick().await; // Initial tick to make it fire immediately if needed or align future ticks

    loop {
        tokio::select! {
            event = network_channel_receiver.recv() => {
                match event {
                    Some(event) => {
                        handle_network_event(
                            event,
                            &mut nodes,
                            &network_channel_sender,
                            &incoming_message_sender,
                            &network_state_sender,
                        ).await;
                    },
                    None => {
                        info!("Network Manager's node event receiver closed. Shutting down.");
                        break;
                    }
                }
            },
            // --- Heartbeat and Peer Health Check ---
            _ = heartbeat_timer.tick() => {
                handle_heartbeat_check(
                    &mut nodes,
                    heartbeat_timeout,
                    &network_channel_sender,
                ).await;
            }
        }
    }
    info!("Network Manager shutting down.");
}

async fn handle_network_event(
    event: NetworkEvent,
    nodes: &mut HashMap<SocketAddrV4, NodeInfo>,
    network_channel_sender: &mpsc::Sender<NetworkEvent>,
    incoming_message_sender: &mpsc::Sender<Message>,
    network_state_sender: &broadcast::Sender<NetworkChangeEvent>,
) {
    match event {
        NetworkEvent::ConnectNode(addr, stream) => {
            if nodes.contains_key(&addr) {
                debug!("Node {} already exists. Skipping addition.", addr);
                return;
            }
            info!("Adding new node: {}", addr);

            let (peer_command_sender, peer_command_receiver) = mpsc::channel::<PeerCommand>(100);

            let node_info = NodeInfo {
                sender: peer_command_sender,
                last_heartbeat: Instant::now(),
            };

            nodes.insert(addr, node_info);

            let network_channel_sender_clone = network_channel_sender.clone();
            let message_event_sender_clone = incoming_message_sender.clone();
            if let Err(e) = network_state_sender.send(NetworkChangeEvent::NodeAdded(addr)) {
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
            if nodes.remove(&addr).is_some() {
                debug!("Successfully removed node: {}", addr);
                if let Err(e) = network_state_sender.send(NetworkChangeEvent::NodeRemoved(addr)) {
                    error!("Failed to send NodeRemoved event for {}: {}", addr, e);
                }
            } else {
                debug!("Attempted to disconnect unknown node: {}", addr);
            }
        }
        NetworkEvent::ReceiveHeartbeat(addr) => {
            if let Some(node_info) = nodes.get_mut(&addr) {
                node_info.last_heartbeat = Instant::now();
                debug!("Received heartbeat from node: {}", addr);
            } else {
                warn!("Received heartbeat from unknown node: {}. Ignoring.", addr);
            }
        }
        NetworkEvent::SendMessage(outgoing_message) => {
            debug!("Received outgoing message: {:?}", outgoing_message);
            match outgoing_message.recipient {
                Recipient::All => {
                    for (addr, node_info) in nodes.iter() {
                        if let Err(e) = node_info.sender.send(PeerCommand::SendMessage(outgoing_message.message.clone())).await {
                            warn!("Failed to send message to {}: {}", addr, e);
                        }
                    }
                }
                Recipient::Specific(addr) => {
                    for target_addr in addr {
                        if let Some(node_info) = nodes.get(&target_addr) {
                            if let Err(e) = node_info.sender.send(PeerCommand::SendMessage(outgoing_message.message.clone())).await {
                                warn!("Failed to send message to {}: {}", target_addr, e);
                            }
                        } else {
                            warn!("Attempted to send message to unknown node: {}", target_addr);
                        }
                    }
                }
            }
            
        }
        
    }
}

async fn handle_heartbeat_check(
    nodes: &mut HashMap<SocketAddrV4, NodeInfo>,
    heartbeat_timeout: Duration,
    network_channel_sender: &mpsc::Sender<NetworkEvent>,
) {
    debug!("Heartbeat tick: Checking peers and sending heartbeats.");
    let mut dead_peers = Vec::new();

    let current_nodes_for_heartbeat: Vec<_> = nodes.iter()
        .map(|(addr, info)| (*addr, info.sender.clone(), info.last_heartbeat))
        .collect();

    for (addr, sender, last_heartbeat) in current_nodes_for_heartbeat {
        let heartbeat_msg = Message::Heartbeat;
        if let Err(e) = sender.send(PeerCommand::SendMessage(heartbeat_msg)).await {
            warn!("Failed to send heartbeat to {} (channel closed?): {}", addr, e);
            dead_peers.push(addr);
            continue;
        }

        if last_heartbeat.elapsed() > heartbeat_timeout {
            warn!("Peer {} timed out (no heartbeat for {:?}). Marking for disconnection.", addr, last_heartbeat.elapsed());
            dead_peers.push(addr);
        }
    }

    // Trigger disconnection for dead peers outside the iteration loop
    for addr in dead_peers {
        if let Err(e) = network_channel_sender.send(NetworkEvent::DisconnectNode(addr)).await {
            error!("Failed to self-send DisconnectNode event for timed out peer {}: {}", addr, e);
        }
    }
}