// src/network/peer_handler.rs
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use bincode::{config, decode_from_slice, encode_to_vec};
use tracing::{debug, error, info, instrument, warn};
use anyhow::{Context, Result};
use crate::common::types::{NetworkEvent, PeerCommand};
use crate::common::types::Message;
use crate::ensure_ipv4_addr;

pub async fn handle(
    mut stream: TcpStream,
    network_channel_sender: Sender<NetworkEvent>,
    incoming_message_sender: Sender<Message>,
    mut peer_command_receiver: Receiver<PeerCommand>,
) -> Result<()> {
    let peer_addr = ensure_ipv4_addr!(stream.peer_addr().context("Failed to get peer address")?)
        .context("Peer address is not IPv4")?;

    info!("Peer handler started for {}", peer_addr);

    let mut read_buffer = vec![0; 4096]; // Buffer for reading incoming data

    loop {
        tokio::select! {
            command = peer_command_receiver.recv() => {
                match command {
                    Some(PeerCommand::SendMessage(message_event)) => {
                        debug!("Received command to send message to {}: {:?}", peer_addr, message_event);
                        let encoded_msg = encode_to_vec(&message_event, config::standard())
                            .context("Failed to encode MessageEvent for sending")?;

                        if let Err(e) = stream.write_all(&encoded_msg).await {
                            error!("Failed to send encoded message to peer {}: {:?}", peer_addr, e);
                            if let Err(send_err) = network_channel_sender.send(NetworkEvent::DisconnectNode(peer_addr)).await {
                                error!("Failed to send DisconnectNode event for {} after write error: {}", peer_addr, send_err);
                            }
                            break;
                        }
                        debug!("Successfully sent message to peer {}.", peer_addr);
                    }
                    None => {
                        info!("All command senders for peer {} dropped. Shutting down handler.", peer_addr);
                        break;
                    }
                }
            }

            read_result = stream.read(&mut read_buffer) => {
                let bytes_read = match read_result {
                    Ok(0) => {
                        info!("Connection closed by peer: {}", peer_addr);
                        if let Err(e) = network_channel_sender.send(NetworkEvent::DisconnectNode(peer_addr)).await {
                            error!("Failed to send DisconnectNode event for {}: {}", peer_addr, e);
                        }
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        error!("Error reading from peer stream {}: {:?}", peer_addr, e);
                        if let Err(send_err) = network_channel_sender.send(NetworkEvent::DisconnectNode(peer_addr)).await {
                            error!("Failed to send DisconnectNode event for {} after read error: {}", peer_addr, send_err);
                        }
                        break;
                    }
                };

                debug!("Received {} bytes from peer {}.", bytes_read, peer_addr);

                match decode_from_slice::<Message, _>(&read_buffer[..bytes_read], config::standard()) {
                    Ok((message_event, _)) => {
                        if let Message::Heartbeat = message_event {
                            debug!("Received heartbeat from peer: {}", peer_addr);
                            if let Err(e) = network_channel_sender.send(NetworkEvent::ReceiveHeartbeat(peer_addr)).await {
                                error!("Failed to send ReceiveHeartbeat event for {}: {}", peer_addr, e);
                            }
                        } else {
                            debug!("Decoded incoming message event from {}: {:?}", peer_addr, message_event);
                            if let Err(e) = incoming_message_sender.send(message_event).await {
                                error!("Failed to send incoming message to MessageBroadcaster from {}: {}", peer_addr, e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to decode MessageEvent from peer {}: {}. Raw bytes: {:?}", peer_addr, e, &read_buffer[..bytes_read]);
                    }
                }
            }
        }
    }

    info!("Peer handler for {} finished.", peer_addr);
    Ok(())
}
