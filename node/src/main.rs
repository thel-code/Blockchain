mod client_gateway;

fn main() {
    // find other nodes
    // gossip protocol

    // open connection for clients
    let client_gateway_socket_addr = std::net::SocketAddrV4::new(
        std::net::Ipv4Addr::new(127, 0, 0, 1),
        8080,
    );
    let client_gateway = client_gateway::listener::create_listener_thread(client_gateway_socket_addr);
    client_gateway.join().expect("Failed to join client gateway thread");
}
