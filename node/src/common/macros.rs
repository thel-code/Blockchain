#[macro_export]
macro_rules! ensure_ipv4_addr {
    ($socket_addr_expr:expr) => {
        // This block ensures the macro expands to an expression that can be used directly
        // and handles the error propagation with `anyhow::Result` and `?`.
        {
            let __socket_addr = $socket_addr_expr; // Use a unique name to avoid variable capture
            match __socket_addr {
                std::net::SocketAddr::V4(addr_v4) => Ok(addr_v4),
                _ => {
                    // This creates an anyhow error, which can then be propagated with `?`
                    Err(anyhow::anyhow!(
                        "Expected IPv4 address, but found non-IPv4: {}",
                        __socket_addr
                    ))
                }
            }
        }
    }
}
