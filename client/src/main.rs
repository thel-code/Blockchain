use std::io::Write;
use std::net::SocketAddrV4;
use std::str::FromStr;
use bincode::{config::standard, encode_to_vec};
mod transaction;
mod node_gateway;

fn main() {
    // Initialize the node gateway with a specific address
    let node_gateway_socket_addr = SocketAddrV4::new(
        std::net::Ipv4Addr::new(127, 0, 0, 1),
        8080,
    );

    let mut socket = node_gateway::create_stream(node_gateway_socket_addr).unwrap();
    loop {
        println!("Enter a transaction (from, to, amount) or 'exit' to quit:");
        let input: String = input().expect("Invalid input");

        if input.trim().eq_ignore_ascii_case("exit") {
            break;
        }

        // Parse the input into a transaction
        let parts: Vec<&str> = input.split(',').map(str::trim).collect();
        if parts.len() != 3 {
            println!("Invalid transaction format. Please use 'from, to, amount'.");
            continue;
        }

        let from = parts[0].to_string();
        let to = parts[1].to_string();
        let amount: u64 = match parts[2].parse() {
            Ok(value) => value,
            Err(_) => {
                println!("Invalid amount. Please enter a valid number.");
                continue;
            }
        };

        // Create and send the transaction
        let transaction = transaction::Transaction { from, to, amount };
        socket.write_all(&encode_to_vec(&transaction, standard())
            .expect("Failed to serialize transaction"))
            .expect("Failed to send transaction");
    }

}

fn input<T: FromStr>() -> Result<T, <T as FromStr>::Err> {
    let mut input: String = String::with_capacity(64);

    std::io::stdin()
        .read_line(&mut input)
        .expect("Input could not be read");

    input.parse()
}