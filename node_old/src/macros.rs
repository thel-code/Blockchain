
#[macro_export]
macro_rules! handle_socket {
    ($stream:expr, $handler:expr) => {
        {
            let mut stream = $stream;
            thread::spawn(move || {
                let mut buffer = [0; 1024];
                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => {
                            // Connection was closed by the client.
                            match stream.peer_addr() {
                                Ok(addr) => println!("Connection closed by client: {}", addr),
                                Err(_) => println!("Connection closed by client with unknown address."),
                            };
                            break;
                        }
                        Ok(n) => {
                            // Data received, call the handler with the data slice.
                            ($handler)(&buffer[..n]);
                        }
                        Err(e) => {
                            // An error occurred while reading from the stream.
                            eprintln!("Failed to read from stream: {}", e);
                            break;
                        }
                    }
                }
            });
        }
    };
}
