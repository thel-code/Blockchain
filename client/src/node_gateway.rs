use std::io;
use std::net::{SocketAddr, SocketAddrV4, TcpStream};

pub fn create_stream(s_addr : SocketAddrV4) -> io::Result<TcpStream> {
    let stream = TcpStream::connect(&s_addr)?;
    Ok(stream)
}