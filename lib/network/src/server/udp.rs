use tokio::net::UdpSocket;
use tracing::{debug, error, info, warn};

pub async fn run_udp_server(addr: &str) -> tokio::io::Result<()> {
    let socket = UdpSocket::bind(addr).await?;
    info!("UDP Server running on {}", addr);

    let mut buf = [0; 1024];

    loop {
        match socket.recv_from(&mut buf).await {
            Ok((n, peer)) => {
                debug!("Received {} bytes from {}", n, peer);

                // TODO: add support for parsing the data according to custom UDP-based protocol.

                // For simplicity, our placeholder echoes the received message back to the sender.
                match socket.send_to(&buf[..n], &peer).await {
                    Ok(_) => debug!("Response sent to {}", peer),
                    Err(e) => warn!("Failed to send response to {}: {:?}", peer, e),
                }
            }
            Err(e) => {
                error!("Error receiving data: {:?}", e);
                // TODO: add support for handling errors (e.g. retrying, reporting, etc.)
            }
        }
    }
}
