use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

pub async fn run_tcp_server(addr: &str) -> tokio::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Server running on {}", addr);

    loop {
        let (mut socket, addr) = match listener.accept().await {
            Ok((socket, addr)) => {
                info!("New connection: {}", addr);
                (socket, addr)
            }
            Err(e) => {
                error!("Failed to accept connection: {:?}", e);
                continue;
            }
        };

        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                match socket.read(&mut buf).await {
                    Ok(n) if n == 0 => {
                        debug!("Connection closed: {}", addr);
                        return;
                    }
                    Ok(n) => {
                        debug!("Received data: {} bytes from {}", n, addr);

                        // Here we would parse the data according to the PostgreSQL wire protocol
                        // and handle the commands. This is where `protocol.rs` comes into play.

                        debug!("Sending data: {} bytes to {}", n, addr);

                        if let Err(e) = socket.write_all(&buf[..n]).await {
                            error!("Failed to write to socket {}: {:?}", addr, e);
                            return;
                        }
                    }
                    Err(e) => {
                        error!("Failed to read from socket {}: {:?}", addr, e);
                        return;
                    }
                }
            }
        });
    }
}
