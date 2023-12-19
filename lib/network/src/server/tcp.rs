use crate::protocol::{Message, Protocol};
use tokio::io::{self};
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info};

pub async fn run_tcp_server(addr: &str) -> tokio::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Server running on {}", addr);

    loop {
        let (mut socket, _addr) = match listener.accept().await {
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
            if let Err(e) = handle_client_query(&mut socket).await {
                error!("Error handling client query: {:?}", e);
            }
        });
    }
}

async fn handle_client_query(stream: &mut TcpStream) -> io::Result<()> {
    while let Some(message) = Protocol::parse_incoming(stream).await? {
        match message {
            Message::QueryMessage { query } => {
                info!("Received query: {}", query);

                // TODO: execute query on db here

                // Send back a CommandCompleteMessage as a placeholder
                let response = Message::CommandCompleteMessage {
                    tag: "QUERY EXECUTED".to_string(),
                };
                Protocol::send_message(stream, response).await?;
            }
            Message::TerminationMessage => {
                info!("Termination request received. Closing connection.");
                return Ok(());
            }
            _ => {} // Handle other message types
        }
    }

    Ok(())
}
