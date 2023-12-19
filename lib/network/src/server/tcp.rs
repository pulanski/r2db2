use crate::protocol::{Message, Protocol};
use dashmap::DashMap;
use driver::Driver;
use std::sync::Arc;
use tokio::io::{self};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

#[derive(Debug)]
pub struct DbServer {
    server_address: String,
    client_connections: Arc<DashMap<String, TcpStream>>,
    driver: Driver,
}

impl DbServer {
    pub fn new(server_address: String) -> Self {
        let db_path = "test.db".to_owned();
        DbServer {
            server_address,
            client_connections: Arc::new(DashMap::new()),
            driver: Driver::new(&db_path).expect("Failed to create driver"),
        }
    }

    pub async fn run(&self, addr: &str) -> io::Result<()> {
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
}

#[tracing::instrument(skip(stream))]
async fn handle_client_query(stream: &mut TcpStream) -> io::Result<()> {
    debug!("Handling client query...");
    while let Some(message) = Protocol::parse_incoming(stream).await? {
        match message {
            Message::StartupMessage { protocol_version } => {
                info!(
                    "Startup request with protocol version: {}",
                    protocol_version
                );

                // TODO: Perform initialization or setup required for a new client (e.g. authentication)
                // ...
            }
            Message::QueryMessage { query } => {
                info!("Received query: `{}`", query);

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
            _ => {
                let error_response = Message::ErrorResponse {
                    error: "Unsupported message type".to_string(),
                };
                Protocol::send_message(stream, error_response).await?;
            }
        }
    }

    Ok(())
}
