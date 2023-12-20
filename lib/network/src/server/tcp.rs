use crate::protocol::handler::{ConnectionHandler, Protocol};
use crate::protocol::message::{Message, MessageKind};
use anyhow::{Context, Result};
use dashmap::DashMap;
use driver::{Driver, DriverRef};
use rustc_hash::FxHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use typed_builder::TypedBuilder;

pub type ConnectionId = String; // Unique identifier for each connection

#[derive(Debug, TypedBuilder)]
pub struct DbServer {
    server_address: SocketAddr,
    connections: Arc<DashMap<ConnectionId, mpsc::Sender<TcpStream>>>,
    driver: DriverRef,
}

impl DbServer {
    pub fn new(server_address: SocketAddr) -> Self {
        DbServer::builder()
            .server_address(server_address)
            .connections(Arc::new(DashMap::new()))
            .driver(Arc::new(
                Driver::new("test.db").expect("Failed to create driver"),
            ))
            .build()
    }

    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.server_address).await?;
        info!("Server running on {}", &self.server_address);

        while let Ok((socket, addr)) = listener.accept().await {
            let connection_id = generate_connection_id(&addr); // Generate a unique ID for the connection
            let (tx, rx) = mpsc::channel(1); // Create a channel for communication with the connection handler

            info!("New connection {}: {}", connection_id, addr);

            self.connections.insert(connection_id.clone(), tx);

            let mut connection_handler =
                ConnectionHandler::new(socket, rx, self.driver.clone(), self.connections.clone());

            tokio::spawn(async move {
                if let Err(e) = connection_handler.handle_connection().await {
                    error!("Error in connection {}: {:?}", connection_id, e);
                }
            });
        }

        Ok(())
    }
}

pub fn generate_connection_id(addr: &SocketAddr) -> ConnectionId {
    // Generate a unique connection ID based on the client's address
    let mut hasher = FxHasher::default();
    format!("{}:{}", addr.ip(), addr.port()).hash(&mut hasher);
    hasher.finish().to_string()
}

#[tracing::instrument(skip(stream))]
async fn handle_client_query(stream: &mut TcpStream) -> Result<()> {
    debug!("Handling client query...");
    while let Some(message) = Protocol::parse_incoming(stream).await? {
        match message.kind() {
            MessageKind::StartupMessage => {
                let response = Message::serialize_authentication_ok();
                stream
                    .write_all(&response)
                    .await
                    .context("Failed to send authentication response to server")?;
            }
            MessageKind::QueryMessage => {
                let response = Message::serialize_query_response();
                stream
                    .write_all(&response)
                    .await
                    .context("Failed to send query response to server")?;
            }
            MessageKind::TerminationMessage => {
                info!("Termination request received. Closing connection.");
                return Ok(());
            }
            _ => {
                let error_response =
                    Message::error_response("Unsupported message type".to_string());
                Protocol::send_message(stream, error_response).await?;
            }
        }

        // match message {
        //     Message::StartupMessage { protocol_version } => {
        //         info!(
        //             "Startup request with protocol version: {}",
        //             protocol_version
        //         );

        //         // TODO: Perform initialization or setup required for a new client (e.g. authentication)
        //         // ...
        //     }
        //     Message::QueryMessage { query } => {
        //         info!("Received query: `{}`", query);

        //         // TODO: execute query on db here

        //         // Send back a CommandCompleteMessage as a placeholder
        //         let response = Message::CommandCompleteMessage {
        //             tag: "QUERY EXECUTED".to_string(),
        //         };
        //         Protocol::send_message(stream, response).await?;
        //     }
        //     Message::TerminationMessage => {
        //         info!("Termination request received. Closing connection.");
        //         return Ok(());
        //     }
        //     _ => {
        //         let error_response = Message::ErrorResponse {
        //             error: "Unsupported message type".to_string(),
        //         };
        //         Protocol::send_message(stream, error_response).await?;
        //     }
        // }
    }

    Ok(())
}
