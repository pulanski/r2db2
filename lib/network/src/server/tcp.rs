use crate::middleware::trace::LoggingMiddleware;
use crate::middleware::{Middleware, MiddlewareStack, MiddlewareStackRef};
use crate::protocol::handler::{ConnectionHandler, Protocol};
use crate::protocol::message::{Message, MessageKind};
use anyhow::{Context, Result};
use dashmap::DashMap;
use driver::{Driver, DriverRef};
use rustc_hash::FxHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument, trace};
use typed_builder::TypedBuilder;

pub type ConnectionId = String; // Unique identifier for each connection

#[derive(Debug, TypedBuilder)]
pub struct DbServer {
    server_address: SocketAddr,
    connections: Arc<DashMap<ConnectionId, mpsc::Sender<TcpStream>>>,
    driver: DriverRef,
    middleware_stack: MiddlewareStackRef,
}

impl DbServer {
    /// Start a new server instance with the given address and
    /// middleware stack.
    pub fn new(server_address: SocketAddr, mut middleware_stack: MiddlewareStack) -> Self {
        // By default, we use the logging middleware
        middleware_stack.add_middleware(LoggingMiddleware::new());

        DbServer::builder()
            .server_address(server_address)
            .connections(Arc::new(DashMap::new()))
            .driver(Arc::new(
                Driver::new("test.db").expect("Failed to create driver"),
            ))
            .middleware_stack(Arc::new(middleware_stack))
            .build()
    }

    pub async fn accept_connections(&self, listener: TcpListener) -> Result<()> {
        while let Ok((socket, addr)) = listener.accept().await {
            let connection_id = generate_connection_id(&addr); // Generate a unique ID for the connection
            let (tx, rx) = mpsc::channel(1); // Create a channel for communication with the connection handler

            self.connections.insert(connection_id.clone(), tx);

            let mut connection_handler = ConnectionHandler::new(
                socket,
                rx,
                self.driver.clone(),
                self.connections.clone(),
                self.middleware_stack.clone(),
            );

            tokio::spawn(async move {
                if let Err(e) = connection_handler.handle_connection().await {
                    error!("Error in connection {}: {:?}", connection_id, e);
                }
            });
        }

        Ok(())
    }

    pub async fn run(&self) -> io::Result<()> {
        let mut current_port = self.server_address.port();
        let max_retries = env::var("MAX_PORT_RETRIES")
            .unwrap_or("5".to_string())
            .parse::<u16>()
            .unwrap_or(5);
        let retry_interval = env::var("PORT_RETRY_INTERVAL_MS")
            .unwrap_or("500".to_string())
            .parse::<u64>()
            .unwrap_or(500);
        let mut attempt = 0;

        loop {
            let address = SocketAddr::new(self.server_address.ip(), current_port);
            match TcpListener::bind(&address).await {
                Ok(listener) => {
                    info!("Server successfully running on {}", &address);
                    tokio::select! {
                        result = self.accept_connections(listener) => {
                            if let Err(e) = result {
                                error!("Error accepting connections: {}", e);
                            }
                        }
                        _ = signal::ctrl_c() => {
                            info!("Shutdown signal received, terminating server...");
                            break;
                        }
                    }
                    break;
                }
                Err(e) => {
                    error!(
                        "Failed to bind to port {}: {}. Retrying in {}ms",
                        current_port, e, retry_interval
                    );
                    if attempt >= max_retries {
                        error!("Reached maximum retry attempts. Unable to start server.");
                        return Err(e);
                    }
                    current_port = current_port.wrapping_add(1);
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(retry_interval)).await;
                }
            }
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
async fn handle_client_request(stream: &mut TcpStream) -> Result<()> {
    debug!("Handling client request");
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
    }

    Ok(())
}
