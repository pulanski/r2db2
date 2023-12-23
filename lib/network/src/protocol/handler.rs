use super::message::Message;
use crate::middleware::MiddlewareStackRef;
use crate::protocol::message::MessageKind;
use crate::protocol::Protocol;
use crate::server::tcp::{generate_connection_id, ConnectionId, SemaphoreRef};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use driver::DriverRef;
use std::io::{self};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::SemaphorePermit;
use tracing::{debug, error, info, trace};
use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
pub struct ConnectionHandler {
    stream: TcpStream,
    receiver: Receiver<TcpStream>,
    driver: DriverRef,
    connections: Arc<DashMap<ConnectionId, bool>>,
    middleware_stack: MiddlewareStackRef,
    // conn_pool_sender: mpsc::Sender<()>, // Sender to release connection pool permit
    // query_throttle_sender: mpsc::Sender<()>, // Sender to release query throttle permit
}

impl ConnectionHandler {
    pub fn new(
        stream: TcpStream,
        receiver: Receiver<TcpStream>,
        driver: DriverRef,
        connections: Arc<DashMap<ConnectionId, bool>>,
        middleware_stack: MiddlewareStackRef,
        // conn_pool_sender: mpsc::Sender<()>,
        // query_throttle_sender: mpsc::Sender<()>,
    ) -> Self {
        ConnectionHandler::builder()
            .stream(stream)
            .receiver(receiver)
            .driver(driver)
            .connections(connections)
            .middleware_stack(middleware_stack)
            // .conn_pool_sender(conn_pool_sender)
            // .query_throttle_sender(query_throttle_sender)
            .build()
    }

    pub async fn handle_connection(&mut self) -> Result<()> {
        // Invoke middleware's on_connect method
        if let Err(e) = self.middleware_stack.handle_connect(&self.stream).await {
            error!("Error in middleware on_connect: {:?}", e);
            return Err(anyhow!("Error in middleware on_connect"));
        }

        // Main loop for handling client requests
        loop {
            match Protocol::parse_incoming(&mut self.stream).await? {
                Some(message) => {
                    // Invoke middleware's before_request method
                    if let Err(e) = self
                        .middleware_stack
                        .handle_before_request(&mut self.stream)
                        .await
                    {
                        error!("Error in middleware before_request: {:?}", e);
                        return Err(anyhow!("Error in handling before_request lifecycle hook within middleware stack."));
                    }

                    self.process_message(message).await?;

                    // Invoke middleware's after_request method
                    if let Err(e) = self
                        .middleware_stack
                        .handle_after_request(&mut self.stream)
                        .await
                    {
                        error!("Error in middleware after_request: {:?}", e);
                        return Err(anyhow!("Error in handling after_request lifecycle hook within middleware stack."));
                    }
                }
                None => {
                    self.handle_disconnect().await?;
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_disconnect(&mut self) -> Result<()> {
        let connection_id = generate_connection_id(&self.stream.peer_addr()?);
        self.connections.remove(&connection_id);

        // Invoke middleware's on_disconnect method
        if let Err(e) = self.middleware_stack.handle_disconnect(&self.stream).await {
            error!("Error in middleware on_disconnect: {:?}", e);
            return Err(anyhow!("Error in middleware on_disconnect"));
        }

        let remaining_connections = self.connections.len();
        let client = self.stream.peer_addr()?;

        // Connection is closing, release the permit back to the connection pool
        // self.conn_pool_sender
        //     .send(())
        //     .await
        //     .expect("Failed to release connection pool permit");

        trace!("Released connection pool permit for client {}", client);

        Ok(if remaining_connections == 0 {
            info!(
                "Client {} has closed the connection. No active connections remaining.",
                client
            );
        } else {
            info!(
                "Client {} has closed the connection. {} connections remaining.",
                remaining_connections, client
            );
        })
    }

    #[tracing::instrument(skip(self, message))]
    async fn process_message(&mut self, message: Message) -> Result<()> {
        debug!("Processing message: {}", message);

        match message.kind() {
            MessageKind::StartupMessage => {
                self.process_startup_message(message).await?;
            }
            MessageKind::QueryMessage => {
                self.process_query_message(message).await?;
            }
            MessageKind::TerminationMessage => {
                self.handle_disconnect().await?;
            }
            _ => {
                self.handle_unknown_message(message).await?;
            }
        }

        Ok(())
    }

    async fn process_startup_message(&mut self, message: Message) -> io::Result<()> {
        // Get the protocol version from the message payload
        let protocol_version = message.protocol_version();

        info!(
            "Startup request with protocol version: {}",
            protocol_version
        );

        // TODO: Perform initialization or setup required for a new client (e.g. authentication)
        // ...

        // Send back a CommandCompleteMessage as a placeholder
        let response = Message::command_complete_message("STARTUP COMPLETE".to_string());
        Protocol::send_message(&mut self.stream, response).await?;
        Ok(())
    }

    async fn process_query_message(&mut self, query_message: Message) -> io::Result<()> {
        // Get the query from the message payload
        let query = query_message.query();

        info!("Received query: `{}`", query);

        // TODO: execute query on db here

        // Send back a CommandCompleteMessage as a placeholder
        // let response = Message::CommandCompleteMessage {
        //     tag: "QUERY EXECUTED".to_string(),
        // };

        let response = Message::command_complete_message("QUERY EXECUTED".to_string());
        Protocol::send_message(&mut self.stream, response).await?;
        Ok(())
    }

    async fn handle_unknown_message(&mut self, message: Message) -> io::Result<()> {
        let error_response = Message::error_response(
            "Unsupported message type: ".to_string() + &message.to_string(),
        );
        Protocol::send_message(&mut self.stream, error_response).await?;
        Ok(())
    }
}
