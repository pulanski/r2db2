use super::message::{Message, MessageFormat};
use crate::middleware::MiddlewareStackRef;
use crate::protocol::message::MessageKind;
use crate::server::tcp::{generate_connection_id, ConnectionId};
use anyhow::{anyhow, Result};
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use driver::DriverRef;
use std::io::{self, Result as IoResult};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info, trace};
use typed_builder::TypedBuilder;

#[derive(Debug, TypedBuilder)]
pub struct ConnectionHandler {
    stream: TcpStream,
    receiver: Receiver<TcpStream>,
    driver: DriverRef,
    connections: Arc<DashMap<ConnectionId, Sender<TcpStream>>>,
    middleware_stack: MiddlewareStackRef,
}

impl ConnectionHandler {
    pub fn new(
        stream: TcpStream,
        receiver: Receiver<TcpStream>,
        driver: DriverRef,
        connections: Arc<DashMap<ConnectionId, Sender<TcpStream>>>,
        middleware_stack: MiddlewareStackRef,
    ) -> Self {
        ConnectionHandler::builder()
            .stream(stream)
            .receiver(receiver)
            .driver(driver)
            .connections(connections)
            .middleware_stack(middleware_stack)
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

pub struct Protocol;

impl Protocol {
    // Parses incoming data from the client
    // return a type which implements the MessageFormat trait (e.g. Message)
    pub async fn parse_incoming<R: AsyncReadExt + Unpin>(
        stream: &mut R,
    ) -> IoResult<Option<Message>> {
        let mut header = [0_u8; 5];
        if stream.read_exact(&mut header).await.is_err() {
            return Ok(None); // Handle connection close, return None
        }

        let message_kind = header[0];
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        trace!(
            "Received message: `{}` ({} bytes including header)",
            Message::kind_to_string(message_kind),
            length
        );

        // Check for a reasonable message length to prevent capacity overflow
        if length <= 5 || length > 10_000 {
            error!("Invalid message length: {}. Closing connection.", length);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid message length",
            ));
        }

        let mut buffer = vec![0; (length - Message::HEADER_LENGTH as i32) as usize];
        stream.read_exact(&mut buffer).await?;

        match MessageKind::from_u8(message_kind) {
            MessageKind::QueryMessage => {
                let query = String::from_utf8_lossy(&buffer).to_string();

                let message = Message::query_message(query);

                Ok(Some(message))
            }
            MessageKind::StartupMessage => {
                let protocol_version =
                    i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
                Ok(Some(Message::startup_message(protocol_version)))
            }
            MessageKind::CommandCompleteMessage => {
                let tag = String::from_utf8_lossy(&buffer).to_string();
                Ok(Some(Message::command_complete_message(tag)))
            }
            _ => unimplemented!("Message type not yet implemented: {}", message_kind),
        }
    }

    // Serializes and sends a message to the client
    pub async fn send_message<W: AsyncWriteExt + Unpin>(
        stream: &mut W,
        message: Message,
    ) -> IoResult<()> {
        let mut buffer = BytesMut::new();

        trace!(
            "Sending message: {} ({} bytes) over the wire.",
            message.kind(),
            message.len()
        );

        let message_kind = message.kind().to_u8();
        let payload = message.payload();

        // Write the message header to the buffer
        buffer.put_u8(message_kind);
        buffer.extend_from_slice(&i32::to_be_bytes(
            (payload.len() + Message::HEADER_LENGTH as usize) as i32,
        ));

        // Write the message payload to the buffer
        buffer.extend_from_slice(&payload);

        trace!("Sending payload: {:?}", String::from_utf8_lossy(&payload));

        stream.write_all(&buffer).await
    }
}
