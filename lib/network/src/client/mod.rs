use anyhow::{Context, Result};
use cli::{ClientArgs, NetworkProtocol};
use getset::{Getters, Setters};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{error, info};
use typed_builder::TypedBuilder;

use crate::protocol::{Message, Protocol};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Failed to connect to the server")]
    ConnectionError(#[from] std::io::Error),
}

#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct DbClient {
    server_address: String,
    protocol: NetworkProtocol,
    timeout: Option<u64>,
    ssl: bool,
    socket: Option<TcpStream>,
}

impl DbClient {
    // Create a new client
    pub fn new(server_address: String) -> Self {
        DbClient::builder()
            .server_address(server_address)
            .protocol(NetworkProtocol::TCP) // Default to TCP
            .ssl(false) // Default to no SSL
            .timeout(None) // Default to no timeout
            .socket(None) // Clients should connect explicitly
            .build()
    }

    // Connect to the server
    pub async fn connect(&self) -> Result<TcpStream> {
        TcpStream::connect(&self.server_address)
            .await
            .with_context(|| format!("Failed to connect to server at {}", &self.server_address))
    }

    async fn send_message(&self, message: &Message) -> Result<()> {
        let mut stream = self.connect().await?;
        let serialized_message = message.serialize();
        stream
            .write_all(&serialized_message)
            .await
            .context("Failed to send message to the server")?;

        Ok(())
    }

    // General method to process responses
    async fn process_response(stream: &mut TcpStream) -> Result<String> {
        if let Some(message) = Protocol::parse_incoming(stream).await? {
            match message {
                Message::DataRowMessage { row } => Ok(format!("Data Row: {:?}", row)),
                Message::CommandCompleteMessage { tag } => {
                    Ok(format!("Command Completed: {}", tag))
                }
                Message::ErrorResponse { error } => Err(anyhow::Error::msg(error)),
                _ => Err(anyhow::Error::msg("Unexpected response type")),
            }
        } else {
            Err(anyhow::Error::msg("No response received"))
        }
    }

    // Send a startup message
    pub async fn send_startup_message(&self) -> Result<()> {
        let startup_message = Message::StartupMessage {
            protocol_version: 1,
        };
        self.send_message(&startup_message).await
    }

    // Receive a response from the server
    async fn receive_response(&self, stream: &mut TcpStream) -> Result<String> {
        let mut buffer = [0; 1024];
        let n = stream
            .read(&mut buffer)
            .await
            .context("Failed to read response from server")?;

        let response = String::from_utf8_lossy(&buffer[..n]).to_string();
        Ok(response)
    }

    // Send a SQL query to the server
    pub async fn send_sql_query(&self, query: &str) -> Result<String> {
        let mut stream = self.connect().await?;

        let query_message = Message::serialize_query(query);
        stream
            .write_all(&query_message)
            .await
            .context("Failed to send query to the server")?;

        let response = self.receive_response(&mut stream).await?;

        Ok(response)
    }
}

pub async fn start_client(args: &ClientArgs) {
    info!(host = ?args.host(), port = ?args.port(), "Starting client");

    let server_address = format!("{}:{}", args.host(), args.port());
    let client = DbClient::new(server_address);

    // Example: sending a SELECT query
    match client.send_sql_query("SELECT * FROM users;").await {
        Ok(r) => {
            info!(
                "Query executed successfully: {:?}",
                r // client.receive_response(&mut r.as_bytes()).await
            );
        }
        Err(e) => error!("Failed to execute query: {:?}", e),
    }

    // Example: sending an INSERT query
    if let Err(e) = client
        .send_sql_query(
            r#"INSERT INTO users (name, email, age) VALUES ("john doe", "jdoe@cs.edu", 28);"#,
        )
        .await
    {
        error!("Failed to send query: {:?}", e);
    }
}
