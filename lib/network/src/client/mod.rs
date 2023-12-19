use anyhow::{Context, Result};
use cli::{ClientArgs, NetworkProtocol};
use getset::{Getters, Setters};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{error, info};
use typed_builder::TypedBuilder;

use crate::protocol::Message;
pub mod tcp;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Failed to connect to the server")]
    ConnectionError(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct DbClient {
    server_address: String,
    protocol: NetworkProtocol,
}

impl DbClient {
    // Create a new client
    pub fn new(server_address: String) -> Self {
        DbClient::builder()
            .server_address(server_address)
            .protocol(NetworkProtocol::TCP) // Default to TCP
            .build()
    }

    // Connect to the server
    pub async fn connect(&self) -> Result<TcpStream> {
        TcpStream::connect(&self.server_address)
            .await
            .with_context(|| format!("Failed to connect to server at {}", &self.server_address))
    }

    // Send a request to the server
    pub async fn send_request(&self, request: &str) -> Result<()> {
        let mut stream = self.connect().await?;
        // Implement the logic to send the request
        // ...

        Ok(())
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
    pub async fn send_sql_query(&self, query: &str) -> Result<()> {
        let mut stream = self.connect().await?;

        let query_message = Message::serialize_query(query);
        stream
            .write_all(&query_message)
            .await
            .context("Failed to send query to the server")?;

        // Optionally, wait for the response
        let response = self.receive_response(&mut stream).await?;
        info!("Response from server: {}", response);

        Ok(())
    }
}

pub async fn start_client(args: &ClientArgs) {
    info!(host = ?args.host(), port = ?args.port(), "Starting client");

    let server_address = format!("{}:{}", args.host(), args.port());
    let client = DbClient::new(server_address);

    // Example: sending a SELECT query
    if let Err(e) = client.send_sql_query("SELECT * FROM users;").await {
        error!("Failed to send query: {:?}", e);
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
