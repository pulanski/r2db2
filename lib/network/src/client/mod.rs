use crate::protocol::{handler::Protocol, message::Message};
use anyhow::{Context, Result};
use cli::{ClientArgs, NetworkProtocol};
use getset::{Getters, Setters};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{debug, error, info, trace};
use typed_builder::TypedBuilder;

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
    stream: Option<TcpStream>,
}

impl DbClient {
    // Create a new client
    pub fn new(server_address: String) -> Self {
        DbClient::builder()
            .server_address(server_address)
            .protocol(NetworkProtocol::TCP) // Default to TCP
            .ssl(false) // Default to no SSL
            .timeout(None) // Default to no timeout
            .stream(None) // Clients should connect explicitly
            .build()
    }

    // Connect to the server
    pub async fn connect(&self) -> Result<TcpStream> {
        trace!("Connecting to server at {}", &self.server_address);
        TcpStream::connect(&self.server_address)
            .await
            .with_context(|| format!("Failed to connect to server at {}", &self.server_address))
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

    // Send a startup message to the server
    pub async fn send_startup_message(&mut self) -> Result<String> {
        if !self.stream.is_some() {
            self.stream = Some(self.connect().await?); // Connect if not already connected
        }

        let startup_message = Message::serialize_startup_message();

        let mut stream = self.stream.as_mut().unwrap();
        stream
            .write_all(&startup_message)
            .await
            .context("Failed to send startup message to the server")?;

        let response = DbClient::process_response(&mut stream).await?;

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

pub async fn start_client(args: &ClientArgs) -> Result<()> {
    info!(host = ?args.host(), port = ?args.port(), "Starting client");

    let server_address = format!("{}:{}", args.host(), args.port());
    let mut client = DbClient::new(server_address);

    // TODO: Clients should engage in a handshake with the server (e.g. SSL)
    let res = client.send_startup_message().await?;
    info!("Received response from server: {}", res);

    // Example: sending a SELECT query
    debug!("Sending query to server");
    match client.send_sql_query("SELECT * FROM users;").await {
        Ok(r) => {
            info!("Query executed successfully: {:?}", r);
        }
        Err(e) => error!("Failed to execute query: {:?}", e),
    }

    // Example: sending an INSERT query
    // if let Err(e) = client
    //     .send_sql_query(
    //         r#"INSERT INTO users (name, email, age) VALUES ("john doe", "jdoe@cs.edu", 28);"#,
    //     )
    //     .await
    // {
    //     error!("Failed to send query: {:?}", e);
    // }

    Ok(())
}
