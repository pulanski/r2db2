use anyhow::{anyhow, Context, Result};
use cli::{ClientArgs, NetworkProtocol};
use getset::{Getters, Setters};
use thiserror::Error;
use tokio::time::{sleep, Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{debug, error, info, trace, warn};
use typed_builder::TypedBuilder;

use crate::protocol::{
    message::{Message, MessageKind},
    Protocol,
};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Connection error: {0}")]
    ConnectionError(#[from] std::io::Error),

    #[error("Response error: {0}")]
    ResponseError(String),
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
    pub async fn connect(&mut self) -> Result<()> {
        // If we already have a stream, return
        if self.stream.is_some() {
            trace!("Already connected to server");
            return Ok(());
        }

        trace!("Connecting to server at {}", &self.server_address);
        // Otherwise, create a new stream
        let stream = TcpStream::connect(&self.server_address)
            .await
            .context("Failed to connect to server")?;

        // Set the stream
        self.set_stream(Some(stream));

        Ok(())
    }

    // General method to process responses
    async fn process_response(stream: &mut TcpStream) -> Result<()> {
        if let Some(message) = Protocol::parse_incoming(stream).await? {
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
                MessageKind::DataRowMessage => todo!(),
                MessageKind::CommandCompleteMessage => {
                    let tag = "PLACEHOLDER";
                    let response = Message::serialize_command_complete(tag);
                    stream
                        .write_all(&response)
                        .await
                        .context("Failed to send command complete response to server")?;
                }
                MessageKind::TerminationMessage => todo!(),
                MessageKind::ErrorResponse => todo!(),
                MessageKind::AuthenticationRequest => todo!(),
                MessageKind::ReadyForQuery => todo!(),
            }
        } else {
            error!("Failed to parse incoming message");
        }

        info!("Received response from server");

        Ok(())
    }

    async fn receive_and_process_response(stream: &mut TcpStream) -> Result<()> {
        let response = DbClient::receive_response(stream).await?;

        // TODO: handle reponse: parse it, log it, handle errors, etc.
        info!("Received response from server: {}", response);

        Ok(())
    }

    async fn receive_response(stream: &mut TcpStream) -> Result<String> {
        let mut buffer = [0; 1024];
        let n = stream
            .read(&mut buffer)
            .await
            .context("Failed to read response from server")?;

        let response = String::from_utf8_lossy(&buffer[..n]).to_string();
        Ok(response)
    }

    // Send a startup message to the server and process the response
    pub async fn send_startup_message(&mut self) -> Result<()> {
        self.connect().await?;

        let startup_message = Message::serialize_startup_message();

        trace!("Sending startup message");
        if let Some(stream) = &mut self.stream {
            stream
                .write_all(&startup_message)
                .await
                .context("Failed to send startup message to the server")?;

            DbClient::receive_and_process_response(stream).await?;
        }

        Ok(())
    }

    // Send a SQL query to the server and process the response
    pub async fn send_sql_query(&mut self, query: &str) -> Result<()> {
        // self.connect().await?;
        let query_message = Message::serialize_query(query);

        trace!("Sending query message");

        if let Some(stream) = &mut self.stream {
            stream.write_all(&query_message).await.context(format!(
                "Failed to send query message '{}' to the server",
                query
            ))?;

            DbClient::receive_and_process_response(stream).await?;
        }

        Ok(())
    }

    pub async fn connect_with_retry(
        &mut self,
        max_retries: u32,
        base_delay: Duration,
    ) -> Result<()> {
        let mut retries = 0;
        let mut delay = base_delay;

        while retries < max_retries {
            match self.connect().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!("Failed to connect: {}. Retrying in {:?}...", e, delay);
                    sleep(delay).await;
                    retries += 1;
                    delay *= 2; // Exponential backoff
                }
            }
        }

        error!("Failed to connect after {} retries", max_retries);
        Err(anyhow!(
            "Unable to connect to server after multiple retries"
        ))
    }
}

pub async fn start_client(args: &ClientArgs) -> Result<()> {
    info!(host = ?args.host(), port = ?args.port(), "Starting client");

    let server_address = format!("{}:{}", args.host(), args.port());
    let mut client = DbClient::new(server_address);

    if let Err(e) = client.connect_with_retry(5, Duration::from_secs(1)).await {
        error!("Failed to establish a connection: {:?}", e);
        return Err(e);
    }

    // TODO: Clients should engage in a handshake with the server (e.g. SSL)
    client.send_startup_message().await?;

    // Example: sending a SELECT query
    debug!("Sending query to server");
    match client.send_sql_query("SELECT * FROM users;").await {
        Ok(_) => {
            info!("Query executed successfully");
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

    Ok(())
}
