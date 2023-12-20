//! # Message
//!
//! Below is a list of all the messages that can be sent to and from the server.
//!
//! ## Message format
//!
//! _Protocol version 1.0_
//!
//! All messages have the same format:
//!
//! ```text
//! +---------------+-------------------+----------------------------------+
//! | Message Type  | Message Length    | Message Payload                  |
//! | (1 byte)      | (4 bytes)         | (Variable length)                |
//! +---------------+-------------------+----------------------------------+
//! ```
//!
//! - Message Type: A single byte indicating the type of the message (e.g., 0x01 for StartupMessage).
//! - Message Length: A 4-byte integer (32 bits) representing the total length of the message, including the header.
//! - Message Payload: The actual data/content of the message, varying in length and format depending on the message type.
//!
//! ## Message types
//!
//! | Type |        Name            |              Description              |         Data Flow       |
//! | -    | ---------------------- | ------------------------------------  | ----------------------- |
//! | 1    | StartupMessage         | Initiates a connection to the server  | Client -> Server        |
//! | 2    | QueryMessage           | Executes a query                      | Client -> Server        |
//! | 3    | DataRowMessage         | A row of data                         | Server -> Client        |
//! | 4    | CommandCompleteMessage | The result of a command               | Server -> Client        |
//! | 5    | TerminationMessage     | Terminates a connection to the server | Client <-> Server       |
//! | 6    | ErrorResponse          | An error response                     | Server -> Client        |
//! | 7    | AuthenticationRequest  | Authentication request                | Server -> Client        |
//! | 8    | ReadyForQuery          | Ready for query                       | Server -> Client        |

use crate::auth::{password::PasswordAuthenticator, token::TokenAuthenticator};
use anyhow::Result;
use bytes::{BufMut, BytesMut};
use core::fmt;
use getset::{Getters, Setters};
use std::mem;
use tracing::{error, warn};
use typed_builder::TypedBuilder;

/// Represents the different kinds of messages in the protocol.
#[derive(Debug, Clone, Copy)]
pub enum MessageKind {
    /// Message sent by the client to initiate a connection.
    StartupMessage = 0x01,
    /// Message sent by the client to execute a SQL query.
    QueryMessage = 0x02,
    /// Message sent by the server containing a row of query result data.
    DataRowMessage = 0x03,
    /// Message sent by the server indicating the completion of a command.
    CommandCompleteMessage = 0x04,
    /// Message to terminate a connection, can be sent by both client and server.
    TerminationMessage = 0x05,
    /// Message sent by the server in response to an error.
    ErrorResponse = 0x06,
    /// Message sent by the server requesting authentication from the client.
    AuthenticationRequest = 0x07,
    /// Message sent by the server indicating it is ready for a new query.
    ReadyForQuery = 0x08,
}

/// Common functionality shared by all messages.
pub trait MessageFormat {
    /// Returns the kind of the message.
    fn kind(&self) -> MessageKind;
    /// Returns the payload of the message as a byte sequence.
    fn payload(&self) -> BytesMut;

    /// Calculates the total length of the message (header + payload).
    fn len(&self) -> usize {
        Message::HEADER_LENGTH as usize + self.payload().len()
    }

    /// Serializes the message into a byte sequence for transmission.
    fn serialize(&self) -> BytesMut {
        let mut res = BytesMut::new();
        res.put_u8(self.kind().to_u8()); // Message type
        res.put_u32(self.len() as u32); // Message length including itself
        res.extend_from_slice(&self.payload()); // Message payload
        res
    }
}

impl MessageKind {
    pub fn from_u8(byte: u8) -> MessageKind {
        match byte {
            0x01 => MessageKind::StartupMessage,
            0x02 => MessageKind::QueryMessage,
            0x03 => MessageKind::DataRowMessage,
            0x04 => MessageKind::CommandCompleteMessage,
            0x05 => MessageKind::TerminationMessage,
            0x06 => MessageKind::ErrorResponse,
            _ => {
                warn!("Unknown message type: {}", byte);
                MessageKind::ErrorResponse
            }
        }
    }

    pub fn to_u8(&self) -> u8 {
        match self {
            MessageKind::StartupMessage => 0x01,
            MessageKind::QueryMessage => 0x02,
            MessageKind::DataRowMessage => 0x03,
            MessageKind::CommandCompleteMessage => 0x04,
            MessageKind::TerminationMessage => 0x05,
            MessageKind::ErrorResponse => 0x06,
            MessageKind::AuthenticationRequest => 0x07,
            MessageKind::ReadyForQuery => 0x08,
        }
    }
}

impl fmt::Display for MessageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            MessageKind::StartupMessage => "StartupMessage",
            MessageKind::QueryMessage => "QueryMessage",
            MessageKind::DataRowMessage => "DataRowMessage",
            MessageKind::CommandCompleteMessage => "CommandCompleteMessage",
            MessageKind::TerminationMessage => "TerminationMessage",
            MessageKind::ErrorResponse => "ErrorResponse",
            MessageKind::AuthenticationRequest => "AuthenticationRequest",
            MessageKind::ReadyForQuery => "ReadyForQuery",
        };

        write!(f, "{}", kind)
    }
}

#[derive(Debug)]
pub enum Message {
    StartupMessage(StartupMessage),
    QueryMessage(QueryMessage),
    DataRowMessage(DataRowMessage),
    CommandCompleteMessage(CommandCompleteMessage),
    TerminationMessage(TerminationMessage),
    ErrorResponse(ErrorResponse),
    ReadyForQuery(ReadyForQueryMessage),
    AuthenticationRequest(AuthenticationRequestMessage),
}

impl MessageFormat for Message {
    fn kind(&self) -> MessageKind {
        match self {
            Message::StartupMessage(_) => MessageKind::StartupMessage,
            Message::QueryMessage(_) => MessageKind::QueryMessage,
            Message::DataRowMessage(_) => MessageKind::DataRowMessage,
            Message::CommandCompleteMessage(_) => MessageKind::CommandCompleteMessage,
            Message::TerminationMessage(_) => MessageKind::TerminationMessage,
            Message::ErrorResponse(_) => MessageKind::ErrorResponse,
            Message::ReadyForQuery(_) => MessageKind::ReadyForQuery,
            Message::AuthenticationRequest(_) => MessageKind::AuthenticationRequest,
        }
    }

    fn payload(&self) -> BytesMut {
        match self {
            Message::StartupMessage(message) => message.payload(),
            Message::QueryMessage(message) => message.payload(),
            Message::DataRowMessage(message) => message.payload(),
            Message::CommandCompleteMessage(message) => message.payload(),
            Message::TerminationMessage(message) => message.payload(),
            Message::ErrorResponse(message) => message.payload(),
            Message::ReadyForQuery(message) => message.payload(),
            Message::AuthenticationRequest(message) => message.payload(),
        }
    }
}

impl Message {
    pub const PROTOCOL_VERSION: u32 = 1; // v1.0
    pub const HEADER_LENGTH: u32 = 5; // 4 bytes for length field + 1 byte for type

    pub fn kind_to_string(buffer: u8) -> String {
        MessageKind::from_u8(buffer).to_string()
    }

    pub fn kind(&self) -> MessageKind {
        match self {
            Message::StartupMessage(_) => MessageKind::StartupMessage,
            Message::QueryMessage(_) => MessageKind::QueryMessage,
            Message::DataRowMessage(_) => MessageKind::DataRowMessage,
            Message::CommandCompleteMessage(_) => MessageKind::CommandCompleteMessage,
            Message::TerminationMessage(_) => MessageKind::TerminationMessage,
            Message::ErrorResponse(_) => MessageKind::ErrorResponse,
            Message::ReadyForQuery(_) => MessageKind::ReadyForQuery,
            Message::AuthenticationRequest(_) => MessageKind::AuthenticationRequest,
        }
    }

    pub fn serialize_query(query: &str) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(MessageKind::QueryMessage.to_u8());

        let query_bytes = query.as_bytes();
        let len = (query_bytes.len() as u32) + Message::HEADER_LENGTH;

        buffer.put_u32(len); // Message length including itself
        buffer.put(query_bytes); // The actual SQL query

        buffer
    }

    pub fn serialize_startup_message() -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(MessageKind::StartupMessage.to_u8());

        let len = (mem::size_of::<u32>() as u32) + Message::HEADER_LENGTH;
        buffer.put_u32(len); // Message length including itself
        buffer.put_u32(Message::PROTOCOL_VERSION); // Protocol version

        buffer
    }

    pub fn serialize_query_response() -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(MessageKind::DataRowMessage.to_u8());

        let len = (mem::size_of::<u32>() as u32) + Message::HEADER_LENGTH;
        buffer.put_u32(len); // Message length including itself
        buffer.put_u32(1); // Number of columns

        buffer
    }

    pub fn serialize_authentication_ok() -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(MessageKind::StartupMessage.to_u8());

        let len = (mem::size_of::<u32>() as u32) + Message::HEADER_LENGTH;
        buffer.put_u32(len); // Message length including itself
        buffer.put_u32(0); // Authentication OK (0)

        buffer
    }

    pub fn serialize_command_complete(tag: &str) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(MessageKind::CommandCompleteMessage.to_u8());

        let len = (tag.len() as u32) + Message::HEADER_LENGTH;
        buffer.put_u32(len); // Message length including itself
        buffer.put(tag.as_bytes()); // Actual command tag

        buffer
    }

    pub fn query_message(query: String) -> Message {
        Message::QueryMessage(QueryMessage::builder().query(query).build())
    }

    pub fn startup_message(protocol_version: i32) -> Message {
        Message::StartupMessage(
            StartupMessage::builder()
                .protocol_version(protocol_version as u32)
                .build(),
        )
    }

    pub fn error_response(error: String) -> Message {
        Message::ErrorResponse(ErrorResponse::builder().error(error).build())
    }

    pub fn command_complete_message(tag: String) -> Message {
        Message::CommandCompleteMessage(CommandCompleteMessage::builder().tag(tag).build())
    }

    pub fn authentication_ok() -> Message {
        Message::StartupMessage(StartupMessage::builder().protocol_version(0).build())
    }

    pub fn termination_message() -> Message {
        Message::TerminationMessage(TerminationMessage::builder().status(0).build())
    }

    // Serialize an AuthenticationRequestMessage
    pub fn serialize_authentication_request(auth_type: u8) -> BytesMut {
        let mut buffer = BytesMut::new();

        buffer.put_u8(MessageKind::AuthenticationRequest.to_u8());
        buffer.put_u32(4); // Message length including itself
        buffer.put_u8(auth_type);

        buffer
    }

    // Serialize a ReadyForQueryMessage
    pub fn serialize_ready_for_query() -> BytesMut {
        let mut buffer = BytesMut::new();

        buffer.put_u8(MessageKind::ReadyForQuery.to_u8());
        buffer.put_u32(5); // Message length including itself
        buffer.put_u8(0); // Status code

        buffer
    }

    pub fn query(&self) -> String {
        match self {
            Message::QueryMessage(message) => message.query().clone(),
            _ => panic!("Message is not a QueryMessage"),
        }
    }

    pub fn protocol_version(&self) -> String {
        Message::PROTOCOL_VERSION.to_string()
    }
}

/// Represents a message sent by the client to initiate a connection to the server.
///
/// The `StartupMessage` is the first message sent after establishing a connection,
/// carrying information about the protocol version and optionally, authentication credentials.
#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct StartupMessage {
    /// Protocol version number.
    pub protocol_version: u32,
    /// Optional username for authentication.
    #[builder(default, setter(strip_option))]
    pub username: Option<String>,
    /// Optional password for authentication.
    #[builder(default, setter(strip_option))]
    pub password: Option<String>,
    /// Optional token for token-based authentication.
    #[builder(default, setter(strip_option))]
    pub token: Option<String>,
}

/// Represents a message sent by the client to execute a SQL query.
///
/// `QueryMessage` carries the SQL query text which the server is expected to execute.
/// The query can be any valid SQL statement.
#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct QueryMessage {
    /// The SQL query to be executed.
    pub query: String,
}

/// Represents a message sent by the server containing a row of data from a query result.
///
/// `DataRowMessage` is used in response to a `QueryMessage` when the query yields a result set.
/// Each `DataRowMessage` contains data for a single row, structured into columns.
#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct DataRowMessage {
    /// The data for each column in the row.
    pub columns: Vec<String>,
}

/// Represents a message sent by the server to indicate the completion of a command.
///
/// `CommandCompleteMessage` is used to signal the successful execution of a command
/// such as an SQL query. It includes a tag (e.g., "INSERT 0 1") indicating the type and
/// outcome of the command.
#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct CommandCompleteMessage {
    /// A tag representing the status and result of the command.
    pub tag: String,
}

/// Represents a message to terminate a connection, which can be sent by both client and server.
///
/// `TerminationMessage` is used to gracefully close the connection between the client and the server.
/// It contains a status code indicating the reason or manner of the termination.
#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct TerminationMessage {
    /// Status code indicating the termination reason or type.
    pub status: u8,
}

/// Represents a message sent by the server in response to an error.
///
/// `ErrorResponse` is used by the server to notify the client about an error occurred during
/// processing a request. It includes a descriptive error message.
#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct ErrorResponse {
    /// The error message describing what went wrong.
    pub error: String,
}

/// Represents a message sent by the server requesting authentication from the client.
///
/// `AuthenticationRequestMessage` is sent as part of the connection establishment process,
/// prompting the client to provide necessary authentication details, such as a password or token.
#[derive(Debug, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct AuthenticationRequestMessage {
    /// Type of authentication being requested (e.g., password, token).
    pub auth_type: u8,
}

/// Represents a message sent by the server indicating it is ready to receive a new query.
///
/// `ReadyForQueryMessage` signals to the client that the server has completed processing
/// the previous command and is ready to receive the next query.
#[derive(Debug)]
pub struct ReadyForQueryMessage;

impl StartupMessage {
    pub fn authenticate(&self) -> Message {
        match self {
            StartupMessage {
                username: Some(username),
                password: Some(password),
                ..
            } => {
                let password_authenticator = PasswordAuthenticator::new();

                if password_authenticator.authenticate(&username, &password) {
                    // Proceed with connection
                    Message::ReadyForQuery(ReadyForQueryMessage)
                } else {
                    error!(
                        "Request to authenticate with invalid credentials: {} / {}",
                        username, password
                    );
                    Message::error_response("Invalid authentication credentials".to_string())
                }
            }
            StartupMessage {
                token: Some(token), ..
            } => {
                let token_authenticator = TokenAuthenticator::new("my_secret_key".to_string());

                if token_authenticator.authenticate(&token).unwrap_or(false) {
                    // Proceed with connection
                    Message::ReadyForQuery(ReadyForQueryMessage)
                } else {
                    error!("Request to authenticate with invalid token: {}", token);
                    Message::error_response("Invalid authentication credentials".to_string())
                }
            }
            _ => {
                error!(
                    "Invalid authentication credentials (no username/password or token provided)"
                );
                Message::error_response("Invalid authentication credentials".to_string())
            }
        }
    }
}

impl MessageFormat for StartupMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::StartupMessage
    }

    fn payload(&self) -> BytesMut {
        let mut payload = BytesMut::new();
        payload.put_u32(self.protocol_version); // Protocol version

        payload
    }
}

impl MessageFormat for QueryMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::QueryMessage
    }

    fn payload(&self) -> BytesMut {
        let mut payload = BytesMut::new();

        payload.put(self.query.as_bytes()); // The actual SQL query

        payload
    }
}

impl QueryMessage {
    pub fn validate(&self) -> Result<()> {
        // TODO: Validate the query to prevent SQL injection and other vulnerabilities
        // Return Ok(()) if valid, Err with an error message if not

        Ok(())
    }
}

impl MessageFormat for DataRowMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::DataRowMessage
    }

    fn payload(&self) -> BytesMut {
        let mut payload = BytesMut::new();

        payload.put_u32(self.columns.len() as u32); // Number of columns
        for column in &self.columns {
            payload.put(column.as_bytes()); // The actual partial result set
        }

        payload
    }
}

impl MessageFormat for CommandCompleteMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::CommandCompleteMessage
    }

    fn payload(&self) -> BytesMut {
        let mut payload = BytesMut::new();

        payload.put(self.tag.as_bytes()); // Actual command tag

        payload
    }
}

impl MessageFormat for TerminationMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::TerminationMessage
    }

    fn payload(&self) -> BytesMut {
        let mut payload = BytesMut::new();

        payload.put_u8(self.status); // Status code

        payload
    }
}

impl MessageFormat for ErrorResponse {
    fn kind(&self) -> MessageKind {
        MessageKind::ErrorResponse
    }

    fn payload(&self) -> BytesMut {
        let mut payload = BytesMut::new();

        payload.put(self.error.as_bytes()); // Actual error message

        payload
    }
}

impl MessageFormat for AuthenticationRequestMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::AuthenticationRequest
    }

    fn payload(&self) -> BytesMut {
        let mut payload = BytesMut::new();
        payload.put_u8(self.auth_type);
        payload
    }
}

impl MessageFormat for ReadyForQueryMessage {
    fn kind(&self) -> MessageKind {
        MessageKind::ReadyForQuery
    }

    fn payload(&self) -> BytesMut {
        BytesMut::new() // No additional payload for this message
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Message: {} ({} bytes)\n```\n{:#?}\n```",
            self.kind(),
            self.len(),
            self.payload()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_authentication_success() {
        let message = StartupMessage::builder()
            .protocol_version(Message::PROTOCOL_VERSION)
            .username("test".to_string())
            .password("test".to_string())
            .build();

        assert!(matches!(message.authenticate(), Message::ReadyForQuery(_)));
    }

    #[test]
    fn test_password_authentication_failure() {
        let message = StartupMessage::builder()
            .protocol_version(Message::PROTOCOL_VERSION)
            .username("test".to_string())
            .password("wrong_password".to_string())
            .build();

        assert!(matches!(message.authenticate(), Message::ErrorResponse(_)));
    }

    // TODO: more tests for token-based, certificate-based authentication, and error handling
}
