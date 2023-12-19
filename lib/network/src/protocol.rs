use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::Result as IoResult;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::error;

/// Message types
///
/// | Type |        Name            |              Description              |
/// | -    | ---------------------- | ------------------------------------  |
/// | 1    | StartupMessage         | Initiates a connection to the server  |
/// | 2    | QueryMessage           | Executes a query                      |
/// | 3    | DataRowMessage         | A row of data                         |
/// | 4    | CommandCompleteMessage | The result of a command               |
/// | 5    | TerminationMessage     | Terminates a connection to the server |
/// | 6    | ErrorResponse          | An error response                     |

const TYPE_STARTUP: u8 = 0x01;
const TYPE_QUERY: u8 = 0x02;
const TYPE_DATA_ROW: u8 = 0x03;
const TYPE_COMMAND_COMPLETE: u8 = 0x04;
const TYPE_TERMINATION: u8 = 0x05;
const TYPE_ERROR_RESPONSE: u8 = 0x06;

pub struct Protocol;

pub enum Message {
    StartupMessage { protocol_version: i32 },
    QueryMessage { query: String },
    DataRowMessage { row: Vec<String> },
    CommandCompleteMessage { tag: String },
    TerminationMessage,
    ErrorResponse { error: String },
}

impl Message {
    // Serializes a QueryMessage into bytes
    pub fn serialize_query(query: &str) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(TYPE_QUERY); // Message type for Query

        let query_bytes = query.as_bytes();
        let len = (query_bytes.len() as u32) + 5; // 4 bytes for length field + 1 byte for type
        buffer.put_u32(len); // Message length including itself

        buffer.put(query_bytes); // The actual SQL query

        buffer
    }

    // Serialize different message types
    pub fn serialize(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        match self {
            Message::StartupMessage { protocol_version } => {
                buffer.put_u8(TYPE_STARTUP);
                buffer.put_i32(*protocol_version);
            }
            Message::QueryMessage { query } => {
                buffer.put_u8(TYPE_QUERY);
                buffer.extend_from_slice(query.as_bytes());
            }
            Message::DataRowMessage { row } => {
                buffer.put_u8(TYPE_DATA_ROW);
                // ... serialize the DataRowMessage
            }
            Message::CommandCompleteMessage { tag } => {
                buffer.put_u8(TYPE_COMMAND_COMPLETE);
                buffer.extend_from_slice(tag.as_bytes());
            }
            Message::ErrorResponse { error } => {
                buffer.put_u8(TYPE_ERROR_RESPONSE);
                buffer.extend_from_slice(error.as_bytes());
            }
            Message::TerminationMessage => {
                buffer.put_u8(TYPE_TERMINATION);
            }
        }

        let length = buffer.len() as u32 + 5; // Include the length of type and length fields
        buffer.reserve(length as usize);
        buffer.put_u32(length);
        buffer
    }

    pub fn message_type(&self) -> u8 {
        match self {
            Message::StartupMessage { .. } => TYPE_STARTUP,
            Message::QueryMessage { .. } => TYPE_QUERY,
            Message::DataRowMessage { .. } => TYPE_DATA_ROW,
            Message::CommandCompleteMessage { .. } => TYPE_COMMAND_COMPLETE,
            Message::TerminationMessage => TYPE_TERMINATION,
            Message::ErrorResponse { .. } => TYPE_ERROR_RESPONSE,
        }
    }
}

impl Protocol {
    // Parses incoming data from the client
    pub async fn parse_incoming<R: AsyncReadExt + Unpin>(
        stream: &mut R,
    ) -> IoResult<Option<Message>> {
        let mut header = [0_u8; 5];
        if stream.read_exact(&mut header).await.is_err() {
            return Ok(None); // Handle connection close, return None
        }

        let message_type = header[0];
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);

        // Check for a reasonable message length to prevent capacity overflow
        if length <= 5 || length > 10_000 {
            error!("Invalid message length: {}", length);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid message length",
            ));
        }

        let mut buffer = vec![0; (length - 5) as usize];
        stream.read_exact(&mut buffer).await?;

        match message_type {
            TYPE_STARTUP => {
                let protocol_version =
                    i32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
                Ok(Some(Message::StartupMessage { protocol_version }))
            }
            TYPE_QUERY => {
                let query = String::from_utf8_lossy(&buffer).to_string();
                Ok(Some(Message::QueryMessage { query }))
            }
            TYPE_TERMINATION => Ok(Some(Message::TerminationMessage)),
            _ => unimplemented!("Message type not yet implemented: {}", message_type),
        }
    }

    // Serializes and sends a message to the client
    pub async fn send_message<W: AsyncWriteExt + Unpin>(
        stream: &mut W,
        message: Message,
    ) -> IoResult<()> {
        let mut buffer = BytesMut::new();

        match message {
            Message::DataRowMessage { row } => {
                buffer.put_u8(TYPE_DATA_ROW);

                // Serialize the DataRowMessage, calculate length, and put it in buffer

                let length = buffer.len() as u32 + 5; // Include the length of type and length fields
                buffer.reserve(length as usize);
                buffer.put_u32(length);

                // Serialize the row
                for field in row {
                    let field_bytes = field.as_bytes();
                    buffer.put_u32(field_bytes.len() as u32);
                    buffer.extend_from_slice(field_bytes);
                }

                // Write the buffer to the stream
                stream.write_all(&buffer).await?;
            }
            Message::CommandCompleteMessage { tag } => {
                buffer.put_u8(TYPE_COMMAND_COMPLETE);

                // Serialize the CommandCompleteMessage, calculate length, and put it in buffer

                let length = buffer.len() as u32 + 5; // Include the length of type and length fields
                buffer.reserve(length as usize);
                buffer.put_u32(length);

                // Write the buffer to the stream
                stream.write_all(&buffer).await?;
            }
            // ... Handle other message types
            _ => unimplemented!(),
        }

        stream.write_all(&buffer).await
    }
}
