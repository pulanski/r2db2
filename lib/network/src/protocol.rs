use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use std::io::Result as IoResult;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Message types
///
/// | Type |        Name            |              Description              |
/// | -    | ---------------------- | ------------------------------------  |
/// | 0    | StartupMessage         | Initiates a connection to the server  |
/// | 1    | QueryMessage           | Executes a query                      |
/// | 2    | DataRowMessage         | A row of data                         |
/// | 3    | CommandCompleteMessage | The result of a command               |
/// | 4    | TerminationMessage     | Terminates a connection to the server |
/// | 5    | ErrorResponse          | An error response                     |

const TYPE_STARTUP: u8 = 0x01;
const TYPE_QUERY: u8 = 0x02;
const TYPE_DATA_ROW: u8 = 0x03;
const TYPE_COMMAND_COMPLETE: u8 = 0x04;
const TYPE_TERMINATION: u8 = 0x05;

pub struct Protocol;

pub enum Message {
    StartupMessage { protocol_version: i32 },
    QueryMessage { query: String },
    DataRowMessage { row: Vec<String> },
    CommandCompleteMessage { tag: String },
    TerminationMessage,
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

    pub fn message_type(&self) -> u8 {
        match self {
            Message::StartupMessage { .. } => TYPE_STARTUP,
            Message::QueryMessage { .. } => TYPE_QUERY,
            Message::DataRowMessage { .. } => TYPE_DATA_ROW,
            Message::CommandCompleteMessage { .. } => TYPE_COMMAND_COMPLETE,
            Message::TerminationMessage => TYPE_TERMINATION,
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
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) - 5;

        let mut buffer = vec![0; length as usize];
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
            // ... Handle other message types
            _ => unimplemented!(),
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
                // ... serialize the DataRowMessage, calculate length, and put it in buffer
            }
            Message::CommandCompleteMessage { tag } => {
                buffer.put_u8(TYPE_COMMAND_COMPLETE);
                // ... serialize the CommandCompleteMessage, calculate length, and put it in buffer
            }
            // ... Handle other message types
            _ => unimplemented!(),
        }

        stream.write_all(&buffer).await
    }
}
