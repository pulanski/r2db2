use bytes::{Buf, BufMut, BytesMut};
use core::fmt;
use std::mem;
use tracing::{trace, warn};

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

pub const TYPE_STARTUP: u8 = 0x01;
pub const TYPE_QUERY: u8 = 0x02;
pub const TYPE_DATA_ROW: u8 = 0x03;
pub const TYPE_COMMAND_COMPLETE: u8 = 0x04;
pub const TYPE_TERMINATION: u8 = 0x05;
pub const TYPE_ERROR_RESPONSE: u8 = 0x06;

#[derive(Debug)]
pub enum Message {
    StartupMessage { protocol_version: i32 },
    QueryMessage { query: String },
    DataRowMessage { row: Vec<String> },
    CommandCompleteMessage { tag: String },
    TerminationMessage,
    ErrorResponse { error: String },
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Message::StartupMessage { protocol_version } => {
                write!(
                    f,
                    "StartupMessage {{ protocol_version: {} }}",
                    protocol_version
                )
            }
            Message::QueryMessage { query } => write!(f, "QueryMessage {{ query: {} }}", query),
            Message::DataRowMessage { row } => write!(f, "DataRowMessage {{ row: {:?} }}", row),
            Message::CommandCompleteMessage { tag } => {
                write!(f, "CommandCompleteMessage {{ tag: {} }}", tag)
            }
            Message::TerminationMessage => write!(f, "TerminationMessage"),
            Message::ErrorResponse { error } => write!(f, "ErrorResponse {{ error: {} }}", error),
        }
    }
}

impl Message {
    pub const PROTOCOL_VERSION: u32 = 1; // v1.0
    pub const HEADER_LENGTH: u32 = 5; // 4 bytes for length field + 1 byte for type

    pub fn serialize_query(query: &str) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(TYPE_QUERY); // Message type for Query

        let query_bytes = query.as_bytes();
        let len = (query_bytes.len() as u32) + Message::HEADER_LENGTH;

        trace!("Query length: {}", len);
        buffer.put_u32(len); // Message length including itself
        buffer.put(query_bytes); // The actual SQL query

        buffer
    }

    pub fn serialize_startup_message() -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(TYPE_STARTUP); // Message type for StartupMessage

        let len = (mem::size_of::<u32>() as u32) + Message::HEADER_LENGTH;
        buffer.put_u32(len); // Message length including itself
        buffer.put_u32(Message::PROTOCOL_VERSION); // Protocol version

        buffer
    }

    pub fn serialize_command_complete_message(tag: &str) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u8(TYPE_COMMAND_COMPLETE); // Message type for CommandCompleteMessage

        let len = (tag.len() as u32) + Message::HEADER_LENGTH;
        buffer.put_u32(len); // Message length including itself
        buffer.put(tag.as_bytes()); // Actual command tag

        buffer
    }

    pub fn type_to_string(buffer: u8) -> String {
        match buffer {
            TYPE_STARTUP => "[StartupMessage]".to_string(),
            TYPE_QUERY => "[QueryMessage]".to_string(),
            TYPE_DATA_ROW => "[DataRowMessage]".to_string(),
            TYPE_COMMAND_COMPLETE => "[CommandCompleteMessage]".to_string(),
            TYPE_TERMINATION => "[TerminationMessage]".to_string(),
            TYPE_ERROR_RESPONSE => "[ErrorResponse]".to_string(),
            _ => "[Unknown]".to_string(),
        }
    }
}
