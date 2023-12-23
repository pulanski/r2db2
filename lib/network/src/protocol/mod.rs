use self::message::Message;
use crate::protocol::message::{MessageFormat, MessageKind};
use bytes::{BufMut, BytesMut};
use std::io::Result as IoResult;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, trace};

pub mod handler;
pub mod message;

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
