use super::Middleware;
use async_trait::async_trait;
use common::util::time::{elapsed_duration_since, format_duration, now_as_u64};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tracing::{info, warn};

/// Middleware for logging various stages of the connection lifecycle.
///
/// This middleware logs the establishment and termination of connections, as well as the
/// handling of requests, doing so in a thread-safe manner. Utilizes atomic counters
/// to keep track of connection and request start times for accurate duration logging
/// in the [`after_request`] and [`on_disconnect`] methods.
pub struct LoggingMiddleware {
    // Timestamp of when the connection was established.
    connection_start_time: Arc<AtomicU64>,
    // Timestamp of when a request handling started.
    request_start_time: Arc<AtomicU64>,
}

impl LoggingMiddleware {
    pub fn new() -> Self {
        LoggingMiddleware {
            connection_start_time: Arc::new(AtomicU64::new(0)),
            request_start_time: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl Middleware for LoggingMiddleware {
    #[inline]
    fn name(&self) -> String {
        "LoggingMiddleware".to_string()
    }

    #[inline]
    async fn on_connect(&self, stream: &TcpStream) -> anyhow::Result<()> {
        self.connection_start_time
            .store(now_as_u64(), Ordering::SeqCst);
        info!("Connection established with {}", stream.peer_addr()?);
        Ok(())
    }

    #[inline]
    async fn before_request(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        self.request_start_time
            .store(now_as_u64(), Ordering::SeqCst);
        info!("Handling request from {}", stream.peer_addr()?);
        Ok(())
    }

    #[inline]
    async fn after_request(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        let request_duration =
            elapsed_duration_since(self.request_start_time.load(Ordering::SeqCst));
        info!(
            "Request handled successfully for {} (took {})",
            stream.peer_addr()?,
            format_duration(request_duration)
        );
        Ok(())
    }

    #[inline]
    async fn on_disconnect(&self, stream: &TcpStream) -> anyhow::Result<()> {
        let connection_duration =
            elapsed_duration_since(self.connection_start_time.load(Ordering::SeqCst));
        warn!(
            "Connection terminated with {} (lifespan: {})",
            stream.peer_addr()?,
            format_duration(connection_duration)
        );
        Ok(())
    }
}
