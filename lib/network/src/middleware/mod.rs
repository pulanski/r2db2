use anyhow::Result;
use async_trait::async_trait;
use core::fmt;
use getset::{Getters, Setters};
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpStream;
use tracing::trace;
use typed_builder::TypedBuilder;

pub mod trace;

/// The `Middleware` trait defines the interface for middleware components in the server.
///
/// Middleware components are responsible for handling various aspects of the server's request-response lifecycle.
/// Implementors of this trait can perform a range of operations, such as logging, authentication, input validation,
/// and more, at different stages of the connection lifecycle. Each middleware component is executed in the order
/// they are added to the `MiddlewareStack`.
///
/// # Lifecycle Hooks
///
/// - `on_connect`: Called when a new TCP connection is established.
/// - `before_request`: Invoked before a request is processed.
/// - `after_request`: Invoked after a request has been processed.
/// - `on_disconnect`: Called when a TCP connection is terminated.
///
/// Implementors of this trait should ensure that operations are non-blocking and asynchronous,
/// as the server operates in an async runtime environment, with the overarching goal of minimizing
/// the impact of middleware on the server's performance by having asynchrony everywhere.
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Returns the name of the middleware.
    ///
    /// This can be used for logging, debugging, or other purposes where identifying the middleware is useful.
    ///
    /// # Examples
    ///
    /// ```
    /// struct LoggingMiddleware;
    /// impl Middleware for LoggingMiddleware {
    ///     fn name(&self) -> String {
    ///         "LoggingMiddleware".to_string()
    ///     }
    ///     // other hook implementations...
    /// }
    /// ```
    fn name(&self) -> String;

    /// Hook that is called when a new TCP connection is established.
    ///
    /// This method is invoked when a new client connects to the server. It can be used for initializing
    /// per-connection resources, logging connection information, performing initial authentication, etc.
    ///
    /// # Arguments
    ///
    /// * `stream` - A reference to the TCP stream representing the client connection.
    ///
    /// # Errors
    ///
    /// Implementors should return an error if any operation in this hook fails. This will generally result in
    /// the termination of the connection.
    async fn on_connect(&self, stream: &TcpStream) -> Result<()>;

    /// Hook that is called before a request is processed by the server.
    ///
    /// This method allows middleware to perform actions or transformations on the incoming data before
    /// the server processes the request. This can be used for preprocessing tasks like request validation,
    /// logging, etc.
    ///
    /// # Arguments
    ///
    /// * `stream` - A mutable reference to the TCP stream, allowing middleware to modify the incoming data.
    ///
    /// # Errors
    ///
    /// Implementors should return an error if any operation in this hook fails. Depending on the server's
    /// implementation, this may halt further processing of the request.
    async fn before_request(&self, stream: &mut TcpStream) -> Result<()>;

    /// Hook that is called after a request has been processed by the server.
    ///
    /// This method allows middleware to perform actions after the server has processed the request.
    /// Useful for post-processing tasks like logging, modifying the response, gathering metrics, etc.
    ///
    /// # Arguments
    ///
    /// * `stream` - A mutable reference to the TCP stream, allowing middleware to modify the outgoing data.
    ///
    /// # Errors
    ///
    /// Implementors should return an error if any operation in this hook fails. Depending on the server's
    /// implementation, this may affect the final response sent to the client.
    async fn after_request(&self, stream: &mut TcpStream) -> Result<()>;

    /// Hook that is called when a TCP connection is terminated.
    ///
    /// This method is invoked when a client disconnects or the connection is otherwise terminated.
    /// Useful for cleanup tasks, logging disconnection events, releasing resources, etc.
    ///
    /// # Arguments
    ///
    /// * `stream` - A reference to the TCP stream that is being disconnected.
    ///
    /// # Errors
    ///
    /// Implementors should return an error if any operation in this hook fails, although since the connection
    /// is closing, the impact of such errors is typically limited.
    async fn on_disconnect(&self, stream: &TcpStream) -> Result<()>;
}

/// A reference-counted reference to a [`MiddlewareStack`].
pub type MiddlewareStackRef = Arc<MiddlewareStack>;

/// Represents a stack of middleware components.
///
/// This struct manages a sequence of middleware components that can interact with, modify, or
/// observe the flow of data and control in the server lifecycle. Each middleware can perform
/// actions at different stages of a connection lifecycle, including connection establishment,
/// before handling a request, after handling a request, and upon disconnection.
#[derive(Default, Getters, Setters, TypedBuilder)]
#[getset(get = "pub", set = "pub")]
pub struct MiddlewareStack {
    middlewares: Vec<Box<dyn Middleware>>,
}

impl MiddlewareStack {
    /// Constructs a new, empty [`MiddlewareStack`].
    ///
    /// Returns an instance of `MiddlewareStack` with no middleware components.
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    pub fn add_middleware<M: Middleware + 'static>(&mut self, middleware: M) {
        let start = Instant::now();
        let name = middleware.name();
        self.middlewares.push(Box::new(middleware));
        trace!(
            "Added middleware {} to middleware stack in {:?}",
            name,
            start.elapsed()
        );
    }

    pub async fn handle_connect(&self, stream: &TcpStream) -> anyhow::Result<()> {
        for middleware in &self.middlewares {
            middleware.on_connect(stream).await?;
        }
        Ok(())
    }

    pub async fn handle_before_request(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        for middleware in &self.middlewares {
            middleware.before_request(stream).await?;
        }
        Ok(())
    }

    pub async fn handle_after_request(&self, stream: &mut TcpStream) -> anyhow::Result<()> {
        for middleware in &self.middlewares {
            middleware.after_request(stream).await?;
        }
        Ok(())
    }

    pub async fn handle_disconnect(&self, stream: &TcpStream) -> anyhow::Result<()> {
        for middleware in &self.middlewares {
            middleware.on_disconnect(stream).await?;
        }
        Ok(())
    }
}

impl fmt::Debug for MiddlewareStack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MiddlewareStack")
            .field("middlewares", &self.middlewares.len())
            .finish()
    }
}
