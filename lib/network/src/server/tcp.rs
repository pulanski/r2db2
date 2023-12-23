use crate::middleware::trace::LoggingMiddleware;
use crate::middleware::{MiddlewareStack, MiddlewareStackRef};
use crate::protocol::handler::ConnectionHandler;
// use crate::protocol::message::{Message, MessageKind};
use crate::protocol::message::{Message, MessageKind};
use crate::protocol::Protocol;
use anyhow::{anyhow, Context, Result};
use axum::{routing::get, Router};
use dashmap::DashMap;
use driver::{Driver, DriverRef};
use metrics::collector::cpu::CpuUsageCollector;
use metrics::collector::memory::MemoryUsageCollector;
use rustc_hash::FxHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::{mpsc, Mutex, Semaphore};
use tracing::{debug, error, info, trace, warn};
use typed_builder::TypedBuilder;
// use metrics::{counter, gauge, register_counter, register_gauge, register_histogram, Histogram, HistogramOpts, HistogramTimer, HistogramVec, Opts, Registry};
use metrics::manager::{MetricsManager, MetricsManagerRef, MetricsServerError};

/// Unique identifier for each connection
pub type ConnectionId = String;

/// A reference-counted [`Semaphore`] handle that can be shared across threads.
pub type SemaphoreRef = Arc<Semaphore>;

#[derive(Error, Debug)]
pub enum ServerError {
    #[error("Connection pool is full. Max connections: {0}")]
    ConnectionPoolFull(usize),
}

///`DbServer` represents a database server with network capabilities.
/// It manages incoming TCP connections, handles client requests,
/// and orchestrates middleware and metrics collection.
#[derive(Debug, TypedBuilder)]
pub struct DbServer {
    server_address: SocketAddr,
    driver: DriverRef,
    middleware_stack: MiddlewareStackRef,
    metrics_manager: MetricsManagerRef,
    connections: Arc<DashMap<ConnectionId, bool>>, //  Stores a flag indicating whether the connection is active
    conn_pool: SemaphoreRef,                       // Semaphore to limit active connections
}

// conn_pool: (mpsc::Sender<()>, mpsc::Receiver<()>), // used for query throttling (max concurrent queries)
// query_throttle: (mpsc::Sender<()>, mpsc::Receiver<()>), // used for connection pooling (max concurrent connections)
// total_connections: Arc<AtomicUsize>,
// active_connections: Arc<AtomicUsize>,

impl DbServer {
    /// Creates a new instance of `DbServer`.
    ///
    /// Arguments:
    /// - `server_address`: The IP address and port for the server to listen on.
    /// - `middleware_stack`: Middleware components for processing requests.
    /// - `max_transactions`: Maximum number of concurrent transactions the server can handle.
    /// - `max_connections`: Maximum number of concurrent connections the server can handle.
    ///
    /// Returns a new `DbServer` instance.
    pub fn new(
        server_address: SocketAddr,
        mut middleware_stack: MiddlewareStack,
        max_transactions: usize,
        max_connections: usize,
    ) -> Self {
        // By default, we use the logging middleware
        middleware_stack.add_middleware(LoggingMiddleware::new());

        let mut metrics_manager = MetricsManager::new();

        metrics_manager.register_collector(
            CpuUsageCollector::builder()
                .system(Arc::new(Mutex::new(System::new_all())))
                .build(),
        );
        metrics_manager.register_collector(
            MemoryUsageCollector::builder()
                .system(Arc::new(Mutex::new(System::new_all())))
                .build(),
        );

        // let conn_pool = mpsc::channel(max_connections); // Each unit `()` represents an available connection
        // let query_throttle = mpsc::channel(max_transactions); // Each unit `()` represents an available transaction slot

        // // Populate the connection pool
        // for _ in 0..max_connections {
        //     conn_pool
        //         .0
        //         .try_send(())
        //         .expect("Failed to populate connection pool");
        // }

        // // Populate the query throttle
        // for _ in 0..max_transactions {
        //     query_throttle
        //         .0
        //         .try_send(())
        //         .expect("Failed to populate query throttle");
        // }

        DbServer::builder()
            .server_address(server_address)
            .connections(Arc::new(DashMap::new()))
            .driver(Arc::new(
                Driver::new("test.db").expect("Failed to create driver"),
            ))
            .middleware_stack(Arc::new(middleware_stack))
            .metrics_manager(Arc::new(metrics_manager))
            .conn_pool(Arc::new(Semaphore::new(max_connections)))
            .build()
    }

    /// Accept incoming connections and spawn a new connection handler
    /// for each one.

    // pub async fn accept_connections(&mut self, listener: TcpListener) -> Result<()> {
    //     while let Ok((mut socket, addr)) = listener.accept().await {
    //         info!(target: "DbServer::accept_connections", "Accepting new connection from {}", addr);

    //         // Increment the total number of connections
    //         self.total_connections
    //             .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    //         // Try to acquire a permit from the connection pool
    //         match self.conn_pool.1.try_recv() {
    //             Ok(_) => {
    //                 // Increment the number of active connections
    //                 self.active_connections
    //                     .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    //                 info!(target: "DbServer::accept_connections", "Connection accepted from {} (active connections: {})", addr, self.active_connections.load(std::sync::atomic::Ordering::SeqCst));

    //                 let (_, rx) = mpsc::channel(1); // Create a channel for communication with the connection handler

    //                 // Successfully acquired a permit, proceed with handling the connection
    //                 let mut connection_handler = ConnectionHandler::new(
    //                     socket,
    //                     rx,
    //                     self.driver.clone(),
    //                     self.connections.clone(),
    //                     self.middleware_stack.clone(),
    //                     self.conn_pool.0.clone(), // Pass the sender to release the permit
    //                     self.query_throttle.0.clone(), // Pass the sender to release the permit
    //                 );

    //                 let active_connections = self.active_connections.clone();

    //                 tokio::spawn(async move {
    //                     if let Err(e) = connection_handler.handle_connection().await {
    //                         error!("Error handling connection: {:?}", e);
    //                     }

    //                     // Decrement the number of active connections
    //                     active_connections.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    //                 });
    //             }
    //             Err(_) => {
    //                 // Connection pool is full, handle accordingly
    //                 warn!(target: "DbServer::accept_connections", "Connection pool is full, rejecting connection from {}", addr);
    //                 // Send a response to the client before closing the socket
    //                 let response = Message::error_response("Connection pool is full".to_string());

    //                 Protocol::send_message(&mut socket, response).await?;

    //                 // Close the socket
    //                 socket.shutdown().await?;
    //             }
    //         }
    //     }

    //     Ok(())
    // }

    pub async fn accept_connections(&mut self, listener: TcpListener) -> Result<()> {
        while let Ok((socket, addr)) = listener.accept().await {
            // Update connection tracking
            let conn_id = generate_connection_id(&addr);

            let permit = self
                .conn_pool
                .clone()
                .acquire_owned()
                .await
                .expect("Failed to acquire semaphore permit");

            // store a flag indicating the connection is active
            self.connections.insert(conn_id.clone(), true);
            let connections = self.connections.clone();
            let conn_pool = self.conn_pool.clone();

            // filter to only active connections (flag is true)
            debug!(
                "Currently active connections: {}",
                connections.iter().filter(|entry| *entry.value()).count()
            );

            let (_, rx) = mpsc::channel(1); // Create a channel for communication with the connection handler

            // Successfully acquired a permit, proceed with handling the connection
            let mut connection_handler = ConnectionHandler::new(
                socket,
                rx,
                self.driver.clone(),
                self.connections.clone(),
                self.middleware_stack.clone(),
                // self.conn_pool.0.clone(), // Pass the sender to release the permit
                // self.query_throttle.0.clone(), // Pass the sender to release the permit
            );

            tokio::spawn(async move {
                if let Err(e) = connection_handler.handle_connection().await {
                    error!("Error handling connection: {:?}", e);
                }

                // Release resources
                info!("Releasing connection pool permit for client {}", addr);
                debug!(
                    "Connection pool permits available: {}",
                    conn_pool.available_permits()
                );

                connections.remove(&conn_id);
                drop(permit);
            });
        }

        Ok(())
    }

    pub async fn run(&mut self) -> Result<()> {
        // TODO: Make this configurable via CLI args
        let mut current_port = self.server_address.port();
        let max_retries = env::var("MAX_PORT_RETRIES")
            .unwrap_or("5".to_string())
            .parse::<u16>()
            .unwrap_or(5);
        let retry_interval = env::var("PORT_RETRY_INTERVAL_MS")
            .unwrap_or("500".to_string())
            .parse::<u64>()
            .unwrap_or(500);
        let mut attempt = 0;

        loop {
            let address = SocketAddr::new(self.server_address.ip(), current_port);
            match TcpListener::bind(&address).await {
                Ok(listener) => {
                    info!("Server successfully running on {}", &address);
                    tokio::select! {
                        result = self.accept_connections(listener) => {
                            if let Err(e) = result {
                                error!("Error accepting connections: {}", e);
                            }
                        }
                        _ = signal::ctrl_c() => {
                            info!("Shutdown signal received, terminating server...");
                            break;
                        }
                    }
                    break;
                }
                Err(e) => {
                    error!(
                        "Failed to bind to port {}: {}. Retrying in {}ms",
                        current_port, e, retry_interval
                    );
                    if attempt >= max_retries {
                        error!("Reached maximum retry attempts. Unable to start server.");
                        return Err(anyhow!("Unable to start server"));
                    }
                    current_port = current_port.wrapping_add(1);
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(retry_interval)).await;
                }
            }
        }

        Ok(())
    }

    /// Start the metrics server on a background thread with retry logic for port allocation.
    pub async fn start_metrics_server(&self) -> Result<()> {
        let server_ip = self.server_address.ip();
        let default_port = 8080;
        let max_port_walk = 100; // Maximum number of ports to try before giving up

        let mut port = default_port;
        while port < default_port + max_port_walk {
            match TcpListener::bind(SocketAddr::new(server_ip, port)).await {
                Ok(_) => {
                    break;
                }
                Err(_) => {
                    port += 1;

                    if port == default_port + max_port_walk {
                        return Err(MetricsServerError::PortAllocationError(max_port_walk).into());
                    }
                }
            }
        }

        let metrics_manager = self.metrics_manager.clone();

        const METRICS_DELAY: u64 = 15; // seconds

        // Start the metrics collection loop on a background thread
        tokio::spawn(async move {
            debug!(
                "Starting metrics collection every {} seconds",
                METRICS_DELAY
            );
            loop {
                let metrics = metrics_manager.collect_metrics().await;

                info!("-- METRICS ({}) --", chrono::Utc::now());
                for metric in metrics {
                    metric.log_metric();
                }
                tokio::time::sleep(Duration::from_secs(METRICS_DELAY)).await;
            }
        });

        let metrics_manager = self.metrics_manager.clone();
        let metrics_address = SocketAddr::new(server_ip, port);

        tokio::spawn(async move {
            let app = Router::new().route(
                "/metrics",
                get(move || async move { metrics_manager.get_metrics().await }),
            );

            // Run the axum server
            axum::Server::bind(&metrics_address)
                .serve(app.into_make_service())
                .await
                .expect("Failed to start metrics server");
        });

        Ok(())
    }

    /// Start a no-op metrics server which tells the client that metrics are disabled on any request.
    pub fn start_noop_metrics_server(&self) {
        let metrics_address = SocketAddr::new(self.server_address.ip(), 8080);

        tokio::spawn(async move {
            let app = Router::new().route(
                "/metrics",
                get(|| async move {
                    "Metrics server disabled. Enable metrics with the --metrics flag."
                }),
            );

            // Run the axum server
            axum::Server::bind(&metrics_address)
                .serve(app.into_make_service())
                .await
                .expect("Failed to start metrics server");
        });
    }

    pub async fn start_metrics_logging(&mut self) {
        // let active_connections = self.active_connections.clone();

        let wait = 15;
        trace!("Starting metrics logging every {} seconds", wait);

        tokio::spawn(async move {
            let mut delay = tokio::time::interval(Duration::from_secs(wait));

            loop {
                // Current connection pool size
                // let pool_size = active_connections.load(std::sync::atomic::Ordering::SeqCst);
                info!("Current active connections in pool: TODO:");
                // info!("Current active connections in pool: TODO:{}", pool_size);
                // info!(%pool_size, "Current active connections in pool");

                delay.tick().await;
            }
        });
    }
}

pub fn generate_connection_id(addr: &SocketAddr) -> ConnectionId {
    // Generate a unique connection ID based on the client's address
    let mut hasher = FxHasher::default();
    format!("{}:{}", addr.ip(), addr.port()).hash(&mut hasher);
    hasher.finish().to_string()
}
