use cli::{NetworkProtocol, ServeArgs};
use common::{TCP_PORT, UDP_PORT};
use get_if_addrs::get_if_addrs;
use std::net::SocketAddr;
use tracing::{info, warn};

pub mod tcp;
pub mod udp;

pub use udp::run_udp_server;

use crate::middleware;

pub async fn start_server(args: &ServeArgs) {
    let protocol = args.protocol().clone();
    info!(port = args.port(), db_file = ?args.db_file(), verbose = args.verbose(), protocol = ?protocol, "Starting SQL server");

    let tcp_addr = format!("127.0.0.1:{}", TCP_PORT)
        .parse::<SocketAddr>()
        .expect("Failed to parse TCP address");
    let udp_addr = format!("127.0.0.1:{}", UDP_PORT);

    let public_ip = get_public_ip().expect("Failed to get public IP address");
    info!(public_ip = ?public_ip, "Listening at IP address");

    let middleware_stack = middleware::MiddlewareStack::new();

    if protocol == NetworkProtocol::TCP {
        let server = tcp::DbServer::new(tcp_addr, middleware_stack);

        // Start the metrics server (if enabled)
        if *args.metrics() {
            server
                .start_metrics_server()
                .await
                .expect("Metrics server failed to run");
        } else {
            warn!("Metrics server disabled");
            // Start a no-op metrics server which tells the client that metrics are disabled on any request
            server.start_noop_metrics_server();
        }

        info!("We're ready to rumble! (TCP server started)");

        // Run the SQL server
        server.run().await.expect("TCP server failed to run");
    } else if protocol == NetworkProtocol::UDP {
        warn!("UDP server not implemented yet");
        run_udp_server(&udp_addr)
            .await
            .expect("UDP server failed to run");
    } else {
        panic!("Unsupported protocol: {:?}", protocol);
    }
}

fn get_public_ip() -> Option<String> {
    // Get the network interface addresses
    let if_addrs = get_if_addrs().ok()?;

    // Filter for the public IP address
    let public_ip = if_addrs
        .iter()
        .filter(|if_addr| !if_addr.is_loopback())
        .map(|if_addr| if_addr.ip().to_string())
        .next();

    public_ip
}
