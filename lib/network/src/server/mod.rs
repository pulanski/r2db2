pub mod tcp;
pub mod udp;

use cli::{NetworkProtocol, ServeArgs};
use common::{TCP_PORT, UDP_PORT};
use get_if_addrs::get_if_addrs;
use tracing::info;

pub use tcp::run_tcp_server;
pub use udp::run_udp_server;

pub async fn start_server(args: &ServeArgs) {
    let protocol = args.protocol().clone();
    info!(port = args.port(), db_file = ?args.db_file(), verbose = args.verbose(), protocol = ?protocol, "Starting SQL server");

    let tcp_addr = format!("127.0.0.1:{}", TCP_PORT);
    let udp_addr = format!("127.0.0.1:{}", UDP_PORT);

    let public_ip = get_public_ip().expect("Failed to get public IP address");
    info!(public_ip = ?public_ip, "Listening at IP address");

    if protocol == NetworkProtocol::TCP {
        run_tcp_server(&tcp_addr)
            .await
            .expect("TCP server failed to run");
    } else if protocol == NetworkProtocol::UDP {
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
