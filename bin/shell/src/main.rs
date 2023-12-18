use clap::Parser;
use cli::{tui::handle_sql_command, Cli, Commands, MigrateArgs};
use indicatif::ProgressStyle;
use network::server::start_server;
use tracing::info;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// Function to format elapsed time
fn elapsed_subsec(state: &indicatif::ProgressState, writer: &mut dyn std::fmt::Write) {
    let seconds = state.elapsed().as_secs();
    let sub_seconds = (state.elapsed().as_millis() % 1000) / 100;
    let _ = writer.write_str(&format!("{}.{}s", seconds, sub_seconds));
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    let indicatif_layer = IndicatifLayer::new()
        .with_progress_style(
            ProgressStyle::with_template(
                "{span_child_prefix}{span_fields} -- {span_name} {wide_msg} {elapsed_subsec}",
            )
            .unwrap()
            .with_key("elapsed_subsec", elapsed_subsec),
        )
        .with_span_child_prefix_symbol("↳ ")
        .with_span_child_prefix_indent(" ");

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(indicatif_layer)
        .init();

    let args = Cli::parse();
    info!("r2db2 CLI started");

    match args.command() {
        Commands::Sql(args) => {
            info!("Executing SQL command");
            handle_sql_command(args).await;
        }
        Commands::Serve(args) => {
            start_server(args).await;
        }
        Commands::Migrate(args) => {
            info!("Handling database migration");
            handle_migration(args);
        } // Additional command handling
    }
}

fn handle_migration(args: &MigrateArgs) {
    info!(migrations_dir = ?args.migrations_dir(), action = ?args.action(), "Processing migrations");
    // Implement database migration logic
    // ...
}

// use network::server::{run_tcp_server, run_udp_server};

// #[tokio::main]
// async fn main() {
//     // Initialize tracing
//     let indicatif_layer = IndicatifLayer::new()
//         .with_progress_style(
//             ProgressStyle::with_template(
//                 "{span_child_prefix}{span_fields} -- {span_name} {wide_msg} {elapsed_subsec}",
//             )
//             .unwrap()
//             .with_key("elapsed_subsec", elapsed_subsec),
//         )
//         .with_span_child_prefix_symbol("↳ ")
//         .with_span_child_prefix_indent(" ");

//     tracing_subscriber::registry()
//         .with(tracing_subscriber::fmt::layer())
//         .with(indicatif_layer)
//         .init();
// let tcp_addr = "127.0.0.1:2345";
// let udp_addr = "127.0.0.1:2346";

// // You can configure which server to run based on configuration flags
// // We default to TCP-based server (soon to support UDP w/ custom rdt posgres wire protocol)
// if server_protocol == ServerProtocol::TCP {
// run_tcp_server(tcp_addr)
//     .await
//     .expect("TCP server failed to run");
// } else if server_protocol == ServerProtocol::UDP {
// run_udp_server(udp_addr)
//     .await
//     .expect("UDP server failed to run");
// }
// }
