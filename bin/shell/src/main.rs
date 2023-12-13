use clap::{Args, Parser, Subcommand, ValueEnum};
use cli::{tui::handle_sql_command, Cli, Commands, MigrateArgs, ServeArgs, SqlArgs};
use indicatif::ProgressStyle;
use tracing::{info, Level};
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::FmtSubscriber;

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
            info!("Starting TCP server");
            start_tcp_server(args);
        }
        Commands::Migrate(args) => {
            info!("Handling database migration");
            handle_migration(args);
        } // Additional command handling
    }
}

fn start_tcp_server(args: &ServeArgs) {
    // Tracing within TCP server setup
    info!(port = args.port(), db_file = ?args.db_file(), verbose = args.verbose(), "Starting TCP server");
    // Implement TCP server startup logic
    // ...
}

fn handle_migration(args: &MigrateArgs) {
    // Tracing within migration handling
    info!(migrations_dir = ?args.migrations_dir(), action = ?args.action(), "Processing migrations");
    // Implement database migration logic
    // ...
}

// Additional functions and logic for the DBMS can be implemented here
