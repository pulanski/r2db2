use anyhow::Result;
use clap::Parser;
use cli::{tui::handle_sql_command, Cli, Commands, MigrateArgs};
use common::util::trace::initialize_tracing;
use network::client::start_client;
use network::server::start_server;
use std::{process::ExitCode, time::Instant};
use tracing::{info, trace};

#[tokio::main]
async fn main() -> Result<ExitCode> {
    // TODO: Add profiling to various parts end-user facing subsystems (server, client, shell, etc.)
    // #[cfg(debug_assertions)] // Only run profiling in debug mode
    // {
    //     let mut harness = ProfilingHarness::new(METRICS_SERVER_URL, "shell");
    //     harness.add_tag("shell", "main");
    //     let _ = harness
    //         .async_profile(|| async {
    //             match async_main().await {
    //                 Ok(exit_code) => {
    //                     info!("Exit code: {:?}", exit_code);
    //                     Ok(())
    //                 }
    //                 Err(e) => {
    //                     error!("Error: {:?}", e);
    //                     Err(e)
    //                 }
    //             }
    //         })
    //         .await;
    // }
    async_main().await
}

async fn async_main() -> Result<ExitCode> {
    // Initialize tracing
    initialize_tracing()?;

    let start = Instant::now();
    let args = Cli::parse();
    trace!(?args, "Parsed CLI arguments in {:?}", start.elapsed());

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
        }
        Commands::Client(args) => {
            info!("Starting client");
            start_client(args).await?;
        }
    }

    Ok(ExitCode::SUCCESS)
}

fn handle_migration(args: &MigrateArgs) {
    info!(migrations_dir = ?args.migrations_dir(), action = ?args.action(), "Processing migrations");
    // TODO: Implement migrations
}
