use anyhow::Result;
use clap::Parser;
use cli::{tui::handle_sql_command, Cli, Commands, MigrateArgs, SqlArgs};
use common::util::trace::initialize_tracing;
use compile::parser::parse;
use network::client::start_client;
use network::server::start_server;
use std::{process::ExitCode, time::Instant};
use tracing::{info, trace};

#[tokio::main]
async fn main() -> Result<ExitCode> {
    // let query =
    // // "SELECT * FROM users"; // Syntactically valid SQL
    // // "SELECT id, name FROM customers WHERE (age > 25 AND city = 'New York' AND age < 25"; // Unbalanced parenthesis
    // // "FROM orders SELECT * WHERE order_date = '2023-01-01'"; // Missing SELECT in FROM clause
    // "foo '"; // Unterminated string

    // // match sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query) {
    // match parse(query) {
    //     Ok(ast) => {
    //         println!("AST: {:#?}", ast);
    //     }

    //     Err(e) => {
    //         println!("Error: {:#?}", e);
    //     }
    // }

    // Ok(ExitCode::SUCCESS)

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
        None => {
            info!("No command provided, defaulting to test.db");
            handle_sql_command(
                &SqlArgs::builder()
                    .db_path(Some("test.db".to_string()))
                    .command(None)
                    .build(),
            )
            .await?;
        }
        Some(command) => match command {
            Commands::Sql(args) => {
                info!("Executing SQL command");
                handle_sql_command(args).await?;
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
        },
    }

    Ok(ExitCode::SUCCESS)
}

fn handle_migration(args: &MigrateArgs) {
    info!(migrations_dir = ?args.migrations_dir(), action = ?args.action(), "Processing migrations");
    // TODO: Implement migrations
}
