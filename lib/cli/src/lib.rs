use clap::{command, Args, Parser, Subcommand, ValueEnum};
use core::fmt;
use getset::Getters;
use std::path::PathBuf;

pub mod tui;

/// r2db2: A fast OLTP/HTAP DBMS
#[derive(Debug, Parser, Getters)]
#[command(name = "r2db2")]
#[command(about = "r2db2: High-performance OLTP/HTAP DBMS", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    #[getset(get = "pub")]
    command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Start an interactive SQL shell or execute SQL commands/scripts
    Sql(SqlArgs),
    /// Start a server to host a database instance (TCP or UDP)
    Serve(ServeArgs),
    /// Start a client to connect to a database instance (TCP or UDP)
    Client(ClientArgs),
    /// Manage database migrations
    Migrate(MigrateArgs),
    // Additional commands can be added here
}

#[derive(Debug, Args, Getters)]
pub struct SqlArgs {
    /// SQL command or path to a SQL script file
    #[arg(short, long)]
    #[getset(get = "pub")]
    command: Option<String>,
    /// Path to the database file, use ':memory:' for an in-memory database
    #[getset(get = "pub")]
    db_path: Option<String>,
}

#[derive(Debug, Args, Getters)]
pub struct ServeArgs {
    /// Port to host the server on
    #[arg(short, long, default_value_t = 2345)]
    #[getset(get = "pub")]
    port: u16,
    /// Optional: specify a database file to load
    #[arg(short, long)]
    #[getset(get = "pub")]
    db_file: Option<PathBuf>,
    /// Run in verbose mode
    #[arg(short, long)]
    #[getset(get = "pub")]
    verbose: bool,
    /// Specify the network protocol (TCP or UDP) for database connections
    #[arg(short = 's', long, default_value_t = NetworkProtocol::TCP)]
    #[getset(get = "pub")]
    protocol: NetworkProtocol,
    /// Enable the metrics server
    #[arg(short, long, default_value = "false")] // TODO: Change to false once metrics are more stable
    #[getset(get = "pub")]
    metrics: bool,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NetworkProtocol {
    TCP,
    UDP,
}

impl fmt::Display for NetworkProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkProtocol::TCP => write!(f, "tcp"),
            NetworkProtocol::UDP => write!(f, "udp"),
        }
    }
}

#[derive(Debug, Args, Getters)]
pub struct ClientArgs {
    /// Hostname or IP address of the server
    #[arg(short = 'a', long)]
    #[getset(get = "pub")]
    host: String,
    /// Port to connect to
    #[arg(short, long, default_value_t = 2345)]
    #[getset(get = "pub")]
    port: u16,
    /// Run in verbose mode
    #[arg(short, long)]
    #[getset(get = "pub")]
    verbose: bool,
}

#[derive(Debug, Args, Getters)]
#[getset(get = "pub")]
pub struct MigrateArgs {
    /// Path to the migration scripts directory
    #[arg(short, long)]
    migrations_dir: PathBuf,
    /// Specify the migration action (up, down, status)
    #[arg(short, long)]
    action: MigrationAction,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
pub enum MigrationAction {
    Up,
    Down,
    Status,
}
