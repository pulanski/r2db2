#![allow(dead_code)]
use anyhow::Result;
use buffer::{BufferPoolManager, ReplacementPolicy};
use execution::QueryEngine;
use std::{
    io::{self, Write},
    sync::Arc,
    time::{Duration, Instant},
};
use storage::disk::DiskManager;
use tracing::{error, info, instrument, trace};
use typed_builder::TypedBuilder;

pub mod shell;

#[instrument]
async fn parse_query(query: &str) -> Ast {
    // Simulate query parsing
    tokio::time::sleep(Duration::from_secs(rand::random::<u64>() % 5 + 1)).await;
    info!("Query parsed");

    Ast
}

#[instrument]
async fn analyze_query(ast: &Ast) -> AnalyzedPlan {
    // Simulate query analysis
    tokio::time::sleep(Duration::from_secs(rand::random::<u64>() % 5 + 1)).await;
    info!("Query analyzed");

    AnalyzedPlan
}

#[instrument]
async fn optimize_query(analyzed_plan: &AnalyzedPlan) -> OptimizedPlan {
    // Simulate query optimization
    tokio::time::sleep(Duration::from_secs(rand::random::<u64>() % 5 + 1)).await;
    info!("Query optimized");

    OptimizedPlan
}

#[instrument]
async fn plan_query(optimized_plan: &OptimizedPlan) -> PhysicalPlan {
    // Simulate query planning
    tokio::time::sleep(Duration::from_secs(rand::random::<u64>() % 5 + 1)).await;
    info!("Query planned");

    PhysicalPlan
}

#[instrument]
async fn execute_query(physical_plan: &PhysicalPlan) {
    // Simulate query execution
    tokio::time::sleep(Duration::from_secs(rand::random::<u64>() % 5 + 1)).await;
    info!("Query executed");
}

// Placeholder struct definitions
#[derive(Debug)]
struct Ast;
#[derive(Debug)]
struct AnalyzedPlan;
#[derive(Debug)]
struct OptimizedPlan;
#[derive(Debug)]
struct PhysicalPlan;

/// A reference-counted reference to a [`Driver`].
pub type DriverRef = Arc<Driver>;
#[derive(Debug, TypedBuilder)]
pub struct Driver {
    buffer_pool_manager: Arc<BufferPoolManager>,
    disk_manager: Arc<DiskManager>,
    query_engine: QueryEngine,
}

impl Driver {
    // Create a new driver for a database stored in the specified path
    pub fn new(path: &str) -> Result<Self> {
        trace!("Starting driver initialization");
        let disk_start = Instant::now();
        let disk_manager = Arc::new(DiskManager::new(path)?);
        info!("Disk manager initialized in {:?}", disk_start.elapsed());

        let buffer_start = Instant::now();
        let buffer_pool_manager = Arc::new(BufferPoolManager::new_with_size(
            ReplacementPolicy::LRU,
            disk_manager.clone(),
            100,
        ));
        info!(
            "Buffer pool manager initialized in {:?}",
            buffer_start.elapsed()
        );

        let query_engine = QueryEngine::new();

        Ok(Driver::builder()
            .buffer_pool_manager(buffer_pool_manager)
            .disk_manager(disk_manager)
            .query_engine(query_engine)
            .build())
    }

    /// Process a SQL command
    pub async fn process_sql_command(&self, command: &String) {
        match self.query_engine.execute_query(&command).await {
            Ok(_) => info!("Query executed successfully"),
            Err(e) => error!("Failed to execute query: {:?}", e),
        }
    }

    pub async fn start_shell(&self) {
        loop {
            print!("r2db2> ");
            io::stdout().flush().expect("Failed to flush stdout");

            let mut input = String::new();
            io::stdin()
                .read_line(&mut input)
                .expect("Failed to read line");

            let parts: Vec<&str> = input.split_whitespace().collect();

            match parts.as_slice() {
                [".exit"] => break,
                _ => {
                    println!("Invalid command");
                }
            }
        }
    }
}
