mod experimental;

use compile::parser::{parse_sql, Statement};
use datafusion_expr::LogicalPlan;
use regex::Regex;
// use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use core::fmt;
use datafusion::prelude::*;
use datafusion_common::{plan_err, DataFusionError, Result, ScalarValue};
use tracing::{debug, instrument, trace};

pub enum ExternalDataSource {
    CSV,
    Parquet,
    ORC,
}

pub struct QueryEngine {
    context: SessionContext,
    // TODO: Add other fields as necessary,
    // buffer manager, storage layer, etc.
}

impl fmt::Debug for QueryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryEngine").finish()
    }
}

impl QueryEngine {
    pub fn new() -> Self {
        QueryEngine {
            context: SessionContext::new(),
            // Initialize other components
        }
    }

    pub async fn execute_query(&self, sql: &str) -> Result<()> {
        // Determine the type of query (internal database table or external file (CSV, Parquet, etc.))
        if self.is_external_datasource(sql) {
            // Delegate to DataFusion engine
            self.execute_external_datasource_query(sql).await
        } else {
            // Handle with custom query execution
            self.execute_database_query(sql).await
        }
    }

    /// Determine if the query is for an external datasource. External datasources
    /// include CSV, Parquet, JSON, etc. files.
    fn is_external_datasource(&self, sql: &str) -> bool {
        // TODO: make this more robust
        sql.contains(".csv") || sql.contains(".parquet") || sql.contains(".json")
    }

    async fn execute_external_datasource_query(&self, query: &str) -> Result<()> {
        let (rewritten_query, file_path, format) = self.rewrite_query(query)?;

        match format {
            ExternalDataSource::CSV => {
                self.context
                    .register_csv("csv_table", &file_path, CsvReadOptions::new())
                    .await?;
            }
            ExternalDataSource::Parquet => {
                self.context
                    .register_parquet("parquet_table", &file_path, ParquetReadOptions::default())
                    .await?;
            }
            ExternalDataSource::ORC => {
                // Register ORC file when supported
                todo!();
            }
        }

        let df = self.context.sql(&rewritten_query).await?;
        df.show().await?;

        Ok(())
    }

    fn rewrite_query(&self, query: &str) -> Result<(String, String, ExternalDataSource)> {
        // Regex for unquoted file path
        let unquoted_re = Regex::new(r"FROM\s+([^\s']+\.csv|parquet|orc)")
            .expect("Invalid regex for unquoted path");
        // Regex for file path with single quotes
        let quoted_re = Regex::new(r"FROM\s+'([^\s']+\.csv|parquet|orc)'")
            .expect("Invalid regex for quoted path");

        let file_path = if let Some(caps) = unquoted_re.captures(query) {
            caps.get(1).map(|m| m.as_str()).unwrap()
        } else if let Some(caps) = quoted_re.captures(query) {
            caps.get(1).map(|m| m.as_str()).unwrap()
        } else {
            return Err(DataFusionError::Execution(format!(
                "No file found in query: {}",
                query
            )));
        };

        let file_ext = if file_path.ends_with(".csv") {
            "csv"
        } else if file_path.ends_with(".parquet") {
            "parquet"
        } else if file_path.ends_with(".orc") {
            "orc"
        } else {
            return Err(DataFusionError::Execution(format!(
                "Unsupported file format in query: {}",
                query
            )));
        };

        let table_name = match file_ext {
            "csv" => "csv_table",
            "parquet" => "parquet_table",
            "orc" => "orc_table",
            _ => unreachable!(),
        };

        let rewritten_query = query.replace(file_path, table_name);

        debug!("Rewritten query: {}", rewritten_query);
        debug!("File path: {}", file_path);

        let format = match file_ext {
            "csv" => ExternalDataSource::CSV,
            "parquet" => ExternalDataSource::Parquet,
            "orc" => ExternalDataSource::ORC,
            _ => unreachable!(),
        };

        Ok((rewritten_query, file_path.to_string(), format))
    }

    async fn execute_database_query(&self, sql: &str) -> Result<()> {
        let ast = parse_sql(sql)?;
        let logical_plan = self.create_logical_plan(&ast)?;
        let optimized_plan = self.optimize_plan(&logical_plan)?;
        self.execute_optimized_plan(&optimized_plan).await
    }

    fn create_logical_plan(&self, ast: &[Statement]) -> Result<LogicalPlan> {
        // Implement logic to create a logical plan from the AST
        todo!()
    }

    fn optimize_plan(&self, logical_plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Implement optimization logic
        todo!()
    }

    async fn execute_optimized_plan(&self, optimized_plan: &LogicalPlan) -> Result<()> {
        // Implement execution logic for the optimized plan
        todo!()
    }
}
