use std::sync::Arc;

use crate::SqlArgs;
use anyhow::Result;
use driver::{shell::Shell, Driver};
use tracing::info;

pub async fn handle_sql_command(args: &SqlArgs) -> Result<()> {
    let db_path = args
        .db_path()
        .clone()
        .unwrap_or_else(|| "test.db".to_owned());
    let driver = Driver::new(&db_path).expect("Failed to create driver");

    if let Some(command) = args.command() {
        info!("Executing SQL command");
        driver.process_sql_command(command).await;

        return Ok(());
    }

    // Start shell
    let mut shell = Shell::new(Arc::new(driver));
    shell.run().await?;

    info!("SQL command processing completed");

    Ok(())
}
