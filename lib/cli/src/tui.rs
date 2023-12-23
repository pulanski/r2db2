use crate::SqlArgs;
use driver::Driver;
use tracing::{info, instrument};

#[instrument(skip(args))]
pub async fn handle_sql_command(args: &SqlArgs) {
    let db_path = args
        .db_path()
        .clone()
        .unwrap_or_else(|| "test.db".to_owned());
    let driver = Driver::new(&db_path).expect("Failed to create driver");

    driver.process_sql_command(args.command().clone()).await;

    info!("SQL command processing completed");
}
