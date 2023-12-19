use crate::SqlArgs;
use driver::Driver;
use tracing::{info, instrument};

#[instrument]
pub async fn handle_sql_command(args: &SqlArgs) {
    info!(command = ?args.command(), db_path = ?args.db_path(), "Processing SQL command");

    let db_path = args
        .db_path()
        .clone()
        .unwrap_or_else(|| "test.db".to_owned());
    let driver = Driver::new(&db_path).expect("Failed to create driver");

    driver.process_sql_command(args.command().clone()).await;

    // driver.add_database("test".to_owned(),

    // driver.

    // let ast = parse("SELECT * FROM ;").unwrap();
    // debug!("AST: {:#?}", ast);

    // let queries = parse_sql_commands(args.command().clone());

    // let mut handles = Vec::new();
    // for query in queries {
    //     let query_handle = task::spawn(process_query(query));
    //     handles.push(query_handle);
    // }

    // for handle in handles {
    //     handle.await.unwrap();
    // }

    info!("SQL command processing completed");
}
