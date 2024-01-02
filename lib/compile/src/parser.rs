use crate::diagnostics::{report_errors, CompileError, LocatableResult, SyntaxError};
use anyhow::Result;
pub use sqlparser::ast::*;
use sqlparser::dialect::{self, Dialect, PostgreSqlDialect};
use sqlparser::parser::Parser;
pub use sqlparser::parser::ParserError;

// #[derive(Debug)]
// pub enum SyntaxError {
//     MisspelledKeyword,
//     NonExistentColumn,
//     MissingFromClause,
// }

/// Parse the SQL string and return a list of SQL statements.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql)
}

pub fn parse(source: &str) -> LocatableResult<Vec<Statement>> {
    let dialect = PostgreSqlDialect {}; // TODO: Make this configurable
    match sqlparser::parser::Parser::parse_sql(&dialect, source) {
        Ok(ast) => Ok(ast),
        Err(e) => {
            let span = 0..source.len(); // TODO: Get the actual span
            let error = match e {
                sqlparser::parser::ParserError::ParserError(e) => {
                    // let token = e.to_string();
                    // let expected = e.expected_tokens().iter().map(|t| t.to_string()).collect();
                    // SyntaxError::UnexpectedToken { token, expected }
                    todo!()
                }
                sqlparser::parser::ParserError::TokenizerError(e) => {
                    // let token = e.to_string();
                    // let expected = e.expected_tokens().iter().map(|t| t.to_string()).collect();
                    // SyntaxError::UnexpectedToken { token, expected }
                    // default to unterminated string
                    SyntaxError::UnterminatedString
                }
                sqlparser::parser::ParserError::RecursionLimitExceeded => todo!(),
            };

            // Emit the diagnostic
            // pub fn report_errors(writer: &mut impl io::Write, source: &str, errors: &[LocatableError]) {
            report_errors(
                &mut std::io::stderr(),
                source,
                &[(CompileError::SyntaxError(error.clone()), span.clone())],
            );

            Err((CompileError::SyntaxError(error), span))
        }
    }
}
