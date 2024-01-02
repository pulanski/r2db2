use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::SimpleFile,
    term::{
        self,
        termcolor::{self, WriteColor},
    },
};
use std::{io, ops::Range};
use thiserror::Error;

pub type Spanned<T> = (T, Span);
pub type Span = Range<usize>;

pub type LocatableError = Spanned<CompileError>;
pub type LocatableResult<T, E = LocatableError> = std::result::Result<T, E>;

macro_rules! impl_from {
    ($($error:tt),+) => {$(
        impl From<$error> for CompileError {
            fn from(e: $error) -> Self {
                CompileError::$error(e)
            }
        }
    )+};
}

impl_from!(NameError, SyntaxError, TypeError);

pub trait ToDiagnostic {
    fn to_diagnostic(&self, span: &Span) -> Diagnostic<()>;
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CompileError {
    #[error("NameError: {0}")]
    NameError(NameError),
    #[error("SyntaxError: {0}")]
    SyntaxError(SyntaxError),
    #[error("TypeError: {0}")]
    TypeError(TypeError),
}

impl ToDiagnostic for CompileError {
    fn to_diagnostic(&self, span: &Span) -> Diagnostic<()> {
        match self {
            CompileError::NameError(e) => e.to_diagnostic(span),
            CompileError::SyntaxError(e) => e.to_diagnostic(span),
            CompileError::TypeError(e) => e.to_diagnostic(span),
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum NameError {
    #[error("name {name:?} is already defined")]
    AlreadyDefined { name: String },
    #[error("name {name:?} is not defined")]
    NotDefined { name: String },
}

impl ToDiagnostic for NameError {
    fn to_diagnostic(&self, span: &Span) -> Diagnostic<()> {
        Diagnostic::error()
            .with_code("Name Error: ")
            .with_message(self.to_string())
            .with_labels(vec![Label::primary((), span.clone())])
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum SyntaxError {
    #[error("unexpected token: {token:?} expected: {expected:?}")]
    UnexpectedToken {
        token: String,
        expected: Vec<String>,
    },
    #[error("Unterminated string literal. Expected a closing quote (\").")]
    UnterminatedString,
    #[error("unexpected end of file")]
    UnexpectedEOF { expected: Vec<String> },
    #[error("misspelled keyword: {0}")]
    MisspelledKeyword(String),
    #[error("non-existent column: {0}")]
    NonExistentColumn(String),
    #[error("missing FROM clause")]
    MissingFromClause,
}

impl ToDiagnostic for SyntaxError {
    fn to_diagnostic(&self, span: &Span) -> Diagnostic<()> {
        let mut diagnostic = Diagnostic::error()
            .with_code("Syntax Error: ")
            .with_message(self.to_string())
            .with_labels(vec![Label::primary((), span.clone())]);
        match self {
            SyntaxError::UnexpectedEOF { expected, .. }
            | SyntaxError::UnexpectedToken { expected, .. } => {
                diagnostic = diagnostic.with_notes(vec![format!("expected: {}", one_of(expected))]);
            }
            SyntaxError::UnterminatedString => {
                diagnostic = diagnostic.with_notes(vec![String::from("expected: \"")]);
            }
            _ => {}
        };
        diagnostic
    }
}

fn one_of(strings: &[String]) -> String {
    let mut result = String::new();
    for (i, string) in strings.iter().enumerate() {
        if i > 0 {
            result.push_str(", ");
        }

        if i == strings.len() - 1 {
            result.push_str("or ");
        }
        result.push_str(string);
    }
    result
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum TypeError {
    #[error("unsupported operand type(s) for {op}: {lt_type:?} and {rt_type:?}")]
    UnknownBinOp {
        op: String,
        lt_type: String,
        rt_type: String,
    },
    #[error("unsupported operand for {op}: {rt_type:?}")]
    UnknownUnaryOp { op: String, rt_type: String },
}

impl ToDiagnostic for TypeError {
    fn to_diagnostic(&self, span: &Span) -> Diagnostic<()> {
        Diagnostic::error()
            .with_code("Type Error: ")
            .with_message(self.to_string())
            .with_labels(vec![Label::primary((), span.clone())])
    }
}

pub fn report_errors(writer: &mut impl io::Write, source: &str, errors: &[LocatableError]) {
    let mut buffer = termcolor::Buffer::ansi();
    for err in errors {
        report_error(&mut buffer, source, err);
    }
    writer
        .write_all(buffer.as_slice())
        .expect("failed to write to output");
}

pub fn report_error(writer: &mut impl WriteColor, source: &str, (error, span): &LocatableError) {
    let file = SimpleFile::new("<query>", source);
    let config = term::Config::default();
    let diagnostic = error.to_diagnostic(span);

    term::emit(writer, &config, &file, &diagnostic).expect("failed to emit diagnostic");
}
