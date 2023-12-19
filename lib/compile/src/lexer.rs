use crate::diagnostics::{CompileError, LocatableError, SyntaxError};
use logos::{Lexer, Logos};
use thiserror::Error;

#[derive(Debug, Default, Error, PartialEq, Clone)]
pub enum LexerError {
    #[error("Unknown token")]
    #[default]
    UnknownToken,
    #[error("Invalid integer literal")]
    ParseInt,
    #[error("Invalid float literal")]
    ParseFloat,
    #[error("Unterminated string literal")]
    UnterminatedString,
}

impl From<std::num::ParseIntError> for LexerError {
    fn from(_: std::num::ParseIntError) -> Self {
        LexerError::ParseInt
    }
}

impl From<std::num::ParseFloatError> for LexerError {
    fn from(_: std::num::ParseFloatError) -> Self {
        LexerError::ParseFloat
    }
}

fn ident(lex: &mut Lexer<TokenKind>) -> Option<String> {
    let slice = lex.slice();
    let ident = slice[..slice.len()].parse::<String>().ok()?;
    Some(ident)
}

fn string(lex: &mut Lexer<TokenKind>) -> Option<String> {
    let slice = lex.slice();
    let string: String = slice[1..slice.len() - 1].parse().ok()?;
    Some(string)
}

#[derive(Logos, Debug, PartialEq, Clone)]
#[logos(error = LexerError)]
enum TokenKind {
    #[regex(r"[ \n\t\f]+", logos::skip)]
    #[regex(r"--[^\n]*", logos::skip)]
    Ignored,

    #[token("SELECT", ignore(ascii_case))]
    Select,
    #[token("FROM", ignore(ascii_case))]
    From,
    #[token("WHERE", ignore(ascii_case))]
    Where,
    #[token("INSERT", ignore(ascii_case))]
    Insert,
    #[token("INTO", ignore(ascii_case))]
    Into,
    #[token("VALUES", ignore(ascii_case))]
    Values,
    #[token("UPDATE", ignore(ascii_case))]
    Update,
    #[token("SET", ignore(ascii_case))]
    Set,
    #[token("DELETE", ignore(ascii_case))]
    Delete,
    #[token("CREATE", ignore(ascii_case))]
    Create,
    #[token("DROP", ignore(ascii_case))]
    Drop,
    #[token("TABLE", ignore(ascii_case))]
    Table,
    #[token("CROSS", ignore(ascii_case))]
    Cross,

    #[regex("-?[0-9]+", |lex| lex.slice().parse())]
    Integer(i64),
    #[regex("-?[0-9]+\\.[0-9]+", |lex| lex.slice().parse())]
    Float(f64),
    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", ident)]
    Ident(String),
    #[regex(r"'[^']*'", string)]
    String(String),

    #[token("=")]
    Eq,
    #[token("<>")]
    #[token("!=")]
    NotEq,
    #[token("<")]
    Lt,
    #[token(">")]
    Gt,
    #[token("<=")]
    LtEq,
    #[token(">=")]
    GtEq,
    #[token("+")]
    Plus,
    #[token("-")]
    Minus,
    #[token("*")]
    Star,
    #[token("/")]
    Slash,

    #[token(",")]
    Comma,
    #[token(";")]
    Semi,
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,

    #[regex(r"'[^']*", |_| {
        Err(LexerError::UnterminatedString)
    })]
    UnterminatedString,
}

// Basic SQL Queries
#[cfg(test)]
mod basic_queries {
    use super::*;
    use pretty_assertions_sorted::assert_eq;
    use TokenKind::*;

    #[test]
    fn test_select() {
        let lexer = TokenKind::lexer("SELECT * FROM users;");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Star), 7..8),
                (Ok(From), 9..13),
                (Ok(Ident("users".to_string())), 14..19),
                (Ok(Semi), 19..20),
            ],
        );
    }

    #[test]
    fn test_select_where() {
        let lexer = TokenKind::lexer("SELECT * FROM users WHERE id = 1;");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Star), 7..8),
                (Ok(From), 9..13),
                (Ok(Ident("users".to_string())), 14..19),
                (Ok(Where), 20..25),
                (Ok(Ident("id".to_string())), 26..28),
                (Ok(Eq), 29..30),
                (Ok(Integer(1)), 31..32),
                (Ok(Semi), 32..33),
            ],
        );
    }

    #[test]
    fn test_select_where_string() {
        let lexer = TokenKind::lexer("SELECT * FROM users WHERE name = 'Alice';");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Star), 7..8),
                (Ok(From), 9..13),
                (Ok(Ident("users".to_string())), 14..19),
                (Ok(Where), 20..25),
                (Ok(Ident("name".to_string())), 26..30),
                (Ok(Eq), 31..32),
                (Ok(String("Alice".to_string())), 33..40),
                (Ok(Semi), 40..41),
            ],
        );
    }

    #[test]
    fn test_select_where_bool() {
        let lexer = TokenKind::lexer("SELECT * FROM users WHERE active = TRUE;");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Star), 7..8),
                (Ok(From), 9..13),
                (Ok(Ident("users".to_string())), 14..19),
                (Ok(Where), 20..25),
                (Ok(Ident("active".to_string())), 26..32),
                (Ok(Eq), 33..34),
                (Ok(Ident("TRUE".to_string())), 35..39),
                (Ok(Semi), 39..40),
            ],
        );
    }

    #[test]
    fn test_insert() {
        let lexer =
            TokenKind::lexer("INSERT INTO products (name, price) VALUES ('Widget', 19.99);");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Insert), 0..6),
                (Ok(Into), 7..11),
                (Ok(Ident("products".to_string())), 12..20),
                (Ok(LParen), 21..22),
                (Ok(Ident("name".to_string())), 22..26),
                (Ok(Comma), 26..27),
                (Ok(Ident("price".to_string())), 28..33),
                (Ok(RParen), 33..34),
                (Ok(Values), 35..41),
                (Ok(LParen), 42..43),
                (Ok(String("Widget".to_string())), 43..51),
                (Ok(Comma), 51..52),
                (Ok(Float(19.99)), 53..58),
                (Ok(RParen), 58..59),
                (Ok(Semi), 59..60),
            ],
        );
    }

    #[test]
    fn test_update() {
        let lexer = TokenKind::lexer("UPDATE orders SET status = 'Shipped' WHERE id = 1001;");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Update), 0..6),
                (Ok(Ident("orders".to_string())), 7..13),
                (Ok(Set), 14..17),
                (Ok(Ident("status".to_string())), 18..24),
                (Ok(Eq), 25..26),
                (Ok(String("Shipped".to_string())), 27..36),
                (Ok(Where), 37..42),
                (Ok(Ident("id".to_string())), 43..45),
                (Ok(Eq), 46..47),
                (Ok(Integer(1001)), 48..52),
                (Ok(Semi), 52..53),
            ],
        );
    }

    #[test]
    fn test_delete() {
        let lexer = TokenKind::lexer("DELETE FROM logs WHERE created_at < '2023-01-01';");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Delete), 0..6),
                (Ok(From), 7..11),
                (Ok(Ident("logs".to_string())), 12..16),
                (Ok(Where), 17..22),
                (Ok(Ident("created_at".to_string())), 23..33),
                (Ok(Lt), 34..35),
                (Ok(String("2023-01-01".to_string())), 36..48), // TODO: enhance lexer to support dates
                (Ok(Semi), 48..49),
            ],
        );
    }
}

#[cfg(test)]
mod error_cases {
    use super::*;
    use pretty_assertions_sorted::assert_eq;
    use TokenKind::*;

    #[test]
    fn test_unterminated_string() {
        let lexer = TokenKind::lexer("'This is an unterminated string");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(tokens, &[(Err(LexerError::UnterminatedString), 0..31)],);
    }

    #[test]
    fn test_unexpected_token() {
        let lexer = TokenKind::lexer("SELECT * FROM @");

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Star), 7..8),
                (Ok(From), 9..13),
                (Err(LexerError::UnknownToken), 14..15),
            ],
        );
    }
}
