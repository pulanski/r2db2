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
    #[token("WITH", ignore(ascii_case))]
    With,
    #[token("AS", ignore(ascii_case))]
    As,
    #[token("GROUP", ignore(ascii_case))]
    Group,
    #[token("BY", ignore(ascii_case))]
    By,
    #[token("HAVING", ignore(ascii_case))]
    Having,
    #[token("ORDER", ignore(ascii_case))]
    Order,
    #[token("ASC", ignore(ascii_case))]
    Asc,
    #[token("DESC", ignore(ascii_case))]
    Desc,
    #[token("LIMIT", ignore(ascii_case))]
    Limit,
    #[token("OFFSET", ignore(ascii_case))]
    Offset,
    #[token("UNION", ignore(ascii_case))]
    Union,
    #[token("ALL", ignore(ascii_case))]
    All,
    #[token("JOIN", ignore(ascii_case))]
    Join,
    #[token("ON", ignore(ascii_case))]
    On,
    #[token("IN", ignore(ascii_case))]
    In,
    #[token("NOT", ignore(ascii_case))]
    Not,
    #[token("PARTITION", ignore(ascii_case))]
    Partition,
    #[token("OVER", ignore(ascii_case))]
    Over,
    #[token("INNER", ignore(ascii_case))]
    Inner,
    #[token("OUTER", ignore(ascii_case))]
    Outer,
    #[token("LEFT", ignore(ascii_case))]
    Left,
    #[token("RIGHT", ignore(ascii_case))]
    Right,
    #[token("FULL", ignore(ascii_case))]
    Full,
    #[token("RECURSIVE", ignore(ascii_case))]
    Recursive,
    #[token("BEGIN", ignore(ascii_case))]
    Begin,
    #[token("COMMIT", ignore(ascii_case))]
    Commit,
    #[token("ROLLBACK", ignore(ascii_case))]
    Rollback,
    #[token("TRANSACTION", ignore(ascii_case))]
    Transaction,
    #[token("TRUNCATE", ignore(ascii_case))]
    Truncate,
    #[token("TRIGGER", ignore(ascii_case))]
    Trigger,
    #[token("PROCEDURE", ignore(ascii_case))]
    Procedure,
    #[token("EXECUTE", ignore(ascii_case))]
    Execute,
    #[token("BETWEEN", ignore(ascii_case))]
    Between,
    #[token("UNBOUNDED", ignore(ascii_case))]
    Unbounded,
    #[token("PRECEDING", ignore(ascii_case))]
    Preceding,
    #[token("CURRENT", ignore(ascii_case))]
    Current,
    #[token("ROW", ignore(ascii_case))]
    Row,
    #[token("ROWS", ignore(ascii_case))]
    Rows,
    #[token("FOR", ignore(ascii_case))]
    For,
    #[token("AND", ignore(ascii_case))]
    And,
    #[token("OR", ignore(ascii_case))]
    Or,

    #[regex("-?[0-9]+", |lex| lex.slice().parse())]
    Integer(i64),
    #[regex("-?[0-9]+\\.[0-9]+", |lex| lex.slice().parse())]
    Float(f64),
    #[token("TRUE", ignore(ascii_case))]
    True,
    #[token("FALSE", ignore(ascii_case))]
    False,
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
    #[token(".")]
    Dot,
    #[token("->")]
    Arrow,
    #[token("?")]
    Question,

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
                (Ok(True), 35..39),
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
mod advanced_queries {
    use super::*;
    use pretty_assertions_sorted::assert_eq;
    use TokenKind::*;

    #[test]
    fn test_cte() {
        let lexer = TokenKind::lexer(
            "WITH regional_sales AS (SELECT region, SUM(amount) FROM orders GROUP BY region) \
             SELECT region FROM regional_sales WHERE amount > (SELECT SUM(amount) FROM \
             regional_sales) * 0.75;",
        );

        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(With), 0..4),
                (Ok(Ident("regional_sales".to_string())), 5..19),
                (Ok(As), 20..22),
                (Ok(LParen), 23..24),
                (Ok(Select), 24..30),
                (Ok(Ident("region".to_string())), 31..37),
                (Ok(Comma), 37..38),
                (Ok(Ident("SUM".to_string())), 39..42),
                (Ok(LParen), 42..43),
                (Ok(Ident("amount".to_string())), 43..49),
                (Ok(RParen), 49..50),
                (Ok(From), 51..55),
                (Ok(Ident("orders".to_string())), 56..62),
                (Ok(Group), 63..68),
                (Ok(By), 69..71),
                (Ok(Ident("region".to_string())), 72..78),
                (Ok(RParen), 78..79),
                (Ok(Select), 80..86),
                (Ok(Ident("region".to_string())), 87..93),
                (Ok(From), 94..98),
                (Ok(Ident("regional_sales".to_string())), 99..113),
                (Ok(Where), 114..119),
                (Ok(Ident("amount".to_string())), 120..126),
                (Ok(Gt), 127..128),
                (Ok(LParen), 129..130),
                (Ok(Select), 130..136),
                (Ok(Ident("SUM".to_string())), 137..140),
                (Ok(LParen), 140..141),
                (Ok(Ident("amount".to_string())), 141..147),
                (Ok(RParen), 147..148),
                (Ok(From), 149..153),
                (Ok(Ident("regional_sales".to_string())), 154..168),
                (Ok(RParen), 168..169),
                (Ok(Star), 170..171),
                (Ok(Float(0.75)), 172..176),
                (Ok(Semi), 176..177),
            ],
        );
    }

    #[test]
    fn test_nested_queries() {
        let lexer = TokenKind::lexer(
            "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE status = 'Delivered');",
        );
        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Ident("name".to_string())), 7..11),
                (Ok(From), 12..16),
                (Ok(Ident("users".to_string())), 17..22),
                (Ok(Where), 23..28),
                (Ok(Ident("id".to_string())), 29..31),
                (Ok(In), 32..34),
                (Ok(LParen), 35..36),
                (Ok(Select), 36..42),
                (Ok(Ident("user_id".to_string())), 43..50),
                (Ok(From), 51..55),
                (Ok(Ident("orders".to_string())), 56..62),
                (Ok(Where), 63..68),
                (Ok(Ident("status".to_string())), 69..75),
                (Ok(Eq), 76..77),
                (Ok(String("Delivered".to_string())), 78..89),
                (Ok(RParen), 89..90),
                (Ok(Semi), 90..91),
            ],
        );
    }

    #[test]
    fn test_join_operations() {
        let lexer = TokenKind::lexer(
            "SELECT orders.id, customers.name FROM orders JOIN customers ON orders.customer_id = customers.id;",
        );
        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Ident("orders".to_string())), 7..13),
                (Ok(Dot), 13..14),
                (Ok(Ident("id".to_string())), 14..16),
                (Ok(Comma), 16..17),
                (Ok(Ident("customers".to_string())), 18..27),
                (Ok(Dot), 27..28),
                (Ok(Ident("name".to_string())), 28..32),
                (Ok(From), 33..37),
                (Ok(Ident("orders".to_string())), 38..44),
                (Ok(Join), 45..49),
                (Ok(Ident("customers".to_string())), 50..59),
                (Ok(On), 60..62),
                (Ok(Ident("orders".to_string())), 63..69),
                (Ok(Dot), 69..70),
                (Ok(Ident("customer_id".to_string())), 70..81),
                (Ok(Eq), 82..83),
                (Ok(Ident("customers".to_string())), 84..93),
                (Ok(Dot), 93..94),
                (Ok(Ident("id".to_string())), 94..96),
                (Ok(Semi), 96..97),
            ],
        );
    }

    #[test]
    fn test_group_by_having() {
        let lexer = TokenKind::lexer(
            "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 10;",
        );
        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Ident("category".to_string())), 7..15),
                (Ok(Comma), 15..16),
                (Ok(Ident("COUNT".to_string())), 17..22),
                (Ok(LParen), 22..23),
                (Ok(Star), 23..24),
                (Ok(RParen), 24..25),
                (Ok(From), 26..30),
                (Ok(Ident("products".to_string())), 31..39),
                (Ok(Group), 40..45),
                (Ok(By), 46..48),
                (Ok(Ident("category".to_string())), 49..57),
                (Ok(Having), 58..64),
                (Ok(Ident("COUNT".to_string())), 65..70),
                (Ok(LParen), 70..71),
                (Ok(Star), 71..72),
                (Ok(RParen), 72..73),
                (Ok(Gt), 74..75),
                (Ok(Integer(10)), 76..78),
                (Ok(Semi), 78..79),
            ],
        );
    }
}

#[cfg(test)]
mod posgres_specific {
    use super::*;
    use pretty_assertions_sorted::assert_eq;
    use TokenKind::*;

    #[test]
    fn test_window_functions() {
        let lexer = TokenKind::lexer(
            "SELECT name, salary, AVG(salary) OVER (PARTITION BY department) FROM employees",
        );
        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Ident("name".to_string())), 7..11),
                (Ok(Comma), 11..12),
                (Ok(Ident("salary".to_string())), 13..19),
                (Ok(Comma), 19..20),
                (Ok(Ident("AVG".to_string())), 21..24),
                (Ok(LParen), 24..25),
                (Ok(Ident("salary".to_string())), 25..31),
                (Ok(RParen), 31..32),
                (Ok(Over), 33..37),
                (Ok(LParen), 38..39),
                (Ok(Partition), 39..48),
                (Ok(By), 49..51),
                (Ok(Ident("department".to_string())), 52..62),
                (Ok(RParen), 62..63),
                (Ok(From), 64..68),
                (Ok(Ident("employees".to_string())), 69..78),
            ],
        );
    }

    // Recursive Queries
    // WITH RECURSIVE subordinates AS (SELECT id FROM employees WHERE manager_id = 1 UNION ALL SELECT e.id FROM employees e INNER JOIN subordinates s ON s.id = e.manager_id) SELECT * FROM subordinates
    #[test]
    fn test_recursive_queries() {
        let lexer = TokenKind::lexer(
            "WITH RECURSIVE subordinates AS (SELECT id FROM employees WHERE manager_id = 1 UNION ALL SELECT e.id FROM employees e INNER JOIN subordinates s ON s.id = e.manager_id) SELECT * FROM subordinates",
        );
        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(With), 0..4),
                (Ok(Recursive), 5..14),
                (Ok(Ident("subordinates".to_string())), 15..27),
                (Ok(As), 28..30),
                (Ok(LParen), 31..32),
                (Ok(Select), 32..38),
                (Ok(Ident("id".to_string())), 39..41),
                (Ok(From), 42..46),
                (Ok(Ident("employees".to_string())), 47..56),
                (Ok(Where), 57..62),
                (Ok(Ident("manager_id".to_string())), 63..73),
                (Ok(Eq), 74..75),
                (Ok(Integer(1)), 76..77),
                (Ok(Union), 78..83),
                (Ok(All), 84..87),
                (Ok(Select), 88..94),
                (Ok(Ident("e".to_string())), 95..96),
                (Ok(Dot), 96..97),
                (Ok(Ident("id".to_string())), 97..99),
                (Ok(From), 100..104),
                (Ok(Ident("employees".to_string())), 105..114),
                (Ok(Ident("e".to_string())), 115..116),
                (Ok(Inner), 117..122),
                (Ok(Join), 123..127),
                (Ok(Ident("subordinates".to_string())), 128..140),
                (Ok(Ident("s".to_string())), 141..142),
                (Ok(On), 143..145),
                (Ok(Ident("s".to_string())), 146..147),
                (Ok(Dot), 147..148),
                (Ok(Ident("id".to_string())), 148..150),
                (Ok(Eq), 151..152),
                (Ok(Ident("e".to_string())), 153..154),
                (Ok(Dot), 154..155),
                (Ok(Ident("manager_id".to_string())), 155..165),
                (Ok(RParen), 165..166),
                (Ok(Select), 167..173),
                (Ok(Star), 174..175),
                (Ok(From), 176..180),
                (Ok(Ident("subordinates".to_string())), 181..193),
            ],
        );
    }

    #[test]
    fn test_json() {
        let lexer = TokenKind::lexer(
            "SELECT info -> 'name' AS name FROM users WHERE info -> 'tags' ? 'admin'",
        );
        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Ident("info".to_string())), 7..11),
                (Ok(Arrow), 12..14),
                (Ok(String("name".to_string())), 15..21),
                (Ok(As), 22..24),
                (Ok(Ident("name".to_string())), 25..29),
                (Ok(From), 30..34),
                (Ok(Ident("users".to_string())), 35..40),
                (Ok(Where), 41..46),
                (Ok(Ident("info".to_string())), 47..51),
                (Ok(Arrow), 52..54),
                (Ok(String("tags".to_string())), 55..61),
                (Ok(Question), 62..63),
                (Ok(String("admin".to_string())), 64..71),
            ],
        );
    }
}

#[cfg(test)]
mod olap_oltp {
    use super::*;
    use pretty_assertions_sorted::assert_eq;
    use TokenKind::*;

    #[test]
    fn test_transaction() {
        let lexer = TokenKind::lexer(
            "BEGIN; UPDATE accounts SET balance = balance - 100 WHERE id = 1; UPDATE accounts SET balance = balance + 100 WHERE id = 2; COMMIT;",
        );
        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Begin), 0..5),
                (Ok(Semi), 5..6),
                (Ok(Update), 7..13),
                (Ok(Ident("accounts".to_string())), 14..22),
                (Ok(Set), 23..26),
                (Ok(Ident("balance".to_string())), 27..34),
                (Ok(Eq), 35..36),
                (Ok(Ident("balance".to_string())), 37..44),
                (Ok(Minus), 45..46),
                (Ok(Integer(100)), 47..50),
                (Ok(Where), 51..56),
                (Ok(Ident("id".to_string())), 57..59),
                (Ok(Eq), 60..61),
                (Ok(Integer(1)), 62..63),
                (Ok(Semi), 63..64),
                (Ok(Update), 65..71),
                (Ok(Ident("accounts".to_string())), 72..80),
                (Ok(Set), 81..84),
                (Ok(Ident("balance".to_string())), 85..92),
                (Ok(Eq), 93..94),
                (Ok(Ident("balance".to_string())), 95..102),
                (Ok(Plus), 103..104),
                (Ok(Integer(100)), 105..108),
                (Ok(Where), 109..114),
                (Ok(Ident("id".to_string())), 115..117),
                (Ok(Eq), 118..119),
                (Ok(Integer(2)), 120..121),
                (Ok(Semi), 121..122),
                (Ok(Commit), 123..129),
                (Ok(Semi), 129..130),
            ],
        );
    }

    // Complex Analytical Query
    // SELECT region, product, SUM(sales) OVER (PARTITION BY region ORDER BY sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales_data
    #[test]
    fn test_complex_analytical_query() {
        let lexer = TokenKind::lexer(
            "SELECT region, product, SUM(sales) OVER (PARTITION BY region ORDER BY sales DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM sales_data",
        );
        let tokens = lexer.spanned().collect::<Vec<_>>();

        assert_eq!(
            tokens,
            &[
                (Ok(Select), 0..6),
                (Ok(Ident("region".to_string())), 7..13),
                (Ok(Comma), 13..14),
                (Ok(Ident("product".to_string())), 15..22),
                (Ok(Comma), 22..23),
                (Ok(Ident("SUM".to_string())), 24..27),
                (Ok(LParen), 27..28),
                (Ok(Ident("sales".to_string())), 28..33),
                (Ok(RParen), 33..34),
                (Ok(Over), 35..39),
                (Ok(LParen), 40..41),
                (Ok(Partition), 41..50),
                (Ok(By), 51..53),
                (Ok(Ident("region".to_string())), 54..60),
                (Ok(Order), 61..66),
                (Ok(By), 67..69),
                (Ok(Ident("sales".to_string())), 70..75),
                (Ok(Desc), 76..80),
                (Ok(Rows), 81..85),
                (Ok(Between), 86..93),
                (Ok(Unbounded), 94..103),
                (Ok(Preceding), 104..113),
                (Ok(And), 114..117),
                (Ok(Current), 118..125),
                (Ok(Row), 126..129),
                (Ok(RParen), 129..130),
                (Ok(From), 131..135),
                (Ok(Ident("sales_data".to_string())), 136..146),
            ],
        );
    }

    // Triggers and Procedures
    // CREATE TRIGGER audit_log AFTER INSERT ON orders FOR EACH ROW EXECUTE PROCEDURE log_audit()
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
