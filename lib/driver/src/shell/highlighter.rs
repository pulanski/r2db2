use nu_ansi_term::{Color, Style};
use reedline::{Highlighter, StyledText};

pub(crate) struct SqlHighlighter;

impl SqlHighlighter {
    pub(crate) fn new() -> Self {
        SqlHighlighter
    }
}

fn tokenize(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current_token = String::new();

    for ch in line.chars() {
        match ch {
            ' ' | ',' | ';' | '(' | ')' | '.' => {
                if !current_token.is_empty() {
                    tokens.push(current_token.clone());
                    current_token.clear();
                }
                tokens.push(ch.to_string()); // Push punctuation as separate tokens
            }
            _ => current_token.push(ch),
        }
    }

    if !current_token.is_empty() {
        tokens.push(current_token);
    }

    tokens
}

impl Highlighter for SqlHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled_text = StyledText::new();

        let tokens = tokenize(line);

        for token in tokens {
            let style = if is_sql_keyword(&token) {
                Style::new().fg(Color::Green)
            } else if is_numeric(&token) || "*".eq(&token) {
                Style::new().fg(Color::Yellow)
            } else if is_builtin_function(&token) {
                Style::new().fg(Color::LightCyan)
            } else if is_operator(&token) {
                Style::new().fg(Color::Magenta)
            } else if is_punctuation(&token) {
                Style::new().fg(Color::DarkGray)
            } else if is_string(&token) {
                Style::new().fg(Color::LightRed)
            } else if is_identifier(&token) {
                Style::new().fg(Color::LightBlue) // TODO: Differentiate between table and column names (or other identifiers, like aliases, etc., also error on reserved keywords)
            } else {
                Style::new().fg(Color::White)
            };

            styled_text.push((style, token));
        }

        styled_text
    }
}

fn is_sql_keyword(word: &str) -> bool {
    [
        "SELECT",
        "FROM",
        "WHERE",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "TABLE",
        "INTO",
        "VALUES",
        "SET",
        "ORDER",
        "BY",
        "ASC",
        "DESC",
        "LIMIT",
        "OFFSET",
        "JOIN",
        "INNER",
        "LEFT",
        "RIGHT",
        "FULL",
        "OUTER",
        "ON",
        "GROUP",
        "HAVING",
        "UNION",
        "ALL",
        "DISTINCT",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "AS",
        "IS",
        "NULL",
        "LIKE",
        "IN",
        "BETWEEN",
        "EXISTS",
        "CAST",
        "TRUE",
        "FALSE",
        "EXTRACT",
        "DATE",
        "TIME",
        "INTERVAL",
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "OVER",
        "PARTITION",
        "ROWS",
        "PRECEDING",
        "FOLLOWING",
        "CURRENT",
        "ROW",
        "LEAD",
        "LAG",
        "FIRST",
        "LAST",
        "NTH_VALUE",
        "NTILE",
        "PERCENT_RANK",
        "CUME_DIST",
        "DENSE_RANK",
        "RANK",
        // "COUNT",
        // "SUM",
        // "AVG",
        // "MIN",
        // "MAX",
        // "NOW",
        // "NOT",
        // "AND",
        // "OR",
    ]
    .contains(&word.to_uppercase().as_str())
}

fn is_numeric(word: &str) -> bool {
    word.parse::<f64>().is_ok() || word.parse::<i64>().is_ok()
}

fn is_string(word: &str) -> bool {
    word.starts_with('"') && word.ends_with('"') || word.starts_with('\'') && word.ends_with('\'')
}

fn is_identifier(word: &str) -> bool {
    word.chars().all(|c| c.is_alphanumeric() || c == '_')
}

fn is_operator(word: &str) -> bool {
    ["=", "<", ">", "<=", ">=", "!=", "AND", "OR", "NOT"].contains(&word.to_uppercase().as_str())
}

fn is_punctuation(word: &str) -> bool {
    [",", ";", "(", ")", ".", "*", "+", "-", "/", "%"].contains(&word)
}

fn is_builtin_function(word: &str) -> bool {
    ["NOW", "COUNT", "SUM", "AVG", "MIN", "MAX"].contains(&word.to_uppercase().as_str())
}
