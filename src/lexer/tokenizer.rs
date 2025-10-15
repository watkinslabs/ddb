use super::types::{Token, TokenType};
use crate::error::{DdbError, Result};
use nom::{
    branch::alt,
    bytes::complete::{is_not, tag, take_while},
    character::complete::{alpha1, alphanumeric1, char, digit1, multispace1},
    combinator::{map, opt, recognize, value},
    multi::many0,
    sequence::{delimited, pair, preceded},
    IResult,
};

/// Tokenizer for SQL queries
pub struct Tokenizer {
    position: usize,
    line: usize,
    column: usize,
}

impl Tokenizer {
    pub fn new() -> Self {
        Self {
            position: 0,
            line: 1,
            column: 1,
        }
    }

    /// Tokenize a SQL query string
    pub fn tokenize(&mut self, input: &str) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        let mut remaining = input;

        while !remaining.is_empty() {
            // Try to parse a token
            match self.parse_token(remaining) {
                Ok((rest, token_type)) => {
                    let consumed = remaining.len() - rest.len();

                    // Skip whitespace tokens unless we need them
                    if !matches!(token_type, TokenType::Whitespace) {
                        let token = Token::new(
                            token_type,
                            self.position,
                            self.line,
                            self.column,
                        );
                        tokens.push(token);
                    }

                    // Update position tracking
                    for ch in remaining[..consumed].chars() {
                        self.position += 1;
                        if ch == '\n' {
                            self.line += 1;
                            self.column = 1;
                        } else {
                            self.column += 1;
                        }
                    }

                    remaining = rest;
                }
                Err(_) => {
                    return Err(DdbError::ParseError(format!(
                        "Unexpected character at line {}, column {}: '{}'",
                        self.line,
                        self.column,
                        remaining.chars().next().unwrap_or(' ')
                    )));
                }
            }
        }

        // Add EOF token
        tokens.push(Token::new(TokenType::Eof, self.position, self.line, self.column));

        Ok(tokens)
    }

    fn parse_token<'a>(&self, input: &'a str) -> IResult<&'a str, TokenType> {
        alt((
            parse_whitespace,
            parse_comment,
            parse_multi_word_keyword,
            parse_keyword,
            parse_operator,
            parse_delimiter,
            parse_string,
            parse_number,
            parse_identifier,
        ))(input)
    }
}

// Free functions for nom parsers
fn parse_whitespace(input: &str) -> IResult<&str, TokenType> {
    map(multispace1, |_| TokenType::Whitespace)(input)
}

fn parse_comment(input: &str) -> IResult<&str, TokenType> {
    alt((
        // Line comment: -- comment
        map(
            preceded(tag("--"), is_not("\n")),
            |s: &str| TokenType::Comment(s.to_string())
        ),
        // Block comment: /* comment */
        map(
            delimited(tag("/*"), is_not("*/"), tag("*/")),
            |s: &str| TokenType::Comment(s.to_string())
        ),
    ))(input)
}

fn parse_keyword(input: &str) -> IResult<&str, TokenType> {
    // Try to match an identifier first
    let (remaining, word) = recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_"))))
    ))(input)?;

    // Check for word boundary
    if let Some(ch) = remaining.chars().next() {
        if ch.is_alphanumeric() || ch == '_' {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    }

    // Convert to lowercase and match against keywords
    let token_type = match word.to_lowercase().as_str() {
        "select" => TokenType::Select,
        "insert" => TokenType::Insert,
        "update" => TokenType::Update,
        "delete" => TokenType::Delete,
        "upsert" => TokenType::Upsert,
        "create" => TokenType::Create,
        "drop" => TokenType::Drop,
        "use" => TokenType::Use,
        "show" => TokenType::Show,
        "set" => TokenType::Set,
        "from" => TokenType::From,
        "where" => TokenType::Where,
        "limit" => TokenType::Limit,
        "distinct" => TokenType::Distinct,
        "as" => TokenType::As,
        "into" => TokenType::Into,
        "values" => TokenType::Values,
        "table" => TokenType::Table,
        "tables" => TokenType::Tables,
        "columns" => TokenType::Columns,
        "database" => TokenType::Database,
        "begin" => TokenType::Begin,
        "commit" => TokenType::Commit,
        "rollback" => TokenType::Rollback,
        "describe" => TokenType::Describe,
        "and" => TokenType::And,
        "or" => TokenType::Or,
        "not" => TokenType::Not,
        "like" => TokenType::Like,
        "is" => TokenType::Is,
        "null" => TokenType::Null,
        "true" => TokenType::True,
        "false" => TokenType::False,
        "in" => TokenType::In,
        "between" => TokenType::Between,
        "asc" => TokenType::Asc,
        "desc" => TokenType::Desc,
        "order" => TokenType::Order,
        "by" => TokenType::By,
        "file" => TokenType::File,
        "delimiter" => TokenType::Delimiter,
        "variables" => TokenType::Variables,
        "offset" => TokenType::Offset,
        "join" => TokenType::Join,
        "inner" => TokenType::Inner,
        "left" => TokenType::Left,
        "right" => TokenType::Right,
        "full" => TokenType::Full,
        "outer" => TokenType::Outer,
        "on" => TokenType::On,
        "having" => TokenType::Having,
        "group" => TokenType::Group,
        _ => {
            // Not a keyword, fail to match
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }
    };

    Ok((remaining, token_type))
}

fn parse_multi_word_keyword(input: &str) -> IResult<&str, TokenType> {
    alt((
        map(
            alt((tag("ORDER BY"), tag("order by"), tag("Order By"))),
            |_| TokenType::OrderBy
        ),
        map(
            alt((tag("GROUP BY"), tag("group by"), tag("Group By"))),
            |_| TokenType::GroupBy
        ),
    ))(input)
}

fn parse_operator(input: &str) -> IResult<&str, TokenType> {
    alt((
        value(TokenType::LessEqual, tag("<=")),
        value(TokenType::GreaterEqual, tag(">=")),
        value(TokenType::NotEqual, alt((tag("!="), tag("<>")))),
        value(TokenType::Equal, tag("=")),
        value(TokenType::LessThan, tag("<")),
        value(TokenType::GreaterThan, tag(">")),
    ))(input)
}

fn parse_delimiter(input: &str) -> IResult<&str, TokenType> {
    alt((
        value(TokenType::Comma, char(',')),
        value(TokenType::Semicolon, char(';')),
        value(TokenType::LeftParen, char('(')),
        value(TokenType::RightParen, char(')')),
        value(TokenType::LeftBracket, char('[')),
        value(TokenType::RightBracket, char(']')),
        value(TokenType::Dot, char('.')),
        value(TokenType::Star, char('*')),
    ))(input)
}

fn parse_string(input: &str) -> IResult<&str, TokenType> {
    alt((
        // Single-quoted string
        map(
            delimited(char('\''), take_while(|c| c != '\''), char('\'')),
            |s: &str| TokenType::String(s.to_string())
        ),
        // Double-quoted string
        map(
            delimited(char('"'), take_while(|c| c != '"'), char('"')),
            |s: &str| TokenType::String(s.to_string())
        ),
    ))(input)
}

fn parse_number(input: &str) -> IResult<&str, TokenType> {
    map(
        recognize(pair(
            digit1,
            opt(pair(char('.'), digit1))
        )),
        |s: &str| TokenType::Number(s.to_string())
    )(input)
}

fn parse_identifier(input: &str) -> IResult<&str, TokenType> {
    map(
        recognize(pair(
            alt((
                tag("@@"),  // System variables (@@VERSION, @@ROWCOUNT, etc.)
                alpha1,
                tag("_")
            )),
            many0(alt((alphanumeric1, tag("_"))))
        )),
        |s: &str| TokenType::Identifier(s.to_string())
    )(input)
}

impl Default for Tokenizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_select() {
        let mut tokenizer = Tokenizer::new();
        let tokens = tokenizer.tokenize("SELECT * FROM users").unwrap();

        assert_eq!(tokens[0].token_type, TokenType::Select);
        assert_eq!(tokens[1].token_type, TokenType::Star);
        assert_eq!(tokens[2].token_type, TokenType::From);
        assert!(matches!(tokens[3].token_type, TokenType::Identifier(_)));
    }

    #[test]
    fn test_tokenize_where_clause() {
        let mut tokenizer = Tokenizer::new();
        let tokens = tokenizer.tokenize("WHERE id = 123 AND name LIKE 'test%'").unwrap();

        assert_eq!(tokens[0].token_type, TokenType::Where);
        assert!(matches!(tokens[1].token_type, TokenType::Identifier(_)));
        assert_eq!(tokens[2].token_type, TokenType::Equal);
        assert!(matches!(tokens[3].token_type, TokenType::Number(_)));
    }

    #[test]
    fn test_tokenize_strings() {
        let mut tokenizer = Tokenizer::new();
        let tokens = tokenizer.tokenize("'single' \"double\"").unwrap();

        assert!(matches!(tokens[0].token_type, TokenType::String(ref s) if s == "single"));
        assert!(matches!(tokens[1].token_type, TokenType::String(ref s) if s == "double"));
    }
}
