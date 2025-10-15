use std::fmt;

/// Token types recognized by the lexer
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenType {
    // SQL Keywords
    Select,
    Insert,
    Update,
    Delete,
    Upsert,
    Create,
    Drop,
    Use,
    Show,
    Set,
    From,
    Where,
    OrderBy,
    GroupBy,
    Limit,
    Distinct,
    As,
    Into,
    Values,
    Table,
    Tables,
    Columns,
    Database,
    Begin,
    Commit,
    Rollback,
    Describe,
    And,
    Or,
    Not,
    Like,
    Is,
    Null,
    True,
    False,
    In,
    Between,
    Asc,
    Desc,
    Order,
    By,
    File,
    Delimiter,
    Variables,
    Offset,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    On,
    Having,
    Group,

    // Operators
    Multiply,        // * (alias for Star when used in expressions)
    Equal,           // =
    NotEqual,        // !=, <>
    GreaterThan,     // >
    GreaterEqual,    // >=
    LessThan,        // <
    LessEqual,       // <=

    // Delimiters
    Comma,           // ,
    Semicolon,       // ;
    LeftParen,       // (
    RightParen,      // )
    LeftBracket,     // [
    RightBracket,    // ]
    Dot,             // .
    Star,            // *

    // Literals
    String(String),
    Number(String),
    Identifier(String),

    // Special
    Whitespace,
    Comment(String),
    Eof,
}

impl fmt::Display for TokenType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenType::String(s) => write!(f, "'{}'", s),
            TokenType::Number(n) => write!(f, "{}", n),
            TokenType::Identifier(i) => write!(f, "{}", i),
            TokenType::Comment(c) => write!(f, "/* {} */", c),
            _ => write!(f, "{:?}", self),
        }
    }
}

/// A token with position information
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub token_type: TokenType,
    pub position: usize,
    pub line: usize,
    pub column: usize,
}

impl Token {
    pub fn new(token_type: TokenType, position: usize, line: usize, column: usize) -> Self {
        Self {
            token_type,
            position,
            line,
            column,
        }
    }

    pub fn is_keyword(&self) -> bool {
        matches!(
            self.token_type,
            TokenType::Select
                | TokenType::Insert
                | TokenType::Update
                | TokenType::Delete
                | TokenType::Upsert
                | TokenType::Create
                | TokenType::Drop
                | TokenType::Use
                | TokenType::Show
                | TokenType::Set
                | TokenType::From
                | TokenType::Where
                | TokenType::OrderBy
                | TokenType::GroupBy
                | TokenType::Limit
                | TokenType::Distinct
                | TokenType::As
                | TokenType::Into
                | TokenType::Values
                | TokenType::Table
                | TokenType::Tables
                | TokenType::Columns
                | TokenType::Database
                | TokenType::Begin
                | TokenType::Commit
                | TokenType::Rollback
                | TokenType::Describe
                | TokenType::And
                | TokenType::Or
                | TokenType::Not
                | TokenType::Like
                | TokenType::Is
                | TokenType::Null
                | TokenType::True
                | TokenType::False
                | TokenType::In
                | TokenType::Between
                | TokenType::Asc
                | TokenType::Desc
                | TokenType::Join
                | TokenType::Inner
                | TokenType::Left
                | TokenType::Right
                | TokenType::Full
                | TokenType::Outer
                | TokenType::On
                | TokenType::Having
                | TokenType::Group
        )
    }
}
