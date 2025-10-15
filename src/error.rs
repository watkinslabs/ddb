use thiserror::Error;

#[derive(Error, Debug)]
pub enum DdbError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Syntax error: {0}")]
    SyntaxError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Invalid column count: expected {expected}, got {actual}")]
    InvalidColumnCount { expected: usize, actual: usize },

    #[error("Type error: {0}")]
    TypeError(String),

    #[error("Evaluation error: {0}")]
    EvaluationError(String),

    #[error("Lock error: {0}")]
    LockError(String),

    #[error("Transaction error: {0}")]
    TransactionError(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Function error: {0}")]
    FunctionError(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),
}

pub type Result<T> = std::result::Result<T, DdbError>;
