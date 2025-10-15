//! DDB - A serviceless SQL interface for flat files
//!
//! DDB provides SQL-like querying capabilities for delimited text files
//! with low memory footprint and high performance.

pub mod error;
pub mod lexer;
pub mod parser;
pub mod config;
pub mod file_io;
pub mod engine;
pub mod methods;
pub mod functions;
pub mod output;

#[cfg(feature = "mcp")]
pub mod mcp;

pub use error::{DdbError, Result};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }
}
