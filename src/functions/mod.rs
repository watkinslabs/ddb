// SQL function implementations
pub mod aggregate;
pub mod math;
pub mod string;
pub mod conversion;
pub mod datetime;
pub mod system;
pub mod conditional;
pub mod utility;
pub mod registry;

use crate::error::{DdbError, Result};
use serde::{Deserialize, Serialize};

/// A value that can be returned from functions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Date(chrono::NaiveDate),
    DateTime(chrono::NaiveDateTime),
    Time(chrono::NaiveTime),
}

impl Value {
    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => s.clone(),
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            Value::Time(t) => t.format("%H:%M:%S").to_string(),
        }
    }

    /// Try to convert to integer
    pub fn as_i64(&self) -> Result<i64> {
        match self {
            Value::Integer(i) => Ok(*i),
            Value::Float(f) => Ok(*f as i64),
            Value::String(s) => s.parse().map_err(|_| {
                DdbError::TypeError(format!("Cannot convert '{}' to integer", s))
            }),
            Value::Boolean(b) => Ok(if *b { 1 } else { 0 }),
            _ => Err(DdbError::TypeError(format!(
                "Cannot convert {:?} to integer",
                self
            ))),
        }
    }

    /// Try to convert to float
    pub fn as_f64(&self) -> Result<f64> {
        match self {
            Value::Float(f) => Ok(*f),
            Value::Integer(i) => Ok(*i as f64),
            Value::String(s) => s.parse().map_err(|_| {
                DdbError::TypeError(format!("Cannot convert '{}' to float", s))
            }),
            _ => Err(DdbError::TypeError(format!(
                "Cannot convert {:?} to float",
                self
            ))),
        }
    }

    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Convert from string, attempting type inference
    pub fn from_str(s: &str) -> Self {
        // Try to parse as various types
        if s.eq_ignore_ascii_case("null") || s.is_empty() {
            return Value::Null;
        }

        if s.eq_ignore_ascii_case("true") {
            return Value::Boolean(true);
        }

        if s.eq_ignore_ascii_case("false") {
            return Value::Boolean(false);
        }

        // Try integer
        if let Ok(i) = s.parse::<i64>() {
            return Value::Integer(i);
        }

        // Try float
        if let Ok(f) = s.parse::<f64>() {
            return Value::Float(f);
        }

        // Default to string
        Value::String(s.to_string())
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_conversions() {
        assert_eq!(Value::from_str("123"), Value::Integer(123));
        assert_eq!(Value::from_str("12.5"), Value::Float(12.5));
        assert_eq!(Value::from_str("true"), Value::Boolean(true));
        assert_eq!(Value::from_str("null"), Value::Null);
        assert_eq!(Value::from_str("hello"), Value::String("hello".to_string()));
    }

    #[test]
    fn test_value_to_string() {
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Float(3.14).to_string(), "3.14");
        assert_eq!(Value::Boolean(true).to_string(), "true");
        assert_eq!(Value::Null.to_string(), "NULL");
    }
}
