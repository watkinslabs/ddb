// Type conversion functions
use super::Value;
use crate::error::{DdbError, Result};

/// CAST(value AS type) - SQL standard type conversion
pub fn cast(value: &Value, target_type: &str) -> Result<Value> {
    match target_type.to_uppercase().as_str() {
        "INTEGER" | "INT" | "BIGINT" => {
            Ok(Value::Integer(value.as_i64()?))
        }
        "FLOAT" | "REAL" | "DOUBLE" | "DECIMAL" | "NUMERIC" => {
            Ok(Value::Float(value.as_f64()?))
        }
        "VARCHAR" | "CHAR" | "TEXT" | "STRING" => {
            Ok(Value::String(value.to_string()))
        }
        "BOOLEAN" | "BOOL" => {
            let val = match value {
                Value::Boolean(b) => *b,
                Value::Integer(i) => *i != 0,
                Value::Float(f) => *f != 0.0,
                Value::String(s) => {
                    s.eq_ignore_ascii_case("true")
                    || s.eq_ignore_ascii_case("yes")
                    || s.eq_ignore_ascii_case("1")
                }
                Value::Null => false,
                _ => return Err(DdbError::TypeError(format!("Cannot convert {:?} to boolean", value))),
            };
            Ok(Value::Boolean(val))
        }
        "DATE" => {
            let s = value.to_string();
            let date = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d")
                .map_err(|e| DdbError::TypeError(format!("Invalid date format: {}", e)))?;
            Ok(Value::Date(date))
        }
        "DATETIME" | "TIMESTAMP" => {
            let s = value.to_string();
            let dt = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d"))
                .map_err(|e| DdbError::TypeError(format!("Invalid datetime format: {}", e)))?;
            Ok(Value::DateTime(dt))
        }
        _ => Err(DdbError::TypeError(format!("Unknown type: {}", target_type))),
    }
}

/// CONVERT(value, type) - MySQL-style conversion
pub fn convert(value: &Value, target_type: &str) -> Result<Value> {
    cast(value, target_type)
}

/// ATOF(str) - ASCII to float
pub fn atof(value: &Value) -> Result<Value> {
    let s = value.to_string();
    s.parse::<f64>()
        .map(Value::Float)
        .map_err(|_| DdbError::TypeError(format!("Cannot parse '{}' as float", s)))
}

/// ATOI(str) - ASCII to integer
pub fn atoi(value: &Value) -> Result<Value> {
    let s = value.to_string();
    s.parse::<i64>()
        .map(Value::Integer)
        .map_err(|_| DdbError::TypeError(format!("Cannot parse '{}' as integer", s)))
}

/// TO_STRING(value) / STR(value) - Convert to string
pub fn to_string(value: &Value) -> Result<Value> {
    Ok(Value::String(value.to_string()))
}

/// TO_NUMBER(str) - Convert string to number (int or float)
pub fn to_number(value: &Value) -> Result<Value> {
    let s = value.to_string();

    // Try integer first
    if let Ok(i) = s.parse::<i64>() {
        return Ok(Value::Integer(i));
    }

    // Try float
    if let Ok(f) = s.parse::<f64>() {
        return Ok(Value::Float(f));
    }

    Err(DdbError::TypeError(format!("Cannot convert '{}' to number", s)))
}

/// HEX(n) - Convert to hexadecimal string
pub fn hex(value: &Value) -> Result<Value> {
    match value {
        Value::Integer(i) => Ok(Value::String(format!("{:X}", i))),
        _ => {
            let i = value.as_i64()?;
            Ok(Value::String(format!("{:X}", i)))
        }
    }
}

/// BIN(n) - Convert to binary string
pub fn bin(value: &Value) -> Result<Value> {
    match value {
        Value::Integer(i) => Ok(Value::String(format!("{:b}", i))),
        _ => {
            let i = value.as_i64()?;
            Ok(Value::String(format!("{:b}", i)))
        }
    }
}

/// OCT(n) - Convert to octal string
pub fn oct(value: &Value) -> Result<Value> {
    match value {
        Value::Integer(i) => Ok(Value::String(format!("{:o}", i))),
        _ => {
            let i = value.as_i64()?;
            Ok(Value::String(format!("{:o}", i)))
        }
    }
}

/// FORMAT(number, decimals) - Format number with thousands separator
pub fn format(value: &Value, decimals: &Value) -> Result<Value> {
    let num = value.as_f64()?;
    let dec = decimals.as_i64()? as usize;

    let formatted = format!("{:.width$}", num, width = dec);

    // Add thousands separators
    let parts: Vec<&str> = formatted.split('.').collect();
    let integer_part = parts[0];
    let decimal_part = parts.get(1).unwrap_or(&"");

    let mut result = String::new();
    for (i, ch) in integer_part.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, ch);
    }

    if !decimal_part.is_empty() {
        result.push('.');
        result.push_str(decimal_part);
    }

    Ok(Value::String(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cast() {
        assert_eq!(
            cast(&Value::from("123"), "INTEGER").unwrap(),
            Value::Integer(123)
        );
        assert_eq!(
            cast(&Value::Integer(42), "STRING").unwrap(),
            Value::from("42")
        );
    }

    #[test]
    fn test_atof_atoi() {
        assert_eq!(atof(&Value::from("3.14")).unwrap(), Value::Float(3.14));
        assert_eq!(atoi(&Value::from("42")).unwrap(), Value::Integer(42));
    }

    #[test]
    fn test_hex_bin_oct() {
        assert_eq!(hex(&Value::Integer(255)).unwrap(), Value::from("FF"));
        assert_eq!(bin(&Value::Integer(7)).unwrap(), Value::from("111"));
        assert_eq!(oct(&Value::Integer(8)).unwrap(), Value::from("10"));
    }

    #[test]
    fn test_to_number() {
        assert_eq!(to_number(&Value::from("123")).unwrap(), Value::Integer(123));
        assert_eq!(to_number(&Value::from("12.5")).unwrap(), Value::Float(12.5));
    }
}
