// Conditional functions
use super::Value;
use crate::error::Result;

/// IF(condition, true_value, false_value)
pub fn if_fn(condition: &Value, true_val: &Value, false_val: &Value) -> Result<Value> {
    let cond = match condition {
        Value::Boolean(b) => *b,
        Value::Integer(i) => *i != 0,
        Value::Float(f) => *f != 0.0,
        Value::Null => false,
        Value::String(s) => !s.is_empty(),
        _ => true,
    };

    Ok(if cond {
        true_val.clone()
    } else {
        false_val.clone()
    })
}

/// IFNULL(expr, alt_value) - Return alt_value if expr is NULL
pub fn ifnull(expr: &Value, alt_value: &Value) -> Result<Value> {
    if expr.is_null() {
        Ok(alt_value.clone())
    } else {
        Ok(expr.clone())
    }
}

/// NULLIF(expr1, expr2) - Return NULL if expr1 == expr2, otherwise expr1
pub fn nullif(expr1: &Value, expr2: &Value) -> Result<Value> {
    if expr1 == expr2 {
        Ok(Value::Null)
    } else {
        Ok(expr1.clone())
    }
}

/// COALESCE(val1, val2, ...) - Return first non-NULL value
pub fn coalesce(values: &[Value]) -> Result<Value> {
    values
        .iter()
        .find(|v| !v.is_null())
        .cloned()
        .ok_or_else(|| crate::error::DdbError::FunctionError("All values are NULL".to_string()))
}

/// GREATEST(val1, val2, ...) - Return greatest value
pub fn greatest(values: &[Value]) -> Result<Value> {
    values
        .iter()
        .max_by(|a, b| {
            match (a, b) {
                (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
                (Value::Float(x), Value::Float(y)) => {
                    x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Value::String(x), Value::String(y)) => x.cmp(y),
                (Value::Integer(x), Value::Float(y)) => {
                    (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Value::Float(x), Value::Integer(y)) => {
                    x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal)
                }
                _ => std::cmp::Ordering::Equal,
            }
        })
        .cloned()
        .ok_or_else(|| crate::error::DdbError::FunctionError("No values for GREATEST".to_string()))
}

/// LEAST(val1, val2, ...) - Return smallest value
pub fn least(values: &[Value]) -> Result<Value> {
    values
        .iter()
        .min_by(|a, b| {
            match (a, b) {
                (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
                (Value::Float(x), Value::Float(y)) => {
                    x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Value::String(x), Value::String(y)) => x.cmp(y),
                (Value::Integer(x), Value::Float(y)) => {
                    (*x as f64).partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal)
                }
                (Value::Float(x), Value::Integer(y)) => {
                    x.partial_cmp(&(*y as f64)).unwrap_or(std::cmp::Ordering::Equal)
                }
                _ => std::cmp::Ordering::Equal,
            }
        })
        .cloned()
        .ok_or_else(|| crate::error::DdbError::FunctionError("No values for LEAST".to_string()))
}

/// ISNULL(expr) - Check if value is NULL
pub fn isnull(expr: &Value) -> Result<Value> {
    Ok(Value::Boolean(expr.is_null()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_if() {
        assert_eq!(
            if_fn(&Value::Boolean(true), &Value::from("yes"), &Value::from("no")).unwrap(),
            Value::from("yes")
        );
        assert_eq!(
            if_fn(&Value::Boolean(false), &Value::from("yes"), &Value::from("no")).unwrap(),
            Value::from("no")
        );
    }

    #[test]
    fn test_ifnull() {
        assert_eq!(
            ifnull(&Value::Null, &Value::from("default")).unwrap(),
            Value::from("default")
        );
        assert_eq!(
            ifnull(&Value::from("value"), &Value::from("default")).unwrap(),
            Value::from("value")
        );
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            nullif(&Value::Integer(5), &Value::Integer(5)).unwrap(),
            Value::Null
        );
        assert_eq!(
            nullif(&Value::Integer(5), &Value::Integer(10)).unwrap(),
            Value::Integer(5)
        );
    }

    #[test]
    fn test_coalesce() {
        let values = vec![Value::Null, Value::Null, Value::from("first"), Value::from("second")];
        assert_eq!(coalesce(&values).unwrap(), Value::from("first"));
    }

    #[test]
    fn test_greatest_least() {
        let values = vec![Value::Integer(1), Value::Integer(5), Value::Integer(3)];
        assert_eq!(greatest(&values).unwrap(), Value::Integer(5));
        assert_eq!(least(&values).unwrap(), Value::Integer(1));
    }
}
