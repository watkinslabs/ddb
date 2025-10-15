// Math functions
use super::Value;
use crate::error::{DdbError, Result};

/// ABS(x) - Absolute value
pub fn abs(value: &Value) -> Result<Value> {
    match value {
        Value::Integer(i) => Ok(Value::Integer(i.abs())),
        Value::Float(f) => Ok(Value::Float(f.abs())),
        _ => Ok(Value::Float(value.as_f64()?.abs())),
    }
}

/// CEIL(x) / CEILING(x) - Round up to nearest integer
pub fn ceil(value: &Value) -> Result<Value> {
    Ok(Value::Integer(value.as_f64()?.ceil() as i64))
}

/// FLOOR(x) - Round down to nearest integer
pub fn floor(value: &Value) -> Result<Value> {
    Ok(Value::Integer(value.as_f64()?.floor() as i64))
}

/// ROUND(x, [decimals]) - Round to specified decimal places
pub fn round(value: &Value, decimals: Option<&Value>) -> Result<Value> {
    let num = value.as_f64()?;
    let places = decimals.map(|v| v.as_i64()).transpose()?.unwrap_or(0);

    if places == 0 {
        Ok(Value::Integer(num.round() as i64))
    } else {
        let multiplier = 10_f64.powi(places as i32);
        Ok(Value::Float((num * multiplier).round() / multiplier))
    }
}

/// SQRT(x) - Square root
pub fn sqrt(value: &Value) -> Result<Value> {
    let num = value.as_f64()?;
    if num < 0.0 {
        return Err(DdbError::FunctionError("Cannot take square root of negative number".to_string()));
    }
    Ok(Value::Float(num.sqrt()))
}

/// POW(x, y) / POWER(x, y) - x raised to power y
pub fn pow(base: &Value, exponent: &Value) -> Result<Value> {
    let b = base.as_f64()?;
    let e = exponent.as_f64()?;
    Ok(Value::Float(b.powf(e)))
}

/// EXP(x) - e^x
pub fn exp(value: &Value) -> Result<Value> {
    Ok(Value::Float(value.as_f64()?.exp()))
}

/// LN(x) / LOG(x) - Natural logarithm
pub fn ln(value: &Value) -> Result<Value> {
    let num = value.as_f64()?;
    if num <= 0.0 {
        return Err(DdbError::FunctionError("Logarithm of non-positive number".to_string()));
    }
    Ok(Value::Float(num.ln()))
}

/// LOG10(x) - Base-10 logarithm
pub fn log10(value: &Value) -> Result<Value> {
    let num = value.as_f64()?;
    if num <= 0.0 {
        return Err(DdbError::FunctionError("Logarithm of non-positive number".to_string()));
    }
    Ok(Value::Float(num.log10()))
}

/// MOD(x, y) - Modulo operation
pub fn modulo(dividend: &Value, divisor: &Value) -> Result<Value> {
    match (dividend, divisor) {
        (Value::Integer(a), Value::Integer(b)) => {
            if *b == 0 {
                return Err(DdbError::FunctionError("Division by zero".to_string()));
            }
            Ok(Value::Integer(a % b))
        }
        _ => {
            let a = dividend.as_f64()?;
            let b = divisor.as_f64()?;
            if b == 0.0 {
                return Err(DdbError::FunctionError("Division by zero".to_string()));
            }
            Ok(Value::Float(a % b))
        }
    }
}

/// SIGN(x) - Returns -1, 0, or 1 depending on sign
pub fn sign(value: &Value) -> Result<Value> {
    let num = value.as_f64()?;
    Ok(Value::Integer(if num > 0.0 { 1 } else if num < 0.0 { -1 } else { 0 }))
}

/// TRUNC(x, [decimals]) - Truncate to decimal places
pub fn trunc(value: &Value, decimals: Option<&Value>) -> Result<Value> {
    let num = value.as_f64()?;
    let places = decimals.map(|v| v.as_i64()).transpose()?.unwrap_or(0);

    if places == 0 {
        Ok(Value::Integer(num.trunc() as i64))
    } else {
        let multiplier = 10_f64.powi(places as i32);
        Ok(Value::Float((num * multiplier).trunc() / multiplier))
    }
}

/// RAND() / RANDOM() - Random number between 0 and 1
pub fn rand() -> Value {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;
    use std::time::SystemTime;

    let mut hasher = DefaultHasher::new();
    SystemTime::now().hash(&mut hasher);
    let hash = hasher.finish();

    Value::Float((hash % 1000000) as f64 / 1000000.0)
}

/// PI() - Returns pi constant
pub fn pi() -> Value {
    Value::Float(std::f64::consts::PI)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abs() {
        assert_eq!(abs(&Value::Integer(-5)).unwrap(), Value::Integer(5));
        assert_eq!(abs(&Value::Float(-3.14)).unwrap(), Value::Float(3.14));
    }

    #[test]
    fn test_ceil_floor() {
        assert_eq!(ceil(&Value::Float(3.2)).unwrap(), Value::Integer(4));
        assert_eq!(floor(&Value::Float(3.8)).unwrap(), Value::Integer(3));
    }

    #[test]
    fn test_round() {
        assert_eq!(round(&Value::Float(3.456), None).unwrap(), Value::Integer(3));
        assert_eq!(round(&Value::Float(3.456), Some(&Value::Integer(2))).unwrap(), Value::Float(3.46));
    }

    #[test]
    fn test_sqrt() {
        assert_eq!(sqrt(&Value::Integer(16)).unwrap(), Value::Float(4.0));
        assert!(sqrt(&Value::Integer(-1)).is_err());
    }

    #[test]
    fn test_pow() {
        assert_eq!(pow(&Value::Integer(2), &Value::Integer(3)).unwrap(), Value::Float(8.0));
    }
}
