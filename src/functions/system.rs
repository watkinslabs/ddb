// System functions
use super::Value;
use crate::error::Result;
use uuid::Uuid;

/// DATABASE() - Current database name
pub fn database(db_name: Option<&str>) -> Value {
    Value::String(db_name.unwrap_or("main").to_string())
}

/// VERSION() - DDB version
pub fn version() -> Value {
    Value::String(crate::VERSION.to_string())
}

/// UUID() / UUID_GENERATE() - Generate UUID
pub fn uuid() -> Value {
    Value::String(Uuid::new_v4().to_string())
}

/// ROW_NUMBER() - Row number (context-dependent, passed in)
pub fn row_number(row: i64) -> Value {
    Value::Integer(row)
}

/// USER() / CURRENT_USER() - Current user (not applicable for file-based DB)
pub fn user() -> Value {
    Value::String("file_user".to_string())
}

/// CONNECTION_ID() - Connection identifier (file-based, use process ID)
pub fn connection_id() -> Value {
    Value::Integer(std::process::id() as i64)
}

/// LAST_INSERT_ID() - Last inserted ID (would need context)
pub fn last_insert_id(id: Option<i64>) -> Value {
    Value::Integer(id.unwrap_or(0))
}

/// FOUND_ROWS() - Number of rows found (would need context)
pub fn found_rows(count: Option<i64>) -> Value {
    Value::Integer(count.unwrap_or(0))
}

/// BENCHMARK(count, expr) - Run expression count times (for testing)
pub fn benchmark(count: &Value, expr: impl Fn() -> Result<Value>) -> Result<Value> {
    let n = count.as_i64()?;
    let start = std::time::Instant::now();

    for _ in 0..n {
        let _ = expr()?;
    }

    let elapsed = start.elapsed();
    Ok(Value::Float(elapsed.as_secs_f64()))
}

/// SLEEP(seconds) - Sleep for specified seconds
pub fn sleep(seconds: &Value) -> Result<Value> {
    let secs = seconds.as_f64()?;
    std::thread::sleep(std::time::Duration::from_secs_f64(secs));
    Ok(Value::Integer(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database() {
        assert_eq!(database(Some("test")), Value::from("test"));
        assert_eq!(database(None), Value::from("main"));
    }

    #[test]
    fn test_version() {
        let v = version();
        assert!(matches!(v, Value::String(_)));
    }

    #[test]
    fn test_uuid() {
        let u1 = uuid();
        let u2 = uuid();
        assert_ne!(u1, u2); // UUIDs should be unique
    }

    #[test]
    fn test_row_number() {
        assert_eq!(row_number(42), Value::Integer(42));
    }
}
