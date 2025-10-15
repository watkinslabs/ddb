// Date/Time functions
use super::Value;
use crate::error::{DdbError, Result};
use chrono::{Datelike, Local, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

/// NOW() / CURRENT_TIMESTAMP() - Current date and time
pub fn now() -> Value {
    Value::DateTime(Local::now().naive_local())
}

/// CURDATE() / CURRENT_DATE() - Current date
pub fn curdate() -> Value {
    Value::Date(Local::now().date_naive())
}

/// CURTIME() / CURRENT_TIME() - Current time
pub fn curtime() -> Value {
    Value::Time(Local::now().time())
}

/// DATE(datetime) - Extract date part
pub fn date(value: &Value) -> Result<Value> {
    match value {
        Value::Date(d) => Ok(Value::Date(*d)),
        Value::DateTime(dt) => Ok(Value::Date(dt.date())),
        Value::String(s) => {
            let dt = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .map(|dt| dt.date())
                })
                .map_err(|e| DdbError::FunctionError(format!("Invalid date: {}", e)))?;
            Ok(Value::Date(dt))
        }
        _ => Err(DdbError::FunctionError(format!(
            "Cannot extract date from {:?}",
            value
        ))),
    }
}

/// TIME(datetime) - Extract time part
pub fn time(value: &Value) -> Result<Value> {
    match value {
        Value::Time(t) => Ok(Value::Time(*t)),
        Value::DateTime(dt) => Ok(Value::Time(dt.time())),
        Value::String(s) => {
            let t = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| {
                    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .map(|dt| dt.time())
                })
                .map_err(|e| DdbError::FunctionError(format!("Invalid time: {}", e)))?;
            Ok(Value::Time(t))
        }
        _ => Err(DdbError::FunctionError(format!(
            "Cannot extract time from {:?}",
            value
        ))),
    }
}

/// YEAR(date) - Extract year
pub fn year(value: &Value) -> Result<Value> {
    let date_val = date(value)?;
    match date_val {
        Value::Date(d) => Ok(Value::Integer(d.year() as i64)),
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// MONTH(date) - Extract month (1-12)
pub fn month(value: &Value) -> Result<Value> {
    let date_val = date(value)?;
    match date_val {
        Value::Date(d) => Ok(Value::Integer(d.month() as i64)),
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// DAY(date) / DAYOFMONTH(date) - Extract day of month (1-31)
pub fn day(value: &Value) -> Result<Value> {
    let date_val = date(value)?;
    match date_val {
        Value::Date(d) => Ok(Value::Integer(d.day() as i64)),
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// HOUR(time) - Extract hour (0-23)
pub fn hour(value: &Value) -> Result<Value> {
    let time_val = time(value)?;
    match time_val {
        Value::Time(t) => Ok(Value::Integer(t.hour() as i64)),
        _ => Err(DdbError::FunctionError("Invalid time".to_string())),
    }
}

/// MINUTE(time) - Extract minute (0-59)
pub fn minute(value: &Value) -> Result<Value> {
    let time_val = time(value)?;
    match time_val {
        Value::Time(t) => Ok(Value::Integer(t.minute() as i64)),
        _ => Err(DdbError::FunctionError("Invalid time".to_string())),
    }
}

/// SECOND(time) - Extract second (0-59)
pub fn second(value: &Value) -> Result<Value> {
    let time_val = time(value)?;
    match time_val {
        Value::Time(t) => Ok(Value::Integer(t.second() as i64)),
        _ => Err(DdbError::FunctionError("Invalid time".to_string())),
    }
}

/// DAYOFWEEK(date) - Day of week (1=Sunday, 7=Saturday)
pub fn dayofweek(value: &Value) -> Result<Value> {
    let date_val = date(value)?;
    match date_val {
        Value::Date(d) => {
            let weekday = d.weekday().num_days_from_sunday() + 1;
            Ok(Value::Integer(weekday as i64))
        }
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// DAYNAME(date) - Name of day (Monday, Tuesday, etc.)
pub fn dayname(value: &Value) -> Result<Value> {
    let date_val = date(value)?;
    match date_val {
        Value::Date(d) => Ok(Value::String(d.format("%A").to_string())),
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// MONTHNAME(date) - Name of month (January, February, etc.)
pub fn monthname(value: &Value) -> Result<Value> {
    let date_val = date(value)?;
    match date_val {
        Value::Date(d) => Ok(Value::String(d.format("%B").to_string())),
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// DAYOFYEAR(date) - Day of year (1-366)
pub fn dayofyear(value: &Value) -> Result<Value> {
    let date_val = date(value)?;
    match date_val {
        Value::Date(d) => Ok(Value::Integer(d.ordinal() as i64)),
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// WEEK(date) - Week number of year (0-53)
pub fn week(value: &Value) -> Result<Value> {
    let date_val = date(value)?;
    match date_val {
        Value::Date(d) => Ok(Value::Integer(d.iso_week().week() as i64)),
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// QUARTER(date) - Quarter of year (1-4)
pub fn quarter(value: &Value) -> Result<Value> {
    let month_val = month(value)?;
    match month_val {
        Value::Integer(m) => Ok(Value::Integer(((m - 1) / 3) + 1)),
        _ => Err(DdbError::FunctionError("Invalid date".to_string())),
    }
}

/// DATE_FORMAT(date, format) - Format date/time
pub fn date_format(value: &Value, format: &Value) -> Result<Value> {
    let fmt = format.to_string();
    let formatted = match value {
        Value::Date(d) => d.format(&fmt).to_string(),
        Value::DateTime(dt) => dt.format(&fmt).to_string(),
        Value::Time(t) => t.format(&fmt).to_string(),
        _ => {
            let date_val = date(value)?;
            match date_val {
                Value::Date(d) => d.format(&fmt).to_string(),
                _ => return Err(DdbError::FunctionError("Invalid date".to_string())),
            }
        }
    };
    Ok(Value::String(formatted))
}

/// UNIX_TIMESTAMP([date]) - Seconds since Unix epoch
pub fn unix_timestamp(value: Option<&Value>) -> Result<Value> {
    if let Some(v) = value {
        match v {
            Value::DateTime(dt) => Ok(Value::Integer(dt.and_utc().timestamp())),
            _ => {
                let date_val = date(v)?;
                match date_val {
                    Value::Date(d) => {
                        let dt = d.and_hms_opt(0, 0, 0).unwrap();
                        Ok(Value::Integer(dt.and_utc().timestamp()))
                    }
                    _ => Err(DdbError::FunctionError("Invalid date".to_string())),
                }
            }
        }
    } else {
        Ok(Value::Integer(Local::now().timestamp()))
    }
}

/// FROM_UNIXTIME(timestamp) - Convert Unix timestamp to datetime
pub fn from_unixtime(value: &Value) -> Result<Value> {
    let timestamp = value.as_i64()?;
    let dt = chrono::DateTime::from_timestamp(timestamp, 0)
        .ok_or_else(|| DdbError::FunctionError("Invalid timestamp".to_string()))?;
    Ok(Value::DateTime(dt.naive_local()))
}

/// DATEDIFF(date1, date2) - Days between two dates
pub fn datediff(date1: &Value, date2: &Value) -> Result<Value> {
    let d1 = match date(date1)? {
        Value::Date(d) => d,
        _ => return Err(DdbError::FunctionError("Invalid date1".to_string())),
    };

    let d2 = match date(date2)? {
        Value::Date(d) => d,
        _ => return Err(DdbError::FunctionError("Invalid date2".to_string())),
    };

    let diff = d1.signed_duration_since(d2).num_days();
    Ok(Value::Integer(diff))
}

/// DATEADD(date, interval, unit) / DATE_ADD(date, INTERVAL n unit)
/// unit can be: 'day', 'month', 'year', 'hour', 'minute', 'second'
pub fn dateadd(date_val: &Value, interval: &Value, unit: &str) -> Result<Value> {
    let n = interval.as_i64()?;

    match date_val {
        Value::Date(d) => {
            let new_date = match unit.to_lowercase().as_str() {
                "day" | "days" => *d + chrono::Duration::days(n),
                "week" | "weeks" => *d + chrono::Duration::weeks(n),
                "month" | "months" => {
                    if n >= 0 {
                        d.checked_add_months(chrono::Months::new(n as u32))
                    } else {
                        d.checked_sub_months(chrono::Months::new((-n) as u32))
                    }
                    .ok_or_else(|| DdbError::FunctionError("Date out of range".to_string()))?
                }
                "year" | "years" => {
                    if n >= 0 {
                        d.checked_add_months(chrono::Months::new((n * 12) as u32))
                    } else {
                        d.checked_sub_months(chrono::Months::new(((-n) * 12) as u32))
                    }
                    .ok_or_else(|| DdbError::FunctionError("Date out of range".to_string()))?
                }
                _ => return Err(DdbError::FunctionError(format!("Invalid unit for date: {}", unit))),
            };
            Ok(Value::Date(new_date))
        }
        Value::DateTime(dt) => {
            let new_dt = match unit.to_lowercase().as_str() {
                "day" | "days" => *dt + chrono::Duration::days(n),
                "week" | "weeks" => *dt + chrono::Duration::weeks(n),
                "hour" | "hours" => *dt + chrono::Duration::hours(n),
                "minute" | "minutes" => *dt + chrono::Duration::minutes(n),
                "second" | "seconds" => *dt + chrono::Duration::seconds(n),
                "month" | "months" => {
                    let date = dt.date();
                    let new_date = if n >= 0 {
                        date.checked_add_months(chrono::Months::new(n as u32))
                    } else {
                        date.checked_sub_months(chrono::Months::new((-n) as u32))
                    }
                    .ok_or_else(|| DdbError::FunctionError("Date out of range".to_string()))?;
                    new_date.and_time(dt.time())
                }
                "year" | "years" => {
                    let date = dt.date();
                    let new_date = if n >= 0 {
                        date.checked_add_months(chrono::Months::new((n * 12) as u32))
                    } else {
                        date.checked_sub_months(chrono::Months::new(((-n) * 12) as u32))
                    }
                    .ok_or_else(|| DdbError::FunctionError("Date out of range".to_string()))?;
                    new_date.and_time(dt.time())
                }
                _ => return Err(DdbError::FunctionError(format!("Invalid unit: {}", unit))),
            };
            Ok(Value::DateTime(new_dt))
        }
        _ => {
            // Try to parse as date first
            let d = date(date_val)?;
            dateadd(&d, interval, unit)
        }
    }
}

/// DATESUB(date, interval, unit) / DATE_SUB(date, INTERVAL n unit)
pub fn datesub(date_val: &Value, interval: &Value, unit: &str) -> Result<Value> {
    let n = interval.as_i64()?;
    dateadd(date_val, &Value::Integer(-n), unit)
}

/// TIMESTAMPDIFF(unit, datetime1, datetime2) - Difference in specified units
pub fn timestampdiff(unit: &str, datetime1: &Value, datetime2: &Value) -> Result<Value> {
    let dt1 = match date(datetime1)? {
        Value::Date(d) => d.and_hms_opt(0, 0, 0).unwrap(),
        _ => match datetime1 {
            Value::DateTime(dt) => *dt,
            _ => return Err(DdbError::FunctionError("Invalid datetime1".to_string())),
        }
    };

    let dt2 = match date(datetime2)? {
        Value::Date(d) => d.and_hms_opt(0, 0, 0).unwrap(),
        _ => match datetime2 {
            Value::DateTime(dt) => *dt,
            _ => return Err(DdbError::FunctionError("Invalid datetime2".to_string())),
        }
    };

    let diff = dt2.signed_duration_since(dt1);

    let result = match unit.to_lowercase().as_str() {
        "second" | "seconds" => diff.num_seconds(),
        "minute" | "minutes" => diff.num_minutes(),
        "hour" | "hours" => diff.num_hours(),
        "day" | "days" => diff.num_days(),
        "week" | "weeks" => diff.num_weeks(),
        "month" | "months" => {
            let years = (dt2.year() - dt1.year()) as i64;
            let months = (dt2.month() as i32 - dt1.month() as i32) as i64;
            years * 12 + months
        }
        "year" | "years" => (dt2.year() - dt1.year()) as i64,
        _ => return Err(DdbError::FunctionError(format!("Invalid unit: {}", unit))),
    };

    Ok(Value::Integer(result))
}

/// AGE(date1, [date2]) - Age between dates or from date to now
pub fn age(date1: &Value, date2: Option<&Value>) -> Result<Value> {
    let d1 = match date(date1)? {
        Value::Date(d) => d,
        _ => return Err(DdbError::FunctionError("Invalid date".to_string())),
    };

    let d2 = if let Some(v) = date2 {
        match date(v)? {
            Value::Date(d) => d,
            _ => return Err(DdbError::FunctionError("Invalid date".to_string())),
        }
    } else {
        Local::now().date_naive()
    };

    let years = d2.year() - d1.year();
    Ok(Value::Integer(years as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_extraction() {
        let dt = Value::String("2024-03-15".to_string());
        assert_eq!(year(&dt).unwrap(), Value::Integer(2024));
        assert_eq!(month(&dt).unwrap(), Value::Integer(3));
        assert_eq!(day(&dt).unwrap(), Value::Integer(15));
    }

    #[test]
    fn test_time_extraction() {
        let t = Value::String("14:30:45".to_string());
        let time_val = time(&t).unwrap();
        assert_eq!(hour(&time_val).unwrap(), Value::Integer(14));
        assert_eq!(minute(&time_val).unwrap(), Value::Integer(30));
        assert_eq!(second(&time_val).unwrap(), Value::Integer(45));
    }

    #[test]
    fn test_now() {
        // Just make sure it doesn't panic
        let _ = now();
        let _ = curdate();
        let _ = curtime();
    }
}
