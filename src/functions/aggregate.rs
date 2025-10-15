// Aggregate functions (these operate on multiple rows)
use super::Value;
use crate::error::Result;
use rayon::prelude::*;
use std::collections::HashSet;

/// Threshold for using parallel aggregation (1000 rows)
const PARALLEL_THRESHOLD: usize = 1000;

/// COUNT(*) / COUNT(expr) - Count rows or non-null values
pub fn count(values: &[Value], count_nulls: bool) -> Result<Value> {
    if count_nulls {
        Ok(Value::Integer(values.len() as i64))
    } else {
        let count = values.iter().filter(|v| !v.is_null()).count();
        Ok(Value::Integer(count as i64))
    }
}

/// COUNT(DISTINCT expr) - Count distinct non-null values
pub fn count_distinct(values: &[Value]) -> Result<Value> {
    let mut seen = HashSet::new();
    let count = values
        .iter()
        .filter(|v| !v.is_null())
        .filter(|v| seen.insert(v.to_string()))
        .count();
    Ok(Value::Integer(count as i64))
}

/// SUM(expr) - Sum of values with automatic parallelization for large datasets
pub fn sum(values: &[Value]) -> Result<Value> {
    if values.len() >= PARALLEL_THRESHOLD {
        // Parallel sum for large datasets
        let (int_sum, float_sum, has_float) = values
            .par_iter()
            .filter(|v| !v.is_null())
            .fold(
                || (0i64, 0.0f64, false),
                |(mut i_acc, mut f_acc, mut has_f), value| {
                    match value {
                        Value::Integer(i) => i_acc += i,
                        Value::Float(f) => {
                            f_acc += f;
                            has_f = true;
                        }
                        _ => {
                            if let Ok(f) = value.as_f64() {
                                f_acc += f;
                                has_f = true;
                            }
                        }
                    }
                    (i_acc, f_acc, has_f)
                },
            )
            .reduce(
                || (0i64, 0.0f64, false),
                |(i1, f1, h1), (i2, f2, h2)| (i1 + i2, f1 + f2, h1 || h2),
            );

        if has_float {
            Ok(Value::Float(float_sum + int_sum as f64))
        } else {
            Ok(Value::Integer(int_sum))
        }
    } else {
        // Sequential sum for small datasets
        let mut int_sum: i64 = 0;
        let mut float_sum: f64 = 0.0;
        let mut has_float = false;

        for value in values {
            if value.is_null() {
                continue;
            }

            match value {
                Value::Integer(i) => int_sum += i,
                Value::Float(f) => {
                    float_sum += f;
                    has_float = true;
                }
                _ => {
                    if let Ok(f) = value.as_f64() {
                        float_sum += f;
                        has_float = true;
                    }
                }
            }
        }

        if has_float {
            Ok(Value::Float(float_sum + int_sum as f64))
        } else {
            Ok(Value::Integer(int_sum))
        }
    }
}

/// AVG(expr) - Average of values
pub fn avg(values: &[Value]) -> Result<Value> {
    let non_null: Vec<_> = values.iter().filter(|v| !v.is_null()).collect();

    if non_null.is_empty() {
        return Ok(Value::Null);
    }

    let sum_val = sum(&non_null.iter().map(|v| (*v).clone()).collect::<Vec<_>>())?;
    let count = non_null.len() as f64;

    match sum_val {
        Value::Integer(i) => Ok(Value::Float(i as f64 / count)),
        Value::Float(f) => Ok(Value::Float(f / count)),
        _ => Ok(Value::Null),
    }
}

/// MIN(expr) - Minimum value
pub fn min(values: &[Value]) -> Result<Value> {
    values
        .iter()
        .filter(|v| !v.is_null())
        .min_by(|a, b| {
            // Compare values
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
        .ok_or_else(|| crate::error::DdbError::FunctionError("No values for MIN".to_string()))
}

/// MAX(expr) - Maximum value
pub fn max(values: &[Value]) -> Result<Value> {
    values
        .iter()
        .filter(|v| !v.is_null())
        .max_by(|a, b| {
            // Compare values
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
        .ok_or_else(|| crate::error::DdbError::FunctionError("No values for MAX".to_string()))
}

/// GROUP_CONCAT(expr, separator) - Concatenate strings from group
pub fn group_concat(values: &[Value], separator: Option<&str>) -> Result<Value> {
    let sep = separator.unwrap_or(",");
    let result: Vec<String> = values
        .iter()
        .filter(|v| !v.is_null())
        .map(|v| v.to_string())
        .collect();

    Ok(Value::String(result.join(sep)))
}

/// STDDEV(expr) / STDDEV_POP(expr) - Standard deviation (population) with parallel support
pub fn stddev_pop(values: &[Value]) -> Result<Value> {
    let non_null: Vec<_> = values.iter().filter(|v| !v.is_null()).collect();

    if non_null.is_empty() {
        return Ok(Value::Null);
    }

    // Calculate mean
    let mean = match avg(&non_null.iter().map(|v| (*v).clone()).collect::<Vec<_>>())? {
        Value::Float(f) => f,
        Value::Integer(i) => i as f64,
        _ => return Ok(Value::Null),
    };

    // Calculate variance (parallelized for large datasets)
    let variance: f64 = if non_null.len() >= PARALLEL_THRESHOLD {
        non_null
            .par_iter()
            .map(|v| {
                let val = match v {
                    Value::Integer(i) => *i as f64,
                    Value::Float(f) => *f,
                    _ => v.as_f64().unwrap_or(0.0),
                };
                let diff = val - mean;
                diff * diff
            })
            .sum::<f64>()
            / non_null.len() as f64
    } else {
        non_null
            .iter()
            .map(|v| {
                let val = match v {
                    Value::Integer(i) => *i as f64,
                    Value::Float(f) => *f,
                    _ => v.as_f64().unwrap_or(0.0),
                };
                let diff = val - mean;
                diff * diff
            })
            .sum::<f64>()
            / non_null.len() as f64
    };

    Ok(Value::Float(variance.sqrt()))
}

/// VARIANCE(expr) / VAR_POP(expr) - Variance (population) with parallel support
pub fn var_pop(values: &[Value]) -> Result<Value> {
    let non_null: Vec<_> = values.iter().filter(|v| !v.is_null()).collect();

    if non_null.is_empty() {
        return Ok(Value::Null);
    }

    // Calculate mean
    let mean = match avg(&non_null.iter().map(|v| (*v).clone()).collect::<Vec<_>>())? {
        Value::Float(f) => f,
        Value::Integer(i) => i as f64,
        _ => return Ok(Value::Null),
    };

    // Calculate variance (parallelized for large datasets)
    let variance: f64 = if non_null.len() >= PARALLEL_THRESHOLD {
        non_null
            .par_iter()
            .map(|v| {
                let val = match v {
                    Value::Integer(i) => *i as f64,
                    Value::Float(f) => *f,
                    _ => v.as_f64().unwrap_or(0.0),
                };
                let diff = val - mean;
                diff * diff
            })
            .sum::<f64>()
            / non_null.len() as f64
    } else {
        non_null
            .iter()
            .map(|v| {
                let val = match v {
                    Value::Integer(i) => *i as f64,
                    Value::Float(f) => *f,
                    _ => v.as_f64().unwrap_or(0.0),
                };
                let diff = val - mean;
                diff * diff
            })
            .sum::<f64>()
            / non_null.len() as f64
    };

    Ok(Value::Float(variance))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count() {
        let values = vec![Value::Integer(1), Value::Integer(2), Value::Null, Value::Integer(3)];
        assert_eq!(count(&values, true).unwrap(), Value::Integer(4));
        assert_eq!(count(&values, false).unwrap(), Value::Integer(3));
    }

    #[test]
    fn test_sum() {
        let values = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        assert_eq!(sum(&values).unwrap(), Value::Integer(6));

        let float_values = vec![Value::Float(1.5), Value::Float(2.5)];
        assert_eq!(sum(&float_values).unwrap(), Value::Float(4.0));
    }

    #[test]
    fn test_avg() {
        let values = vec![Value::Integer(2), Value::Integer(4), Value::Integer(6)];
        assert_eq!(avg(&values).unwrap(), Value::Float(4.0));
    }

    #[test]
    fn test_min_max() {
        let values = vec![Value::Integer(3), Value::Integer(1), Value::Integer(2)];
        assert_eq!(min(&values).unwrap(), Value::Integer(1));
        assert_eq!(max(&values).unwrap(), Value::Integer(3));
    }

    #[test]
    fn test_group_concat() {
        let values = vec![Value::from("a"), Value::from("b"), Value::from("c")];
        assert_eq!(group_concat(&values, None).unwrap(), Value::from("a,b,c"));
        assert_eq!(group_concat(&values, Some("|")).unwrap(), Value::from("a|b|c"));
    }
}
