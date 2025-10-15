// Expression evaluator for WHERE clauses and SELECT columns
use crate::error::{DdbError, Result};
use crate::functions::Value;
use crate::parser::{BinaryOperator, Expression, Literal, UnaryOperator};
use super::row::Row;
use super::system_vars::SystemVariables;

/// Evaluates expressions against row data
pub struct Evaluator {
    system_vars: SystemVariables,
}

impl Evaluator {
    pub fn new() -> Self {
        Evaluator {
            system_vars: SystemVariables::new(),
        }
    }

    /// Evaluate an expression against a row of data
    pub fn evaluate(&self, expr: &Expression, row: &Row) -> Result<Value> {
        match expr {
            Expression::Literal(lit) => Ok(self.evaluate_literal(lit)),
            Expression::Column(name) => self.evaluate_column(name, row),
            Expression::SystemVariable(name) => self.evaluate_system_variable(name),
            Expression::Function { name, args } => self.evaluate_function(name, args, row),
            Expression::BinaryOp { left, op, right } => {
                self.evaluate_binary_op(left, op, right, row)
            }
            Expression::UnaryOp { op, operand } => self.evaluate_unary_op(op, operand, row),
        }
    }

    /// Evaluate a literal value
    fn evaluate_literal(&self, lit: &Literal) -> Value {
        match lit {
            Literal::String(s) => Value::String(s.clone()),
            Literal::Number(n) => Value::Float(*n),
            Literal::Integer(i) => Value::Integer(*i),
            Literal::Boolean(b) => Value::Boolean(*b),
            Literal::Null => Value::Null,
        }
    }

    /// Evaluate a column reference
    fn evaluate_column(&self, name: &str, row: &Row) -> Result<Value> {
        row.get(name)
            .cloned()
            .ok_or_else(|| DdbError::ExecutionError(format!("Column not found: {}", name)))
    }

    /// Evaluate a system variable (@@VARIABLE)
    fn evaluate_system_variable(&self, name: &str) -> Result<Value> {
        self.system_vars
            .get(name)
            .cloned()
            .ok_or_else(|| DdbError::ExecutionError(format!("Unknown system variable: @@{}", name)))
    }

    /// Evaluate a function call
    fn evaluate_function(&self, name: &str, args: &[Expression], row: &Row) -> Result<Value> {
        // Special handling for COUNT(*) - don't evaluate the "*" as a column
        let is_count_star = name.to_uppercase() == "COUNT"
            && args.len() == 1
            && matches!(&args[0], Expression::Column(col) if col == "*");

        // For COUNT(*), we just need to return a placeholder value
        // The actual counting is done by the executor when aggregating
        if is_count_star {
            // For row-level evaluation (non-aggregate context), COUNT(*) = 1
            return Ok(Value::Integer(1));
        }

        // Evaluate all arguments first
        let arg_values: Result<Vec<Value>> = args.iter().map(|arg| self.evaluate(arg, row)).collect();
        let arg_values = arg_values?;

        // Call the appropriate function
        match name.to_uppercase().as_str() {
            // Math functions
            "ABS" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("ABS requires 1 argument".to_string()));
                }
                crate::functions::math::abs(&arg_values[0])
            }
            "CEIL" | "CEILING" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("CEIL requires 1 argument".to_string()));
                }
                crate::functions::math::ceil(&arg_values[0])
            }
            "FLOOR" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("FLOOR requires 1 argument".to_string()));
                }
                crate::functions::math::floor(&arg_values[0])
            }
            "ROUND" => {
                if arg_values.is_empty() || arg_values.len() > 2 {
                    return Err(DdbError::FunctionError("ROUND requires 1-2 arguments".to_string()));
                }
                let decimals = if arg_values.len() == 2 {
                    Some(&arg_values[1])
                } else {
                    None
                };
                crate::functions::math::round(&arg_values[0], decimals)
            }
            "SQRT" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("SQRT requires 1 argument".to_string()));
                }
                crate::functions::math::sqrt(&arg_values[0])
            }
            "POW" | "POWER" => {
                if arg_values.len() != 2 {
                    return Err(DdbError::FunctionError("POW requires 2 arguments".to_string()));
                }
                crate::functions::math::pow(&arg_values[0], &arg_values[1])
            }

            // String functions
            "UPPER" | "UCASE" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("UPPER requires 1 argument".to_string()));
                }
                crate::functions::string::upper(&arg_values[0])
            }
            "LOWER" | "LCASE" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("LOWER requires 1 argument".to_string()));
                }
                crate::functions::string::lower(&arg_values[0])
            }
            "LENGTH" | "CHAR_LENGTH" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("LENGTH requires 1 argument".to_string()));
                }
                crate::functions::string::length(&arg_values[0])
            }
            "TRIM" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("TRIM requires 1 argument".to_string()));
                }
                crate::functions::string::trim(&arg_values[0])
            }
            "CONCAT" => crate::functions::string::concat(&arg_values),
            "SUBSTR" | "SUBSTRING" => {
                if arg_values.len() < 2 || arg_values.len() > 3 {
                    return Err(DdbError::FunctionError("SUBSTR requires 2-3 arguments".to_string()));
                }
                let len = if arg_values.len() == 3 {
                    Some(&arg_values[2])
                } else {
                    None
                };
                crate::functions::string::substr(&arg_values[0], &arg_values[1], len)
            }

            // Type conversion
            "CAST" => {
                if arg_values.len() != 2 {
                    return Err(DdbError::FunctionError("CAST requires 2 arguments (value, type)".to_string()));
                }
                let type_name = arg_values[1].to_string();
                crate::functions::conversion::cast(&arg_values[0], &type_name)
            }
            "ATOF" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("ATOF requires 1 argument".to_string()));
                }
                crate::functions::conversion::atof(&arg_values[0])
            }
            "ATOI" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("ATOI requires 1 argument".to_string()));
                }
                crate::functions::conversion::atoi(&arg_values[0])
            }

            // Date/Time functions
            "NOW" | "CURRENT_TIMESTAMP" => Ok(crate::functions::datetime::now()),
            "CURDATE" | "CURRENT_DATE" => Ok(crate::functions::datetime::curdate()),
            "CURTIME" | "CURRENT_TIME" => Ok(crate::functions::datetime::curtime()),
            "YEAR" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("YEAR requires 1 argument".to_string()));
                }
                crate::functions::datetime::year(&arg_values[0])
            }
            "MONTH" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("MONTH requires 1 argument".to_string()));
                }
                crate::functions::datetime::month(&arg_values[0])
            }
            "DAY" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("DAY requires 1 argument".to_string()));
                }
                crate::functions::datetime::day(&arg_values[0])
            }
            "DATEDIFF" => {
                if arg_values.len() != 2 {
                    return Err(DdbError::FunctionError("DATEDIFF requires 2 arguments".to_string()));
                }
                crate::functions::datetime::datediff(&arg_values[0], &arg_values[1])
            }
            "DATEADD" => {
                if arg_values.len() != 3 {
                    return Err(DdbError::FunctionError("DATEADD requires 3 arguments (date, interval, unit)".to_string()));
                }
                let unit = arg_values[2].to_string();
                crate::functions::datetime::dateadd(&arg_values[0], &arg_values[1], &unit)
            }

            // Conditional functions
            "IF" => {
                if arg_values.len() != 3 {
                    return Err(DdbError::FunctionError("IF requires 3 arguments".to_string()));
                }
                crate::functions::conditional::if_fn(&arg_values[0], &arg_values[1], &arg_values[2])
            }
            "IFNULL" => {
                if arg_values.len() != 2 {
                    return Err(DdbError::FunctionError("IFNULL requires 2 arguments".to_string()));
                }
                crate::functions::conditional::ifnull(&arg_values[0], &arg_values[1])
            }
            "COALESCE" => crate::functions::conditional::coalesce(&arg_values),

            // System functions
            "VERSION" => Ok(crate::functions::system::version()),
            "DATABASE" => Ok(crate::functions::system::database(None)),
            "UUID" => Ok(crate::functions::system::uuid()),

            // Utility functions
            "HASH" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("HASH requires 1 argument".to_string()));
                }
                crate::functions::utility::hash(&arg_values[0])
            }
            "BASE64_ENCODE" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("BASE64_ENCODE requires 1 argument".to_string()));
                }
                crate::functions::utility::base64_encode(&arg_values[0])
            }
            "BASE64_DECODE" => {
                if arg_values.len() != 1 {
                    return Err(DdbError::FunctionError("BASE64_DECODE requires 1 argument".to_string()));
                }
                crate::functions::utility::base64_decode(&arg_values[0])
            }

            _ => Err(DdbError::FunctionError(format!("Unknown function: {}", name))),
        }
    }

    /// Evaluate a binary operation
    fn evaluate_binary_op(
        &self,
        left: &Expression,
        op: &BinaryOperator,
        right: &Expression,
        row: &Row,
    ) -> Result<Value> {
        let left_val = self.evaluate(left, row)?;
        let right_val = self.evaluate(right, row)?;

        match op {
            BinaryOperator::Equal => Ok(Value::Boolean(self.compare_values(&left_val, &right_val) == std::cmp::Ordering::Equal)),
            BinaryOperator::NotEqual => Ok(Value::Boolean(self.compare_values(&left_val, &right_val) != std::cmp::Ordering::Equal)),
            BinaryOperator::GreaterThan => Ok(Value::Boolean(self.compare_values(&left_val, &right_val) == std::cmp::Ordering::Greater)),
            BinaryOperator::GreaterEqual => {
                let cmp = self.compare_values(&left_val, &right_val);
                Ok(Value::Boolean(cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal))
            }
            BinaryOperator::LessThan => Ok(Value::Boolean(self.compare_values(&left_val, &right_val) == std::cmp::Ordering::Less)),
            BinaryOperator::LessEqual => {
                let cmp = self.compare_values(&left_val, &right_val);
                Ok(Value::Boolean(cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal))
            }
            BinaryOperator::Like => self.evaluate_like(&left_val, &right_val),
            BinaryOperator::And => {
                let left_bool = self.to_boolean(&left_val)?;
                let right_bool = self.to_boolean(&right_val)?;
                Ok(Value::Boolean(left_bool && right_bool))
            }
            BinaryOperator::Or => {
                let left_bool = self.to_boolean(&left_val)?;
                let right_bool = self.to_boolean(&right_val)?;
                Ok(Value::Boolean(left_bool || right_bool))
            }
        }
    }

    /// Evaluate a unary operation
    fn evaluate_unary_op(
        &self,
        op: &UnaryOperator,
        operand: &Expression,
        row: &Row,
    ) -> Result<Value> {
        let val = self.evaluate(operand, row)?;

        match op {
            UnaryOperator::Not => {
                let bool_val = self.to_boolean(&val)?;
                Ok(Value::Boolean(!bool_val))
            }
            UnaryOperator::IsNull => Ok(Value::Boolean(val.is_null())),
        }
    }

    /// Compare two values
    fn compare_values(&self, left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Integer(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }

    /// Evaluate LIKE pattern matching
    /// Optimized to avoid regex compilation for simple patterns
    fn evaluate_like(&self, value: &Value, pattern: &Value) -> Result<Value> {
        let val_str = value.to_string();
        let pattern_str = pattern.to_string();

        // Fast path for common pattern types (avoids expensive regex compilation)
        let matched = if !pattern_str.contains('_') {
            // No single-char wildcards, only % wildcards
            if pattern_str.starts_with('%') && pattern_str.ends_with('%') {
                // %text% -> contains
                let search = &pattern_str[1..pattern_str.len()-1];
                if !search.contains('%') {
                    return Ok(Value::Boolean(val_str.contains(search)));
                }
            } else if pattern_str.starts_with('%') {
                // %text -> ends_with
                let suffix = &pattern_str[1..];
                if !suffix.contains('%') {
                    return Ok(Value::Boolean(val_str.ends_with(suffix)));
                }
            } else if pattern_str.ends_with('%') {
                // text% -> starts_with
                let prefix = &pattern_str[..pattern_str.len()-1];
                if !prefix.contains('%') {
                    return Ok(Value::Boolean(val_str.starts_with(prefix)));
                }
            } else if !pattern_str.contains('%') {
                // No wildcards at all -> exact match
                return Ok(Value::Boolean(val_str == pattern_str));
            }

            // Complex pattern with multiple % - fall through to regex
            self.evaluate_like_regex(&val_str, &pattern_str)?
        } else {
            // Pattern contains _ wildcard - use regex
            self.evaluate_like_regex(&val_str, &pattern_str)?
        };

        Ok(Value::Boolean(matched))
    }

    /// Evaluate LIKE using regex (for complex patterns)
    #[inline]
    fn evaluate_like_regex(&self, val_str: &str, pattern_str: &str) -> Result<bool> {
        // Escape regex special chars, but keep SQL wildcards
        let mut regex_pattern = String::with_capacity(pattern_str.len() * 2);

        for ch in pattern_str.chars() {
            match ch {
                '%' => regex_pattern.push_str(".*"),
                '_' => regex_pattern.push('.'),
                // Escape regex special characters
                '.' | '^' | '$' | '*' | '+' | '?' | '(' | ')' | '[' | ']' |
                '{' | '}' | '\\' | '|' => {
                    regex_pattern.push('\\');
                    regex_pattern.push(ch);
                }
                _ => regex_pattern.push(ch),
            }
        }

        let regex = regex::Regex::new(&format!("^{}$", regex_pattern))
            .map_err(|e| DdbError::ExecutionError(format!("Invalid LIKE pattern: {}", e)))?;

        Ok(regex.is_match(val_str))
    }

    /// Convert value to boolean
    fn to_boolean(&self, value: &Value) -> Result<bool> {
        match value {
            Value::Boolean(b) => Ok(*b),
            Value::Integer(i) => Ok(*i != 0),
            Value::Float(f) => Ok(*f != 0.0),
            Value::String(s) => Ok(!s.is_empty()),
            Value::Null => Ok(false),
            _ => Ok(true),
        }
    }
}

impl Default for Evaluator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Literal;

    #[test]
    fn test_evaluate_literal() {
        let evaluator = Evaluator::new();
        let row = Row::new();

        let expr = Expression::Literal(Literal::Integer(42));
        let result = evaluator.evaluate(&expr, &row).unwrap();
        assert_eq!(result, Value::Integer(42));

        let expr = Expression::Literal(Literal::String("hello".to_string()));
        let result = evaluator.evaluate(&expr, &row).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[test]
    fn test_evaluate_column() {
        let evaluator = Evaluator::new();
        let mut row = Row::new();
        row.set("age", Value::Integer(25));

        let expr = Expression::Column("age".to_string());
        let result = evaluator.evaluate(&expr, &row).unwrap();
        assert_eq!(result, Value::Integer(25));
    }

    #[test]
    fn test_evaluate_binary_op() {
        let evaluator = Evaluator::new();
        let mut row = Row::new();
        row.set("age", Value::Integer(25));

        // age > 18
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(Literal::Integer(18))),
        };

        let result = evaluator.evaluate(&expr, &row).unwrap();
        assert_eq!(result, Value::Boolean(true));
    }

    #[test]
    fn test_evaluate_function() {
        let evaluator = Evaluator::new();
        let mut row = Row::new();
        row.set("name", Value::String("john".to_string()));

        // UPPER(name)
        let expr = Expression::Function {
            name: "UPPER".to_string(),
            args: vec![Expression::Column("name".to_string())],
        };

        let result = evaluator.evaluate(&expr, &row).unwrap();
        assert_eq!(result, Value::String("JOHN".to_string()));
    }

    #[test]
    fn test_evaluate_like() {
        let evaluator = Evaluator::new();
        let row = Row::new();

        let value = Value::String("hello world".to_string());
        let pattern = Value::String("hello%".to_string());

        let result = evaluator.evaluate_like(&value, &pattern).unwrap();
        assert_eq!(result, Value::Boolean(true));

        let pattern = Value::String("%world".to_string());
        let result = evaluator.evaluate_like(&value, &pattern).unwrap();
        assert_eq!(result, Value::Boolean(true));

        // _ is a single-character wildcard in SQL LIKE
        let pattern = Value::String("hello_world".to_string());
        let result = evaluator.evaluate_like(&value, &pattern).unwrap();
        assert_eq!(result, Value::Boolean(true)); // _ matches the space character
    }
}
