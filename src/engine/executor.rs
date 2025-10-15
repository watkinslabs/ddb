// Query executor for SELECT statements
use crate::config::Table;
use crate::engine::{evaluator::Evaluator, row::Row};
use crate::error::{DdbError, Result};
use crate::file_io::CsvReader;
use crate::functions::Value;
use crate::parser::{OrderDirection, SelectColumn, SelectStatement};

pub struct QueryExecutor {
    evaluator: Evaluator,
}

impl QueryExecutor {
    pub fn new() -> Self {
        QueryExecutor {
            evaluator: Evaluator::new(),
        }
    }

    /// Execute a SELECT statement
    pub fn execute_select(&self, stmt: &SelectStatement, table: &Table) -> Result<Vec<Row>> {
        // Check if query uses aggregate functions
        let has_aggregates = self.has_aggregate_functions(&stmt.columns);

        if has_aggregates {
            return self.execute_aggregate_select(stmt, table);
        }

        // Read data from file
        let mut reader = CsvReader::new(&table.data_file, table.field_delimiter, true)?;

        let mut rows = Vec::new();

        // Filter rows based on WHERE clause
        while let Some(row) = reader.next_row()? {
            if let Some(where_expr) = &stmt.where_clause {
                let result = self.evaluator.evaluate(where_expr, &row)?;
                let matches = self.to_boolean(&result)?;
                if !matches {
                    continue; // Skip this row
                }
            }

            // Project columns (SELECT clause)
            let projected_row = self.project_row(&row, &stmt.columns)?;
            rows.push(projected_row);
        }

        // Apply DISTINCT
        if stmt.distinct {
            rows = self.deduplicate_rows(rows);
        }

        // Apply ORDER BY
        if !stmt.order_by.is_empty() {
            rows = self.sort_rows(rows, &stmt.order_by)?;
        }

        // Apply LIMIT
        if let Some(limit) = &stmt.limit {
            let start = limit.offset.unwrap_or(0);
            rows = rows.into_iter().skip(start).take(limit.count).collect();
        }

        Ok(rows)
    }

    /// Check if SELECT columns contain aggregate functions
    fn has_aggregate_functions(&self, columns: &[SelectColumn]) -> bool {
        columns.iter().any(|col| match col {
            SelectColumn::Function { name, .. } => {
                matches!(
                    name.to_uppercase().as_str(),
                    "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE" | "GROUP_CONCAT"
                )
            }
            _ => false,
        })
    }

    /// Execute SELECT with aggregate functions
    fn execute_aggregate_select(&self, stmt: &SelectStatement, table: &Table) -> Result<Vec<Row>> {
        // Read and filter all rows first
        let mut reader = CsvReader::new(&table.data_file, table.field_delimiter, true)?;
        let mut all_rows = Vec::new();

        while let Some(row) = reader.next_row()? {
            if let Some(where_expr) = &stmt.where_clause {
                let result = self.evaluator.evaluate(where_expr, &row)?;
                let matches = self.to_boolean(&result)?;
                if !matches {
                    continue;
                }
            }
            all_rows.push(row);
        }

        // Build result row with aggregates
        let mut result_row = Row::new();

        for col_spec in &stmt.columns {
            match col_spec {
                SelectColumn::Function { name, args, alias } => {
                    // Special handling for COUNT(*)
                    let aggregate_result = if self.is_count_star(name, args) {
                        // COUNT(*) - return row count directly
                        Value::Integer(all_rows.len() as i64)
                    } else {
                        // Collect values for this column
                        let values: Result<Vec<Value>> = all_rows
                            .iter()
                            .map(|row| {
                                if args.len() == 1 {
                                    self.evaluator.evaluate(&args[0], row)
                                } else {
                                    Err(DdbError::ExecutionError(
                                        "Aggregate functions require exactly one argument".to_string(),
                                    ))
                                }
                            })
                            .collect();

                        let values = values?;

                        // Apply aggregate function
                        match name.to_uppercase().as_str() {
                            "COUNT" => crate::functions::aggregate::count(&values, true)?,
                            "SUM" => crate::functions::aggregate::sum(&values)?,
                            "AVG" => crate::functions::aggregate::avg(&values)?,
                            "MIN" => crate::functions::aggregate::min(&values)?,
                            "MAX" => crate::functions::aggregate::max(&values)?,
                            "STDDEV" | "STDDEV_POP" => crate::functions::aggregate::stddev_pop(&values)?,
                            "VARIANCE" | "VAR_POP" => crate::functions::aggregate::var_pop(&values)?,
                            _ => {
                                return Err(DdbError::ExecutionError(format!(
                                    "Unknown aggregate function: {}",
                                    name
                                )))
                            }
                        }
                    };

                    let default_name = format!("{}(...)", name);
                    let output_name = alias.as_ref().unwrap_or(&default_name);
                    result_row.set(output_name, aggregate_result);
                }
                SelectColumn::Column { name, alias } => {
                    // Non-aggregate column in aggregate query - use first value
                    // (This is technically not standard SQL without GROUP BY, but we'll allow it)
                    let value = all_rows
                        .first()
                        .and_then(|row| row.get(name).cloned())
                        .unwrap_or(Value::Null);
                    let output_name = alias.as_ref().unwrap_or(name);
                    result_row.set(output_name, value);
                }
                SelectColumn::Wildcard => {
                    return Err(DdbError::ExecutionError(
                        "Cannot use * with aggregate functions (use COUNT(*) instead)".to_string(),
                    ));
                }
            }
        }

        Ok(vec![result_row])
    }

    /// Check if this is COUNT(*)
    fn is_count_star(&self, name: &str, args: &[crate::parser::Expression]) -> bool {
        name.to_uppercase() == "COUNT"
            && args.len() == 1
            && matches!(&args[0], crate::parser::Expression::Column(col) if col == "*")
    }

    /// Project columns from a row based on SELECT clause
    fn project_row(&self, row: &Row, columns: &[SelectColumn]) -> Result<Row> {
        let mut new_row = Row::new();

        for col_spec in columns {
            match col_spec {
                SelectColumn::Wildcard => {
                    // Include all columns
                    for (name, value) in row.as_map() {
                        new_row.set(name, value.clone());
                    }
                }
                SelectColumn::Column { name, alias } => {
                    // Check if this is a system variable (@@VARIABLE)
                    let value = if name.starts_with("@@") {
                        let var_name = &name[2..];
                        let var_expr = crate::parser::Expression::SystemVariable(var_name.to_string());
                        self.evaluator.evaluate(&var_expr, row)?
                    } else {
                        row.get(name)
                            .cloned()
                            .ok_or_else(|| DdbError::ColumnNotFound(name.clone()))?
                    };
                    let output_name = alias.as_ref().unwrap_or(name);
                    new_row.set(output_name, value);
                }
                SelectColumn::Function { name, args, alias } => {
                    // Evaluate function
                    let func_expr = crate::parser::Expression::Function {
                        name: name.clone(),
                        args: args.clone(),
                    };
                    let result = self.evaluator.evaluate(&func_expr, row)?;

                    let default_name = format!("{}(...)", name);
                    let output_name = alias.as_ref().unwrap_or(&default_name);
                    new_row.set(output_name, result);
                }
            }
        }

        Ok(new_row)
    }

    /// Remove duplicate rows
    fn deduplicate_rows(&self, mut rows: Vec<Row>) -> Vec<Row> {
        let mut seen = std::collections::HashSet::new();
        rows.retain(|row| {
            let key = self.row_to_key(row);
            seen.insert(key)
        });
        rows
    }

    /// Convert row to a hashable key for deduplication
    fn row_to_key(&self, row: &Row) -> Vec<String> {
        let mut keys: Vec<_> = row.column_names();
        keys.sort();
        keys.iter()
            .map(|name| {
                row.get(name)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            })
            .collect()
    }

    /// Sort rows based on ORDER BY clause
    fn sort_rows(
        &self,
        mut rows: Vec<Row>,
        order_by: &[crate::parser::OrderByColumn],
    ) -> Result<Vec<Row>> {
        rows.sort_by(|a, b| {
            for order_col in order_by {
                let a_val = a.get(&order_col.column).cloned().unwrap_or(Value::Null);
                let b_val = b.get(&order_col.column).cloned().unwrap_or(Value::Null);

                let cmp = self.compare_values(&a_val, &b_val);

                let cmp = match order_col.direction {
                    OrderDirection::Asc => cmp,
                    OrderDirection::Desc => cmp.reverse(),
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Compare two values for sorting
    fn compare_values(&self, left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Integer(a), Value::Float(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (Value::Float(a), Value::Integer(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
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

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{BinaryOperator, Expression, Literal};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_table() -> (Table, NamedTempFile) {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "id,name,age").unwrap();
        writeln!(file, "1,Alice,30").unwrap();
        writeln!(file, "2,Bob,25").unwrap();
        writeln!(file, "3,Charlie,35").unwrap();
        file.flush().unwrap();

        let table = Table {
            name: "users".to_string(),
            database: "test".to_string(),
            data_file: file.path().to_string_lossy().to_string(),
            columns: vec![],
            field_delimiter: ',',
            data_starts_on: 0,
            comment_char: None,
        };

        (table, file)
    }

    #[test]
    fn test_execute_select_all() {
        let (table, _file) = create_test_table();
        let executor = QueryExecutor::new();

        let stmt = SelectStatement {
            distinct: false,
            columns: vec![SelectColumn::Wildcard],
            from: Some("users".to_string()),
            where_clause: None,
            order_by: vec![],
            limit: None,
        };

        let results = executor.execute_select(&stmt, &table).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_execute_select_with_where() {
        let (table, _file) = create_test_table();
        let executor = QueryExecutor::new();

        // WHERE age > 25
        let where_expr = Expression::BinaryOp {
            left: Box::new(Expression::Column("age".to_string())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(Literal::Integer(25))),
        };

        let stmt = SelectStatement {
            distinct: false,
            columns: vec![SelectColumn::Wildcard],
            from: Some("users".to_string()),
            where_clause: Some(where_expr),
            order_by: vec![],
            limit: None,
        };

        let results = executor.execute_select(&stmt, &table).unwrap();
        assert_eq!(results.len(), 2); // Alice (30) and Charlie (35)
    }

    #[test]
    fn test_execute_select_with_columns() {
        let (table, _file) = create_test_table();
        let executor = QueryExecutor::new();

        let stmt = SelectStatement {
            distinct: false,
            columns: vec![
                SelectColumn::Column {
                    name: "name".to_string(),
                    alias: None,
                },
                SelectColumn::Column {
                    name: "age".to_string(),
                    alias: None,
                },
            ],
            from: Some("users".to_string()),
            where_clause: None,
            order_by: vec![],
            limit: None,
        };

        let results = executor.execute_select(&stmt, &table).unwrap();
        assert_eq!(results.len(), 3);

        // Check that only selected columns are present
        let row = &results[0];
        assert!(row.get("name").is_some());
        assert!(row.get("age").is_some());
        // id should not be present
        assert!(row.get("id").is_none());
    }

    #[test]
    fn test_execute_select_with_limit() {
        let (table, _file) = create_test_table();
        let executor = QueryExecutor::new();

        let stmt = SelectStatement {
            distinct: false,
            columns: vec![SelectColumn::Wildcard],
            from: Some("users".to_string()),
            where_clause: None,
            order_by: vec![],
            limit: Some(crate::parser::Limit {
                offset: None,
                count: 2,
            }),
        };

        let results = executor.execute_select(&stmt, &table).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_execute_select_with_order_by() {
        let (table, _file) = create_test_table();
        let executor = QueryExecutor::new();

        let stmt = SelectStatement {
            distinct: false,
            columns: vec![SelectColumn::Wildcard],
            from: Some("users".to_string()),
            where_clause: None,
            order_by: vec![crate::parser::OrderByColumn {
                column: "age".to_string(),
                direction: OrderDirection::Desc,
            }],
            limit: None,
        };

        let results = executor.execute_select(&stmt, &table).unwrap();
        assert_eq!(results.len(), 3);

        // Check order: Charlie (35), Alice (30), Bob (25)
        assert_eq!(
            results[0].get("name"),
            Some(&Value::String("Charlie".to_string()))
        );
        assert_eq!(
            results[2].get("name"),
            Some(&Value::String("Bob".to_string()))
        );
    }
}
