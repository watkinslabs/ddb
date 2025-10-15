// Query executor for SELECT, INSERT, UPDATE, DELETE statements
use crate::config::{Table, TableCatalog};
use crate::engine::{evaluator::Evaluator, row::Row};
use crate::error::{DdbError, Result};
use crate::file_io::{CsvReader, FileLock};
use crate::functions::Value;
use crate::parser::{DeleteStatement, InsertStatement, JoinClause, JoinType, OrderDirection, SelectColumn, SelectStatement, UpdateStatement, UpsertStatement};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};

pub struct QueryExecutor {
    evaluator: Evaluator,
}

impl QueryExecutor {
    pub fn new() -> Self {
        QueryExecutor {
            evaluator: Evaluator::new(),
        }
    }

    /// Execute a SELECT statement with optional catalog for JOIN support
    pub fn execute_select(&self, stmt: &SelectStatement, table: &Table) -> Result<Vec<Row>> {
        self.execute_select_with_catalog(stmt, table, None)
    }

    /// Execute a SELECT statement with catalog support for JOINs
    pub fn execute_select_with_catalog(
        &self,
        stmt: &SelectStatement,
        table: &Table,
        catalog: Option<&TableCatalog>,
    ) -> Result<Vec<Row>> {
        // Check if query uses aggregate functions or GROUP BY
        let has_aggregates = self.has_aggregate_functions(&stmt.columns);
        let has_group_by = !stmt.group_by.is_empty();

        if has_aggregates || has_group_by {
            return self.execute_aggregate_select_with_catalog(stmt, table, catalog);
        }

        // Read data from main table
        let mut reader = CsvReader::new(&table.data_file, table.field_delimiter, true)?;
        let mut rows = Vec::new();
        while let Some(row) = reader.next_row()? {
            rows.push(row);
        }

        // Process JOINs if any
        if !stmt.joins.is_empty() {
            rows = self.execute_joins(rows, &stmt.joins, catalog)?;
        }

        // Filter rows based on WHERE clause
        rows.retain(|row| {
            if let Some(where_expr) = &stmt.where_clause {
                if let Ok(result) = self.evaluator.evaluate(where_expr, row) {
                    if let Ok(matches) = self.to_boolean(&result) {
                        return matches;
                    }
                }
            }
            stmt.where_clause.is_none()
        });

        // Project columns (SELECT clause)
        rows = rows
            .into_iter()
            .filter_map(|row| self.project_row(&row, &stmt.columns).ok())
            .collect();

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

    /// Execute JOINs on the result set
    fn execute_joins(
        &self,
        left_rows: Vec<Row>,
        joins: &[JoinClause],
        catalog: Option<&TableCatalog>,
    ) -> Result<Vec<Row>> {
        let mut result = left_rows;

        for join in joins {
            // Get the table to join with
            let catalog = catalog.ok_or_else(|| {
                DdbError::ExecutionError(
                    "JOIN requires table catalog. Use execute_select_with_catalog.".to_string(),
                )
            })?;

            let right_table = catalog.get_table(&join.table).ok_or_else(|| {
                DdbError::ExecutionError(format!("Table '{}' not found for JOIN", join.table))
            })?;

            // Read all rows from the right table
            let mut right_reader =
                CsvReader::new(&right_table.data_file, right_table.field_delimiter, true)?;
            let mut right_rows = Vec::new();
            while let Some(row) = right_reader.next_row()? {
                right_rows.push(row);
            }

            // Perform the join
            result = self.perform_join(result, right_rows, join)?;
        }

        Ok(result)
    }

    /// Perform a single join operation
    fn perform_join(
        &self,
        left_rows: Vec<Row>,
        right_rows: Vec<Row>,
        join: &JoinClause,
    ) -> Result<Vec<Row>> {
        let mut joined_rows = Vec::new();

        match join.join_type {
            JoinType::Inner => {
                // INNER JOIN: Only rows where ON condition matches
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        let combined = self.merge_rows(left_row, right_row);
                        let result = self.evaluator.evaluate(&join.on_condition, &combined)?;
                        if self.to_boolean(&result)? {
                            joined_rows.push(combined);
                        }
                    }
                }
            }
            JoinType::Left => {
                // LEFT JOIN: All rows from left, with NULLs for right if no match
                for left_row in &left_rows {
                    let mut matched = false;
                    for right_row in &right_rows {
                        let combined = self.merge_rows(left_row, right_row);
                        let result = self.evaluator.evaluate(&join.on_condition, &combined)?;
                        if self.to_boolean(&result)? {
                            joined_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        // No match found, add left row with NULLs for right columns
                        let row_with_nulls = left_row.clone();
                        // Add NULL values for right table columns
                        // (In practice, the projection step will handle missing columns)
                        joined_rows.push(row_with_nulls);
                    }
                }
            }
            JoinType::Right => {
                // RIGHT JOIN: All rows from right, with NULLs for left if no match
                for right_row in &right_rows {
                    let mut matched = false;
                    for left_row in &left_rows {
                        let combined = self.merge_rows(left_row, right_row);
                        let result = self.evaluator.evaluate(&join.on_condition, &combined)?;
                        if self.to_boolean(&result)? {
                            joined_rows.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        // No match found, add right row with NULLs for left columns
                        joined_rows.push(right_row.clone());
                    }
                }
            }
            JoinType::Full => {
                // FULL OUTER JOIN: All rows from both tables
                let mut matched_left = vec![false; left_rows.len()];
                let mut matched_right = vec![false; right_rows.len()];

                for (left_idx, left_row) in left_rows.iter().enumerate() {
                    for (right_idx, right_row) in right_rows.iter().enumerate() {
                        let combined = self.merge_rows(left_row, right_row);
                        let result = self.evaluator.evaluate(&join.on_condition, &combined)?;
                        if self.to_boolean(&result)? {
                            joined_rows.push(combined);
                            matched_left[left_idx] = true;
                            matched_right[right_idx] = true;
                        }
                    }
                }

                // Add unmatched left rows
                for (idx, left_row) in left_rows.iter().enumerate() {
                    if !matched_left[idx] {
                        joined_rows.push(left_row.clone());
                    }
                }

                // Add unmatched right rows
                for (idx, right_row) in right_rows.iter().enumerate() {
                    if !matched_right[idx] {
                        joined_rows.push(right_row.clone());
                    }
                }
            }
        }

        Ok(joined_rows)
    }

    /// Merge two rows into one (for JOINs)
    fn merge_rows(&self, left: &Row, right: &Row) -> Row {
        let mut merged = left.clone();
        for (name, value) in right.as_map() {
            merged.set(name, value.clone());
        }
        merged
    }

    /// Execute SELECT with aggregate functions
    #[allow(dead_code)]
    fn execute_aggregate_select(&self, stmt: &SelectStatement, table: &Table) -> Result<Vec<Row>> {
        self.execute_aggregate_select_with_catalog(stmt, table, None)
    }

    /// Execute SELECT with aggregate functions and catalog support
    fn execute_aggregate_select_with_catalog(
        &self,
        stmt: &SelectStatement,
        table: &Table,
        catalog: Option<&TableCatalog>,
    ) -> Result<Vec<Row>> {
        // Read all rows from main table
        let mut reader = CsvReader::new(&table.data_file, table.field_delimiter, true)?;
        let mut all_rows = Vec::new();
        while let Some(row) = reader.next_row()? {
            all_rows.push(row);
        }

        // Process JOINs if any
        if !stmt.joins.is_empty() {
            all_rows = self.execute_joins(all_rows, &stmt.joins, catalog)?;
        }

        // Filter rows based on WHERE clause
        all_rows.retain(|row| {
            if let Some(where_expr) = &stmt.where_clause {
                if let Ok(result) = self.evaluator.evaluate(where_expr, row) {
                    if let Ok(matches) = self.to_boolean(&result) {
                        return matches;
                    }
                }
                return false;
            }
            true
        });

        // If GROUP BY is present, group rows and aggregate
        if !stmt.group_by.is_empty() {
            return self.execute_group_by_aggregation(all_rows, stmt);
        }

        // No GROUP BY - aggregate all rows into one result
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

    /// Execute GROUP BY aggregation
    fn execute_group_by_aggregation(
        &self,
        all_rows: Vec<Row>,
        stmt: &SelectStatement,
    ) -> Result<Vec<Row>> {
        use std::collections::HashMap;

        // Group rows by GROUP BY columns
        let mut groups: HashMap<Vec<String>, Vec<Row>> = HashMap::new();

        for row in all_rows {
            // Build group key from GROUP BY columns
            let mut group_key = Vec::new();
            for col_name in &stmt.group_by {
                let value = row.get(col_name)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string());
                group_key.push(value);
            }

            groups.entry(group_key).or_insert_with(Vec::new).push(row);
        }

        // Build result rows - one per group
        let mut result_rows = Vec::new();

        for (_group_key, group_rows) in groups {
            let mut result_row = Row::new();

            // Add GROUP BY columns to result
            for (_idx, col_name) in stmt.group_by.iter().enumerate() {
                if let Some(value) = group_rows.first().and_then(|row| row.get(col_name)) {
                    result_row.set(col_name, value.clone());
                }
            }

            // Compute aggregates for this group
            for col_spec in &stmt.columns {
                match col_spec {
                    SelectColumn::Function { name, args, alias } => {
                        let aggregate_result = if self.is_count_star(name, args) {
                            Value::Integer(group_rows.len() as i64)
                        } else {
                            // Collect values for this column in this group
                            let values: Result<Vec<Value>> = group_rows
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
                        // Non-aggregate column - should be in GROUP BY
                        if !stmt.group_by.contains(name) {
                            return Err(DdbError::ExecutionError(format!(
                                "Column '{}' must appear in GROUP BY clause or be used in an aggregate function",
                                name
                            )));
                        }
                        // Already added above in GROUP BY section
                        if let Some(alias_name) = alias {
                            if let Some(value) = result_row.get(name) {
                                result_row.set(alias_name, value.clone());
                            }
                        }
                    }
                    SelectColumn::Wildcard => {
                        return Err(DdbError::ExecutionError(
                            "Cannot use * with GROUP BY (select specific columns)".to_string(),
                        ));
                    }
                }
            }

            // Apply HAVING clause to filter groups
            if let Some(having_expr) = &stmt.having {
                let result = self.evaluator.evaluate(having_expr, &result_row)?;
                let matches = self.to_boolean(&result)?;
                if !matches {
                    continue; // Skip this group
                }
            }

            result_rows.push(result_row);
        }

        Ok(result_rows)
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

    /// Execute an INSERT statement
    pub fn execute_insert(&self, stmt: &InsertStatement, table: &Table) -> Result<usize> {
        // Acquire exclusive lock on the file
        let lock = FileLock::new(&table.data_file)?;
        lock.lock_exclusive()?;

        // Open file in append mode
        let file = OpenOptions::new()
            .append(true)
            .open(&table.data_file)?;

        let mut writer = BufWriter::new(file);
        let mut rows_inserted = 0;

        // Get column names from table or from INSERT statement
        let _column_names = if stmt.columns.is_empty() {
            // No columns specified - use table columns
            table.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>()
        } else {
            stmt.columns.clone()
        };

        // Process each row of values
        for value_row in &stmt.values {
            if !stmt.columns.is_empty() && value_row.len() != stmt.columns.len() {
                return Err(DdbError::ExecutionError(format!(
                    "Column count ({}) doesn't match value count ({})",
                    stmt.columns.len(),
                    value_row.len()
                )));
            }

            // Build CSV row
            let mut csv_row = Vec::new();
            for expr in value_row {
                // Evaluate expression to get value
                let empty_row = Row::new();
                let value = self.evaluator.evaluate(expr, &empty_row)?;
                csv_row.push(self.value_to_csv_string(&value));
            }

            // Write row to file
            writeln!(writer, "{}", csv_row.join(&table.field_delimiter.to_string()))?;

            rows_inserted += 1;
        }

        writer.flush()?;

        // Lock is automatically released when dropped
        Ok(rows_inserted)
    }

    /// Execute an UPSERT statement
    /// If a row with the key_column value exists, update it; otherwise insert it
    pub fn execute_upsert(&self, stmt: &UpsertStatement, table: &Table) -> Result<(usize, usize)> {
        // Acquire exclusive lock on the file
        let lock = FileLock::new(&table.data_file)?;
        lock.lock_exclusive()?;

        // Read all rows
        let file = File::open(&table.data_file)?;
        let reader = BufReader::new(file);
        let mut lines: Vec<String> = reader.lines().collect::<std::result::Result<Vec<_>, _>>()?;

        if lines.is_empty() {
            return Err(DdbError::ExecutionError("Cannot UPSERT into empty file".to_string()));
        }

        // Parse header
        let header = lines[0].clone();
        let column_names: Vec<String> = header
            .split(table.field_delimiter)
            .map(|s| s.trim().to_string())
            .collect();

        // Verify key column exists
        let key_col_idx = column_names
            .iter()
            .position(|c| c == &stmt.key_column)
            .ok_or_else(|| {
                DdbError::ExecutionError(format!(
                    "Key column '{}' not found in table",
                    stmt.key_column
                ))
            })?;

        // Verify stmt columns match
        if stmt.columns.len() != column_names.len() {
            return Err(DdbError::ExecutionError(format!(
                "Column count mismatch: UPSERT specifies {} columns, table has {}",
                stmt.columns.len(),
                column_names.len()
            )));
        }

        let mut rows_inserted = 0;
        let mut rows_updated = 0;

        // Process each value row in the UPSERT
        for value_row in &stmt.values {
            if value_row.len() != stmt.columns.len() {
                return Err(DdbError::ExecutionError(format!(
                    "Value count ({}) doesn't match column count ({})",
                    value_row.len(),
                    stmt.columns.len()
                )));
            }

            // Evaluate expressions to get values
            let empty_row = Row::new();
            let mut values = Vec::new();
            for expr in value_row {
                let value = self.evaluator.evaluate(expr, &empty_row)?;
                values.push(value);
            }

            // Get key value (value corresponding to key_column)
            let key_col_pos = stmt
                .columns
                .iter()
                .position(|c| c == &stmt.key_column)
                .ok_or_else(|| {
                    DdbError::ExecutionError(format!(
                        "Key column '{}' not found in UPSERT columns",
                        stmt.key_column
                    ))
                })?;
            let key_value_str = self.value_to_csv_string(&values[key_col_pos]);

            // Search for existing row with this key
            let mut found_row_idx = None;
            for i in (table.data_starts_on as usize + 1)..lines.len() {
                let line = &lines[i];
                if line.trim().is_empty() {
                    continue;
                }

                let row_values: Vec<String> = line
                    .split(table.field_delimiter)
                    .map(|s| s.trim().to_string())
                    .collect();

                if key_col_idx < row_values.len() && row_values[key_col_idx] == key_value_str {
                    found_row_idx = Some(i);
                    break;
                }
            }

            // Build CSV row
            let csv_row: Vec<String> = values.iter().map(|v| self.value_to_csv_string(v)).collect();
            let csv_line = csv_row.join(&table.field_delimiter.to_string());

            if let Some(idx) = found_row_idx {
                // Update existing row
                lines[idx] = csv_line;
                rows_updated += 1;
            } else {
                // Insert new row
                lines.push(csv_line);
                rows_inserted += 1;
            }
        }

        // Rewrite file
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&table.data_file)?;

        let mut writer = BufWriter::new(file);
        for line in &lines {
            writeln!(writer, "{}", line)?;
        }
        writer.flush()?;

        Ok((rows_inserted, rows_updated))
    }

    /// Execute an UPDATE statement
    pub fn execute_update(&self, stmt: &UpdateStatement, table: &Table) -> Result<usize> {
        // Acquire exclusive lock on the file
        let lock = FileLock::new(&table.data_file)?;
        lock.lock_exclusive()?;

        // Read all rows
        let file = File::open(&table.data_file)?;
        let reader = BufReader::new(file);
        let mut lines: Vec<String> = reader.lines().collect::<std::result::Result<Vec<_>, _>>()?;

        if lines.is_empty() {
            return Ok(0);
        }

        // Parse header
        let header = lines[0].clone();
        let column_names: Vec<String> = header
            .split(table.field_delimiter)
            .map(|s| s.trim().to_string())
            .collect();

        let mut rows_updated = 0;

        // Process data rows
        for i in (table.data_starts_on as usize + 1)..lines.len() {
            let line = &lines[i];

            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Parse row
            let values: Vec<String> = line
                .split(table.field_delimiter)
                .map(|s| s.trim().to_string())
                .collect();

            let mut row = Row::new();
            for (idx, col_name) in column_names.iter().enumerate() {
                if idx < values.len() {
                    row.set(col_name, self.parse_value(&values[idx]));
                }
            }

            // Check if row matches WHERE clause
            let matches = if let Some(where_expr) = &stmt.where_clause {
                let result = self.evaluator.evaluate(where_expr, &row)?;
                self.to_boolean(&result)?
            } else {
                true // No WHERE clause, update all rows
            };

            if matches {
                // Update the row
                for (column, value_expr) in &stmt.assignments {
                    let new_value = self.evaluator.evaluate(value_expr, &row)?;
                    row.set(column, new_value);
                }

                // Rebuild CSV line
                let mut csv_values = Vec::new();
                for col_name in &column_names {
                    if let Some(value) = row.get(col_name) {
                        csv_values.push(self.value_to_csv_string(value));
                    } else {
                        csv_values.push(String::new());
                    }
                }

                lines[i] = csv_values.join(&table.field_delimiter.to_string());
                rows_updated += 1;
            }
        }

        // Rewrite file
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&table.data_file)?;

        let mut writer = BufWriter::new(file);
        for line in &lines {
            writeln!(writer, "{}", line)?;
        }
        writer.flush()?;

        Ok(rows_updated)
    }

    /// Execute a DELETE statement
    pub fn execute_delete(&self, stmt: &DeleteStatement, table: &Table) -> Result<usize> {
        // Acquire exclusive lock on the file
        let lock = FileLock::new(&table.data_file)?;
        lock.lock_exclusive()?;

        // Read all rows
        let file = File::open(&table.data_file)?;
        let reader = BufReader::new(file);
        let lines: Vec<String> = reader.lines().collect::<std::result::Result<Vec<_>, _>>()?;

        if lines.is_empty() {
            return Ok(0);
        }

        // Parse header
        let header = lines[0].clone();
        let column_names: Vec<String> = header
            .split(table.field_delimiter)
            .map(|s| s.trim().to_string())
            .collect();

        let mut kept_lines = vec![header]; // Keep header
        let mut rows_deleted = 0;

        // Process data rows
        for i in (table.data_starts_on as usize + 1)..lines.len() {
            let line = &lines[i];

            // Skip empty lines
            if line.trim().is_empty() {
                continue;
            }

            // Parse row
            let values: Vec<String> = line
                .split(table.field_delimiter)
                .map(|s| s.trim().to_string())
                .collect();

            let mut row = Row::new();
            for (idx, col_name) in column_names.iter().enumerate() {
                if idx < values.len() {
                    row.set(col_name, self.parse_value(&values[idx]));
                }
            }

            // Check if row matches WHERE clause
            let should_delete = if let Some(where_expr) = &stmt.where_clause {
                let result = self.evaluator.evaluate(where_expr, &row)?;
                self.to_boolean(&result)?
            } else {
                true // No WHERE clause, delete all rows
            };

            if should_delete {
                rows_deleted += 1;
            } else {
                kept_lines.push(line.clone());
            }
        }

        // Rewrite file with kept lines only
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&table.data_file)?;

        let mut writer = BufWriter::new(file);
        for line in &kept_lines {
            writeln!(writer, "{}", line)?;
        }
        writer.flush()?;

        Ok(rows_deleted)
    }

    /// Convert a Value to CSV string representation
    fn value_to_csv_string(&self, value: &Value) -> String {
        match value {
            Value::String(s) => {
                // Escape quotes and wrap in quotes if contains delimiter or quotes
                if s.contains(',') || s.contains('"') || s.contains('\n') {
                    format!("\"{}\"", s.replace('"', "\"\""))
                } else {
                    s.clone()
                }
            }
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Null => String::new(),
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::DateTime(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
            Value::Time(t) => t.format("%H:%M:%S").to_string(),
        }
    }

    /// Parse a string value into a Value
    fn parse_value(&self, s: &str) -> Value {
        if s.is_empty() {
            return Value::Null;
        }

        // Try parsing as integer
        if let Ok(i) = s.parse::<i64>() {
            return Value::Integer(i);
        }

        // Try parsing as float
        if let Ok(f) = s.parse::<f64>() {
            return Value::Float(f);
        }

        // Try parsing as boolean
        match s.to_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "1" => return Value::Boolean(true),
            "false" | "f" | "no" | "n" | "0" => return Value::Boolean(false),
            _ => {}
        }

        // Default to string
        Value::String(s.to_string())
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
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            having: None,
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
            joins: vec![],
            where_clause: Some(where_expr),
            group_by: vec![],
            having: None,
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
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            having: None,
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
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            having: None,
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
            joins: vec![],
            where_clause: None,
            group_by: vec![],
            having: None,
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
