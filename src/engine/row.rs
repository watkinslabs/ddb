// Row representation for query execution
use crate::functions::Value;
use std::collections::HashMap;

/// A row of data with named columns
#[derive(Debug, Clone)]
pub struct Row {
    columns: HashMap<String, Value>,
}

impl Row {
    /// Create a new empty row
    pub fn new() -> Self {
        Row {
            columns: HashMap::new(),
        }
    }

    /// Create a row from column names and values
    pub fn from_values(names: &[String], values: Vec<Value>) -> Self {
        let mut row = Row::new();
        for (name, value) in names.iter().zip(values.iter()) {
            row.set(name, value.clone());
        }
        row
    }

    /// Get a column value by name
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.columns.get(name)
    }

    /// Set a column value
    pub fn set(&mut self, name: &str, value: Value) {
        self.columns.insert(name.to_string(), value);
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }

    /// Get all values in order of provided column names
    pub fn values(&self, columns: &[String]) -> Vec<Value> {
        columns
            .iter()
            .map(|name| self.get(name).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Get all values as a HashMap
    pub fn as_map(&self) -> &HashMap<String, Value> {
        &self.columns
    }

    /// Number of columns in the row
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Check if row is empty
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

impl Default for Row {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_creation() {
        let mut row = Row::new();
        row.set("name", Value::String("John".to_string()));
        row.set("age", Value::Integer(30));

        assert_eq!(row.get("name"), Some(&Value::String("John".to_string())));
        assert_eq!(row.get("age"), Some(&Value::Integer(30)));
        assert_eq!(row.get("missing"), None);
    }

    #[test]
    fn test_row_from_values() {
        let names = vec!["id".to_string(), "name".to_string()];
        let values = vec![Value::Integer(1), Value::String("Alice".to_string())];
        let row = Row::from_values(&names, values);

        assert_eq!(row.get("id"), Some(&Value::Integer(1)));
        assert_eq!(row.get("name"), Some(&Value::String("Alice".to_string())));
    }
}
