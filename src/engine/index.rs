// Index structures for fast lookups in JOINs and WHERE clauses
use crate::engine::row::Row;
use crate::functions::Value;
use std::collections::HashMap;

/// Hash-based index for equality lookups
/// Maps column value -> list of row indices with that value
#[derive(Debug, Clone)]
pub struct HashIndex {
    /// Column name being indexed
    column: String,
    /// Map from column value (as string) to row indices
    index: HashMap<String, Vec<usize>>,
}

impl HashIndex {
    /// Create a new hash index on a specific column
    pub fn new(column: String) -> Self {
        HashIndex {
            column,
            index: HashMap::new(),
        }
    }

    /// Build index from a vector of rows
    pub fn build(column: &str, rows: &[Row]) -> Self {
        let mut index = HashMap::new();

        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(value) = row.get(column) {
                let key = value_to_index_key(value);
                index.entry(key).or_insert_with(Vec::new).push(row_idx);
            }
        }

        HashIndex {
            column: column.to_string(),
            index,
        }
    }

    /// Lookup rows by value - returns row indices
    pub fn lookup(&self, value: &Value) -> Option<&Vec<usize>> {
        let key = value_to_index_key(value);
        self.index.get(&key)
    }

    /// Get the column name this index is built on
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Get number of unique values in index
    pub fn cardinality(&self) -> usize {
        self.index.len()
    }

    /// Get total number of rows indexed
    pub fn row_count(&self) -> usize {
        self.index.values().map(|v| v.len()).sum()
    }
}

/// Convert a value to an index key (string representation)
fn value_to_index_key(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => format!("I:{}", i),
        Value::Float(f) => format!("F:{}", f),
        Value::String(s) => format!("S:{}", s),
        Value::Boolean(b) => format!("B:{}", b),
        Value::Date(d) => format!("D:{}", d.format("%Y-%m-%d")),
        Value::DateTime(dt) => format!("DT:{}", dt.format("%Y-%m-%d %H:%M:%S")),
        Value::Time(t) => format!("T:{}", t.format("%H:%M:%S")),
    }
}

/// Index manager - holds multiple indexes for a table
#[derive(Debug, Clone)]
pub struct IndexManager {
    /// Map from column name to hash index
    indexes: HashMap<String, HashIndex>,
}

impl IndexManager {
    /// Create a new index manager
    pub fn new() -> Self {
        IndexManager {
            indexes: HashMap::new(),
        }
    }

    /// Add an index for a column
    pub fn add_index(&mut self, column: String, index: HashIndex) {
        self.indexes.insert(column, index);
    }

    /// Get an index for a column if it exists
    pub fn get_index(&self, column: &str) -> Option<&HashIndex> {
        self.indexes.get(column)
    }

    /// Check if an index exists for a column
    pub fn has_index(&self, column: &str) -> bool {
        self.indexes.contains_key(column)
    }

    /// Build index for a column from rows
    pub fn build_index(&mut self, column: &str, rows: &[Row]) {
        let index = HashIndex::build(column, rows);
        self.indexes.insert(column.to_string(), index);
    }

    /// Get statistics about all indexes
    pub fn stats(&self) -> Vec<(String, usize, usize)> {
        self.indexes
            .iter()
            .map(|(col, idx)| (col.clone(), idx.cardinality(), idx.row_count()))
            .collect()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_rows() -> Vec<Row> {
        let mut rows = Vec::new();
        for i in 1..=100 {
            let mut row = Row::new();
            row.set("id", Value::Integer(i));
            row.set("dept", Value::String(format!("dept{}", i % 10)));
            row.set("salary", Value::Integer(30000 + i * 1000));
            rows.push(row);
        }
        rows
    }

    #[test]
    fn test_hash_index_build() {
        let rows = create_test_rows();
        let index = HashIndex::build("dept", &rows);

        assert_eq!(index.cardinality(), 10); // 10 unique departments
        assert_eq!(index.row_count(), 100); // 100 total rows
    }

    #[test]
    fn test_hash_index_lookup() {
        let rows = create_test_rows();
        let index = HashIndex::build("dept", &rows);

        let dept5 = Value::String("dept5".to_string());
        let matches = index.lookup(&dept5).unwrap();
        assert_eq!(matches.len(), 10); // 10 rows with dept5
    }

    #[test]
    fn test_index_manager() {
        let rows = create_test_rows();
        let mut manager = IndexManager::new();

        manager.build_index("id", &rows);
        manager.build_index("dept", &rows);

        assert!(manager.has_index("id"));
        assert!(manager.has_index("dept"));
        assert!(!manager.has_index("salary"));

        let stats = manager.stats();
        assert_eq!(stats.len(), 2);
    }
}
