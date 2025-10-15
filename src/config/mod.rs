// Configuration module - database, table, column definitions
pub mod loader;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use loader::{Config, DatabaseConfig, TableCatalog};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub database: String,
    pub data_file: String,
    pub columns: Vec<Column>,
    pub field_delimiter: char,
    pub data_starts_on: usize,
    pub comment_char: Option<char>,
}

impl Table {
    pub fn new(name: String, database: String, data_file: String) -> Self {
        Self {
            name,
            database,
            data_file,
            columns: Vec::new(),
            field_delimiter: ',',
            data_starts_on: 0,
            comment_char: Some('#'),
        }
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: String) -> Self {
        Self {
            name,
            data_type: DataType::String,
            nullable: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
}
