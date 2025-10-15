// Configuration loader for DDB
use crate::config::{Database, Table};
use crate::error::{DdbError, Result};
use crate::lexer::Tokenizer;
use crate::parser::{Parser, Statement};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// DDB configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Name of the default database
    pub default_database: String,

    /// Directory containing SQL schema files
    pub schema_dir: PathBuf,

    /// Default field delimiter
    #[serde(default = "default_delimiter")]
    pub default_delimiter: char,

    /// Trim whitespace from fields
    #[serde(default = "default_true")]
    pub trim_whitespace: bool,

    /// Ignore comment lines
    #[serde(default = "default_true")]
    pub ignore_comments: bool,

    /// Comment character (lines starting with this are ignored)
    #[serde(default = "default_comment_char")]
    pub comment_char: Option<char>,

    /// Line number where data starts (0 = first line)
    #[serde(default)]
    pub data_starts_on: usize,

    /// Whether the first data line is a header
    #[serde(default = "default_true")]
    pub has_header: bool,

    /// Quote character for escaping delimiters
    #[serde(default = "default_quote_char")]
    pub quote_char: Option<char>,

    /// Skip empty lines
    #[serde(default = "default_true")]
    pub skip_empty_lines: bool,

    /// Whether fields are quoted
    #[serde(default = "default_false")]
    pub quoted_fields: bool,

    /// Remove quotes from quoted fields
    #[serde(default = "default_true")]
    pub strip_quotes: bool,

    /// Default output format (table, json, yaml, csv)
    #[serde(default = "default_output_format")]
    pub default_output_format: String,

    /// Ignore errors (continue processing on errors)
    #[serde(default = "default_false")]
    pub ignore_errors: bool,

    /// Databases configuration
    #[serde(default)]
    pub databases: HashMap<String, DatabaseConfig>,
}

fn default_delimiter() -> char {
    ','
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

fn default_comment_char() -> Option<char> {
    Some('#')
}

fn default_quote_char() -> Option<char> {
    Some('"')
}

fn default_output_format() -> String {
    "table".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    pub schema_dir: Option<PathBuf>,

    /// Override default settings for this database
    pub delimiter: Option<char>,
    pub trim_whitespace: Option<bool>,
    pub ignore_comments: Option<bool>,
    pub comment_char: Option<char>,
    pub data_starts_on: Option<usize>,
    pub has_header: Option<bool>,
}

impl Config {
    /// Load configuration from environment or default locations
    /// Checks in order:
    /// 1. DDB_CONFIG environment variable
    /// 2. .ddb/config.yaml in current directory
    /// 3. ~/.ddb/config.yaml
    /// 4. Returns default config if none found
    pub fn load() -> Result<Self> {
        // Check DDB_CONFIG environment variable
        if let Ok(config_path) = std::env::var("DDB_CONFIG") {
            return Self::from_file(&config_path);
        }

        // Check local .ddb/config.yaml
        let local_config = PathBuf::from(".ddb/config.yaml");
        if local_config.exists() {
            return Self::from_file(&local_config);
        }

        // Check home directory ~/.ddb/config.yaml
        if let Some(home) = std::env::var_os("HOME") {
            let home_config = PathBuf::from(home).join(".ddb/config.yaml");
            if home_config.exists() {
                return Self::from_file(&home_config);
            }
        }

        // Return default configuration
        Ok(Self::default())
    }

    /// Load configuration from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = fs::read_to_string(path)
            .map_err(|e| DdbError::ConfigError(format!("Failed to read config file: {}", e)))?;

        serde_yaml::from_str(&contents)
            .map_err(|e| DdbError::ConfigError(format!("Failed to parse config: {}", e)))
    }

    /// Create a default configuration
    /// Also checks DDB_SCHEMA_DIR environment variable
    pub fn default() -> Self {
        let schema_dir = std::env::var("DDB_SCHEMA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".ddb/schemas"));

        Config {
            default_database: "default".to_string(),
            schema_dir,
            default_delimiter: ',',
            trim_whitespace: true,
            ignore_comments: true,
            comment_char: Some('#'),
            data_starts_on: 0,
            has_header: true,
            quote_char: Some('"'),
            skip_empty_lines: true,
            quoted_fields: false,
            strip_quotes: true,
            default_output_format: "table".to_string(),
            ignore_errors: false,
            databases: HashMap::new(),
        }
    }

    /// Get the schema directory for a database
    pub fn get_schema_dir(&self, database: &str) -> PathBuf {
        if let Some(db_config) = self.databases.get(database) {
            if let Some(ref schema_dir) = db_config.schema_dir {
                return schema_dir.clone();
            }
        }
        self.schema_dir.clone()
    }
}

/// Table catalog that holds all loaded table definitions
#[derive(Debug, Clone)]
pub struct TableCatalog {
    databases: HashMap<String, Database>,
    current_database: String,
}

impl TableCatalog {
    pub fn new() -> Self {
        TableCatalog {
            databases: HashMap::new(),
            current_database: "default".to_string(),
        }
    }

    /// Load tables from a configuration
    pub fn load_from_config(config: &Config) -> Result<Self> {
        let mut catalog = TableCatalog::new();
        catalog.current_database = config.default_database.clone();

        // Create default database
        let mut default_db = Database::new(config.default_database.clone());

        // Load SQL files from schema directory
        let schema_dir = &config.schema_dir;
        if schema_dir.exists() {
            catalog.load_schema_directory(
                &config.default_database,
                schema_dir,
                config.default_delimiter,
                &mut default_db,
            )?;
        }

        // Load additional databases
        for (db_name, db_config) in &config.databases {
            let mut db = Database::new(db_name.clone());
            if let Some(ref schema_dir) = db_config.schema_dir {
                if schema_dir.exists() {
                    catalog.load_schema_directory(
                        db_name,
                        schema_dir,
                        config.default_delimiter,
                        &mut db,
                    )?;
                }
            }
            catalog.databases.insert(db_name.clone(), db);
        }

        catalog.databases.insert(config.default_database.clone(), default_db);

        Ok(catalog)
    }

    /// Load all SQL files from a directory
    fn load_schema_directory(
        &self,
        database_name: &str,
        dir: &Path,
        default_delimiter: char,
        db: &mut Database,
    ) -> Result<()> {
        let entries = fs::read_dir(dir)
            .map_err(|e| DdbError::ConfigError(format!("Failed to read schema directory: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                DdbError::ConfigError(format!("Failed to read directory entry: {}", e))
            })?;

            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("sql") {
                self.load_sql_file(database_name, &path, default_delimiter, db)?;
            }
        }

        Ok(())
    }

    /// Load and parse a SQL file
    fn load_sql_file(
        &self,
        database_name: &str,
        path: &Path,
        default_delimiter: char,
        db: &mut Database,
    ) -> Result<()> {
        let contents = fs::read_to_string(path).map_err(|e| {
            DdbError::ConfigError(format!(
                "Failed to read SQL file {}: {}",
                path.display(),
                e
            ))
        })?;

        // Split by semicolons to handle multiple statements
        for statement_str in contents.split(';') {
            // Strip comment lines (lines starting with --)
            let statement_str: String = statement_str
                .lines()
                .filter(|line| !line.trim().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");

            let statement_str = statement_str.trim();
            if statement_str.is_empty() {
                continue;
            }

            // Tokenize and parse
            let mut tokenizer = Tokenizer::new();
            let tokens = tokenizer.tokenize(statement_str).map_err(|e| {
                DdbError::ConfigError(format!(
                    "Failed to tokenize SQL in {}: {}",
                    path.display(),
                    e
                ))
            })?;

            let mut parser = Parser::new(tokens);
            match parser.parse() {
                Ok(Statement::CreateTable(create_stmt)) => {
                    // Convert to Table structure
                    let table = Table {
                        name: create_stmt.name.clone(),
                        database: database_name.to_string(),
                        data_file: if create_stmt.file_path.is_empty() {
                            format!("data/{}.csv", create_stmt.name)
                        } else {
                            create_stmt.file_path.clone()
                        },
                        columns: vec![], // Column definitions from CREATE TABLE
                        field_delimiter: create_stmt.delimiter.unwrap_or(default_delimiter),
                        data_starts_on: 0,
                        comment_char: None,
                    };

                    db.add_table(table);
                }
                Ok(_) => {
                    // Ignore other statement types
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to parse statement in {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }

        Ok(())
    }

    /// Get a table by name from the current database
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.databases
            .get(&self.current_database)
            .and_then(|db| db.get_table(name))
    }

    /// Get a table from a specific database
    pub fn get_table_from_db(&self, database: &str, table: &str) -> Option<&Table> {
        self.databases.get(database).and_then(|db| db.get_table(table))
    }

    /// Set the current database
    pub fn use_database(&mut self, name: &str) -> Result<()> {
        if self.databases.contains_key(name) {
            self.current_database = name.to_string();
            Ok(())
        } else {
            Err(DdbError::ConfigError(format!("Database '{}' not found", name)))
        }
    }

    /// Get the current database name
    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    /// List all databases
    pub fn list_databases(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }

    /// List all tables in the current database
    pub fn list_tables(&self) -> Vec<String> {
        self.databases
            .get(&self.current_database)
            .map(|db| db.tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Add a table to the current database
    pub fn add_table(&mut self, table: Table) -> Result<()> {
        let db = self
            .databases
            .get_mut(&self.current_database)
            .ok_or_else(|| DdbError::ConfigError("Current database not found".to_string()))?;

        db.add_table(table);
        Ok(())
    }
}

impl Default for TableCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_load_sql_file() {
        let temp_dir = TempDir::new().unwrap();
        let sql_file = temp_dir.path().join("test.sql");

        let mut file = fs::File::create(&sql_file).unwrap();
        writeln!(
            file,
            "CREATE TABLE users (id, name, email) FILE 'data/users.csv' DELIMITER ',';"
        )
        .unwrap();
        file.flush().unwrap();

        let mut config = Config::default();
        config.default_database = "test".to_string();
        config.schema_dir = temp_dir.path().to_path_buf();

        let catalog = TableCatalog::load_from_config(&config).unwrap();
        let table = catalog.get_table("users");

        assert!(table.is_some());
        let table = table.unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.data_file, "data/users.csv");
        assert_eq!(table.field_delimiter, ',');
    }

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.default_database, "default");
        assert_eq!(config.default_delimiter, ',');
    }
}
