//! MCP Tool implementations for DDB

use crate::config::{Config, Table, TableCatalog};
use crate::engine::QueryExecutor;
use crate::lexer::Tokenizer;
use crate::output::{format_results, OutputFormat};
use crate::parser::Parser as SqlParser;
use crate::parser::Statement;
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

pub struct ToolHandler {
    config: Arc<RwLock<Config>>,
    catalog: Arc<RwLock<TableCatalog>>,
}

impl ToolHandler {
    pub fn new(config: Arc<RwLock<Config>>, catalog: Arc<RwLock<TableCatalog>>) -> Self {
        Self { config, catalog }
    }

    /// List all available tools
    pub fn list_tools(&self) -> Vec<Value> {
        vec![
            json!({
                "name": "execute_query",
                "description": "Execute a SQL query against DDB tables or CSV files",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": { "type": "string", "description": "SQL query to execute" },
                        "output_format": { "type": "string", "enum": ["json", "yaml", "csv", "table"], "default": "json" }
                    },
                    "required": ["query"]
                }
            }),
            json!({
                "name": "list_tables",
                "description": "List all configured tables",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "database": { "type": "string", "description": "Filter by database" }
                    }
                }
            }),
            json!({
                "name": "describe_table",
                "description": "Get table schema information",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table_name": { "type": "string", "description": "Table name" }
                    },
                    "required": ["table_name"]
                }
            }),
        ]
    }

    /// Call a tool
    pub async fn call_tool(&self, name: &str, arguments: Option<Value>) -> Result<Value> {
        let args = arguments.unwrap_or(json!({}));

        match name {
            "execute_query" => self.execute_query(args).await,
            "list_tables" => self.list_tables(args).await,
            "describe_table" => self.describe_table(args).await,
            _ => Err(anyhow!("Unknown tool: {}", name)),
        }
    }

    async fn execute_query(&self, args: Value) -> Result<Value> {
        let query = args["query"].as_str().ok_or_else(|| anyhow!("Missing query"))?;
        let output_format = args["output_format"].as_str().unwrap_or("json");

        info!("Executing: {}", query);

        let mut tokenizer = Tokenizer::new();
        let tokens = tokenizer.tokenize(query)?;
        let mut parser = SqlParser::new(tokens);
        let statement = parser.parse()?;

        match statement {
            Statement::Select(select_stmt) => {
                let catalog = self.catalog.read().await;
                let table_name = select_stmt.from.as_ref().ok_or_else(|| anyhow!("No FROM clause"))?;
                let table = catalog.get_table(table_name).ok_or_else(|| anyhow!("Table not found"))?.clone();

                let executor = QueryExecutor::new();
                let results = executor.execute_select(&select_stmt, &table)?;
                let format = OutputFormat::from_str(output_format);
                let formatted = format_results(&results, format)?;

                Ok(json!({
                    "success": true,
                    "rows": results.len(),
                    "data": formatted
                }))
            }
            _ => Ok(json!({ "success": false, "error": "Only SELECT supported" }))
        }
    }

    async fn list_tables(&self, _args: Value) -> Result<Value> {
        let catalog = self.catalog.read().await;
        let tables = catalog.list_tables();
        Ok(json!({ "success": true, "tables": tables }))
    }

    async fn describe_table(&self, args: Value) -> Result<Value> {
        let table_name = args["table_name"].as_str().ok_or_else(|| anyhow!("Missing table_name"))?;
        let catalog = self.catalog.read().await;
        let table = catalog.get_table(table_name).ok_or_else(|| anyhow!("Table not found"))?;

        let columns: Vec<Value> = table.columns.iter().map(|col| {
            json!({ "name": col.name, "type": format!("{:?}", col.data_type), "nullable": col.nullable })
        }).collect();

        Ok(json!({
            "success": true,
            "table": {
                "name": table.name,
                "database": table.database,
                "file": table.data_file,
                "columns": columns
            }
        }))
    }
}
