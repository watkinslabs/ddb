//! MCP Resource implementations for DDB

use crate::config::{Config, TableCatalog};
use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct ResourceHandler {
    config: Arc<RwLock<Config>>,
    catalog: Arc<RwLock<TableCatalog>>,
}

impl ResourceHandler {
    pub fn new(config: Arc<RwLock<Config>>, catalog: Arc<RwLock<TableCatalog>>) -> Self {
        Self { config, catalog }
    }

    pub async fn list_resources(&self) -> Result<Vec<Value>> {
        let catalog = self.catalog.read().await;
        let mut resources = vec![
            json!({
                "uri": "ddb://config",
                "name": "DDB Configuration",
                "description": "Current DDB configuration settings",
                "mimeType": "application/json"
            }),
        ];

        for table_name in catalog.list_tables() {
            resources.push(json!({
                "uri": format!("ddb://tables/{}/schema", table_name),
                "name": format!("Schema for {}", table_name),
                "description": format!("Table schema for {}", table_name),
                "mimeType": "application/json"
            }));
        }

        Ok(resources)
    }

    pub async fn read_resource(&self, uri: &str) -> Result<Value> {
        if uri == "ddb://config" {
            let config = self.config.read().await;
            return Ok(json!({
                "default_database": config.default_database,
                "default_delimiter": config.default_delimiter.to_string()
            }));
        }

        if uri.starts_with("ddb://tables/") && uri.ends_with("/schema") {
            let table_name = uri
                .trim_start_matches("ddb://tables/")
                .trim_end_matches("/schema");

            let catalog = self.catalog.read().await;
            let table = catalog.get_table(table_name).ok_or_else(|| anyhow!("Table not found"))?;

            let columns: Vec<Value> = table.columns.iter().enumerate().map(|(i, col)| {
                json!({
                    "index": i,
                    "name": col.name,
                    "type": format!("{:?}", col.data_type),
                    "nullable": col.nullable
                })
            }).collect();

            return Ok(json!({
                "table": table.name,
                "database": table.database,
                "file": table.data_file,
                "columns": columns
            }));
        }

        Err(anyhow!("Resource not found: {}", uri))
    }
}
