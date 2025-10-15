//! MCP Prompt implementations for DDB

use anyhow::{anyhow, Result};
use serde_json::{json, Value};

pub struct PromptHandler;

impl PromptHandler {
    pub fn new() -> Self {
        Self
    }

    pub fn list_prompts(&self) -> Vec<Value> {
        vec![
            json!({
                "name": "query_data",
                "description": "Generate a SQL query to retrieve data from a table",
                "arguments": [
                    { "name": "table", "description": "Table name", "required": true },
                    { "name": "columns", "description": "Columns to select", "required": false }
                ]
            }),
            json!({
                "name": "analyze_data",
                "description": "Generate queries for data analysis",
                "arguments": [
                    { "name": "table", "description": "Table name", "required": true },
                    { "name": "metric", "description": "Metric to analyze", "required": true }
                ]
            }),
        ]
    }

    pub async fn get_prompt(&self, name: &str, arguments: Option<Value>) -> Result<Value> {
        let args = arguments.unwrap_or(json!({}));

        match name {
            "query_data" => {
                let table = args["table"].as_str().ok_or_else(|| anyhow!("Missing table"))?;
                let columns = args["columns"].as_str().unwrap_or("*");

                let query = format!("SELECT {} FROM {}", columns, table);
                Ok(json!({
                    "messages": [{
                        "role": "assistant",
                        "content": {
                            "type": "text",
                            "text": format!("Here's a query for {}:\n\n```sql\n{}\n```", table, query)
                        }
                    }]
                }))
            }
            "analyze_data" => {
                let table = args["table"].as_str().ok_or_else(|| anyhow!("Missing table"))?;
                let metric = args["metric"].as_str().unwrap_or("COUNT");

                let query = if metric.to_uppercase() == "COUNT" {
                    format!("SELECT COUNT(*) FROM {}", table)
                } else {
                    format!("SELECT {}(*) FROM {}", metric.to_uppercase(), table)
                };

                Ok(json!({
                    "messages": [{
                        "role": "assistant",
                        "content": {
                            "type": "text",
                            "text": format!("Here's an analysis query:\n\n```sql\n{}\n```", query)
                        }
                    }]
                }))
            }
            _ => Err(anyhow!("Unknown prompt: {}", name)),
        }
    }
}

impl Default for PromptHandler {
    fn default() -> Self {
        Self::new()
    }
}
