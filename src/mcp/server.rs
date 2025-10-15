//! DDB MCP Server implementation using JSON-RPC over stdio

use crate::config::{Config, TableCatalog};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, debug};

use super::tools::ToolHandler;
use super::resources::ResourceHandler;
use super::prompts::PromptHandler;

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    method: String,
    params: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

/// DDB MCP Server
pub struct DdbMcpServer {
    _config: Arc<RwLock<Config>>,
    _catalog: Arc<RwLock<TableCatalog>>,
    tool_handler: ToolHandler,
    resource_handler: ResourceHandler,
    prompt_handler: PromptHandler,
}

impl DdbMcpServer {
    /// Create a new DDB MCP server
    pub fn new() -> Result<Self> {
        // Load configuration
        let config = Config::load().unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load config: {}", e);
            eprintln!("Using default configuration");
            Config::default()
        });

        // Load table catalog
        let catalog = TableCatalog::load_from_config(&config).unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load table catalog: {}", e);
            TableCatalog::new()
        });

        let config = Arc::new(RwLock::new(config));
        let catalog = Arc::new(RwLock::new(catalog));

        Ok(Self {
            _config: config.clone(),
            _catalog: catalog.clone(),
            tool_handler: ToolHandler::new(config.clone(), catalog.clone()),
            resource_handler: ResourceHandler::new(config.clone(), catalog.clone()),
            prompt_handler: PromptHandler::new(),
        })
    }

    /// Run the MCP server (processes stdio)
    pub async fn run(self) -> Result<()> {
        info!("Starting DDB MCP Server v{}", crate::VERSION);
        info!("Reading from stdin, writing to stdout");

        let stdin = io::stdin();
        let mut stdout = io::stdout();

        for line in stdin.lock().lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }

            debug!("Received: {}", line);

            // Parse JSON-RPC request
            let request: Result<JsonRpcRequest, _> = serde_json::from_str(&line);

            let response = match request {
                Ok(req) => self.handle_request(req).await,
                Err(e) => JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    id: None,
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32700,
                        message: format!("Parse error: {}", e),
                        data: None,
                    }),
                },
            };

            // Send response
            let response_str = serde_json::to_string(&response)?;
            debug!("Sending: {}", response_str);
            writeln!(stdout, "{}", response_str)?;
            stdout.flush()?;
        }

        Ok(())
    }

    async fn handle_request(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let result = match request.method.as_str() {
            "initialize" => self.handle_initialize(request.params).await,
            "tools/list" => self.handle_tools_list().await,
            "tools/call" => self.handle_tools_call(request.params).await,
            "resources/list" => self.handle_resources_list().await,
            "resources/read" => self.handle_resources_read(request.params).await,
            "prompts/list" => self.handle_prompts_list().await,
            "prompts/get" => self.handle_prompts_get(request.params).await,
            _ => Err(format!("Method not found: {}", request.method)),
        };

        match result {
            Ok(value) => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: Some(value),
                error: None,
            },
            Err(msg) => JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: None,
                error: Some(JsonRpcError {
                    code: -32603,
                    message: msg,
                    data: None,
                }),
            },
        }
    }

    async fn handle_initialize(&self, _params: Option<Value>) -> Result<Value, String> {
        Ok(serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {},
                "resources": {},
                "prompts": {}
            },
            "serverInfo": {
                "name": "ddb-mcp-server",
                "version": crate::VERSION
            }
        }))
    }

    async fn handle_tools_list(&self) -> Result<Value, String> {
        let tools = self.tool_handler.list_tools();
        Ok(serde_json::json!({
            "tools": tools
        }))
    }

    async fn handle_tools_call(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("Missing parameters")?;
        let name = params["name"].as_str().ok_or("Missing tool name")?;
        let arguments = params.get("arguments").cloned();

        self.tool_handler
            .call_tool(name, arguments)
            .await
            .map_err(|e| e.to_string())
    }

    async fn handle_resources_list(&self) -> Result<Value, String> {
        let resources = self.resource_handler
            .list_resources()
            .await
            .map_err(|e| e.to_string())?;

        Ok(serde_json::json!({
            "resources": resources
        }))
    }

    async fn handle_resources_read(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("Missing parameters")?;
        let uri = params["uri"].as_str().ok_or("Missing resource URI")?;

        let content = self.resource_handler
            .read_resource(uri)
            .await
            .map_err(|e| e.to_string())?;

        Ok(serde_json::json!({
            "contents": [
                {
                    "uri": uri,
                    "mimeType": "application/json",
                    "text": serde_json::to_string_pretty(&content).unwrap()
                }
            ]
        }))
    }

    async fn handle_prompts_list(&self) -> Result<Value, String> {
        let prompts = self.prompt_handler.list_prompts();
        Ok(serde_json::json!({
            "prompts": prompts
        }))
    }

    async fn handle_prompts_get(&self, params: Option<Value>) -> Result<Value, String> {
        let params = params.ok_or("Missing parameters")?;
        let name = params["name"].as_str().ok_or("Missing prompt name")?;
        let arguments = params.get("arguments").cloned();

        self.prompt_handler
            .get_prompt(name, arguments)
            .await
            .map_err(|e| e.to_string())
    }
}

impl Default for DdbMcpServer {
    fn default() -> Self {
        Self::new().expect("Failed to create DDB MCP Server")
    }
}
