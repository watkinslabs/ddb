//! MCP (Model Context Protocol) server implementation for DDB
//!
//! Provides tools, resources, and prompts for interacting with DDB through MCP.

pub mod server;
pub mod tools;
pub mod resources;
pub mod prompts;

pub use server::DdbMcpServer;
