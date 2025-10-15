use clap::{Parser, Subcommand};
use ddb_core::{
    config::{Config, Table, TableCatalog},
    engine::QueryExecutor,
    lexer::Tokenizer,
    output::{format_results, OutputFormat},
    parser::Parser as SqlParser,
    parser::Statement,
    VERSION,
};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ddb")]
#[command(author, version, about = "A serviceless SQL interface for flat files", long_about = None)]
struct Cli {
    /// SQL query to execute
    #[arg(short, long)]
    query: Option<String>,

    /// Data file path
    #[arg(short = 'f', long)]
    file: Option<PathBuf>,

    /// Field delimiter (default: comma)
    #[arg(short = 'd', long, default_value = ",")]
    delimiter: String,

    /// Database configuration directory
    #[arg(short = 'c', long)]
    config: Option<PathBuf>,

    /// Output format (json, yaml, csv, table)
    #[arg(short, long, default_value = "table")]
    output: String,

    /// Enable debug mode
    #[arg(long)]
    debug: bool,

    /// Start MCP (Model Context Protocol) server mode
    #[cfg(feature = "mcp")]
    #[arg(long)]
    mcp: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start interactive mode
    Interactive,

    /// Show version information
    Version,
}

fn main() {
    let cli = Cli::parse();

    // Handle MCP mode first (if feature enabled)
    #[cfg(feature = "mcp")]
    if cli.mcp {
        start_mcp_server();
        return;
    }

    // Initialize regular env_logger for CLI mode
    env_logger::init();

    match &cli.command {
        Some(Commands::Version) => {
            println!("DDB v{}", VERSION);
            return;
        }
        Some(Commands::Interactive) => {
            println!("Interactive mode not yet implemented");
            std::process::exit(1);
        }
        None => {
            if let Some(query) = &cli.query {
                // Load configuration
                let config = Config::load().unwrap_or_else(|e| {
                    eprintln!("Warning: Failed to load config: {}", e);
                    eprintln!("Using default configuration");
                    Config::default()
                });

                if cli.debug {
                    println!("=== Configuration ===");
                    println!("Database: {}", config.default_database);
                    println!("Schema Dir: {:?}", config.schema_dir);
                    println!("Delimiter: {:?}", config.default_delimiter);
                    println!();
                }

                // Load table catalog
                let catalog = TableCatalog::load_from_config(&config).unwrap_or_else(|e| {
                    eprintln!("Warning: Failed to load table catalog: {}", e);
                    TableCatalog::new()
                });

                if cli.debug && !catalog.list_tables().is_empty() {
                    println!("=== Loaded Tables ===");
                    for table in catalog.list_tables() {
                        println!("  - {}", table);
                    }
                    println!();
                }

                if let Err(e) = execute_query(&cli, query, &config, &catalog) {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            } else {
                eprintln!("Error: No query provided. Use --query or run 'ddb version'");
                std::process::exit(1);
            }
        }
    }
}

fn execute_query(cli: &Cli, query: &str, config: &Config, catalog: &TableCatalog) -> Result<(), Box<dyn std::error::Error>> {
    // Tokenize
    let mut tokenizer = Tokenizer::new();
    let tokens = tokenizer.tokenize(query)?;

    if cli.debug {
        println!("=== Tokens ===");
        for token in &tokens {
            println!("  {:?}", token);
        }
        println!();
    }

    // Parse
    let mut parser = SqlParser::new(tokens);
    let statement = parser.parse()?;

    if cli.debug {
        println!("=== AST ===");
        println!("{:#?}", statement);
        println!();
    }

    // Execute
    match statement {
        Statement::Select(select_stmt) => {
            // Get table definition
            let table = if let Some(ref path) = cli.file {
                // Use explicit --file argument
                let delimiter = cli
                    .delimiter
                    .chars()
                    .next()
                    .unwrap_or(config.default_delimiter);

                Table {
                    name: "temp".to_string(),
                    database: "temp".to_string(),
                    data_file: path.to_string_lossy().to_string(),
                    columns: vec![],
                    field_delimiter: delimiter,
                    data_starts_on: config.data_starts_on,
                    comment_char: config.comment_char,
                }
            } else if let Some(ref table_name) = select_stmt.from {
                // Look up table in catalog
                catalog.get_table(table_name)
                    .ok_or_else(|| format!("Table '{}' not found in catalog. Use --file to specify a file, or add table definition to schema directory.", table_name))?
                    .clone()
            } else {
                return Err("SELECT requires a FROM clause or --file argument".into());
            };

            if cli.debug {
                println!("=== Table Info ===");
                println!("Name: {}", table.name);
                println!("File: {}", table.data_file);
                println!("Delimiter: {:?}", table.field_delimiter);
                println!();
            }

            // Execute query
            let executor = QueryExecutor::new();
            let results = executor.execute_select(&select_stmt, &table)?;

            if cli.debug {
                println!("=== Results ===");
                println!("{} rows returned", results.len());
                println!();
            }

            // Format output
            let output_format = if !cli.output.is_empty() {
                OutputFormat::from_str(&cli.output)
            } else {
                OutputFormat::from_str(&config.default_output_format)
            };
            let formatted = format_results(&results, output_format)?;
            println!("{}", formatted);

            Ok(())
        }
        Statement::Insert(insert_stmt) => {
            // Get table definition
            let table = if let Some(ref path) = cli.file {
                // Use explicit --file argument
                let delimiter = cli
                    .delimiter
                    .chars()
                    .next()
                    .unwrap_or(config.default_delimiter);

                Table {
                    name: "temp".to_string(),
                    database: "temp".to_string(),
                    data_file: path.to_string_lossy().to_string(),
                    columns: vec![],
                    field_delimiter: delimiter,
                    data_starts_on: config.data_starts_on,
                    comment_char: config.comment_char,
                }
            } else {
                // Look up table in catalog
                catalog.get_table(&insert_stmt.table)
                    .ok_or_else(|| format!("Table '{}' not found in catalog. Use --file to specify a file, or add table definition to schema directory.", insert_stmt.table))?
                    .clone()
            };

            if cli.debug {
                println!("=== Table Info ===");
                println!("Name: {}", table.name);
                println!("File: {}", table.data_file);
                println!("Delimiter: {:?}", table.field_delimiter);
                println!();
            }

            // Execute INSERT
            let executor = QueryExecutor::new();
            let rows_inserted = executor.execute_insert(&insert_stmt, &table)?;

            if cli.debug {
                println!("=== Results ===");
                println!("{} rows inserted", rows_inserted);
                println!();
            } else {
                println!("{} rows inserted", rows_inserted);
            }

            Ok(())
        }
        Statement::Update(update_stmt) => {
            // Get table definition
            let table = if let Some(ref path) = cli.file {
                // Use explicit --file argument
                let delimiter = cli
                    .delimiter
                    .chars()
                    .next()
                    .unwrap_or(config.default_delimiter);

                Table {
                    name: "temp".to_string(),
                    database: "temp".to_string(),
                    data_file: path.to_string_lossy().to_string(),
                    columns: vec![],
                    field_delimiter: delimiter,
                    data_starts_on: config.data_starts_on,
                    comment_char: config.comment_char,
                }
            } else {
                // Look up table in catalog
                catalog.get_table(&update_stmt.table)
                    .ok_or_else(|| format!("Table '{}' not found in catalog. Use --file to specify a file, or add table definition to schema directory.", update_stmt.table))?
                    .clone()
            };

            if cli.debug {
                println!("=== Table Info ===");
                println!("Name: {}", table.name);
                println!("File: {}", table.data_file);
                println!("Delimiter: {:?}", table.field_delimiter);
                println!();
            }

            // Execute UPDATE
            let executor = QueryExecutor::new();
            let rows_updated = executor.execute_update(&update_stmt, &table)?;

            if cli.debug {
                println!("=== Results ===");
                println!("{} rows updated", rows_updated);
                println!();
            } else {
                println!("{} rows updated", rows_updated);
            }

            Ok(())
        }
        Statement::Upsert(upsert_stmt) => {
            // Get table definition
            let table = if let Some(ref path) = cli.file {
                // Use explicit --file argument
                let delimiter = cli
                    .delimiter
                    .chars()
                    .next()
                    .unwrap_or(config.default_delimiter);

                Table {
                    name: "temp".to_string(),
                    database: "temp".to_string(),
                    data_file: path.to_string_lossy().to_string(),
                    columns: vec![],
                    field_delimiter: delimiter,
                    data_starts_on: config.data_starts_on,
                    comment_char: config.comment_char,
                }
            } else {
                // Look up table in catalog
                catalog.get_table(&upsert_stmt.table)
                    .ok_or_else(|| format!("Table '{}' not found in catalog. Use --file to specify a file, or add table definition to schema directory.", upsert_stmt.table))?
                    .clone()
            };

            if cli.debug {
                println!("=== Table Info ===");
                println!("Name: {}", table.name);
                println!("File: {}", table.data_file);
                println!("Delimiter: {:?}", table.field_delimiter);
                println!();
            }

            // Execute UPSERT
            let executor = QueryExecutor::new();
            let (rows_inserted, rows_updated) = executor.execute_upsert(&upsert_stmt, &table)?;

            if cli.debug {
                println!("=== Results ===");
                println!("{} rows inserted, {} rows updated", rows_inserted, rows_updated);
                println!();
            } else {
                println!("{} rows inserted, {} rows updated", rows_inserted, rows_updated);
            }

            Ok(())
        }
        Statement::Delete(delete_stmt) => {
            // Get table definition
            let table = if let Some(ref path) = cli.file {
                // Use explicit --file argument
                let delimiter = cli
                    .delimiter
                    .chars()
                    .next()
                    .unwrap_or(config.default_delimiter);

                Table {
                    name: "temp".to_string(),
                    database: "temp".to_string(),
                    data_file: path.to_string_lossy().to_string(),
                    columns: vec![],
                    field_delimiter: delimiter,
                    data_starts_on: config.data_starts_on,
                    comment_char: config.comment_char,
                }
            } else {
                // Look up table in catalog
                catalog.get_table(&delete_stmt.table)
                    .ok_or_else(|| format!("Table '{}' not found in catalog. Use --file to specify a file, or add table definition to schema directory.", delete_stmt.table))?
                    .clone()
            };

            if cli.debug {
                println!("=== Table Info ===");
                println!("Name: {}", table.name);
                println!("File: {}", table.data_file);
                println!("Delimiter: {:?}", table.field_delimiter);
                println!();
            }

            // Execute DELETE
            let executor = QueryExecutor::new();
            let rows_deleted = executor.execute_delete(&delete_stmt, &table)?;

            if cli.debug {
                println!("=== Results ===");
                println!("{} rows deleted", rows_deleted);
                println!();
            } else {
                println!("{} rows deleted", rows_deleted);
            }

            Ok(())
        }
        Statement::CreateTable(_) => {
            Err("CREATE TABLE statements are not yet implemented".into())
        }
        Statement::DropTable(_) => {
            Err("DROP TABLE statements are not yet implemented".into())
        }
        Statement::Use(_) => {
            Err("USE statements are not yet implemented".into())
        }
        Statement::Show(_) => {
            Err("SHOW statements are not yet implemented".into())
        }
        Statement::Set(_) => {
            Err("SET statements are not yet implemented".into())
        }
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Err("Transaction statements are not yet implemented".into())
        }
    }
}

#[cfg(feature = "mcp")]
fn start_mcp_server() {
    use ddb_core::mcp::DdbMcpServer;
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    // Initialize tracing for MCP mode
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ddb_mcp=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    // Create and run the MCP server
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
    runtime.block_on(async {
        let server = DdbMcpServer::new().expect("Failed to create MCP server");
        eprintln!("DDB MCP Server v{} starting...", VERSION);
        eprintln!("Server capabilities: tools, resources, prompts");

        if let Err(e) = server.run().await {
            eprintln!("MCP server error: {}", e);
            std::process::exit(1);
        }
    });
}
