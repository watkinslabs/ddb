package cmd

import (
	"ddb/internal/config"
	"ddb/internal/export"
	"ddb/internal/query"
	"ddb/pkg/types"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use:   "query [SQL]",
	Short: "Execute a SQL query against configured tables",
	Long: `Execute a SQL query against text files. You can use table configurations
from a config directory, or specify files inline with --file flags.

Examples:
  agent-ddb query "SELECT * FROM users WHERE age > 25"
  agent-ddb query "SELECT name, email FROM users" --config-dir ./configs
  agent-ddb query "SELECT * FROM data" --file data:/path/to/file.csv --format csv
  agent-ddb query "SELECT * FROM users" --output json --export results.json`,
	Args: cobra.ExactArgs(1),
	RunE: runQuery,
}

var (
	configDir    string
	outputFormat string
	exportFile   string
	inlineFiles  []string
	workers      int
	chunkSize    int
	bufferSize   int
	delimiter    string
	hasHeader    bool
	parallel     bool
	verbose      bool
	timeout      int
	
	// Advanced CSV options
	quote        string
	escape       string
	maxColumns   int
	trimSpaces   bool
	allowQuoted  bool
	strictQuotes bool
	skipEmpty    bool
)

func init() {
	queryCmd.Flags().StringVarP(&configDir, "config-dir", "c", "", "Directory containing table configuration files")
	
	// Add completions for config directory
	queryCmd.RegisterFlagCompletionFunc("config-dir", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return nil, cobra.ShellCompDirectiveFilterDirs
	})
	queryCmd.Flags().StringVarP(&outputFormat, "output", "o", "table", "Output format (table, csv, json, jsonl, yaml, excel)")
	queryCmd.Flags().StringVarP(&exportFile, "export", "e", "", "Export results to file")
	
	// Add completions for output format
	queryCmd.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{"table", "csv", "json", "jsonl", "yaml", "excel"}, cobra.ShellCompDirectiveNoFileComp
	})
	
	// Add completions for export file (suggest file extensions based on output format)
	queryCmd.RegisterFlagCompletionFunc("export", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{".csv", ".json", ".jsonl", ".yaml", ".xlsx", ".txt"}, cobra.ShellCompDirectiveFilterFileExt
	})
	queryCmd.Flags().StringArrayVarP(&inlineFiles, "file", "f", []string{}, "Inline file definition: table_name:/path/to/file.ext")
	
	// Add completions for file flag (suggest data file extensions)
	queryCmd.RegisterFlagCompletionFunc("file", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return []string{".csv", ".json", ".jsonl", ".yaml", ".yml", ".parquet", ".tsv"}, cobra.ShellCompDirectiveFilterFileExt
	})
	queryCmd.Flags().IntVarP(&workers, "workers", "w", 0, "Number of worker threads (0 = auto-detect)")
	queryCmd.Flags().IntVar(&chunkSize, "chunk-size", 1000, "Number of rows per chunk")
	queryCmd.Flags().IntVar(&bufferSize, "buffer-size", 100, "Chunk buffer size")
	queryCmd.Flags().StringVar(&delimiter, "delimiter", ",", "Default delimiter for CSV files")
	queryCmd.Flags().BoolVar(&hasHeader, "header", true, "Default header setting for CSV files")
	queryCmd.Flags().BoolVar(&parallel, "parallel", false, "Enable parallel processing for all files")
	queryCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	queryCmd.Flags().IntVarP(&timeout, "timeout", "t", 300, "Query timeout in seconds")
	
	// Advanced CSV parsing flags
	queryCmd.Flags().StringVar(&quote, "quote", "\"", "Quote character for CSV files")
	queryCmd.Flags().StringVar(&escape, "escape", "\\", "Escape character for CSV files")
	queryCmd.Flags().IntVar(&maxColumns, "max-columns", 0, "Maximum columns to parse (0 = unlimited)")
	queryCmd.Flags().BoolVar(&trimSpaces, "trim-spaces", true, "Trim leading/trailing spaces")
	queryCmd.Flags().BoolVar(&allowQuoted, "allow-quoted", true, "Allow quoted fields")
	queryCmd.Flags().BoolVar(&strictQuotes, "strict-quotes", false, "Require quotes for all fields")
	queryCmd.Flags().BoolVar(&skipEmpty, "skip-empty", true, "Skip empty lines")
}

func runQuery(cmd *cobra.Command, args []string) error {
	sqlQuery := args[0]
	
	if verbose {
		fmt.Fprintf(os.Stderr, "Executing query: %s\n", sqlQuery)
		start := time.Now()
		defer func() {
			fmt.Fprintf(os.Stderr, "Query completed in %v\n", time.Since(start))
		}()
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	// Load table configurations
	tableConfigs, err := loadTableConfigurations()
	if err != nil {
		return fmt.Errorf("failed to load table configurations: %w", err)
	}

	if len(tableConfigs) == 0 {
		return fmt.Errorf("no table configurations found. Use --config-dir or --file flags")
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Loaded %d table(s): ", len(tableConfigs))
		var names []string
		for name := range tableConfigs {
			names = append(names, name)
		}
		fmt.Fprintf(os.Stderr, "%s\n", strings.Join(names, ", "))
	}

	// Show progress bar for large files (only if not outputting to stdout)
	var progressBar *progressbar.ProgressBar
	if exportFile != "" && shouldShowProgress(tableConfigs) {
		totalSize := calculateTotalSize(tableConfigs)
		progressBar = progressbar.NewOptions64(totalSize,
			progressbar.OptionSetDescription("Processing files..."),
			progressbar.OptionSetWriter(os.Stderr),
			progressbar.OptionShowBytes(true),
			progressbar.OptionSetWidth(40),
			progressbar.OptionThrottle(100*time.Millisecond),
			progressbar.OptionShowCount(),
			progressbar.OptionOnCompletion(func() {
				fmt.Fprintf(os.Stderr, "\n")
			}),
		)
		defer progressBar.Close()
	}

	// Create and execute query
	executor := query.NewExecutor()
	results, err := executor.Execute(ctx, sqlQuery, tableConfigs)
	if err != nil {
		return fmt.Errorf("query execution failed: %w", err)
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "Query returned %d rows\n", results.Count)
		fmt.Fprintf(os.Stderr, "Result columns: %s\n", strings.Join(results.Columns, ", "))
		if len(results.Rows) > 0 {
			fmt.Fprintf(os.Stderr, "Sample values from first row:\n")
			for i, col := range results.Columns {
				if i >= 3 { // Limit to first 3 columns to avoid clutter
					fmt.Fprintf(os.Stderr, "  ... and %d more columns\n", len(results.Columns)-3)
					break
				}
				if value, exists := results.Rows[0][col]; exists {
					fmt.Fprintf(os.Stderr, "  %s: %v\n", col, value)
				}
			}
		}
	}

	// Export results
	return exportResults(results)
}

func loadTableConfigurations() (map[string]*types.TableConfig, error) {
	tableConfigs := make(map[string]*types.TableConfig)
	
	// Load from config directory
	if configDir != "" {
		configManager := config.NewManager(configDir)
		configs, err := configManager.ListConfigs()
		if err != nil {
			return nil, fmt.Errorf("failed to list configurations: %w", err)
		}
		
		for _, config := range configs {
			// Apply global overrides
			applyGlobalConfigOverrides(config)
			tableConfigs[config.Name] = config
			
			if verbose {
				fmt.Fprintf(os.Stderr, "Loaded config for table '%s' from file '%s' (format: %s)\n", config.Name, config.FilePath, config.Format)
				if fileInfo, err := os.Stat(config.FilePath); err == nil {
					fmt.Fprintf(os.Stderr, "  File size: %d bytes, Chunk size: %d, Workers: %d\n", 
						fileInfo.Size(), config.ChunkSize, config.WorkerThreads)
				}
			}
		}
	}
	
	// Load inline file definitions
	for _, fileSpec := range inlineFiles {
		config, err := parseInlineFileSpec(fileSpec)
		if err != nil {
			return nil, fmt.Errorf("failed to parse file spec '%s': %w", fileSpec, err)
		}
		
		// Apply global overrides
		applyGlobalConfigOverrides(config)
		tableConfigs[config.Name] = config
		
		if verbose {
			fmt.Fprintf(os.Stderr, "Created inline config for table '%s' from file '%s' (format: %s)\n", config.Name, config.FilePath, config.Format)
			if fileInfo, err := os.Stat(config.FilePath); err == nil {
				fmt.Fprintf(os.Stderr, "  File size: %d bytes, Chunk size: %d, Workers: %d\n", 
					fileInfo.Size(), config.ChunkSize, config.WorkerThreads)
			}
		}
	}
	
	return tableConfigs, nil
}

func parseInlineFileSpec(spec string) (*types.TableConfig, error) {
	parts := strings.SplitN(spec, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid file spec format, expected 'table_name:/path/to/file'")
	}
	
	tableName := parts[0]
	filePath := parts[1]
	
	// Check if file exists
	if _, err := os.Stat(filePath); err != nil {
		return nil, fmt.Errorf("file not accessible: %w", err)
	}
	
	// Detect format from file extension
	ext := strings.ToLower(filepath.Ext(filePath))
	format := "csv" // default
	switch ext {
	case ".json":
		format = "json"
	case ".jsonl", ".ndjson":
		format = "jsonl"
	case ".yaml", ".yml":
		format = "yaml"
	case ".csv", ".tsv":
		format = "csv"
	case ".parquet":
		format = "parquet"
	}
	
	config := &types.TableConfig{
		Name:      tableName,
		FilePath:  filePath,
		Format:    format,
		Delimiter: delimiter,
		HasHeader: hasHeader,
	}
	
	return config, nil
}

func applyGlobalConfigOverrides(config *types.TableConfig) {
	// Apply global settings
	if workers > 0 {
		config.WorkerThreads = workers
	} else if config.WorkerThreads == 0 {
		config.WorkerThreads = runtime.NumCPU()
	}
	
	if chunkSize > 0 {
		config.ChunkSize = chunkSize
	}
	if config.ChunkSize == 0 {
		config.ChunkSize = 1000
	}
	
	if bufferSize > 0 {
		config.BufferSize = bufferSize
	}
	if config.BufferSize == 0 {
		config.BufferSize = 100
	}
	
	// Apply parallel setting if specified
	config.ParallelReading = parallel
	
	// Apply advanced CSV settings
	if config.Format == "csv" {
		if quote != "" {
			config.Quote = quote
		}
		if config.Quote == "" {
			config.Quote = "\""
		}
		
		if escape != "" {
			config.Escape = escape
		}
		if config.Escape == "" {
			config.Escape = "\\"
		}
		
		if maxColumns > 0 {
			config.MaxColumns = maxColumns
		}
		
		config.TrimSpaces = trimSpaces
		config.AllowQuoted = allowQuoted
		config.StrictQuotes = strictQuotes
		config.SkipEmptyLines = skipEmpty
	}
	
	// Determine if parallel reading should be enabled
	if config.ParallelReading {
		// Check file size to determine if parallel reading is beneficial
		if fileInfo, err := os.Stat(config.FilePath); err == nil {
			fileSize := fileInfo.Size()
			minSizeForParallel := int64(1024 * config.WorkerThreads) // 1KB per worker
			if fileSize < minSizeForParallel {
				config.ParallelReading = false
				if verbose {
					fmt.Fprintf(os.Stderr, "Disabling parallel reading for small file: %s (%d bytes)\n", 
						config.FilePath, fileSize)
				}
			}
		}
	}
}

func exportResults(results types.ResultSet) error {
	// Determine output destination
	var output *os.File
	if exportFile != "" {
		file, err := os.Create(exportFile)
		if err != nil {
			return fmt.Errorf("failed to create export file: %w", err)
		}
		defer file.Close()
		output = file
	} else {
		output = os.Stdout
	}
	
	// Get exporter
	exportOptions := make(map[string]interface{})
	if outputFormat == "csv" {
		exportOptions["delimiter"] = delimiter
	}
	if outputFormat == "json" {
		exportOptions["pretty"] = true
	}
	
	exporter, err := export.GetExporter(outputFormat, exportOptions)
	if err != nil {
		return fmt.Errorf("failed to create exporter: %w", err)
	}
	
	// Export results
	if err := exporter.Export(output, results); err != nil {
		return fmt.Errorf("failed to export results: %w", err)
	}
	
	return nil
}

// shouldShowProgress determines if we should show a progress bar
func shouldShowProgress(tableConfigs map[string]*types.TableConfig) bool {
	const minSizeForProgress = 1024 * 1024 // 1MB minimum
	totalSize := calculateTotalSize(tableConfigs)
	return totalSize > minSizeForProgress
}

// calculateTotalSize calculates the total size of all input files
func calculateTotalSize(tableConfigs map[string]*types.TableConfig) int64 {
	var totalSize int64
	for _, config := range tableConfigs {
		if fileInfo, err := os.Stat(config.FilePath); err == nil {
			totalSize += fileInfo.Size()
		}
	}
	return totalSize
}