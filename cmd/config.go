package cmd

import (
	"ddb/internal/config"
	"ddb/pkg/types"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage table configurations",
	Long:  "Create, list, and manage table configuration files",
}

var createConfigCmd = &cobra.Command{
	Use:   "create [table-name] [file-path]",
	Short: "Create a new table configuration",
	Long:  "Create a new table configuration file for the specified table and file",
	Args:  cobra.ExactArgs(2),
	RunE:  runCreateConfig,
}

var listConfigCmd = &cobra.Command{
	Use:   "list",
	Short: "List all table configurations",
	Long:  "List all table configurations in the specified directory",
	RunE:  runListConfig,
}

var validateConfigCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate table configurations",
	Long:  "Validate all table configurations in the specified directory",
	RunE:  runValidateConfig,
}

var (
	configFile   string
	configFormat string
	autoDetect   bool
)

func init() {
	configCmd.AddCommand(createConfigCmd)
	configCmd.AddCommand(listConfigCmd)
	configCmd.AddCommand(validateConfigCmd)

	// Flags for create command
	createConfigCmd.Flags().StringVar(&configFile, "config", "", "Output configuration file path")
	createConfigCmd.Flags().StringVar(&configFormat, "format", "yaml", "Configuration file format (yaml, json)")
	createConfigCmd.Flags().BoolVar(&autoDetect, "auto-detect", true, "Auto-detect file format and columns")
	createConfigCmd.Flags().StringVar(&delimiter, "delimiter", ",", "CSV delimiter")
	createConfigCmd.Flags().BoolVar(&hasHeader, "header", true, "CSV has header row")
	createConfigCmd.Flags().BoolVar(&parallel, "parallel", false, "Enable parallel processing")
	createConfigCmd.Flags().IntVar(&workers, "workers", 0, "Number of worker threads (0 = auto)")
	createConfigCmd.Flags().IntVar(&chunkSize, "chunk-size", 1000, "Chunk size")
	createConfigCmd.Flags().IntVar(&bufferSize, "buffer-size", 100, "Buffer size")

	// Flags for list and validate commands
	listConfigCmd.Flags().StringVarP(&configDir, "config-dir", "c", ".", "Configuration directory")
	validateConfigCmd.Flags().StringVarP(&configDir, "config-dir", "c", ".", "Configuration directory")
}

func runCreateConfig(cmd *cobra.Command, args []string) error {
	tableName := args[0]
	filePath := args[1]

	// Check if file exists
	if _, err := os.Stat(filePath); err != nil {
		return fmt.Errorf("file not accessible: %w", err)
	}

	// Auto-detect format if enabled
	fileFormat := "csv"
	if autoDetect {
		ext := strings.ToLower(filepath.Ext(filePath))
		switch ext {
		case ".json":
			fileFormat = "json"
		case ".jsonl", ".ndjson":
			fileFormat = "jsonl"
		case ".yaml", ".yml":
			fileFormat = "yaml"
		case ".csv":
			fileFormat = "csv"
		case ".tsv":
			fileFormat = "csv"
			delimiter = "\t"
		case ".parquet":
			fileFormat = "parquet"
		}
	}

	// Create table configuration
	tableConfig := &types.TableConfig{
		Name:      tableName,
		FilePath:  filePath,
		Format:    fileFormat,
		Delimiter: delimiter,
		HasHeader: hasHeader,
		
		// Performance settings
		ParallelReading: parallel,
		WorkerThreads:   workers,
		ChunkSize:       chunkSize,
		BufferSize:      bufferSize,
	}

	// Auto-detect worker threads
	if tableConfig.WorkerThreads == 0 {
		tableConfig.WorkerThreads = runtime.NumCPU()
	}

	// Auto-detect columns if CSV with header
	if autoDetect && fileFormat == "csv" && hasHeader {
		columns, err := detectCSVColumns(filePath, delimiter)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: Could not auto-detect columns: %v\n", err)
		} else {
			tableConfig.Columns = columns
		}
	}

	// Determine output file path
	outputPath := configFile
	if outputPath == "" {
		ext := ".yaml"
		if configFormat == "json" {
			ext = ".json"
		}
		outputPath = tableName + ext
	}

	// Save configuration
	configManager := config.NewManager("")
	if err := configManager.SaveConfig(tableConfig, outputPath); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	fmt.Printf("Configuration created: %s\n", outputPath)
	
	// Show configuration summary
	fmt.Printf("\nTable: %s\n", tableConfig.Name)
	fmt.Printf("File: %s\n", tableConfig.FilePath)
	fmt.Printf("Format: %s\n", tableConfig.Format)
	if len(tableConfig.Columns) > 0 {
		fmt.Printf("Columns: %d detected\n", len(tableConfig.Columns))
		for i, col := range tableConfig.Columns {
			if i < 5 { // Show first 5 columns
				fmt.Printf("  - %s (%v)\n", col.Name, col.Type)
			}
		}
		if len(tableConfig.Columns) > 5 {
			fmt.Printf("  ... and %d more\n", len(tableConfig.Columns)-5)
		}
	}

	return nil
}

func runListConfig(cmd *cobra.Command, args []string) error {
	configManager := config.NewManager(configDir)
	configs, err := configManager.ListConfigs()
	if err != nil {
		return fmt.Errorf("failed to list configurations: %w", err)
	}

	if len(configs) == 0 {
		fmt.Printf("No configurations found in %s\n", configDir)
		return nil
	}

	fmt.Printf("Found %d table configuration(s) in %s:\n\n", len(configs), configDir)
	
	for _, config := range configs {
		fmt.Printf("Table: %s\n", config.Name)
		fmt.Printf("  File: %s\n", config.FilePath)
		fmt.Printf("  Format: %s\n", config.Format)
		
		// Check if file exists
		if _, err := os.Stat(config.FilePath); err != nil {
			fmt.Printf("  Status: ❌ File not accessible\n")
		} else {
			fmt.Printf("  Status: ✅ File accessible\n")
		}
		
		fmt.Printf("  Columns: %d\n", len(config.Columns))
		if config.ParallelReading {
			fmt.Printf("  Parallel: Yes (%d workers)\n", config.WorkerThreads)
		} else {
			fmt.Printf("  Parallel: No\n")
		}
		fmt.Println()
	}

	return nil
}

func runValidateConfig(cmd *cobra.Command, args []string) error {
	configManager := config.NewManager(configDir)
	configs, err := configManager.ListConfigs()
	if err != nil {
		return fmt.Errorf("failed to list configurations: %w", err)
	}

	if len(configs) == 0 {
		fmt.Printf("No configurations found in %s\n", configDir)
		return nil
	}

	fmt.Printf("Validating %d configuration(s)...\n\n", len(configs))
	
	var validCount, invalidCount int
	
	for _, config := range configs {
		fmt.Printf("Validating table '%s'...", config.Name)
		
		// Check if file exists and is readable
		fileInfo, err := os.Stat(config.FilePath)
		if err != nil {
			fmt.Printf(" ❌ File not accessible: %v\n", err)
			invalidCount++
			continue
		}
		
		// Check file size for parallel processing recommendations
		if config.ParallelReading && fileInfo.Size() < 1024*int64(config.WorkerThreads) {
			fmt.Printf(" ⚠️  Warning: File might be too small for parallel processing\n")
		}
		
		// Validate format
		if config.Format == "" {
			fmt.Printf(" ❌ No format specified\n")
			invalidCount++
			continue
		}
		
		// Check if we can read the file format
		validFormats := []string{"csv", "json", "jsonl", "yaml", "parquet"}
		validFormat := false
		for _, valid := range validFormats {
			if config.Format == valid {
				validFormat = true
				break
			}
		}
		
		if !validFormat {
			fmt.Printf(" ❌ Unsupported format: %s\n", config.Format)
			invalidCount++
			continue
		}
		
		fmt.Printf(" ✅ Valid\n")
		validCount++
	}
	
	fmt.Printf("\nValidation complete: %d valid, %d invalid\n", validCount, invalidCount)
	
	if invalidCount > 0 {
		return fmt.Errorf("validation failed for %d configuration(s)", invalidCount)
	}
	
	return nil
}

func detectCSVColumns(filePath, delimiter string) ([]types.Column, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Read first few lines to detect column types
	// scanner := fmt.Sprintf("head -n 10 %s", filePath)
	
	// This is a simplified implementation
	// In a full version, you'd parse the CSV and analyze data types
	
	var columns []types.Column
	
	// For now, just create generic string columns
	// A full implementation would analyze the actual data
	columnNames := []string{"col_0", "col_1", "col_2", "col_3", "col_4"}
	
	for i, name := range columnNames {
		columns = append(columns, types.Column{
			Name:     name,
			Type:     types.DataTypeString,
			Index:    i,
			Required: false,
		})
	}
	
	return columns, nil
}