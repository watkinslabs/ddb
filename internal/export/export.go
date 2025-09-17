package export

import (
	"ddb/pkg/types"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

// Exporter interface for different export formats
type Exporter interface {
	Export(writer io.Writer, resultSet types.ResultSet) error
	ContentType() string
	FileExtension() string
}

// CSVExporter exports results to CSV format
type CSVExporter struct {
	Delimiter rune
}

func NewCSVExporter(delimiter string) *CSVExporter {
	delim := ','
	if delimiter != "" {
		delim = rune(delimiter[0])
	}
	return &CSVExporter{Delimiter: delim}
}

func (e *CSVExporter) Export(writer io.Writer, resultSet types.ResultSet) error {
	csvWriter := csv.NewWriter(writer)
	csvWriter.Comma = e.Delimiter
	defer csvWriter.Flush()

	// Write header
	if err := csvWriter.Write(resultSet.Columns); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write data rows
	for _, row := range resultSet.Rows {
		record := make([]string, len(resultSet.Columns))
		for i, col := range resultSet.Columns {
			if value, exists := row[col]; exists {
				record[i] = fmt.Sprintf("%v", value)
			} else {
				record[i] = ""
			}
		}
		if err := csvWriter.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return nil
}

func (e *CSVExporter) ContentType() string {
	return "text/csv"
}

func (e *CSVExporter) FileExtension() string {
	return ".csv"
}

// JSONExporter exports results to JSON format
type JSONExporter struct {
	Pretty bool
}

func NewJSONExporter(pretty bool) *JSONExporter {
	return &JSONExporter{Pretty: pretty}
}

func (e *JSONExporter) Export(writer io.Writer, resultSet types.ResultSet) error {
	var output interface{}
	
	// Convert rows to slice of maps for JSON serialization
	jsonRows := make([]map[string]interface{}, len(resultSet.Rows))
	for i, row := range resultSet.Rows {
		jsonRow := make(map[string]interface{})
		for _, col := range resultSet.Columns {
			if value, exists := row[col]; exists {
				jsonRow[col] = value
			} else {
				jsonRow[col] = nil
			}
		}
		jsonRows[i] = jsonRow
	}

	// Create result structure
	output = map[string]interface{}{
		"columns": resultSet.Columns,
		"data":    jsonRows,
		"count":   resultSet.Count,
	}

	var err error
	if e.Pretty {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		err = encoder.Encode(output)
	} else {
		err = json.NewEncoder(writer).Encode(output)
	}

	if err != nil {
		return fmt.Errorf("failed to write JSON: %w", err)
	}

	return nil
}

func (e *JSONExporter) ContentType() string {
	return "application/json"
}

func (e *JSONExporter) FileExtension() string {
	return ".json"
}

// JSONLinesExporter exports results to JSON Lines format (one JSON object per line)
type JSONLinesExporter struct{}

func NewJSONLinesExporter() *JSONLinesExporter {
	return &JSONLinesExporter{}
}

func (e *JSONLinesExporter) Export(writer io.Writer, resultSet types.ResultSet) error {
	for _, row := range resultSet.Rows {
		jsonRow := make(map[string]interface{})
		for _, col := range resultSet.Columns {
			if value, exists := row[col]; exists {
				jsonRow[col] = value
			} else {
				jsonRow[col] = nil
			}
		}

		if err := json.NewEncoder(writer).Encode(jsonRow); err != nil {
			return fmt.Errorf("failed to write JSON line: %w", err)
		}
	}

	return nil
}

func (e *JSONLinesExporter) ContentType() string {
	return "application/x-ndjson"
}

func (e *JSONLinesExporter) FileExtension() string {
	return ".jsonl"
}

// YAMLExporter exports results to YAML format
type YAMLExporter struct{}

func NewYAMLExporter() *YAMLExporter {
	return &YAMLExporter{}
}

func (e *YAMLExporter) Export(writer io.Writer, resultSet types.ResultSet) error {
	// Convert rows to slice of maps for YAML serialization
	yamlRows := make([]map[string]interface{}, len(resultSet.Rows))
	for i, row := range resultSet.Rows {
		yamlRow := make(map[string]interface{})
		for _, col := range resultSet.Columns {
			if value, exists := row[col]; exists {
				yamlRow[col] = value
			} else {
				yamlRow[col] = nil
			}
		}
		yamlRows[i] = yamlRow
	}

	// Create result structure
	output := map[string]interface{}{
		"columns": resultSet.Columns,
		"data":    yamlRows,
		"count":   resultSet.Count,
	}

	if err := yaml.NewEncoder(writer).Encode(output); err != nil {
		return fmt.Errorf("failed to write YAML: %w", err)
	}

	return nil
}

func (e *YAMLExporter) ContentType() string {
	return "application/x-yaml"
}

func (e *YAMLExporter) FileExtension() string {
	return ".yaml"
}

// TableExporter exports results as a formatted table (for console output)
type TableExporter struct {
	MaxColWidth int
}

func NewTableExporter() *TableExporter {
	return &TableExporter{MaxColWidth: 50}
}

func (e *TableExporter) Export(writer io.Writer, resultSet types.ResultSet) error {
	if len(resultSet.Rows) == 0 {
		_, err := fmt.Fprintf(writer, "No results found.\n")
		return err
	}

	// Calculate column widths
	colWidths := make(map[string]int)
	for _, col := range resultSet.Columns {
		colWidths[col] = len(col)
	}

	for _, row := range resultSet.Rows {
		for _, col := range resultSet.Columns {
			if value, exists := row[col]; exists {
				valueStr := fmt.Sprintf("%v", value)
				if len(valueStr) > colWidths[col] {
					width := len(valueStr)
					if width > e.MaxColWidth {
						width = e.MaxColWidth
					}
					colWidths[col] = width
				}
			}
		}
	}

	// Print header
	e.printSeparator(writer, resultSet.Columns, colWidths)
	e.printRow(writer, resultSet.Columns, colWidths, resultSet.Columns)
	e.printSeparator(writer, resultSet.Columns, colWidths)

	// Print data rows
	for _, row := range resultSet.Rows {
		values := make([]string, len(resultSet.Columns))
		for i, col := range resultSet.Columns {
			if value, exists := row[col]; exists {
				valueStr := fmt.Sprintf("%v", value)
				if len(valueStr) > e.MaxColWidth {
					valueStr = valueStr[:e.MaxColWidth-3] + "..."
				}
				values[i] = valueStr
			} else {
				values[i] = ""
			}
		}
		e.printRow(writer, resultSet.Columns, colWidths, values)
	}

	e.printSeparator(writer, resultSet.Columns, colWidths)
	fmt.Fprintf(writer, "(%d rows)\n", resultSet.Count)

	return nil
}

func (e *TableExporter) printSeparator(writer io.Writer, columns []string, colWidths map[string]int) {
	fmt.Fprint(writer, "+")
	for _, col := range columns {
		fmt.Fprint(writer, strings.Repeat("-", colWidths[col]+2))
		fmt.Fprint(writer, "+")
	}
	fmt.Fprintln(writer)
}

func (e *TableExporter) printRow(writer io.Writer, columns []string, colWidths map[string]int, values []string) {
	fmt.Fprint(writer, "|")
	for i, col := range columns {
		value := ""
		if i < len(values) {
			value = values[i]
		}
		fmt.Fprintf(writer, " %-*s |", colWidths[col], value)
	}
	fmt.Fprintln(writer)
}

func (e *TableExporter) ContentType() string {
	return "text/plain"
}

func (e *TableExporter) FileExtension() string {
	return ".txt"
}

// GetExporter returns an exporter for the specified format
func GetExporter(format string, options map[string]interface{}) (Exporter, error) {
	switch strings.ToLower(format) {
	case "csv":
		delimiter := ","
		if d, ok := options["delimiter"].(string); ok {
			delimiter = d
		}
		return NewCSVExporter(delimiter), nil

	case "json":
		pretty := false
		if p, ok := options["pretty"].(bool); ok {
			pretty = p
		}
		return NewJSONExporter(pretty), nil

	case "jsonl", "json-lines", "ndjson":
		return NewJSONLinesExporter(), nil

	case "yaml", "yml":
		return NewYAMLExporter(), nil

	case "table", "text":
		return NewTableExporter(), nil

	default:
		return nil, fmt.Errorf("unsupported export format: %s", format)
	}
}