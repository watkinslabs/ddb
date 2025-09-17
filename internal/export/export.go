package export

import (
	"ddb/pkg/types"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/xuri/excelize/v2"
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

// ExcelExporter exports results to Excel format
type ExcelExporter struct {
	SheetName string
}

func NewExcelExporter(sheetName string) *ExcelExporter {
	if sheetName == "" {
		sheetName = "Data"
	}
	return &ExcelExporter{SheetName: sheetName}
}

func (e *ExcelExporter) Export(writer io.Writer, resultSet types.ResultSet) error {
	// Create a new Excel file
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Printf("Error closing Excel file: %v\n", err)
		}
	}()

	// Create or use existing sheet
	sheetIndex, err := f.NewSheet(e.SheetName)
	if err != nil {
		return fmt.Errorf("failed to create sheet: %w", err)
	}
	f.SetActiveSheet(sheetIndex)

	// Delete default Sheet1 if we created a custom sheet
	if e.SheetName != "Sheet1" {
		err = f.DeleteSheet("Sheet1")
		if err != nil {
			return fmt.Errorf("failed to delete default sheet: %w", err)
		}
	}

	// Set headers with styling
	headerStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold: true,
			Size: 12,
		},
		Fill: excelize.Fill{
			Type:    "pattern",
			Color:   []string{"#E6E6FA"},
			Pattern: 1,
		},
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create header style: %w", err)
	}

	// Write headers
	for i, col := range resultSet.Columns {
		cellRef, err := excelize.CoordinatesToCellName(i+1, 1)
		if err != nil {
			return fmt.Errorf("failed to get cell reference for header: %w", err)
		}
		if err := f.SetCellValue(e.SheetName, cellRef, col); err != nil {
			return fmt.Errorf("failed to set header cell: %w", err)
		}
		if err := f.SetCellStyle(e.SheetName, cellRef, cellRef, headerStyle); err != nil {
			return fmt.Errorf("failed to set header style: %w", err)
		}
	}

	// Create data style with borders
	dataStyle, err := f.NewStyle(&excelize.Style{
		Border: []excelize.Border{
			{Type: "left", Color: "000000", Style: 1},
			{Type: "top", Color: "000000", Style: 1},
			{Type: "bottom", Color: "000000", Style: 1},
			{Type: "right", Color: "000000", Style: 1},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create data style: %w", err)
	}

	// Write data rows
	for rowIdx, row := range resultSet.Rows {
		for colIdx, col := range resultSet.Columns {
			cellRef, err := excelize.CoordinatesToCellName(colIdx+1, rowIdx+2) // +2 because headers are in row 1
			if err != nil {
				return fmt.Errorf("failed to get cell reference for data: %w", err)
			}
			
			var value interface{}
			if v, exists := row[col]; exists {
				value = v
			} else {
				value = ""
			}
			
			if err := f.SetCellValue(e.SheetName, cellRef, value); err != nil {
				return fmt.Errorf("failed to set data cell: %w", err)
			}
			if err := f.SetCellStyle(e.SheetName, cellRef, cellRef, dataStyle); err != nil {
				return fmt.Errorf("failed to set data style: %w", err)
			}
		}
	}

	// Auto-adjust column widths
	for i, col := range resultSet.Columns {
		colName, err := excelize.ColumnNumberToName(i + 1)
		if err != nil {
			return fmt.Errorf("failed to get column name: %w", err)
		}
		
		// Calculate optimal width based on header and data
		maxWidth := float64(len(col))
		for _, row := range resultSet.Rows {
			if value, exists := row[col]; exists {
				valueStr := fmt.Sprintf("%v", value)
				if float64(len(valueStr)) > maxWidth {
					maxWidth = float64(len(valueStr))
				}
			}
		}
		
		// Set reasonable bounds for column width
		if maxWidth < 8 {
			maxWidth = 8
		}
		if maxWidth > 50 {
			maxWidth = 50
		}
		
		if err := f.SetColWidth(e.SheetName, colName, colName, maxWidth+1); err != nil {
			return fmt.Errorf("failed to set column width: %w", err)
		}
	}

	// Add a summary row with count
	summaryRow := len(resultSet.Rows) + 3 // +3 for header + data + empty row
	summaryCell, err := excelize.CoordinatesToCellName(1, summaryRow)
	if err != nil {
		return fmt.Errorf("failed to get summary cell reference: %w", err)
	}
	
	summaryText := fmt.Sprintf("Total Rows: %d", resultSet.Count)
	if err := f.SetCellValue(e.SheetName, summaryCell, summaryText); err != nil {
		return fmt.Errorf("failed to set summary cell: %w", err)
	}
	
	summaryStyle, err := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold: true,
			Color: "666666",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create summary style: %w", err)
	}
	if err := f.SetCellStyle(e.SheetName, summaryCell, summaryCell, summaryStyle); err != nil {
		return fmt.Errorf("failed to set summary style: %w", err)
	}

	// Write to the provided writer
	if err := f.Write(writer); err != nil {
		return fmt.Errorf("failed to write Excel file: %w", err)
	}

	return nil
}

func (e *ExcelExporter) ContentType() string {
	return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
}

func (e *ExcelExporter) FileExtension() string {
	return ".xlsx"
}

// ParquetExporter exports results to Parquet format using Apache Arrow
type ParquetExporter struct {
	CompressionCodec string
}

func NewParquetExporter(compression string) *ParquetExporter {
	// Validate compression type
	switch strings.ToLower(compression) {
	case "snappy", "gzip", "lz4", "zstd", "none":
		return &ParquetExporter{CompressionCodec: strings.ToLower(compression)}
	default:
		return &ParquetExporter{CompressionCodec: "snappy"} // Default to Snappy
	}
}

func (e *ParquetExporter) Export(writer io.Writer, resultSet types.ResultSet) error {
	if len(resultSet.Rows) == 0 {
		return fmt.Errorf("no data to export to Parquet")
	}

	// Create Arrow memory allocator
	pool := memory.NewGoAllocator()

	// Analyze data types from the first few rows
	schema, err := e.inferSchema(resultSet, pool)
	if err != nil {
		return fmt.Errorf("failed to infer schema: %w", err)
	}

	// Create record batch
	record, err := e.createRecord(resultSet, schema, pool)
	if err != nil {
		return fmt.Errorf("failed to create record: %w", err)
	}
	defer record.Release()

	// Create Parquet writer properties with compression
	var props *parquet.WriterProperties
	switch e.CompressionCodec {
	case "gzip":
		props = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Gzip))
	case "snappy":
		props = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	case "lz4":
		props = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Lz4))
	case "zstd":
		props = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	case "none":
		props = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	default:
		props = parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	}

	// Create Arrow writer properties
	arrowProps := pqarrow.DefaultWriterProps()

	// Write to Parquet
	pqWriter, err := pqarrow.NewFileWriter(schema, writer, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer pqWriter.Close()

	if err := pqWriter.Write(record); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

func (e *ParquetExporter) inferSchema(resultSet types.ResultSet, pool memory.Allocator) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(resultSet.Columns))
	
	for i, colName := range resultSet.Columns {
		// Analyze the column data to determine the best Arrow type
		dataType := e.inferColumnType(resultSet, colName)
		fields[i] = arrow.Field{
			Name: colName,
			Type: dataType,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func (e *ParquetExporter) inferColumnType(resultSet types.ResultSet, colName string) arrow.DataType {
	// Sample first few rows to infer type
	sampleSize := 10
	if len(resultSet.Rows) < sampleSize {
		sampleSize = len(resultSet.Rows)
	}

	hasString := false
	hasFloat := false
	hasInt := false

	for i := 0; i < sampleSize; i++ {
		value, exists := resultSet.Rows[i][colName]
		if !exists || value == nil {
			continue
		}

		switch value.(type) {
		case string:
			hasString = true
		case int, int32, int64:
			hasInt = true
		case float32, float64:
			hasFloat = true
		default:
			// Try to parse as number
			str := fmt.Sprintf("%v", value)
			if strings.Contains(str, ".") {
				hasFloat = true
			} else {
				// Try to determine if it's numeric
				hasString = true
			}
		}
	}

	// Determine the most appropriate type
	if hasString {
		return arrow.BinaryTypes.String
	} else if hasFloat {
		return arrow.PrimitiveTypes.Float64
	} else if hasInt {
		return arrow.PrimitiveTypes.Int64
	} else {
		// Default to string for safety
		return arrow.BinaryTypes.String
	}
}

func (e *ParquetExporter) createRecord(resultSet types.ResultSet, schema *arrow.Schema, pool memory.Allocator) (arrow.Record, error) {
	builders := make([]array.Builder, len(resultSet.Columns))
	
	// Create builders for each column
	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.STRING:
			builders[i] = array.NewStringBuilder(pool)
		case arrow.FLOAT64:
			builders[i] = array.NewFloat64Builder(pool)
		case arrow.INT64:
			builders[i] = array.NewInt64Builder(pool)
		default:
			builders[i] = array.NewStringBuilder(pool)
		}
	}

	// Fill builders with data
	for _, row := range resultSet.Rows {
		for i, colName := range resultSet.Columns {
			value, exists := row[colName]
			
			if !exists || value == nil {
				builders[i].AppendNull()
				continue
			}

			switch schema.Field(i).Type.ID() {
			case arrow.STRING:
				stringBuilder := builders[i].(*array.StringBuilder)
				stringBuilder.Append(fmt.Sprintf("%v", value))
				
			case arrow.FLOAT64:
				float64Builder := builders[i].(*array.Float64Builder)
				switch v := value.(type) {
				case float64:
					float64Builder.Append(v)
				case float32:
					float64Builder.Append(float64(v))
				case int:
					float64Builder.Append(float64(v))
				case int64:
					float64Builder.Append(float64(v))
				default:
					// Try to parse as float
					if floatVal, err := parseFloat(fmt.Sprintf("%v", value)); err == nil {
						float64Builder.Append(floatVal)
					} else {
						float64Builder.AppendNull()
					}
				}
				
			case arrow.INT64:
				int64Builder := builders[i].(*array.Int64Builder)
				switch v := value.(type) {
				case int64:
					int64Builder.Append(v)
				case int:
					int64Builder.Append(int64(v))
				case int32:
					int64Builder.Append(int64(v))
				default:
					// Try to parse as int
					if intVal, err := parseInt(fmt.Sprintf("%v", value)); err == nil {
						int64Builder.Append(intVal)
					} else {
						int64Builder.AppendNull()
					}
				}
				
			default:
				stringBuilder := builders[i].(*array.StringBuilder)
				stringBuilder.Append(fmt.Sprintf("%v", value))
			}
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	// Create record
	return array.NewRecord(schema, arrays, int64(len(resultSet.Rows))), nil
}

// Helper functions for type conversion
func parseFloat(s string) (float64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}
	var f float64
	n, err := fmt.Sscanf(s, "%f", &f)
	if err != nil || n != 1 {
		return 0, fmt.Errorf("invalid float: %s", s)
	}
	return f, nil
}

func parseInt(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}
	var i int64
	n, err := fmt.Sscanf(s, "%d", &i)
	if err != nil || n != 1 {
		return 0, fmt.Errorf("invalid integer: %s", s)
	}
	return i, nil
}

func (e *ParquetExporter) ContentType() string {
	return "application/octet-stream"
}

func (e *ParquetExporter) FileExtension() string {
	return ".parquet"
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

	case "excel", "xlsx":
		sheetName := "Data"
		if s, ok := options["sheet"].(string); ok {
			sheetName = s
		}
		return NewExcelExporter(sheetName), nil

	case "parquet":
		compression := "snappy"
		if c, ok := options["compression"].(string); ok {
			compression = c
		}
		return NewParquetExporter(compression), nil

	default:
		return nil, fmt.Errorf("unsupported export format: %s", format)
	}
}