package parser

import (
	"ddb/pkg/types"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	ChunkSize = 1000 // Number of rows per chunk
)

// FileReaderImpl implements the FileReader interface
type FileReaderImpl struct {
	chunkID int
}

// NewFileReader creates a new file reader
func NewFileReader() *FileReaderImpl {
	return &FileReaderImpl{
		chunkID: 0,
	}
}

// Read reads data from the provided reader based on the format
func (fr *FileReaderImpl) Read(ctx context.Context, reader io.Reader, config types.TableConfig) (<-chan types.Chunk, error) {
	chunks := make(chan types.Chunk, 10)

	go func() {
		defer close(chunks)

		// Check if the file is compressed and decompress if needed
		decompressedReader, err := fr.decompressIfNeeded(reader, config.FilePath)
		if err != nil {
			// For compression errors, we'll let the individual format readers handle it
			// by using the original reader and letting them fail naturally
			decompressedReader = reader
		}

		switch strings.ToLower(config.Format) {
		case "csv":
			fr.readCSV(ctx, decompressedReader, config, chunks)
		case "json":
			fr.readJSON(ctx, decompressedReader, config, chunks)
		case "jsonl", "json-lines":
			fr.readJSONLines(ctx, decompressedReader, config, chunks)
		case "yaml":
			fr.readYAML(ctx, decompressedReader, config, chunks)
		case "parquet":
			// Parquet files handle compression internally, use original reader
			fr.readParquet(ctx, reader, config, chunks)
		default:
			fr.readCSV(ctx, decompressedReader, config, chunks) // Default to CSV
		}
	}()

	return chunks, nil
}

// readCSV reads CSV data in chunks using advanced CSV parser
func (fr *FileReaderImpl) readCSV(ctx context.Context, reader io.Reader, config types.TableConfig, chunks chan<- types.Chunk) {
	// Use advanced CSV parser for complex scenarios
	csvParser := NewAdvancedCSVParser(config)

	var headers []string
	var rowCount int
	var currentChunk []types.Row
	var startPos int64

	// Parse CSV in streaming fashion
	err := csvParser.ParseCSVStream(reader, func(record []string) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Handle headers
		if config.HasHeader && headers == nil {
			headers = record
			return nil
		}

		// Convert record to Row
		row := make(types.Row)
		for i, value := range record {
			var columnName string
			var columnType types.DataType = types.DataTypeString

			if len(headers) > i {
				columnName = headers[i]
			} else if len(config.Columns) > i {
				columnName = config.Columns[i].Name
				columnType = config.Columns[i].Type
			} else {
				columnName = fmt.Sprintf("col_%d", i)
			}

			// Convert value based on column type
			row[columnName] = fr.convertValue(value, columnType)
		}

		currentChunk = append(currentChunk, row)
		rowCount++

		// Send chunk when it reaches the size limit
		if len(currentChunk) >= ChunkSize {
			chunk := fr.createChunk(currentChunk, startPos, int64(rowCount))
			chunks <- chunk
			currentChunk = []types.Row{}
			startPos = int64(rowCount)
		}

		return nil
	})

	if err != nil {
		fmt.Printf("CSV parsing error: %v\n", err)
		return
	}

	// Send remaining data as the final chunk
	if len(currentChunk) > 0 {
		chunk := fr.createChunk(currentChunk, startPos, int64(rowCount))
		chunks <- chunk
	}
}

// readJSON reads JSON array data
func (fr *FileReaderImpl) readJSON(ctx context.Context, reader io.Reader, config types.TableConfig, chunks chan<- types.Chunk) {
	decoder := json.NewDecoder(reader)

	// Read opening bracket
	token, err := decoder.Token()
	if err != nil {
		return
	}
	if token != json.Delim('[') {
		return
	}

	var currentChunk []types.Row
	var rowCount int
	var startPos int64

	// Read array elements
	for decoder.More() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var obj map[string]interface{}
		if err := decoder.Decode(&obj); err != nil {
			continue
		}

		row := make(types.Row)
		for key, value := range obj {
			row[key] = value
		}

		currentChunk = append(currentChunk, row)
		rowCount++

		if len(currentChunk) >= ChunkSize {
			chunk := fr.createChunk(currentChunk, startPos, int64(rowCount))
			chunks <- chunk
			currentChunk = []types.Row{}
			startPos = int64(rowCount)
		}
	}

	// Send remaining data
	if len(currentChunk) > 0 {
		chunk := fr.createChunk(currentChunk, startPos, int64(rowCount))
		chunks <- chunk
	}
}

// readJSONLines reads JSON Lines format
func (fr *FileReaderImpl) readJSONLines(ctx context.Context, reader io.Reader, config types.TableConfig, chunks chan<- types.Chunk) {
	scanner := bufio.NewScanner(reader)
	
	var currentChunk []types.Row
	var rowCount int
	var startPos int64

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}

		row := make(types.Row)
		for key, value := range obj {
			row[key] = value
		}

		currentChunk = append(currentChunk, row)
		rowCount++

		if len(currentChunk) >= ChunkSize {
			chunk := fr.createChunk(currentChunk, startPos, int64(rowCount))
			chunks <- chunk
			currentChunk = []types.Row{}
			startPos = int64(rowCount)
		}
	}

	if len(currentChunk) > 0 {
		chunk := fr.createChunk(currentChunk, startPos, int64(rowCount))
		chunks <- chunk
	}
}

// readYAML reads YAML data
func (fr *FileReaderImpl) readYAML(ctx context.Context, reader io.Reader, config types.TableConfig, chunks chan<- types.Chunk) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return
	}

	var objects []map[string]interface{}
	if err := yaml.Unmarshal(data, &objects); err != nil {
		return
	}

	var currentChunk []types.Row
	var startPos int64

	for i, obj := range objects {
		select {
		case <-ctx.Done():
			return
		default:
		}

		row := make(types.Row)
		for key, value := range obj {
			row[key] = value
		}

		currentChunk = append(currentChunk, row)

		if len(currentChunk) >= ChunkSize {
			chunk := fr.createChunk(currentChunk, startPos, int64(i+1))
			chunks <- chunk
			currentChunk = []types.Row{}
			startPos = int64(i + 1)
		}
	}

	if len(currentChunk) > 0 {
		chunk := fr.createChunk(currentChunk, startPos, int64(len(objects)))
		chunks <- chunk
	}
}

// createChunk creates a chunk with hash
func (fr *FileReaderImpl) createChunk(rows []types.Row, startPos, endPos int64) types.Chunk {
	fr.chunkID++
	
	// Create hash of chunk data
	hasher := md5.New()
	for _, row := range rows {
		for key, value := range row {
			hasher.Write([]byte(fmt.Sprintf("%s:%v", key, value)))
		}
	}
	hash := fmt.Sprintf("%x", hasher.Sum(nil))

	return types.Chunk{
		ID:       fmt.Sprintf("chunk_%d", fr.chunkID),
		Hash:     hash,
		Rows:     rows,
		StartPos: startPos,
		EndPos:   endPos,
	}
}

// convertValue converts string value to the appropriate type
func (fr *FileReaderImpl) convertValue(value string, dataType types.DataType) interface{} {
	switch dataType {
	case types.DataTypeInt:
		if val, err := strconv.Atoi(value); err == nil {
			return val
		}
	case types.DataTypeFloat:
		if val, err := strconv.ParseFloat(value, 64); err == nil {
			return val
		}
	case types.DataTypeBool:
		if val, err := strconv.ParseBool(value); err == nil {
			return val
		}
	case types.DataTypeDecimal:
		if val, err := strconv.ParseFloat(value, 64); err == nil {
			return val
		}
	}
	return value // Default to string
}

// decompressIfNeeded detects and decompresses compressed files
func (fr *FileReaderImpl) decompressIfNeeded(reader io.Reader, filePath string) (io.Reader, error) {
	// Check if file is gzipped based on extension
	if strings.HasSuffix(strings.ToLower(filePath), ".gz") {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		return gzipReader, nil
	}

	// For future: Add support for other compression formats
	// if strings.HasSuffix(strings.ToLower(filePath), ".bz2") { ... }
	// if strings.HasSuffix(strings.ToLower(filePath), ".xz") { ... }

	// Return original reader if not compressed
	return reader, nil
}

// Close cleans up resources
func (fr *FileReaderImpl) Close() error {
	return nil
}