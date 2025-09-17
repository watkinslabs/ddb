package parser

import (
	"ddb/pkg/types"
	"bufio"
	"context"
	"crypto/md5"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

)

// ParallelFileReader implements parallel file reading with configurable worker threads
type ParallelFileReader struct {
	workerCount int
	chunkSize   int
	bufferSize  int
}

// NewParallelFileReader creates a new parallel file reader
func NewParallelFileReader(workerCount, chunkSize, bufferSize int) *ParallelFileReader {
	if workerCount <= 0 {
		workerCount = runtime.NumCPU()
	}
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	if bufferSize <= 0 {
		bufferSize = 100
	}

	return &ParallelFileReader{
		workerCount: workerCount,
		chunkSize:   chunkSize,
		bufferSize:  bufferSize,
	}
}

// WorkUnit represents a unit of work for parallel processing
type WorkUnit struct {
	ID       int
	Data     []byte
	StartPos int64
	EndPos   int64
	Format   string
	Config   types.TableConfig
}

// Read reads data from the provided reader using parallel processing
func (pr *ParallelFileReader) Read(ctx context.Context, reader io.Reader, config types.TableConfig) (<-chan types.Chunk, error) {
	chunks := make(chan types.Chunk, pr.bufferSize)

	// For parallel reading, we need to work with seekable files
	if file, ok := reader.(*os.File); ok && config.ParallelReading {
		go pr.readFileParallel(ctx, file, config, chunks)
	} else {
		// Fall back to sequential reading
		go pr.readSequential(ctx, reader, config, chunks)
	}

	return chunks, nil
}

// readFileParallel reads a file in parallel by splitting it into segments
func (pr *ParallelFileReader) readFileParallel(ctx context.Context, file *os.File, config types.TableConfig, chunks chan<- types.Chunk) {
	defer close(chunks)

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return
	}
	fileSize := fileInfo.Size()

	if fileSize == 0 {
		return
	}

	// For CSV files with headers, we need special handling
	var headerLine []byte
	if config.HasHeader && config.Format == "csv" {
		// Read the first line to get headers
		file.Seek(0, 0)
		scanner := bufio.NewScanner(file)
		if scanner.Scan() {
			headerLine = append([]byte{}, scanner.Bytes()...)
			headerLine = append(headerLine, '\n')
		}
	}

	// Calculate segments for parallel processing
	segmentSize := fileSize / int64(pr.workerCount)
	if segmentSize < 1024 {
		// File too small for parallel processing
		pr.readSequential(ctx, file, config, chunks)
		return
	}

	// Create work units
	workUnits := make(chan WorkUnit, pr.workerCount)
	resultChunks := make(chan types.Chunk, pr.bufferSize)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < pr.workerCount; i++ {
		wg.Add(1)
		go pr.worker(ctx, &wg, workUnits, resultChunks, headerLine)
	}

	// Generate work units
	go func() {
		defer close(workUnits)
		
		startOffset := int64(0)
		if config.HasHeader && config.Format == "csv" {
			startOffset = int64(len(headerLine))
		}

		for i := 0; i < pr.workerCount; i++ {
			start := startOffset + int64(i)*segmentSize
			end := start + segmentSize
			
			// Last segment gets remainder
			if i == pr.workerCount-1 {
				end = fileSize
			}

			if start >= fileSize {
				break
			}

			// Adjust boundaries to line endings for text formats
			if config.Format == "csv" || config.Format == "jsonl" {
				start, end = pr.adjustBoundaries(file, start, end, fileSize)
			}

			// Read segment data
			segmentData, err := pr.readSegment(file, start, end)
			if err != nil {
				continue
			}

			workUnit := WorkUnit{
				ID:       i,
				Data:     segmentData,
				StartPos: start,
				EndPos:   end,
				Format:   config.Format,
				Config:   config,
			}

			select {
			case workUnits <- workUnit:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect results and send them in order
	go func() {
		defer close(resultChunks)
		wg.Wait()
	}()

	// Forward results to output channel
	var chunkID int
	for chunk := range resultChunks {
		chunk.ID = fmt.Sprintf("chunk_%d", chunkID)
		chunkID++
		
		select {
		case chunks <- chunk:
		case <-ctx.Done():
			return
		}
	}
}

// worker processes work units in parallel
func (pr *ParallelFileReader) worker(ctx context.Context, wg *sync.WaitGroup, workUnits <-chan WorkUnit, results chan<- types.Chunk, headerLine []byte) {
	defer wg.Done()

	for workUnit := range workUnits {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Process the work unit
		chunks := pr.processWorkUnit(ctx, workUnit, headerLine)
		for _, chunk := range chunks {
			select {
			case results <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}
}

// processWorkUnit processes a single work unit and returns chunks
func (pr *ParallelFileReader) processWorkUnit(ctx context.Context, unit WorkUnit, headerLine []byte) []types.Chunk {
	var chunks []types.Chunk
	data := unit.Data

	// Add header line for CSV processing
	if len(headerLine) > 0 && unit.Config.Format == "csv" {
		data = append(headerLine, data...)
	}

	reader := strings.NewReader(string(data))

	switch strings.ToLower(unit.Config.Format) {
	case "csv":
		chunks = pr.processCSVWorkUnit(ctx, reader, unit)
	case "json":
		chunks = pr.processJSONWorkUnit(ctx, reader, unit)
	case "jsonl", "json-lines":
		chunks = pr.processJSONLWorkUnit(ctx, reader, unit)
	case "yaml":
		chunks = pr.processYAMLWorkUnit(ctx, reader, unit)
	default:
		chunks = pr.processCSVWorkUnit(ctx, reader, unit)
	}

	return chunks
}

// processCSVWorkUnit processes CSV data in a work unit
func (pr *ParallelFileReader) processCSVWorkUnit(ctx context.Context, reader io.Reader, unit WorkUnit) []types.Chunk {
	var chunks []types.Chunk
	var currentChunk []types.Row
	var headers []string
	
	csvReader := csv.NewReader(reader)
	if unit.Config.Delimiter != "" {
		csvReader.Comma = rune(unit.Config.Delimiter[0])
	}

	rowCount := 0
	for {
		select {
		case <-ctx.Done():
			return chunks
		default:
		}

		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		// Handle headers
		if unit.Config.HasHeader && headers == nil {
			headers = record
			continue
		}

		// Convert record to Row
		row := make(types.Row)
		for i, value := range record {
			var columnName string
			var columnType types.DataType = types.DataTypeString

			if len(headers) > i {
				columnName = headers[i]
			} else if len(unit.Config.Columns) > i {
				columnName = unit.Config.Columns[i].Name
				columnType = unit.Config.Columns[i].Type
			} else {
				columnName = fmt.Sprintf("col_%d", i)
			}

			row[columnName] = pr.convertValue(value, columnType)
		}

		currentChunk = append(currentChunk, row)
		rowCount++

		// Create chunk when it reaches size limit
		if len(currentChunk) >= pr.chunkSize {
			chunk := pr.createChunk(currentChunk, unit.StartPos+int64(rowCount-len(currentChunk)), unit.StartPos+int64(rowCount))
			chunks = append(chunks, chunk)
			currentChunk = []types.Row{}
		}
	}

	// Add remaining data
	if len(currentChunk) > 0 {
		chunk := pr.createChunk(currentChunk, unit.StartPos+int64(rowCount-len(currentChunk)), unit.StartPos+int64(rowCount))
		chunks = append(chunks, chunk)
	}

	return chunks
}

// processJSONLWorkUnit processes JSON Lines data
func (pr *ParallelFileReader) processJSONLWorkUnit(ctx context.Context, reader io.Reader, unit WorkUnit) []types.Chunk {
	var chunks []types.Chunk
	var currentChunk []types.Row
	
	scanner := bufio.NewScanner(reader)
	rowCount := 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return chunks
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

		if len(currentChunk) >= pr.chunkSize {
			chunk := pr.createChunk(currentChunk, unit.StartPos+int64(rowCount-len(currentChunk)), unit.StartPos+int64(rowCount))
			chunks = append(chunks, chunk)
			currentChunk = []types.Row{}
		}
	}

	if len(currentChunk) > 0 {
		chunk := pr.createChunk(currentChunk, unit.StartPos+int64(rowCount-len(currentChunk)), unit.StartPos+int64(rowCount))
		chunks = append(chunks, chunk)
	}

	return chunks
}

// processJSONWorkUnit and processYAMLWorkUnit would be similar...
func (pr *ParallelFileReader) processJSONWorkUnit(ctx context.Context, reader io.Reader, unit WorkUnit) []types.Chunk {
	// For JSON arrays, parallel processing is more complex
	// For now, fall back to sequential processing within the work unit
	return []types.Chunk{}
}

func (pr *ParallelFileReader) processYAMLWorkUnit(ctx context.Context, reader io.Reader, unit WorkUnit) []types.Chunk {
	// Similar to JSON, YAML parallel processing is complex
	return []types.Chunk{}
}

// adjustBoundaries adjusts segment boundaries to line endings
func (pr *ParallelFileReader) adjustBoundaries(file *os.File, start, end, fileSize int64) (int64, int64) {
	// Adjust start to next line boundary (unless it's 0)
	if start > 0 {
		file.Seek(start, 0)
		scanner := bufio.NewScanner(file)
		if scanner.Scan() {
			// Move to start of next line
			start = start + int64(len(scanner.Bytes())) + 1
		}
	}

	// Adjust end to line boundary
	if end < fileSize {
		file.Seek(end, 0)
		scanner := bufio.NewScanner(file)
		if scanner.Scan() {
			// Include the complete line
			end = end + int64(len(scanner.Bytes())) + 1
		}
	}

	return start, end
}

// readSegment reads a segment of the file
func (pr *ParallelFileReader) readSegment(file *os.File, start, end int64) ([]byte, error) {
	size := end - start
	data := make([]byte, size)
	
	_, err := file.ReadAt(data, start)
	if err != nil && err != io.EOF {
		return nil, err
	}
	
	return data, nil
}

// readSequential falls back to sequential reading
func (pr *ParallelFileReader) readSequential(ctx context.Context, reader io.Reader, config types.TableConfig, chunks chan<- types.Chunk) {
	defer close(chunks)
	
	// Use the original FileReaderImpl for sequential reading
	sequentialReader := NewFileReader()
	sequentialChunks, err := sequentialReader.Read(ctx, reader, config)
	if err != nil {
		return
	}

	for chunk := range sequentialChunks {
		select {
		case chunks <- chunk:
		case <-ctx.Done():
			return
		}
	}
}

// Helper functions (same as in regular parser)
func (pr *ParallelFileReader) createChunk(rows []types.Row, startPos, endPos int64) types.Chunk {
	// Create hash of chunk data
	hasher := md5.New()
	for _, row := range rows {
		for key, value := range row {
			hasher.Write([]byte(fmt.Sprintf("%s:%v", key, value)))
		}
	}
	hash := fmt.Sprintf("%x", hasher.Sum(nil))

	return types.Chunk{
		ID:       "", // Will be set by caller
		Hash:     hash,
		Rows:     rows,
		StartPos: startPos,
		EndPos:   endPos,
	}
}

func (pr *ParallelFileReader) convertValue(value string, dataType types.DataType) interface{} {
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
	return value
}

// Close cleans up resources
func (pr *ParallelFileReader) Close() error {
	return nil
}