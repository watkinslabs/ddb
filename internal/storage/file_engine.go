package storage

import (
	"ddb/internal/parser"
	"ddb/pkg/types"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
)

// FileEngine implements storage with file locking for write operations
type FileEngine struct {
	mu       sync.RWMutex
	metadata map[string]*types.TableConfig
	fileLocks map[string]*sync.RWMutex // Per-file locks
	lockMu   sync.Mutex              // Protects fileLocks map
}

// NewFileEngine creates a new file-based storage engine
func NewFileEngine() *FileEngine {
	return &FileEngine{
		metadata:  make(map[string]*types.TableConfig),
		fileLocks: make(map[string]*sync.RWMutex),
	}
}

// getFileLock gets or creates a file-specific lock
func (e *FileEngine) getFileLock(filePath string) *sync.RWMutex {
	e.lockMu.Lock()
	defer e.lockMu.Unlock()
	
	if lock, exists := e.fileLocks[filePath]; exists {
		return lock
	}
	
	lock := &sync.RWMutex{}
	e.fileLocks[filePath] = lock
	return lock
}

// acquireExclusiveLock acquires an exclusive file lock
func (e *FileEngine) acquireExclusiveLock(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file for locking: %w", err)
	}
	
	// Try to acquire exclusive lock with timeout
	locked := make(chan bool, 1)
	go func() {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
		locked <- err == nil
	}()
	
	select {
	case success := <-locked:
		if !success {
			file.Close()
			return nil, fmt.Errorf("failed to acquire file lock")
		}
		return file, nil
	case <-time.After(10 * time.Second):
		file.Close()
		return nil, fmt.Errorf("timeout acquiring file lock")
	}
}

// Query implements read operations (same as streaming engine)
func (e *FileEngine) Query(ctx context.Context, query types.QueryPlan) (types.ResultSet, error) {
	// Get table configuration
	config, err := e.GetTableMetadata(query.TableName)
	if err != nil {
		return types.ResultSet{}, fmt.Errorf("table not found: %s", query.TableName)
	}

	// Acquire read lock
	fileLock := e.getFileLock(config.FilePath)
	fileLock.RLock()
	defer fileLock.RUnlock()

	// Open the target file
	file, err := os.Open(config.FilePath)
	if err != nil {
		return types.ResultSet{}, fmt.Errorf("failed to open file %s: %w", config.FilePath, err)
	}
	defer file.Close()

	// Use streaming engine logic for queries
	streamingEngine := NewStreamingEngine()
	streamingEngine.SetTableMetadata(query.TableName, config)
	return streamingEngine.Query(ctx, query)
}

// Insert implements INSERT operations
func (e *FileEngine) Insert(ctx context.Context, query types.QueryPlan) (int, error) {
	config, err := e.GetTableMetadata(query.TableName)
	if err != nil {
		return 0, fmt.Errorf("table not found: %s", query.TableName)
	}

	// Acquire exclusive lock
	fileLock := e.getFileLock(config.FilePath)
	fileLock.Lock()
	defer fileLock.Unlock()

	// Open file for appending
	file, err := os.OpenFile(config.FilePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file for writing: %w", err)
	}
	defer file.Close()

	rowsInserted := 0
	switch config.Format {
	case "csv":
		rowsInserted, err = e.insertCSV(file, query, config)
	case "json":
		rowsInserted, err = e.insertJSON(file, query, config)
	case "jsonl":
		rowsInserted, err = e.insertJSONL(file, query, config)
	default:
		return 0, fmt.Errorf("unsupported format for insert: %s", config.Format)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to insert data: %w", err)
	}

	return rowsInserted, nil
}

// insertCSV appends CSV rows to file
func (e *FileEngine) insertCSV(file *os.File, query types.QueryPlan, config *types.TableConfig) (int, error) {
	writer := csv.NewWriter(file)
	if config.Delimiter != "" {
		writer.Comma = rune(config.Delimiter[0])
	}
	defer writer.Flush()

	rowsInserted := 0
	for _, valueRow := range query.Values {
		record := make([]string, len(query.Columns))
		for i, value := range valueRow {
			if i < len(record) {
				record[i] = fmt.Sprintf("%v", value)
			}
		}
		
		if err := writer.Write(record); err != nil {
			return rowsInserted, err
		}
		rowsInserted++
	}

	return rowsInserted, nil
}

// insertJSONL appends JSON Lines to file
func (e *FileEngine) insertJSONL(file *os.File, query types.QueryPlan, config *types.TableConfig) (int, error) {
	rowsInserted := 0
	for _, valueRow := range query.Values {
		row := make(map[string]interface{})
		for i, value := range valueRow {
			if i < len(query.Columns) {
				row[query.Columns[i]] = value
			}
		}
		
		data, err := json.Marshal(row)
		if err != nil {
			return rowsInserted, err
		}
		
		if _, err := file.Write(append(data, '\n')); err != nil {
			return rowsInserted, err
		}
		rowsInserted++
	}

	return rowsInserted, nil
}

// insertJSON handles JSON format (more complex - need to rewrite entire file)
func (e *FileEngine) insertJSON(file *os.File, query types.QueryPlan, config *types.TableConfig) (int, error) {
	// For JSON arrays, we need to read existing data, append new data, and rewrite
	// This is more complex and less efficient - consider using JSONL instead
	return 0, fmt.Errorf("JSON array format insert not implemented - use JSONL format instead")
}

// Update implements UPDATE operations
func (e *FileEngine) Update(ctx context.Context, query types.QueryPlan) (int, error) {
	// SAFETY: Require WHERE clause for UPDATE
	if query.Where == nil {
		return 0, fmt.Errorf("UPDATE requires WHERE clause (use WHERE 1=1 to update all rows)")
	}

	config, err := e.GetTableMetadata(query.TableName)
	if err != nil {
		return 0, fmt.Errorf("table not found: %s", query.TableName)
	}

	// Acquire exclusive lock
	fileLock := e.getFileLock(config.FilePath)
	fileLock.Lock()
	defer fileLock.Unlock()

	// Read existing data
	existingData, err := e.readAllData(config)
	if err != nil {
		return 0, fmt.Errorf("failed to read existing data: %w", err)
	}

	// Apply updates
	rowsUpdated := 0
	for i, row := range existingData {
		// Check WHERE condition
		if query.Where != nil {
			match, err := query.Where.Evaluate(row)
			if err != nil || !e.isTruthy(match) {
				continue
			}
		}

		// Apply SET clauses
		for column, expr := range query.SetClauses {
			value, err := expr.Evaluate(row)
			if err != nil {
				continue
			}
			existingData[i][column] = value
		}
		rowsUpdated++
	}

	// Write back to file
	if err := e.writeAllData(config, existingData); err != nil {
		return 0, fmt.Errorf("failed to write updated data: %w", err)
	}

	return rowsUpdated, nil
}

// Delete implements DELETE operations
func (e *FileEngine) Delete(ctx context.Context, query types.QueryPlan) (int, error) {
	// SAFETY: Require WHERE clause for DELETE
	if query.Where == nil {
		return 0, fmt.Errorf("DELETE requires WHERE clause (use WHERE 1=1 to delete all rows)")
	}

	config, err := e.GetTableMetadata(query.TableName)
	if err != nil {
		return 0, fmt.Errorf("table not found: %s", query.TableName)
	}

	// Acquire exclusive lock
	fileLock := e.getFileLock(config.FilePath)
	fileLock.Lock()
	defer fileLock.Unlock()

	// Read existing data
	existingData, err := e.readAllData(config)
	if err != nil {
		return 0, fmt.Errorf("failed to read existing data: %w", err)
	}

	// Filter out rows matching WHERE condition
	var remainingData []types.Row
	rowsDeleted := 0
	
	for _, row := range existingData {
		// Check WHERE condition (guaranteed to exist due to safety check)
		match, err := query.Where.Evaluate(row)
		shouldDelete := err == nil && e.isTruthy(match)

		if shouldDelete {
			rowsDeleted++
		} else {
			remainingData = append(remainingData, row)
		}
	}

	// Write remaining data back to file
	if err := e.writeAllData(config, remainingData); err != nil {
		return 0, fmt.Errorf("failed to write remaining data: %w", err)
	}

	return rowsDeleted, nil
}

// Upsert implements UPSERT operations (INSERT or UPDATE)
func (e *FileEngine) Upsert(ctx context.Context, query types.QueryPlan) (int, error) {
	// For now, implement as simple INSERT
	// In production, you'd define primary keys and check for conflicts
	return e.Insert(ctx, query)
}

// readAllData reads all data from a file into memory
func (e *FileEngine) readAllData(config *types.TableConfig) ([]types.Row, error) {
	file, err := os.Open(config.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := parser.NewFileReader()
	chunks, err := reader.Read(context.Background(), file, *config)
	if err != nil {
		return nil, err
	}

	var allRows []types.Row
	for chunk := range chunks {
		allRows = append(allRows, chunk.Rows...)
	}

	return allRows, nil
}

// writeAllData writes all data back to a file
func (e *FileEngine) writeAllData(config *types.TableConfig, rows []types.Row) error {
	// Create temporary file
	tempFile := config.FilePath + ".tmp"
	file, err := os.Create(tempFile)
	if err != nil {
		return err
	}

	switch config.Format {
	case "csv":
		err = e.writeCSV(file, rows, config)
	case "jsonl":
		err = e.writeJSONL(file, rows, config)
	default:
		file.Close()
		os.Remove(tempFile)
		return fmt.Errorf("unsupported format for write: %s", config.Format)
	}

	file.Close()
	if err != nil {
		os.Remove(tempFile)
		return err
	}

	// Atomically replace original file
	return os.Rename(tempFile, config.FilePath)
}

// writeCSV writes rows to CSV format
func (e *FileEngine) writeCSV(file *os.File, rows []types.Row, config *types.TableConfig) error {
	writer := csv.NewWriter(file)
	if config.Delimiter != "" {
		writer.Comma = rune(config.Delimiter[0])
	}
	defer writer.Flush()

	// Write header if configured
	if config.HasHeader && len(config.Columns) > 0 {
		header := make([]string, len(config.Columns))
		for i, col := range config.Columns {
			header[i] = col.Name
		}
		if err := writer.Write(header); err != nil {
			return err
		}
	}

	// Write data rows
	for _, row := range rows {
		record := make([]string, len(config.Columns))
		for i, col := range config.Columns {
			if value, exists := row[col.Name]; exists {
				record[i] = fmt.Sprintf("%v", value)
			}
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// writeJSONL writes rows to JSON Lines format
func (e *FileEngine) writeJSONL(file *os.File, rows []types.Row, config *types.TableConfig) error {
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return err
		}
		if _, err := file.Write(append(data, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// Helper methods (same as streaming engine)
func (e *FileEngine) isTruthy(value interface{}) bool {
	if value == nil {
		return false
	}
	
	switch v := value.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case float64:
		return v != 0
	case string:
		return v != ""
	default:
		return true
	}
}

// Store is not used in file engine
func (e *FileEngine) Store(ctx context.Context, tableID string, chunk types.Chunk) error {
	return fmt.Errorf("file engine does not store chunks - it reads/writes files directly")
}

// GetTableMetadata returns metadata for a table
func (e *FileEngine) GetTableMetadata(tableID string) (*types.TableConfig, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	if config, exists := e.metadata[tableID]; exists {
		return config, nil
	}
	return nil, fmt.Errorf("table metadata not found: %s", tableID)
}

// SetTableMetadata sets metadata for a table
func (e *FileEngine) SetTableMetadata(tableID string, config *types.TableConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	e.metadata[tableID] = config
}

// Clear removes metadata for a table
func (e *FileEngine) Clear(tableID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	delete(e.metadata, tableID)
	return nil
}

// RegisterTable registers a file as a queryable table
func (e *FileEngine) RegisterTable(tableID string, config *types.TableConfig) error {
	// Validate that the file exists
	if _, err := os.Stat(config.FilePath); err != nil {
		return fmt.Errorf("file not accessible: %w", err)
	}
	
	e.SetTableMetadata(tableID, config)
	return nil
}