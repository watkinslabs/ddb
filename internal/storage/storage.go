package storage

import (
	"ddb/pkg/types"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// Engine implements the StorageEngine interface
type Engine struct {
	mu       sync.RWMutex
	tables   map[string]*Table
	metadata map[string]*types.TableConfig
}

// Table represents an in-memory table with hash maps for efficient lookups
type Table struct {
	chunks   map[string]*types.Chunk
	indexes  map[string]map[interface{}][]string // column -> value -> chunk IDs
	rowCount int
}

// NewEngine creates a new storage engine
func NewEngine() *Engine {
	return &Engine{
		tables:   make(map[string]*Table),
		metadata: make(map[string]*types.TableConfig),
	}
}

// Store stores a chunk in the specified table
func (e *Engine) Store(ctx context.Context, tableID string, chunk types.Chunk) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Create table if it doesn't exist
	if _, exists := e.tables[tableID]; !exists {
		e.tables[tableID] = &Table{
			chunks:  make(map[string]*types.Chunk),
			indexes: make(map[string]map[interface{}][]string),
		}
	}

	table := e.tables[tableID]
	
	// Store the chunk
	table.chunks[chunk.ID] = &chunk
	table.rowCount += len(chunk.Rows)

	// Build indexes for fast lookups
	e.buildIndexes(table, chunk)

	return nil
}

// buildIndexes builds hash map indexes for the chunk
func (e *Engine) buildIndexes(table *Table, chunk types.Chunk) {
	for _, row := range chunk.Rows {
		for columnName, value := range row {
			// Initialize column index if it doesn't exist
			if table.indexes[columnName] == nil {
				table.indexes[columnName] = make(map[interface{}][]string)
			}

			// Add chunk ID to the value's list
			chunkList := table.indexes[columnName][value]
			chunkList = append(chunkList, chunk.ID)
			table.indexes[columnName][value] = chunkList
		}
	}
}

// Query executes a query plan against the storage engine
func (e *Engine) Query(ctx context.Context, query types.QueryPlan) (types.ResultSet, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	table, exists := e.tables[query.TableName]
	if !exists {
		return types.ResultSet{}, fmt.Errorf("table not found: %s", query.TableName)
	}

	// Get relevant chunks based on WHERE clause
	relevantChunks := e.getRelevantChunks(table, query.Where)

	// Collect all matching rows
	var allRows []types.Row
	for _, chunkID := range relevantChunks {
		chunk := table.chunks[chunkID]
		for _, row := range chunk.Rows {
			// Apply WHERE clause
			if query.Where != nil {
				match, err := query.Where.Evaluate(row)
				if err != nil {
					continue
				}
				if !e.isTruthy(match) {
					continue
				}
			}
			allRows = append(allRows, row)
		}
	}

	// Apply GROUP BY
	if len(query.GroupBy) > 0 {
		allRows = e.applyGroupBy(allRows, query.GroupBy)
	}

	// Apply ORDER BY
	if len(query.OrderBy) > 0 {
		e.applyOrderBy(allRows, query.OrderBy)
	}

	// Apply LIMIT
	if query.Limit != nil {
		start := query.Limit.Offset
		end := start + query.Limit.Count
		if start >= len(allRows) {
			allRows = []types.Row{}
		} else if end > len(allRows) {
			allRows = allRows[start:]
		} else {
			allRows = allRows[start:end]
		}
	}

	// Select specific columns
	resultRows := e.selectColumns(allRows, query.Columns)

	return types.ResultSet{
		Columns: query.Columns,
		Rows:    resultRows,
		Count:   len(resultRows),
	}, nil
}

// getRelevantChunks uses indexes to find chunks that might contain matching rows
func (e *Engine) getRelevantChunks(table *Table, where types.Expression) []string {
	if where == nil {
		// Return all chunks
		chunks := make([]string, 0, len(table.chunks))
		for chunkID := range table.chunks {
			chunks = append(chunks, chunkID)
		}
		return chunks
	}

	// For now, return all chunks (in a full implementation, we'd analyze the WHERE clause)
	chunks := make([]string, 0, len(table.chunks))
	for chunkID := range table.chunks {
		chunks = append(chunks, chunkID)
	}
	return chunks
}

// selectColumns selects only the specified columns from rows
func (e *Engine) selectColumns(rows []types.Row, columns []string) []types.Row {
	if len(columns) == 0 || (len(columns) == 1 && columns[0] == "*") {
		return rows
	}

	result := make([]types.Row, len(rows))
	for i, row := range rows {
		newRow := make(types.Row)
		for _, col := range columns {
			if value, exists := row[col]; exists {
				newRow[col] = value
			}
		}
		result[i] = newRow
	}
	return result
}

// applyGroupBy groups rows by the specified columns
func (e *Engine) applyGroupBy(rows []types.Row, groupBy []string) []types.Row {
	groups := make(map[string][]types.Row)
	
	for _, row := range rows {
		// Create group key
		var keyParts []string
		for _, col := range groupBy {
			if value, exists := row[col]; exists {
				keyParts = append(keyParts, fmt.Sprintf("%v", value))
			}
		}
		key := strings.Join(keyParts, "|")
		
		groups[key] = append(groups[key], row)
	}

	// Return first row from each group (simplified aggregation)
	var result []types.Row
	for _, group := range groups {
		if len(group) > 0 {
			result = append(result, group[0])
		}
	}
	
	return result
}

// applyOrderBy sorts rows by the specified columns
func (e *Engine) applyOrderBy(rows []types.Row, orderBy []types.OrderByClause) {
	sort.Slice(rows, func(i, j int) bool {
		for _, clause := range orderBy {
			valI, existsI := rows[i][clause.Column]
			valJ, existsJ := rows[j][clause.Column]
			
			if !existsI || !existsJ {
				return existsI
			}
			
			cmp := e.compareValues(valI, valJ)
			if cmp != 0 {
				if clause.Desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})
}

// compareValues compares two values of unknown types
func (e *Engine) compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Convert to strings for comparison
	strA := fmt.Sprintf("%v", a)
	strB := fmt.Sprintf("%v", b)
	
	if strA < strB {
		return -1
	}
	if strA > strB {
		return 1
	}
	return 0
}

// isTruthy checks if a value is truthy
func (e *Engine) isTruthy(value interface{}) bool {
	if value == nil {
		return false
	}
	
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() != 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return v.Float() != 0
	case reflect.String:
		return v.String() != ""
	case reflect.Slice, reflect.Map, reflect.Array:
		return v.Len() != 0
	default:
		return true
	}
}

// GetTableMetadata returns metadata for a table
func (e *Engine) GetTableMetadata(tableID string) (*types.TableConfig, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	if config, exists := e.metadata[tableID]; exists {
		return config, nil
	}
	return nil, fmt.Errorf("table metadata not found: %s", tableID)
}

// SetTableMetadata sets metadata for a table
func (e *Engine) SetTableMetadata(tableID string, config *types.TableConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	e.metadata[tableID] = config
}

// Clear removes all data for a table
func (e *Engine) Clear(tableID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	delete(e.tables, tableID)
	delete(e.metadata, tableID)
	
	return nil
}