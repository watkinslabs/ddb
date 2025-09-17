package storage

import (
	"ddb/internal/parser"
	"ddb/pkg/types"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

// StreamingEngine evaluates queries on-the-fly without storing all data in memory
type StreamingEngine struct {
	mu       sync.RWMutex
	metadata map[string]*types.TableConfig
}

// NewStreamingEngine creates a new streaming storage engine
func NewStreamingEngine() *StreamingEngine {
	return &StreamingEngine{
		metadata: make(map[string]*types.TableConfig),
	}
}

// Query executes a query by streaming through the target file and evaluating on-the-fly
func (e *StreamingEngine) Query(ctx context.Context, query types.QueryPlan) (types.ResultSet, error) {
	// Get table configuration
	config, err := e.GetTableMetadata(query.TableName)
	if err != nil {
		return types.ResultSet{}, fmt.Errorf("table not found: %s", query.TableName)
	}

	// Open the target file
	file, err := os.Open(config.FilePath)
	if err != nil {
		return types.ResultSet{}, fmt.Errorf("failed to open file %s: %w", config.FilePath, err)
	}
	defer file.Close()

	// Create file reader (parallel or sequential based on config)
	var chunks <-chan types.Chunk
	
	if config.ParallelReading {
		parallelReader := parser.NewParallelFileReader(config.WorkerThreads, config.ChunkSize, config.BufferSize)
		chunks, err = parallelReader.Read(ctx, file, *config)
	} else {
		reader := parser.NewFileReader()
		chunks, err = reader.Read(ctx, file, *config)
	}
	if err != nil {
		return types.ResultSet{}, fmt.Errorf("failed to read file: %w", err)
	}

	// Process query with streaming evaluation
	return e.streamingQuery(ctx, chunks, query)
}

// streamingQuery processes chunks as they come in, evaluating the query on-the-fly
func (e *StreamingEngine) streamingQuery(ctx context.Context, chunks <-chan types.Chunk, query types.QueryPlan) (types.ResultSet, error) {
	var results []types.Row
	
	// For aggregations that need all data (GROUP BY, ORDER BY), we need temporary storage
	groupResults := make(map[string]types.Row)
	var ungroupedResults []types.Row

	hasGroupBy := len(query.GroupBy) > 0
	hasOrderBy := len(query.OrderBy) > 0
	
	// Track how many rows we've processed for LIMIT with OFFSET
	var processedCount int
	limitReached := false

	for chunk := range chunks {
		select {
		case <-ctx.Done():
			return types.ResultSet{}, ctx.Err()
		default:
		}

		for _, row := range chunk.Rows {
			// Apply WHERE clause first
			if query.Where != nil {
				match, err := query.Where.Evaluate(row)
				if err != nil || !e.isTruthy(match) {
					continue
				}
			}

			// Apply column/expression selection
			var selectedRow types.Row
			if len(query.SelectExprs) > 0 {
				// Use new expression-based selection
				selectedRow = e.evaluateSelectExpressions(row, query.SelectExprs)
			} else if len(query.Columns) == 1 && query.Columns[0] == "*" {
				// Wildcard selection
				selectedRow = row
			} else {
				// Legacy column selection
				selectedRow = e.selectColumns([]types.Row{row}, query.Columns)[0]
			}

			if hasGroupBy {
				// Handle GROUP BY - accumulate in groups
				groupKey := e.buildGroupKey(row, query.GroupBy)
				if _, exists := groupResults[groupKey]; !exists {
					groupResults[groupKey] = selectedRow
				}
				// Here you could add aggregation logic (COUNT, SUM, etc.)
			} else if hasOrderBy {
				// Need all rows for sorting
				ungroupedResults = append(ungroupedResults, selectedRow)
			} else {
				// No GROUP BY or ORDER BY - can apply LIMIT immediately
				if query.Limit != nil {
					if processedCount < query.Limit.Offset {
						processedCount++
						continue
					}
					if len(results) >= query.Limit.Count {
						limitReached = true
						break
					}
				}
				
				results = append(results, selectedRow)
			}
		}

		if limitReached {
			break
		}
	}

	// Finalize results based on query type
	if hasGroupBy {
		// Convert grouped results to slice
		for _, row := range groupResults {
			results = append(results, row)
		}
	} else if hasOrderBy {
		results = ungroupedResults
	}

	// Apply ORDER BY if needed
	if hasOrderBy {
		e.applyOrderBy(results, query.OrderBy)
	}

	// Apply LIMIT after ORDER BY
	if query.Limit != nil && !limitReached {
		start := query.Limit.Offset
		end := start + query.Limit.Count
		if start >= len(results) {
			results = []types.Row{}
		} else if end > len(results) {
			results = results[start:]
		} else {
			results = results[start:end]
		}
	}

	// Determine final column names
	var finalColumns []string
	if len(query.SelectExprs) > 0 {
		// Use columns from SelectExprs
		for _, selectExpr := range query.SelectExprs {
			if selectExpr.Alias != "" {
				finalColumns = append(finalColumns, selectExpr.Alias)
			} else {
				// Use expression string as column name
				exprStr := selectExpr.Expression.String()
				if strings.Contains(exprStr, ".") {
					// For table.column, use just the column part
					parts := strings.Split(exprStr, ".")
					if len(parts) >= 2 {
						finalColumns = append(finalColumns, parts[len(parts)-1])
					} else {
						finalColumns = append(finalColumns, exprStr)
					}
				} else {
					finalColumns = append(finalColumns, exprStr)
				}
			}
		}
	} else if len(query.Columns) == 1 && query.Columns[0] == "*" {
		// Extract column names from first row
		if len(results) > 0 {
			for key := range results[0] {
				finalColumns = append(finalColumns, key)
			}
		}
	} else {
		finalColumns = query.Columns
	}

	return types.ResultSet{
		Columns: finalColumns,
		Rows:    results,
		Count:   len(results),
	}, nil
}

// buildGroupKey creates a key for grouping rows
func (e *StreamingEngine) buildGroupKey(row types.Row, groupBy []string) string {
	var keyParts []string
	for _, col := range groupBy {
		if value, exists := row[col]; exists {
			keyParts = append(keyParts, fmt.Sprintf("%v", value))
		} else {
			keyParts = append(keyParts, "NULL")
		}
	}
	return strings.Join(keyParts, "|")
}

// selectColumns selects only the specified columns from rows
func (e *StreamingEngine) selectColumns(rows []types.Row, columns []string) []types.Row {
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

// evaluateSelectExpressions evaluates SELECT expressions and returns a row with results
func (e *StreamingEngine) evaluateSelectExpressions(row types.Row, selectExprs []types.SelectExpression) types.Row {
	result := make(types.Row)
	
	for _, selectExpr := range selectExprs {
		// Handle wildcard special case by checking expression string
		exprStr := selectExpr.Expression.String()
		if exprStr == "*" {
			// For wildcard, add all columns from the row
			for key, value := range row {
				result[key] = value
			}
			continue
		}
		
		// Evaluate the expression
		value, err := selectExpr.Expression.Evaluate(row)
		if err != nil {
			// If evaluation fails, use nil or empty string
			value = nil
		}
		
		// Determine the column name for the result
		columnName := selectExpr.Alias
		if columnName == "" {
			// Use the expression string as column name
			columnName = exprStr
			
			// Try to extract a cleaner name for simple column expressions
			if strings.Contains(exprStr, ".") {
				// For table.column, use just the column part
				parts := strings.Split(exprStr, ".")
				if len(parts) >= 2 {
					columnName = parts[len(parts)-1]
				}
			}
		}
		
		result[columnName] = value
	}
	
	return result
}

// applyOrderBy sorts rows by the specified columns
func (e *StreamingEngine) applyOrderBy(rows []types.Row, orderBy []types.OrderByClause) {
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

// compareValues compares two values
func (e *StreamingEngine) compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

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
func (e *StreamingEngine) isTruthy(value interface{}) bool {
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

// Store is not used in streaming engine - data is read directly from files
func (e *StreamingEngine) Store(ctx context.Context, tableID string, chunk types.Chunk) error {
	return fmt.Errorf("streaming engine does not store data - it reads directly from files")
}

// GetTableMetadata returns metadata for a table
func (e *StreamingEngine) GetTableMetadata(tableID string) (*types.TableConfig, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	if config, exists := e.metadata[tableID]; exists {
		return config, nil
	}
	return nil, fmt.Errorf("table metadata not found: %s", tableID)
}

// SetTableMetadata sets metadata for a table
func (e *StreamingEngine) SetTableMetadata(tableID string, config *types.TableConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	e.metadata[tableID] = config
}

// Clear removes metadata for a table (no data to clear in streaming engine)
func (e *StreamingEngine) Clear(tableID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	delete(e.metadata, tableID)
	return nil
}

// RegisterTable registers a file as a queryable table
func (e *StreamingEngine) RegisterTable(tableID string, config *types.TableConfig) error {
	// Validate that the file exists and is readable
	if _, err := os.Stat(config.FilePath); err != nil {
		return fmt.Errorf("file not accessible: %w", err)
	}
	
	e.SetTableMetadata(tableID, config)
	return nil
}

// Insert implements INSERT operations (not supported in streaming engine)
func (e *StreamingEngine) Insert(ctx context.Context, query types.QueryPlan) (int, error) {
	return 0, fmt.Errorf("INSERT not supported in streaming engine - use file engine for write operations")
}

// Update implements UPDATE operations (not supported in streaming engine)
func (e *StreamingEngine) Update(ctx context.Context, query types.QueryPlan) (int, error) {
	return 0, fmt.Errorf("UPDATE not supported in streaming engine - use file engine for write operations")
}

// Delete implements DELETE operations (not supported in streaming engine)
func (e *StreamingEngine) Delete(ctx context.Context, query types.QueryPlan) (int, error) {
	return 0, fmt.Errorf("DELETE not supported in streaming engine - use file engine for write operations")
}

// Upsert implements UPSERT operations (not supported in streaming engine)
func (e *StreamingEngine) Upsert(ctx context.Context, query types.QueryPlan) (int, error) {
	return 0, fmt.Errorf("UPSERT not supported in streaming engine - use file engine for write operations")
}