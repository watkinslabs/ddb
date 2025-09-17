package query

import (
	"ddb/internal/storage"
	"ddb/pkg/types"
	"context"
	"fmt"
	"strings"
)

// Executor implements the QueryExecutor interface
type Executor struct {
	engine types.StorageEngine
	parser *Parser
}

// NewExecutor creates a new query executor
func NewExecutor() *Executor {
	return &Executor{
		engine: storage.NewStreamingEngine(),
		parser: NewParser(),
	}
}

// Execute executes a SQL query against the configured tables
func (e *Executor) Execute(ctx context.Context, sql string, tableConfigs map[string]*types.TableConfig) (types.ResultSet, error) {
	// Parse the SQL query
	plan, err := e.ParseQuery(sql)
	if err != nil {
		return types.ResultSet{}, fmt.Errorf("failed to parse query: %w", err)
	}

	// For write operations, use FileEngine instead of StreamingEngine
	if plan.Type == types.QueryTypeInsert || plan.Type == types.QueryTypeUpdate || 
	   plan.Type == types.QueryTypeDelete || plan.Type == types.QueryTypeUpsert {
		return e.executeWriteOperation(ctx, plan, tableConfigs)
	}

	// Register table configurations with the streaming engine for read operations
	streamingEngine := e.engine.(*storage.StreamingEngine)
	for tableName, config := range tableConfigs {
		if err := streamingEngine.RegisterTable(tableName, config); err != nil {
			return types.ResultSet{}, fmt.Errorf("failed to register table %s: %w", tableName, err)
		}
	}

	// Handle JOINs if present
	if len(plan.Joins) > 0 {
		return e.executeJoinQuery(ctx, plan, tableConfigs)
	}

	// Execute simple query
	return e.engine.Query(ctx, plan)
}

// executeJoinQuery handles queries with JOINs
func (e *Executor) executeJoinQuery(ctx context.Context, plan types.QueryPlan, tableConfigs map[string]*types.TableConfig) (types.ResultSet, error) {
	// For simplicity, we'll implement basic nested loop joins
	// In a production system, you'd implement more efficient join algorithms
	
	// Execute main table query - always select * for joins, column filtering happens later
	// Note: WHERE clause is applied after JOIN, not before, since it might reference joined columns
	mainPlan := types.QueryPlan{
		Type:      types.QueryTypeSelect,
		TableName: plan.TableName,
		Columns:   []string{"*"}, // Always select all columns for JOIN left table
		Where:     nil,           // WHERE clause applied after JOIN
	}
	
	leftResults, err := e.engine.Query(ctx, mainPlan)
	if err != nil {
		return types.ResultSet{}, fmt.Errorf("failed to execute main query: %w", err)
	}

	// Process each join
	currentResults := leftResults
	for _, join := range plan.Joins {
		// Get right table results
		rightTableConfig, exists := tableConfigs[join.TableName]
		if !exists {
			return types.ResultSet{}, fmt.Errorf("join table not found: %s", join.TableName)
		}

		// Register right table
		streamingEngine := e.engine.(*storage.StreamingEngine)
		if err := streamingEngine.RegisterTable(join.TableName, rightTableConfig); err != nil {
			return types.ResultSet{}, fmt.Errorf("failed to register join table %s: %w", join.TableName, err)
		}

		// Execute query on right table
		rightPlan := types.QueryPlan{
			Type:      types.QueryTypeSelect,
			TableName: join.TableName,
			Columns:   []string{"*"},
		}
		
		rightResults, err := e.engine.Query(ctx, rightPlan)
		if err != nil {
			return types.ResultSet{}, fmt.Errorf("failed to execute join query: %w", err)
		}

		// Perform the join
		currentResults, err = e.performJoin(currentResults, rightResults, join, plan.TableName)
		if err != nil {
			return types.ResultSet{}, fmt.Errorf("failed to perform join: %w", err)
		}
	}

	// Apply final SELECT, GROUP BY, ORDER BY, LIMIT
	return e.applyFinalOperations(currentResults, plan)
}

// performJoin performs the actual join operation
func (e *Executor) performJoin(left, right types.ResultSet, join types.JoinClause, leftTableName string) (types.ResultSet, error) {
	var resultRows []types.Row
	
	// Combine column names (handle duplicates by prefixing table name)
	var resultColumns []string
	for _, col := range left.Columns {
		resultColumns = append(resultColumns, col)
	}
	for _, col := range right.Columns {
		// Check for duplicate column names
		duplicate := false
		for _, leftCol := range left.Columns {
			if col == leftCol {
				duplicate = true
				break
			}
		}
		if duplicate {
			resultColumns = append(resultColumns, join.TableName+"."+col)
		} else {
			resultColumns = append(resultColumns, col)
		}
	}

	// Use the actual table names as aliases
	leftAlias := leftTableName
	
	// Perform join based on type
	switch join.Type {
	case types.JoinTypeInner:
		resultRows = e.performInnerJoin(left.Rows, right.Rows, join, leftAlias, resultColumns)
	case types.JoinTypeLeft:
		resultRows = e.performLeftJoin(left.Rows, right.Rows, join, leftAlias, resultColumns)
	case types.JoinTypeRight:
		resultRows = e.performRightJoin(left.Rows, right.Rows, join, leftAlias, resultColumns)
	case types.JoinTypeFull:
		resultRows = e.performFullJoin(left.Rows, right.Rows, join, leftAlias, resultColumns)
	default:
		resultRows = e.performInnerJoin(left.Rows, right.Rows, join, leftAlias, resultColumns)
	}

	return types.ResultSet{
		Columns: resultColumns,
		Rows:    resultRows,
		Count:   len(resultRows),
	}, nil
}

// performInnerJoin performs an inner join
func (e *Executor) performInnerJoin(leftRows, rightRows []types.Row, join types.JoinClause, leftAlias string, columns []string) []types.Row {
	var result []types.Row

	for _, leftRow := range leftRows {
		for _, rightRow := range rightRows {
			// Create combined row
			rightAlias := join.TableAlias
			if rightAlias == "" {
				rightAlias = join.TableName
			}
			combinedRow := e.combineRows(leftRow, rightRow, leftAlias, rightAlias)
			
			// Evaluate join condition
			if join.Condition != nil {
				match, err := join.Condition.Evaluate(combinedRow)
				if err != nil || !e.isTruthy(match) {
					continue
				}
			}

			result = append(result, combinedRow)
		}
	}

	return result
}

// performLeftJoin performs a left join
func (e *Executor) performLeftJoin(leftRows, rightRows []types.Row, join types.JoinClause, leftAlias string, columns []string) []types.Row {
	var result []types.Row

	for _, leftRow := range leftRows {
		matched := false
		
		for _, rightRow := range rightRows {
			rightAlias := join.TableAlias
			if rightAlias == "" {
				rightAlias = join.TableName
			}
			combinedRow := e.combineRows(leftRow, rightRow, leftAlias, rightAlias)
			
			if join.Condition != nil {
				match, err := join.Condition.Evaluate(combinedRow)
				if err != nil || !e.isTruthy(match) {
					continue
				}
			}

			result = append(result, combinedRow)
			matched = true
		}

		// If no match found, add left row with nulls for right side
		if !matched {
			rightAlias := join.TableAlias
			if rightAlias == "" {
				rightAlias = join.TableName
			}
			combinedRow := e.combineRows(leftRow, nil, leftAlias, rightAlias)
			result = append(result, combinedRow)
		}
	}

	return result
}

// performRightJoin performs a right join
func (e *Executor) performRightJoin(leftRows, rightRows []types.Row, join types.JoinClause, leftAlias string, columns []string) []types.Row {
	var result []types.Row

	for _, rightRow := range rightRows {
		matched := false
		
		for _, leftRow := range leftRows {
			rightAlias := join.TableAlias
			if rightAlias == "" {
				rightAlias = join.TableName
			}
			combinedRow := e.combineRows(leftRow, rightRow, leftAlias, rightAlias)
			
			if join.Condition != nil {
				match, err := join.Condition.Evaluate(combinedRow)
				if err != nil || !e.isTruthy(match) {
					continue
				}
			}

			result = append(result, combinedRow)
			matched = true
		}

		// If no match found, add right row with nulls for left side
		if !matched {
			rightAlias := join.TableAlias
			if rightAlias == "" {
				rightAlias = join.TableName
			}
			combinedRow := e.combineRows(nil, rightRow, leftAlias, rightAlias)
			result = append(result, combinedRow)
		}
	}

	return result
}

// performFullJoin performs a full outer join
func (e *Executor) performFullJoin(leftRows, rightRows []types.Row, join types.JoinClause, leftAlias string, columns []string) []types.Row {
	var result []types.Row
	usedRightRows := make(map[int]bool)

	// First pass: match left rows
	for _, leftRow := range leftRows {
		matched := false
		
		for i, rightRow := range rightRows {
			rightAlias := join.TableAlias
			if rightAlias == "" {
				rightAlias = join.TableName
			}
			combinedRow := e.combineRows(leftRow, rightRow, leftAlias, rightAlias)
			
			if join.Condition != nil {
				match, err := join.Condition.Evaluate(combinedRow)
				if err != nil || !e.isTruthy(match) {
					continue
				}
			}

			result = append(result, combinedRow)
			usedRightRows[i] = true
			matched = true
		}

		// If no match found, add left row with nulls
		if !matched {
			rightAlias := join.TableAlias
			if rightAlias == "" {
				rightAlias = join.TableName
			}
			combinedRow := e.combineRows(leftRow, nil, leftAlias, rightAlias)
			result = append(result, combinedRow)
		}
	}

	// Second pass: add unmatched right rows
	for i, rightRow := range rightRows {
		if !usedRightRows[i] {
			rightAlias := join.TableAlias
			if rightAlias == "" {
				rightAlias = join.TableName
			}
			combinedRow := e.combineRows(nil, rightRow, leftAlias, rightAlias)
			result = append(result, combinedRow)
		}
	}

	return result
}

// combineRows combines left and right rows for joins with proper aliasing
func (e *Executor) combineRows(leftRow, rightRow types.Row, leftAlias, rightAlias string) types.Row {
	combined := make(types.Row)
	
	// Add left row data with left table prefixes for duplicate columns
	if leftRow != nil {
		for key, value := range leftRow {
			// Always add the original key for backward compatibility
			combined[key] = value
			// Also add with left prefix if it might conflict
			if rightRow != nil {
				if _, exists := rightRow[key]; exists {
					if leftAlias != "" && leftAlias != "left" {
						combined[leftAlias+"."+key] = value
					} else {
						combined["left."+key] = value
					}
				}
			}
		}
	}
	
	// Add right row data with conflict resolution
	if rightRow != nil {
		for key, value := range rightRow {
			// Check for conflicts with left row
			if leftRow != nil {
				if _, exists := leftRow[key]; exists {
					// Prefix with right table alias/name to avoid conflict
					if rightAlias != "" {
						combined[rightAlias+"."+key] = value
					} else {
						combined["right."+key] = value
					}
				} else {
					combined[key] = value
				}
			} else {
				combined[key] = value
			}
		}
	}
	
	return combined
}

// applyFinalOperations applies SELECT, GROUP BY, ORDER BY, LIMIT to join results
func (e *Executor) applyFinalOperations(results types.ResultSet, plan types.QueryPlan) (types.ResultSet, error) {
	currentResults := results
	
	// Apply WHERE clause for JOIN queries (since it's not applied before JOIN)
	if plan.Where != nil {
		var filteredRows []types.Row
		for _, row := range currentResults.Rows {
			match, err := plan.Where.Evaluate(row)
			if err != nil || !e.isTruthy(match) {
				continue
			}
			filteredRows = append(filteredRows, row)
		}
		currentResults = types.ResultSet{
			Columns: currentResults.Columns,
			Rows:    filteredRows,
			Count:   len(filteredRows),
		}
	}
	
	// Apply column selection
	if len(plan.Columns) > 0 && !(len(plan.Columns) == 1 && plan.Columns[0] == "*") {
		finalRows := make([]types.Row, len(currentResults.Rows))
		for i, row := range currentResults.Rows {
			newRow := make(types.Row)
			for _, col := range plan.Columns {
				if value, exists := row[col]; exists {
					newRow[col] = value
				}
			}
			finalRows[i] = newRow
		}
		currentResults = types.ResultSet{
			Columns: plan.Columns,
			Rows:    finalRows,
			Count:   len(finalRows),
		}
	}
	
	// Apply GROUP BY
	if len(plan.GroupBy) > 0 {
		groupMap := make(map[string]types.Row)
		for _, row := range currentResults.Rows {
			// Build group key
			var keyParts []string
			for _, col := range plan.GroupBy {
				if value, exists := row[col]; exists {
					keyParts = append(keyParts, fmt.Sprintf("%v", value))
				} else {
					keyParts = append(keyParts, "NULL")
				}
			}
			key := strings.Join(keyParts, "|")
			
			// Use first row in each group (in a full implementation, you'd apply aggregations)
			if _, exists := groupMap[key]; !exists {
				groupMap[key] = row
			}
		}
		
		// Convert map back to slice
		var groupedRows []types.Row
		for _, row := range groupMap {
			groupedRows = append(groupedRows, row)
		}
		
		currentResults = types.ResultSet{
			Columns: currentResults.Columns,
			Rows:    groupedRows,
			Count:   len(groupedRows),
		}
	}
	
	return currentResults, nil
}

// executeWriteOperation handles INSERT, UPDATE, DELETE, UPSERT operations using FileEngine
func (e *Executor) executeWriteOperation(ctx context.Context, plan types.QueryPlan, tableConfigs map[string]*types.TableConfig) (types.ResultSet, error) {
	// Create a FileEngine for write operations
	fileEngine := storage.NewFileEngine()
	
	// Register table configurations with the file engine
	for tableName, config := range tableConfigs {
		if err := fileEngine.RegisterTable(tableName, config); err != nil {
			return types.ResultSet{}, fmt.Errorf("failed to register table %s: %w", tableName, err)
		}
	}
	
	// Execute the write operation
	var rowsAffected int
	var err error
	
	switch plan.Type {
	case types.QueryTypeInsert:
		rowsAffected, err = fileEngine.Insert(ctx, plan)
	case types.QueryTypeUpdate:
		rowsAffected, err = fileEngine.Update(ctx, plan)
	case types.QueryTypeDelete:
		rowsAffected, err = fileEngine.Delete(ctx, plan)
	case types.QueryTypeUpsert:
		rowsAffected, err = fileEngine.Upsert(ctx, plan)
	default:
		return types.ResultSet{}, fmt.Errorf("unsupported write operation: %v", plan.Type)
	}
	
	if err != nil {
		return types.ResultSet{}, err
	}
	
	// Return a result set indicating rows affected
	result := types.ResultSet{
		Columns: []string{"rows_affected"},
		Rows:    []types.Row{{"rows_affected": rowsAffected}},
		Count:   1,
	}
	
	return result, nil
}

// ParseQuery parses a SQL query string into a QueryPlan
func (e *Executor) ParseQuery(sql string) (types.QueryPlan, error) {
	return e.parser.ParseQuery(sql)
}

// Helper function
func (e *Executor) isTruthy(value interface{}) bool {
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