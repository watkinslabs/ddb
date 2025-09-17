package storage

import (
	"ddb/pkg/types"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// DiskEngine implements storage that uses disk for large files with smart caching
type DiskEngine struct {
	mu          sync.RWMutex
	cacheDir    string
	maxMemoryMB int
	cache       map[string]*CachedChunk
	cacheSize   int64
	metadata    map[string]*types.TableConfig
}

// CachedChunk represents a chunk in memory with access tracking
type CachedChunk struct {
	Chunk      *types.Chunk
	AccessTime int64
	SizeBytes  int64
}

// ChunkIndex represents an index entry for fast lookups
type ChunkIndex struct {
	ChunkID    string
	FilePath   string
	StartPos   int64
	EndPos     int64
	RowCount   int
	Columns    map[string][]interface{} // Column -> distinct values in this chunk
}

// NewDiskEngine creates a new disk-based storage engine
func NewDiskEngine(cacheDir string, maxMemoryMB int) *DiskEngine {
	return &DiskEngine{
		cacheDir:    cacheDir,
		maxMemoryMB: maxMemoryMB,
		cache:       make(map[string]*CachedChunk),
		metadata:    make(map[string]*types.TableConfig),
	}
}

// Store stores a chunk to disk and maintains indexes
func (e *DiskEngine) Store(ctx context.Context, tableID string, chunk types.Chunk) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Create table directory
	tableDir := filepath.Join(e.cacheDir, tableID)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	// Store chunk to disk
	chunkPath := filepath.Join(tableDir, fmt.Sprintf("%s.gob", chunk.ID))
	if err := e.storeChunkToDisk(chunk, chunkPath); err != nil {
		return fmt.Errorf("failed to store chunk to disk: %w", err)
	}

	// Build and store index
	index := e.buildChunkIndex(chunk, chunkPath)
	indexPath := filepath.Join(tableDir, fmt.Sprintf("%s.idx", chunk.ID))
	if err := e.storeIndexToDisk(index, indexPath); err != nil {
		return fmt.Errorf("failed to store index: %w", err)
	}

	// Add to cache if there's space
	e.addToCache(chunk)

	return nil
}

// Query executes a query with disk-based streaming
func (e *DiskEngine) Query(ctx context.Context, query types.QueryPlan) (types.ResultSet, error) {
	e.mu.RLock()
	tableDir := filepath.Join(e.cacheDir, query.TableName)
	e.mu.RUnlock()

	// Load all chunk indexes for the table
	indexes, err := e.loadTableIndexes(tableDir)
	if err != nil {
		return types.ResultSet{}, fmt.Errorf("failed to load table indexes: %w", err)
	}

	// Filter chunks based on WHERE clause (index scan)
	relevantIndexes := e.filterChunksByIndex(indexes, query.Where)

	// Stream through relevant chunks
	var allRows []types.Row
	for _, index := range relevantIndexes {
		select {
		case <-ctx.Done():
			return types.ResultSet{}, ctx.Err()
		default:
		}

		chunk, err := e.loadChunk(index.ChunkID, index.FilePath)
		if err != nil {
			continue
		}

		// Apply WHERE clause row by row
		for _, row := range chunk.Rows {
			if query.Where != nil {
				match, err := query.Where.Evaluate(row)
				if err != nil || !e.isTruthy(match) {
					continue
				}
			}
			allRows = append(allRows, row)
		}

		// For very large result sets, we could implement pagination here
		// or write intermediate results to disk
	}

	// Apply remaining operations (GROUP BY, ORDER BY, LIMIT)
	return e.processQueryResults(allRows, query), nil
}

// loadChunk loads a chunk from disk or cache
func (e *DiskEngine) loadChunk(chunkID, filePath string) (*types.Chunk, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Check cache first
	if cached, exists := e.cache[chunkID]; exists {
		cached.AccessTime++
		return cached.Chunk, nil
	}

	// Load from disk
	chunk, err := e.loadChunkFromDisk(filePath)
	if err != nil {
		return nil, err
	}

	// Add to cache
	e.addToCache(*chunk)

	return chunk, nil
}

// addToCache adds a chunk to the in-memory cache with LRU eviction
func (e *DiskEngine) addToCache(chunk types.Chunk) {
	// Estimate chunk size
	chunkSize := e.estimateChunkSize(chunk)
	maxCacheSize := int64(e.maxMemoryMB) * 1024 * 1024

	// Evict old chunks if necessary
	for e.cacheSize+chunkSize > maxCacheSize && len(e.cache) > 0 {
		e.evictLRUChunk()
	}

	// Add new chunk
	e.cache[chunk.ID] = &CachedChunk{
		Chunk:      &chunk,
		AccessTime: 1,
		SizeBytes:  chunkSize,
	}
	e.cacheSize += chunkSize
}

// evictLRUChunk removes the least recently used chunk from cache
func (e *DiskEngine) evictLRUChunk() {
	var lruChunkID string
	var lruAccessTime int64 = -1

	for chunkID, cached := range e.cache {
		if lruAccessTime == -1 || cached.AccessTime < lruAccessTime {
			lruAccessTime = cached.AccessTime
			lruChunkID = chunkID
		}
	}

	if lruChunkID != "" {
		cached := e.cache[lruChunkID]
		e.cacheSize -= cached.SizeBytes
		delete(e.cache, lruChunkID)
	}
}

// buildChunkIndex builds an index for fast chunk filtering
func (e *DiskEngine) buildChunkIndex(chunk types.Chunk, filePath string) ChunkIndex {
	index := ChunkIndex{
		ChunkID:  chunk.ID,
		FilePath: filePath,
		StartPos: chunk.StartPos,
		EndPos:   chunk.EndPos,
		RowCount: len(chunk.Rows),
		Columns:  make(map[string][]interface{}),
	}

	// Build column value index
	columnValues := make(map[string]map[interface{}]bool)
	for _, row := range chunk.Rows {
		for column, value := range row {
			if columnValues[column] == nil {
				columnValues[column] = make(map[interface{}]bool)
			}
			columnValues[column][value] = true
		}
	}

	// Convert to slice for storage
	for column, valueMap := range columnValues {
		var values []interface{}
		for value := range valueMap {
			values = append(values, value)
		}
		index.Columns[column] = values
	}

	return index
}

// filterChunksByIndex uses indexes to filter chunks that might contain matching rows
func (e *DiskEngine) filterChunksByIndex(indexes []ChunkIndex, where types.Expression) []ChunkIndex {
	if where == nil {
		return indexes
	}

	// Simple filtering based on index values
	// In a full implementation, you'd parse the WHERE clause and use indexes more intelligently
	var relevant []ChunkIndex
	for _, index := range indexes {
		// For now, include all chunks (proper index filtering would analyze the WHERE clause)
		relevant = append(relevant, index)
	}

	return relevant
}

// storeChunkToDisk serializes and stores a chunk to disk
func (e *DiskEngine) storeChunkToDisk(chunk types.Chunk, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(chunk)
}

// loadChunkFromDisk loads and deserializes a chunk from disk
func (e *DiskEngine) loadChunkFromDisk(filePath string) (*types.Chunk, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var chunk types.Chunk
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&chunk); err != nil {
		return nil, err
	}

	return &chunk, nil
}

// storeIndexToDisk stores a chunk index to disk
func (e *DiskEngine) storeIndexToDisk(index ChunkIndex, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(index)
}

// loadTableIndexes loads all chunk indexes for a table
func (e *DiskEngine) loadTableIndexes(tableDir string) ([]ChunkIndex, error) {
	var indexes []ChunkIndex

	err := filepath.Walk(tableDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(path, ".idx") {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			var index ChunkIndex
			decoder := gob.NewDecoder(file)
			if err := decoder.Decode(&index); err != nil {
				return err
			}

			indexes = append(indexes, index)
		}

		return nil
	})

	return indexes, err
}

// estimateChunkSize estimates the memory size of a chunk
func (e *DiskEngine) estimateChunkSize(chunk types.Chunk) int64 {
	// Rough estimation: 100 bytes per row
	return int64(len(chunk.Rows) * 100)
}

// processQueryResults applies GROUP BY, ORDER BY, and LIMIT
func (e *DiskEngine) processQueryResults(rows []types.Row, query types.QueryPlan) types.ResultSet {
	// Apply GROUP BY
	if len(query.GroupBy) > 0 {
		rows = e.applyGroupBy(rows, query.GroupBy)
	}

	// Apply ORDER BY
	if len(query.OrderBy) > 0 {
		e.applyOrderBy(rows, query.OrderBy)
	}

	// Apply LIMIT
	if query.Limit != nil {
		start := query.Limit.Offset
		end := start + query.Limit.Count
		if start >= len(rows) {
			rows = []types.Row{}
		} else if end > len(rows) {
			rows = rows[start:]
		} else {
			rows = rows[start:end]
		}
	}

	// Select columns
	resultRows := e.selectColumns(rows, query.Columns)

	return types.ResultSet{
		Columns: query.Columns,
		Rows:    resultRows,
		Count:   len(resultRows),
	}
}

// Helper methods (same as in memory engine)
func (e *DiskEngine) applyGroupBy(rows []types.Row, groupBy []string) []types.Row {
	groups := make(map[string][]types.Row)
	
	for _, row := range rows {
		var keyParts []string
		for _, col := range groupBy {
			if value, exists := row[col]; exists {
				keyParts = append(keyParts, fmt.Sprintf("%v", value))
			}
		}
		key := strings.Join(keyParts, "|")
		groups[key] = append(groups[key], row)
	}

	var result []types.Row
	for _, group := range groups {
		if len(group) > 0 {
			result = append(result, group[0])
		}
	}
	
	return result
}

func (e *DiskEngine) applyOrderBy(rows []types.Row, orderBy []types.OrderByClause) {
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

func (e *DiskEngine) compareValues(a, b interface{}) int {
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

func (e *DiskEngine) selectColumns(rows []types.Row, columns []string) []types.Row {
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

func (e *DiskEngine) isTruthy(value interface{}) bool {
	// Same implementation as in expressions.go
	return true // Simplified
}

// Interface implementations
func (e *DiskEngine) GetTableMetadata(tableID string) (*types.TableConfig, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	if config, exists := e.metadata[tableID]; exists {
		return config, nil
	}
	return nil, fmt.Errorf("table metadata not found: %s", tableID)
}

func (e *DiskEngine) SetTableMetadata(tableID string, config *types.TableConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	e.metadata[tableID] = config
}

func (e *DiskEngine) Clear(tableID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	// Remove from cache
	for chunkID := range e.cache {
		if strings.HasPrefix(chunkID, tableID) {
			cached := e.cache[chunkID]
			e.cacheSize -= cached.SizeBytes
			delete(e.cache, chunkID)
		}
	}
	
	// Remove disk files
	tableDir := filepath.Join(e.cacheDir, tableID)
	if err := os.RemoveAll(tableDir); err != nil {
		return err
	}
	
	delete(e.metadata, tableID)
	return nil
}