package parser

import (
	"context"
	"ddb/pkg/types"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
)

// readParquet reads Parquet data in chunks
func (fr *FileReaderImpl) readParquet(ctx context.Context, reader io.Reader, config types.TableConfig, chunks chan<- types.Chunk) {
	// For Parquet files, we need a file path since Arrow requires seekable access
	// If reader is from a file, we can get the path from config or create a temp file
	tempFile, err := fr.createTempFile(reader)
	if err != nil {
		fmt.Printf("Parquet parsing error - could not create temp file: %v\n", err)
		return
	}
	defer os.Remove(tempFile)

	// Open the Parquet file
	pf, err := file.OpenParquetFile(tempFile, false)
	if err != nil {
		fmt.Printf("Parquet parsing error - could not open file: %v\n", err)
		return
	}
	defer pf.Close()

	// Get file metadata
	metadata := pf.MetaData()
	schema := metadata.Schema
	numRowGroups := pf.NumRowGroups()

	var currentChunk []types.Row
	var totalRows int64
	var startPos int64

	// Process each row group
	for i := 0; i < numRowGroups; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rowGroup := pf.RowGroup(i)
		numRows := rowGroup.MetaData().NumRows()

		// Read data from all columns in this row group
		columnData := make(map[string][]interface{})
		columnNames := make([]string, 0, schema.NumColumns())

		// Read each column
		for j := 0; j < schema.NumColumns(); j++ {
			colSchema := schema.Column(j)
			columnName := colSchema.Name()
			columnNames = append(columnNames, columnName)

			columnChunk, err := rowGroup.Column(j)
			if err != nil {
				fmt.Printf("Error reading column %s: %v\n", columnName, err)
				continue
			}

			values, err := fr.readParquetColumn(columnChunk, int(numRows))
			if err != nil {
				fmt.Printf("Error reading column data for %s: %v\n", columnName, err)
				continue
			}

			columnData[columnName] = values
		}

		// Convert column data to rows
		for rowIdx := 0; rowIdx < int(numRows); rowIdx++ {
			row := make(types.Row)
			for _, colName := range columnNames {
				if colData, exists := columnData[colName]; exists && rowIdx < len(colData) {
					row[colName] = colData[rowIdx]
				}
			}

			currentChunk = append(currentChunk, row)
			totalRows++

			// Send chunk when it reaches the size limit
			if len(currentChunk) >= ChunkSize {
				chunk := fr.createChunk(currentChunk, startPos, totalRows)
				chunks <- chunk
				currentChunk = []types.Row{}
				startPos = totalRows
			}
		}
	}

	// Send remaining data as the final chunk
	if len(currentChunk) > 0 {
		chunk := fr.createChunk(currentChunk, startPos, totalRows)
		chunks <- chunk
	}
}

// createTempFile creates a temporary file from the reader
func (fr *FileReaderImpl) createTempFile(reader io.Reader) (string, error) {
	tempFile, err := os.CreateTemp("", "parquet_*.parquet")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	_, err = io.Copy(tempFile, reader)
	if err != nil {
		os.Remove(tempFile.Name())
		return "", err
	}

	return tempFile.Name(), nil
}

// readParquetColumn reads data from a single column
func (fr *FileReaderImpl) readParquetColumn(columnChunk file.ColumnChunkReader, numRows int) ([]interface{}, error) {
	values := make([]interface{}, 0, numRows)

	switch typedReader := columnChunk.(type) {
	case *file.BooleanColumnChunkReader:
		vals := make([]bool, numRows)
		defLevels := make([]int16, numRows)
		repLevels := make([]int16, numRows)
		
		read, _, err := typedReader.ReadBatch(int64(numRows), vals, defLevels, repLevels)
		if err != nil {
			return nil, err
		}
		
		for i := 0; i < int(read); i++ {
			values = append(values, vals[i])
		}

	case *file.Int32ColumnChunkReader:
		vals := make([]int32, numRows)
		defLevels := make([]int16, numRows)
		repLevels := make([]int16, numRows)
		
		read, _, err := typedReader.ReadBatch(int64(numRows), vals, defLevels, repLevels)
		if err != nil {
			return nil, err
		}
		
		for i := 0; i < int(read); i++ {
			values = append(values, int(vals[i]))
		}

	case *file.Int64ColumnChunkReader:
		vals := make([]int64, numRows)
		defLevels := make([]int16, numRows)
		repLevels := make([]int16, numRows)
		
		read, _, err := typedReader.ReadBatch(int64(numRows), vals, defLevels, repLevels)
		if err != nil {
			return nil, err
		}
		
		for i := 0; i < int(read); i++ {
			values = append(values, vals[i])
		}

	case *file.Float32ColumnChunkReader:
		vals := make([]float32, numRows)
		defLevels := make([]int16, numRows)
		repLevels := make([]int16, numRows)
		
		read, _, err := typedReader.ReadBatch(int64(numRows), vals, defLevels, repLevels)
		if err != nil {
			return nil, err
		}
		
		for i := 0; i < int(read); i++ {
			values = append(values, float64(vals[i]))
		}

	case *file.Float64ColumnChunkReader:
		vals := make([]float64, numRows)
		defLevels := make([]int16, numRows)
		repLevels := make([]int16, numRows)
		
		read, _, err := typedReader.ReadBatch(int64(numRows), vals, defLevels, repLevels)
		if err != nil {
			return nil, err
		}
		
		for i := 0; i < int(read); i++ {
			values = append(values, vals[i])
		}

	case *file.ByteArrayColumnChunkReader:
		vals := make([]parquet.ByteArray, numRows)
		defLevels := make([]int16, numRows)
		repLevels := make([]int16, numRows)
		
		read, _, err := typedReader.ReadBatch(int64(numRows), vals, defLevels, repLevels)
		if err != nil {
			return nil, err
		}
		
		for i := 0; i < int(read); i++ {
			values = append(values, string(vals[i]))
		}

	default:
		return nil, fmt.Errorf("unsupported column type: %T", typedReader)
	}

	return values, nil
}