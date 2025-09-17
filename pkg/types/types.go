package types

import (
	"context"
	"io"
)

// DataType represents the supported data types
type DataType int

const (
	DataTypeString DataType = iota
	DataTypeInt
	DataTypeFloat
	DataTypeDecimal
	DataTypeBool
)

// Column represents a table column definition
type Column struct {
	Name     string   `yaml:"name" json:"name"`
	Type     DataType `yaml:"type" json:"type"`
	Index    int      `yaml:"index" json:"index"`
	Required bool     `yaml:"required" json:"required"`
}

// TableConfig represents table configuration from SQL config files
type TableConfig struct {
	Name      string `yaml:"name" json:"name"`
	FilePath  string `yaml:"file_path" json:"file_path"`
	Format    string `yaml:"format" json:"format"` // csv, json, yaml
	Delimiter string `yaml:"delimiter" json:"delimiter"`
	HasHeader bool   `yaml:"has_header" json:"has_header"`
	Columns   []Column `yaml:"columns" json:"columns"`
	
	// CSV-specific configuration
	Quote          string `yaml:"quote" json:"quote"`                     // Quote character (default: ")
	Escape         string `yaml:"escape" json:"escape"`                   // Escape character (default: \)
	MaxColumns     int    `yaml:"max_columns" json:"max_columns"`         // Max columns to parse (0 = unlimited)
	TrimSpaces     bool   `yaml:"trim_spaces" json:"trim_spaces"`         // Trim leading/trailing spaces
	AllowQuoted    bool   `yaml:"allow_quoted" json:"allow_quoted"`       // Allow quoted fields
	StrictQuotes   bool   `yaml:"strict_quotes" json:"strict_quotes"`     // Require quotes for all fields
	SkipEmptyLines bool   `yaml:"skip_empty_lines" json:"skip_empty_lines"` // Skip empty lines
	
	// Performance configuration
	ParallelReading bool `yaml:"parallel_reading" json:"parallel_reading"`
	WorkerThreads   int  `yaml:"worker_threads" json:"worker_threads"`
	ChunkSize       int  `yaml:"chunk_size" json:"chunk_size"`
	BufferSize      int  `yaml:"buffer_size" json:"buffer_size"`
}

// Row represents a single data row
type Row map[string]interface{}

// Chunk represents a chunk of data with metadata
type Chunk struct {
	ID       string
	Hash     string
	Rows     []Row
	StartPos int64
	EndPos   int64
}

// FileReader interface for reading different file formats
type FileReader interface {
	Read(ctx context.Context, reader io.Reader, config TableConfig) (<-chan Chunk, error)
	Close() error
}

// StorageEngine interface for in-memory storage operations
type StorageEngine interface {
	Store(ctx context.Context, tableID string, chunk Chunk) error
	Query(ctx context.Context, query QueryPlan) (ResultSet, error)
	Insert(ctx context.Context, query QueryPlan) (int, error) // returns rows affected
	Update(ctx context.Context, query QueryPlan) (int, error) // returns rows affected
	Delete(ctx context.Context, query QueryPlan) (int, error) // returns rows affected
	Upsert(ctx context.Context, query QueryPlan) (int, error) // returns rows affected
	GetTableMetadata(tableID string) (*TableConfig, error)
	Clear(tableID string) error
}

// QueryPlan represents a parsed SQL query
type QueryPlan struct {
	Type           QueryType
	TableName      string
	TableAlias     string
	Columns        []string                // Legacy: simple column names (for backwards compatibility)
	SelectExprs    []SelectExpression      // New: full expressions with aliases
	Values         [][]interface{}         // For INSERT/UPSERT
	SetClauses     map[string]Expression   // For UPDATE/UPSERT
	Where          Expression
	GroupBy        []string
	OrderBy        []OrderByClause
	Limit          *LimitClause
	Joins          []JoinClause
}

// SelectExpression represents a column expression in SELECT clause
type SelectExpression struct {
	Expression Expression
	Alias      string  // Optional alias (AS alias_name)
}

// QueryType represents the type of SQL query
type QueryType int

const (
	QueryTypeSelect QueryType = iota
	QueryTypeInsert
	QueryTypeUpdate
	QueryTypeDelete
	QueryTypeUpsert
)

// Expression represents a SQL expression
type Expression interface {
	Evaluate(row Row) (interface{}, error)
	String() string
}

// OrderByClause represents ORDER BY clause
type OrderByClause struct {
	Column string
	Desc   bool
}

// LimitClause represents LIMIT clause
type LimitClause struct {
	Offset int
	Count  int
}

// JoinClause represents JOIN clause
type JoinClause struct {
	Type       JoinType
	TableName  string
	TableAlias string
	Condition  Expression
}

// JoinType represents the type of join
type JoinType int

const (
	JoinTypeInner JoinType = iota
	JoinTypeLeft
	JoinTypeRight
	JoinTypeFull
	JoinTypeOuter
)

// ResultSet represents query results
type ResultSet struct {
	Columns []string
	Rows    []Row
	Count   int
}

// ConfigManager interface for managing table configurations
type ConfigManager interface {
	LoadConfig(configPath string) (*TableConfig, error)
	SaveConfig(config *TableConfig, configPath string) error
	ListConfigs() ([]*TableConfig, error)
}

// QueryExecutor interface for executing SQL queries
type QueryExecutor interface {
	Execute(ctx context.Context, sql string, tableConfigs map[string]*TableConfig) (ResultSet, error)
	ParseQuery(sql string) (QueryPlan, error)
}