package parser

import (
	"ddb/pkg/types"
	"bufio"
	"fmt"
	"io"
	"strings"
)

// AdvancedCSVParser handles complex CSV parsing with quotes, escapes, and column limits
type AdvancedCSVParser struct {
	delimiter    rune
	quote        rune
	escape       rune
	maxColumns   int
	trimSpaces   bool
	allowQuoted  bool
	strictQuotes bool
	skipEmpty    bool
}

// NewAdvancedCSVParser creates a new advanced CSV parser with configuration
func NewAdvancedCSVParser(config types.TableConfig) *AdvancedCSVParser {
	parser := &AdvancedCSVParser{
		delimiter:    ',',
		quote:        '"',
		escape:       '\\',
		maxColumns:   0, // unlimited
		trimSpaces:   true,
		allowQuoted:  true,
		strictQuotes: false,
		skipEmpty:    true,
	}

	// Apply configuration
	if config.Delimiter != "" {
		parser.delimiter = rune(config.Delimiter[0])
	}
	if config.Quote != "" {
		parser.quote = rune(config.Quote[0])
	}
	if config.Escape != "" {
		parser.escape = rune(config.Escape[0])
	}
	if config.MaxColumns > 0 {
		parser.maxColumns = config.MaxColumns
	}
	
	parser.trimSpaces = config.TrimSpaces
	parser.allowQuoted = config.AllowQuoted
	parser.strictQuotes = config.StrictQuotes
	parser.skipEmpty = config.SkipEmptyLines

	return parser
}

// ParseLine parses a single CSV line with advanced options
func (p *AdvancedCSVParser) ParseLine(line string) ([]string, error) {
	if p.skipEmpty && strings.TrimSpace(line) == "" {
		return nil, nil // Skip empty lines
	}

	var fields []string
	var current strings.Builder
	var inQuotes bool
	var escaped bool
	
	runes := []rune(line)
	
	for i, r := range runes {
		switch {
		case escaped:
			// Previous character was escape - add this character literally
			current.WriteRune(r)
			escaped = false
			
		case r == p.escape && p.allowQuoted:
			// Escape character - next character will be literal
			escaped = true
			
		case r == p.quote && p.allowQuoted:
			if inQuotes {
				// Check for doubled quotes (quote escaping)
				if i+1 < len(runes) && runes[i+1] == p.quote {
					current.WriteRune(p.quote)
					i++ // Skip next quote
				} else {
					// End of quoted field
					inQuotes = false
				}
			} else {
				// Start of quoted field
				inQuotes = true
			}
			
		case r == p.delimiter && !inQuotes:
			// Field delimiter - end current field
			field := current.String()
			if p.trimSpaces && !inQuotes {
				field = strings.TrimSpace(field)
			}
			fields = append(fields, field)
			current.Reset()
			
			// Check max columns limit
			if p.maxColumns > 0 && len(fields) >= p.maxColumns {
				// Remaining content goes into last field
				remaining := string(runes[i+1:])
				if p.trimSpaces {
					remaining = strings.TrimSpace(remaining)
				}
				if len(fields) == p.maxColumns {
					// Replace last field with concatenated content
					fields[len(fields)-1] = fields[len(fields)-1] + string(p.delimiter) + remaining
				}
				break
			}
			
		default:
			// Regular character
			current.WriteRune(r)
		}
	}
	
	// Add final field
	field := current.String()
	if p.trimSpaces {
		field = strings.TrimSpace(field)
	}
	fields = append(fields, field)
	
	// Validate strict quotes
	if p.strictQuotes {
		for _, field := range fields {
			if !p.isProperlyQuoted(field) {
				return nil, fmt.Errorf("field not properly quoted: %s", field)
			}
		}
	}
	
	return fields, nil
}

// isProperlyQuoted checks if a field is properly quoted
func (p *AdvancedCSVParser) isProperlyQuoted(field string) bool {
	if len(field) < 2 {
		return !p.strictQuotes
	}
	return field[0] == byte(p.quote) && field[len(field)-1] == byte(p.quote)
}

// ReadCSV reads an entire CSV file with advanced parsing
func (p *AdvancedCSVParser) ReadCSV(reader io.Reader) ([][]string, error) {
	var records [][]string
	scanner := bufio.NewScanner(reader)
	
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		
		fields, err := p.ParseLine(line)
		if err != nil {
			return nil, fmt.Errorf("error parsing line %d: %w", lineNum, err)
		}
		
		// Skip nil results (empty lines)
		if fields != nil {
			records = append(records, fields)
		}
	}
	
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading CSV: %w", err)
	}
	
	return records, nil
}

// ParseCSVStream parses CSV in streaming fashion for large files
func (p *AdvancedCSVParser) ParseCSVStream(reader io.Reader, handler func([]string) error) error {
	scanner := bufio.NewScanner(reader)
	
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		
		fields, err := p.ParseLine(line)
		if err != nil {
			return fmt.Errorf("error parsing line %d: %w", lineNum, err)
		}
		
		// Skip nil results (empty lines)
		if fields != nil {
			if err := handler(fields); err != nil {
				return fmt.Errorf("error handling line %d: %w", lineNum, err)
			}
		}
	}
	
	return scanner.Err()
}

// CSVExample demonstrates various CSV parsing scenarios
func CSVExample() {
	examples := []struct {
		name   string
		config types.TableConfig
		data   string
	}{
		{
			name: "Standard CSV",
			config: types.TableConfig{
				Delimiter:      ",",
				Quote:          "\"",
				TrimSpaces:     true,
				AllowQuoted:    true,
				SkipEmptyLines: true,
			},
			data: `id,name,email
1,"John Doe","john@example.com"
2,"Jane, Smith","jane@test.com"`,
		},
		{
			name: "Colon Delimiter with URL in Last Column",
			config: types.TableConfig{
				Delimiter:      ":",
				Quote:          "\"",
				MaxColumns:     3, // Limit to 3 columns
				TrimSpaces:     true,
				AllowQuoted:    true,
				SkipEmptyLines: true,
			},
			data: `id:name:website
1:John:https://example.com:8080/path
2:Jane:http://test.com:3000/api/v1`,
		},
		{
			name: "Pipe Delimiter with Quotes",
			config: types.TableConfig{
				Delimiter:      "|",
				Quote:          "'",
				TrimSpaces:     true,
				AllowQuoted:    true,
				SkipEmptyLines: true,
			},
			data: `id|description|tags
1|'This is a description with | pipes'|'tag1,tag2'
2|'Another description'|'single-tag'`,
		},
		{
			name: "Tab Separated with Escape Sequences",
			config: types.TableConfig{
				Delimiter:      "\t",
				Quote:          "\"",
				Escape:         "\\",
				TrimSpaces:     false, // Keep tabs/spaces
				AllowQuoted:    true,
				SkipEmptyLines: true,
			},
			data: "id\tdata\tnotes\n1\t\"value with \\t tab\"\t\"note with \\\"quotes\\\"\"\n2\tsimple\tno quotes needed",
		},
	}

	for _, example := range examples {
		fmt.Printf("\n=== %s ===\n", example.name)
		parser := NewAdvancedCSVParser(example.config)
		
		records, err := parser.ReadCSV(strings.NewReader(example.data))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		
		for i, record := range records {
			fmt.Printf("Row %d: %v\n", i+1, record)
		}
	}
}

// WriteCSV writes data back to CSV format with proper quoting
func (p *AdvancedCSVParser) WriteCSV(writer io.Writer, records [][]string) error {
	for _, record := range records {
		for i, field := range record {
			if i > 0 {
				writer.Write([]byte(string(p.delimiter)))
			}
			
			// Determine if field needs quoting
			needsQuoting := p.fieldNeedsQuoting(field)
			
			if needsQuoting || p.strictQuotes {
				// Write quoted field
				writer.Write([]byte(string(p.quote)))
				
				// Escape quotes within the field
				escaped := strings.ReplaceAll(field, string(p.quote), string(p.quote)+string(p.quote))
				writer.Write([]byte(escaped))
				
				writer.Write([]byte(string(p.quote)))
			} else {
				// Write unquoted field
				writer.Write([]byte(field))
			}
		}
		writer.Write([]byte("\n"))
	}
	
	return nil
}

// fieldNeedsQuoting determines if a field needs to be quoted
func (p *AdvancedCSVParser) fieldNeedsQuoting(field string) bool {
	// Field needs quoting if it contains:
	// - Delimiter character
	// - Quote character  
	// - Newline characters
	// - Leading/trailing spaces (if trimSpaces is enabled)
	
	if strings.ContainsRune(field, p.delimiter) {
		return true
	}
	if strings.ContainsRune(field, p.quote) {
		return true
	}
	if strings.Contains(field, "\n") || strings.Contains(field, "\r") {
		return true
	}
	if p.trimSpaces && (strings.HasPrefix(field, " ") || strings.HasSuffix(field, " ")) {
		return true
	}
	
	return false
}