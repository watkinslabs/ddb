package parser

import (
	"ddb/pkg/types"
	"bufio"
	"fmt"
	"io"
	"strings"
)

// INIParser handles INI file parsing and conversion to row format
type INIParser struct{}

// NewINIParser creates a new INI parser
func NewINIParser() *INIParser {
	return &INIParser{}
}

// Parse converts INI data to rows
// Each INI entry becomes a row with columns: section, key, value
func (p *INIParser) Parse(reader io.Reader) ([]types.Row, error) {
	var rows []types.Row
	scanner := bufio.NewScanner(reader)
	
	currentSection := ""
	lineNum := 0
	
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "#") {
			continue
		}
		
		// Handle sections [section_name]
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = line[1 : len(line)-1]
			continue
		}
		
		// Handle key=value pairs
		if strings.Contains(line, "=") {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimSpace(parts[0])
				value := strings.TrimSpace(parts[1])
				
				// Remove quotes if present
				if (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) ||
				   (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
					value = value[1 : len(value)-1]
				}
				
				row := types.Row{
					"section": currentSection,
					"key":     key,
					"value":   value,
					"line":    lineNum,
				}
				rows = append(rows, row)
			}
		}
	}
	
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading INI: %w", err)
	}
	
	return rows, nil
}