package query

import (
	"ddb/pkg/types"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// LiteralExpression represents a literal value
type LiteralExpression struct {
	Value interface{}
}

func (e *LiteralExpression) Evaluate(row types.Row) (interface{}, error) {
	return e.Value, nil
}

func (e *LiteralExpression) String() string {
	return fmt.Sprintf("%v", e.Value)
}

// ColumnExpression represents a column reference
type ColumnExpression struct {
	TableAlias string // Optional table alias/name
	Name       string
}

func (e *ColumnExpression) Evaluate(row types.Row) (interface{}, error) {
	// Try exact match first (with table prefix if specified)
	if e.TableAlias != "" {
		fullName := e.TableAlias + "." + e.Name
		if value, exists := row[fullName]; exists {
			return value, nil
		}
	}
	
	// Try without table prefix
	if value, exists := row[e.Name]; exists {
		return value, nil
	}
	
	// Try to find column with any table prefix (for ambiguity resolution)
	var matches []string
	var matchedValue interface{}
	for key, value := range row {
		if strings.HasSuffix(key, "."+e.Name) || key == e.Name {
			matches = append(matches, key)
			matchedValue = value
		}
	}
	
	if len(matches) == 1 {
		return matchedValue, nil
	} else if len(matches) > 1 {
		return nil, fmt.Errorf("ambiguous column reference '%s' - matches: %v", e.Name, matches)
	}
	
	return nil, fmt.Errorf("column not found: %s", e.FullName())
}

func (e *ColumnExpression) String() string {
	return e.FullName()
}

func (e *ColumnExpression) FullName() string {
	if e.TableAlias != "" {
		return e.TableAlias + "." + e.Name
	}
	return e.Name
}

// BinaryExpression represents binary operations
type BinaryExpression struct {
	Left     types.Expression
	Operator string
	Right    types.Expression
}

func (e *BinaryExpression) Evaluate(row types.Row) (interface{}, error) {
	leftVal, err := e.Left.Evaluate(row)
	if err != nil {
		return nil, err
	}

	rightVal, err := e.Right.Evaluate(row)
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(e.Operator) {
	case "=", "==":
		return compareValues(leftVal, rightVal) == 0, nil
	case "!=", "<>":
		return compareValues(leftVal, rightVal) != 0, nil
	case "<":
		return compareValues(leftVal, rightVal) < 0, nil
	case "<=":
		return compareValues(leftVal, rightVal) <= 0, nil
	case ">":
		return compareValues(leftVal, rightVal) > 0, nil
	case ">=":
		return compareValues(leftVal, rightVal) >= 0, nil
	case "AND", "&&":
		return isTruthy(leftVal) && isTruthy(rightVal), nil
	case "OR", "||":
		return isTruthy(leftVal) || isTruthy(rightVal), nil
	case "LIKE":
		return matchLike(toString(leftVal), toString(rightVal)), nil
	case "+":
		return addValues(leftVal, rightVal), nil
	case "-":
		return subtractValues(leftVal, rightVal), nil
	case "*":
		return multiplyValues(leftVal, rightVal), nil
	case "/":
		return divideValues(leftVal, rightVal), nil
	default:
		return nil, fmt.Errorf("unsupported operator: %s", e.Operator)
	}
}

func (e *BinaryExpression) String() string {
	return fmt.Sprintf("(%s %s %s)", e.Left.String(), e.Operator, e.Right.String())
}

// UnaryExpression represents unary operations
type UnaryExpression struct {
	Operator string
	Operand  types.Expression
}

func (e *UnaryExpression) Evaluate(row types.Row) (interface{}, error) {
	val, err := e.Operand.Evaluate(row)
	if err != nil {
		return nil, err
	}

	switch strings.ToUpper(e.Operator) {
	case "NOT", "!":
		return !isTruthy(val), nil
	case "+":
		return val, nil
	case "-":
		return negateValue(val), nil
	case "IS NULL":
		return val == nil || val == "", nil
	case "IS NOT NULL":
		return val != nil && val != "", nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", e.Operator)
	}
}

func (e *UnaryExpression) String() string {
	return fmt.Sprintf("%s%s", e.Operator, e.Operand.String())
}

// InExpression represents IN operations
type InExpression struct {
	Expression types.Expression
	Values     []types.Expression
	Negated    bool
}

func (e *InExpression) Evaluate(row types.Row) (interface{}, error) {
	val, err := e.Expression.Evaluate(row)
	if err != nil {
		return nil, err
	}

	for _, valueExpr := range e.Values {
		listVal, err := valueExpr.Evaluate(row)
		if err != nil {
			continue
		}
		if compareValues(val, listVal) == 0 {
			return !e.Negated, nil
		}
	}

	return e.Negated, nil
}

func (e *InExpression) String() string {
	var values []string
	for _, v := range e.Values {
		values = append(values, v.String())
	}
	not := ""
	if e.Negated {
		not = "NOT "
	}
	return fmt.Sprintf("%s %sIN (%s)", e.Expression.String(), not, strings.Join(values, ", "))
}

// Helper functions

func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Try to convert to numbers first
	if aNum, aOk := toNumber(a); aOk {
		if bNum, bOk := toNumber(b); bOk {
			if aNum < bNum {
				return -1
			}
			if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// Fall back to string comparison
	aStr := toString(a)
	bStr := toString(b)
	if aStr < bStr {
		return -1
	}
	if aStr > bStr {
		return 1
	}
	return 0
}

func isTruthy(value interface{}) bool {
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

// FunctionExpression represents function calls like UPPER(), LENGTH(), etc.
type FunctionExpression struct {
	Name string
	Args []types.Expression
}

func (e *FunctionExpression) Evaluate(row types.Row) (interface{}, error) {
	switch strings.ToUpper(e.Name) {
	case "UPPER":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("UPPER() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		return strings.ToUpper(toString(val)), nil
		
	case "LOWER":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("LOWER() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		return strings.ToLower(toString(val)), nil
		
	case "LENGTH", "LEN":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("LENGTH() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		return len(toString(val)), nil
		
	case "SUBSTR", "SUBSTRING":
		if len(e.Args) < 2 || len(e.Args) > 3 {
			return nil, fmt.Errorf("SUBSTR() requires 2 or 3 arguments")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		startVal, err := e.Args[1].Evaluate(row)
		if err != nil {
			return nil, err
		}
		
		str := toString(val)
		start, ok := toInt(startVal)
		if !ok || start < 1 {
			return "", nil
		}
		
		// Convert to 0-based indexing for Go
		start = start - 1
		
		if start >= len(str) {
			return "", nil
		}
		
		if len(e.Args) == 3 {
			lengthVal, err := e.Args[2].Evaluate(row)
			if err != nil {
				return nil, err
			}
			length, ok := toInt(lengthVal)
			if !ok || length < 0 {
				return "", nil
			}
			end := start + length
			if end > len(str) {
				end = len(str)
			}
			return str[start:end], nil
		}
		
		return str[start:], nil
		
	case "TRIM":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("TRIM() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		return strings.TrimSpace(toString(val)), nil
		
	case "CONCAT":
		if len(e.Args) == 0 {
			return "", nil
		}
		var parts []string
		for _, arg := range e.Args {
			val, err := arg.Evaluate(row)
			if err != nil {
				return nil, err
			}
			parts = append(parts, toString(val))
		}
		return strings.Join(parts, ""), nil
		
	case "REPLACE":
		if len(e.Args) != 3 {
			return nil, fmt.Errorf("REPLACE() requires exactly 3 arguments")
		}
		str, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		old, err := e.Args[1].Evaluate(row)
		if err != nil {
			return nil, err
		}
		new, err := e.Args[2].Evaluate(row)
		if err != nil {
			return nil, err
		}
		return strings.ReplaceAll(toString(str), toString(old), toString(new)), nil
		
	case "LTRIM":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("LTRIM() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		return strings.TrimLeftFunc(toString(val), func(r rune) bool {
			return r == ' ' || r == '\t' || r == '\n' || r == '\r'
		}), nil
		
	case "RTRIM":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("RTRIM() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		return strings.TrimRightFunc(toString(val), func(r rune) bool {
			return r == ' ' || r == '\t' || r == '\n' || r == '\r'
		}), nil
		
	case "LEFT":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("LEFT() requires exactly 2 arguments")
		}
		str, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		lengthVal, err := e.Args[1].Evaluate(row)
		if err != nil {
			return nil, err
		}
		length, ok := toInt(lengthVal)
		if !ok || length < 0 {
			return "", nil
		}
		s := toString(str)
		if length >= len(s) {
			return s, nil
		}
		return s[:length], nil
		
	case "RIGHT":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("RIGHT() requires exactly 2 arguments")
		}
		str, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		lengthVal, err := e.Args[1].Evaluate(row)
		if err != nil {
			return nil, err
		}
		length, ok := toInt(lengthVal)
		if !ok || length < 0 {
			return "", nil
		}
		s := toString(str)
		if length >= len(s) {
			return s, nil
		}
		return s[len(s)-length:], nil
		
	case "ABS":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("ABS() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		if num, ok := toNumber(val); ok {
			if num < 0 {
				return -num, nil
			}
			return num, nil
		}
		return nil, fmt.Errorf("ABS() requires numeric argument")
		
	case "ROUND":
		if len(e.Args) < 1 || len(e.Args) > 2 {
			return nil, fmt.Errorf("ROUND() requires 1 or 2 arguments")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		num, ok := toNumber(val)
		if !ok {
			return nil, fmt.Errorf("ROUND() requires numeric argument")
		}
		
		precision := 0
		if len(e.Args) == 2 {
			precVal, err := e.Args[1].Evaluate(row)
			if err != nil {
				return nil, err
			}
			if p, ok := toInt(precVal); ok {
				precision = p
			}
		}
		
		multiplier := 1.0
		for i := 0; i < precision; i++ {
			multiplier *= 10
		}
		return float64(int(num*multiplier+0.5)) / multiplier, nil
		
	case "FLOOR":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("FLOOR() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		if num, ok := toNumber(val); ok {
			return float64(int(num)), nil
		}
		return nil, fmt.Errorf("FLOOR() requires numeric argument")
		
	case "CEIL", "CEILING":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("CEIL() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		if num, ok := toNumber(val); ok {
			if num == float64(int(num)) {
				return num, nil // Already integer
			} else if num > 0 {
				return float64(int(num) + 1), nil
			} else {
				return float64(int(num)), nil
			}
		}
		return nil, fmt.Errorf("CEIL() requires numeric argument")
		
	case "MOD", "MODULO":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("MOD() requires exactly 2 arguments")
		}
		val1, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		val2, err := e.Args[1].Evaluate(row)
		if err != nil {
			return nil, err
		}
		if num1, ok1 := toNumber(val1); ok1 {
			if num2, ok2 := toNumber(val2); ok2 && num2 != 0 {
				return float64(int(num1) % int(num2)), nil
			}
		}
		return nil, fmt.Errorf("MOD() requires numeric arguments and non-zero divisor")
		
	case "POWER", "POW":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("POWER() requires exactly 2 arguments")
		}
		val1, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		val2, err := e.Args[1].Evaluate(row)
		if err != nil {
			return nil, err
		}
		if base, ok1 := toNumber(val1); ok1 {
			if exp, ok2 := toNumber(val2); ok2 {
				result := 1.0
				for i := 0; i < int(exp); i++ {
					result *= base
				}
				return result, nil
			}
		}
		return nil, fmt.Errorf("POWER() requires numeric arguments")
		
	case "SQRT":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("SQRT() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		if num, ok := toNumber(val); ok && num >= 0 {
			// Simple square root approximation
			if num == 0 {
				return 0.0, nil
			}
			x := num
			for i := 0; i < 10; i++ { // Newton's method iterations
				x = (x + num/x) / 2
			}
			return x, nil
		}
		return nil, fmt.Errorf("SQRT() requires non-negative numeric argument")
		
	// Date/Time functions
	case "NOW":
		if len(e.Args) != 0 {
			return nil, fmt.Errorf("NOW() takes no arguments")
		}
		return time.Now().Format("2006-01-02 15:04:05"), nil
		
	case "DATE":
		if len(e.Args) == 0 {
			// DATE() with no args returns current date
			return time.Now().Format("2006-01-02"), nil
		} else if len(e.Args) == 1 {
			// DATE(datetime_string) extracts date part
			val, err := e.Args[0].Evaluate(row)
			if err != nil {
				return nil, err
			}
			dateStr := toString(val)
			
			// Try to parse various date formats
			formats := []string{
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05Z",
				"2006-01-02T15:04:05",
				"2006-01-02",
			}
			
			for _, format := range formats {
				if t, err := time.Parse(format, dateStr); err == nil {
					return t.Format("2006-01-02"), nil
				}
			}
			return dateStr, nil // Return as-is if can't parse
		} else {
			return nil, fmt.Errorf("DATE() requires 0 or 1 arguments")
		}
		
	case "YEAR":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("YEAR() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		dateStr := toString(val)
		
		// Try to parse and extract year
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z", 
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		
		for _, format := range formats {
			if t, err := time.Parse(format, dateStr); err == nil {
				return t.Year(), nil
			}
		}
		return nil, fmt.Errorf("invalid date format: %s", dateStr)
		
	case "MONTH":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("MONTH() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		dateStr := toString(val)
		
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05", 
			"2006-01-02",
		}
		
		for _, format := range formats {
			if t, err := time.Parse(format, dateStr); err == nil {
				return int(t.Month()), nil
			}
		}
		return nil, fmt.Errorf("invalid date format: %s", dateStr)
		
	case "DAY":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("DAY() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		dateStr := toString(val)
		
		formats := []string{
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05Z",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		
		for _, format := range formats {
			if t, err := time.Parse(format, dateStr); err == nil {
				return t.Day(), nil
			}
		}
		return nil, fmt.Errorf("invalid date format: %s", dateStr)
		
	case "COALESCE":
		// Return first non-null/non-empty value
		for _, arg := range e.Args {
			val, err := arg.Evaluate(row)
			if err != nil {
				continue
			}
			if val != nil && val != "" {
				return val, nil
			}
		}
		return nil, nil
		
	case "ISNULL", "IFNULL":
		// Return second value if first is null/empty
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("ISNULL() requires exactly 2 arguments")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil || val == nil || val == "" {
			return e.Args[1].Evaluate(row)
		}
		return val, nil
		
	case "REVERSE":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("REVERSE() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		str := toString(val)
		runes := []rune(str)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return string(runes), nil
		
	case "REPEAT":
		if len(e.Args) != 2 {
			return nil, fmt.Errorf("REPEAT() requires exactly 2 arguments")
		}
		str, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		count, err := e.Args[1].Evaluate(row)
		if err != nil {
			return nil, err
		}
		if countInt, ok := toInt(count); ok && countInt > 0 {
			return strings.Repeat(toString(str), countInt), nil
		}
		return "", nil
		
	case "SIGN":
		if len(e.Args) != 1 {
			return nil, fmt.Errorf("SIGN() requires exactly 1 argument")
		}
		val, err := e.Args[0].Evaluate(row)
		if err != nil {
			return nil, err
		}
		if num, ok := toNumber(val); ok {
			if num > 0 {
				return 1, nil
			} else if num < 0 {
				return -1, nil
			} else {
				return 0, nil
			}
		}
		return nil, fmt.Errorf("SIGN() requires numeric argument")
		
	default:
		return nil, fmt.Errorf("unknown function: %s", e.Name)
	}
}

func (e *FunctionExpression) String() string {
	var args []string
	for _, arg := range e.Args {
		args = append(args, arg.String())
	}
	return fmt.Sprintf("%s(%s)", e.Name, strings.Join(args, ", "))
}

// Helper function to convert to int
func toInt(value interface{}) (int, bool) {
	switch v := value.(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i, true
		}
	}
	return 0, false
}

func toString(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func toNumber(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func matchLike(text, pattern string) bool {
	// Convert to lowercase for case-insensitive matching
	text = strings.ToLower(text)
	pattern = strings.ToLower(pattern)
	
	// Handle SQL LIKE wildcards: % (any chars) and _ (single char)
	return matchLikePattern(text, pattern)
}

func matchLikePattern(text, pattern string) bool {
	// Base cases
	if pattern == "" {
		return text == ""
	}
	if text == "" {
		// Pattern can only match empty text if it's all %
		for _, r := range pattern {
			if r != '%' {
				return false
			}
		}
		return true
	}
	
	// Get first character of pattern
	if len(pattern) > 0 {
		switch pattern[0] {
		case '%':
			// % matches zero or more characters
			// Try matching rest of pattern at current position or advance text
			if matchLikePattern(text, pattern[1:]) {
				return true
			}
			return matchLikePattern(text[1:], pattern)
			
		case '_':
			// _ matches exactly one character
			if len(text) > 0 {
				return matchLikePattern(text[1:], pattern[1:])
			}
			return false
			
		default:
			// Regular character must match exactly
			if len(text) > 0 && text[0] == pattern[0] {
				return matchLikePattern(text[1:], pattern[1:])
			}
			return false
		}
	}
	return false
}

func addValues(a, b interface{}) interface{} {
	if aNum, aOk := toNumber(a); aOk {
		if bNum, bOk := toNumber(b); bOk {
			return aNum + bNum
		}
	}
	return toString(a) + toString(b)
}

func subtractValues(a, b interface{}) interface{} {
	if aNum, aOk := toNumber(a); aOk {
		if bNum, bOk := toNumber(b); bOk {
			return aNum - bNum
		}
	}
	return nil
}

func multiplyValues(a, b interface{}) interface{} {
	if aNum, aOk := toNumber(a); aOk {
		if bNum, bOk := toNumber(b); bOk {
			return aNum * bNum
		}
	}
	return nil
}

func divideValues(a, b interface{}) interface{} {
	if aNum, aOk := toNumber(a); aOk {
		if bNum, bOk := toNumber(b); bOk && bNum != 0 {
			return aNum / bNum
		}
	}
	return nil
}

func negateValue(value interface{}) interface{} {
	if num, ok := toNumber(value); ok {
		return -num
	}
	return nil
}