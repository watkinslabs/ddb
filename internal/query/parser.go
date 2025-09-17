package query

import (
	"ddb/pkg/types"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Parser implements a simple SQL parser for SELECT statements
type Parser struct {
	tokens []string
	pos    int
}

// NewParser creates a new SQL parser
func NewParser() *Parser {
	return &Parser{}
}

// ParseQuery parses a SQL query string into a QueryPlan
func (p *Parser) ParseQuery(sql string) (types.QueryPlan, error) {
	// Tokenize the SQL
	p.tokens = p.tokenize(sql)
	p.pos = 0

	if len(p.tokens) == 0 {
		return types.QueryPlan{}, fmt.Errorf("empty query")
	}

	// Parse based on query type
	switch strings.ToUpper(p.tokens[0]) {
	case "SELECT":
		return p.parseSelect()
	case "INSERT":
		return p.parseInsert()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "UPSERT":
		return p.parseUpsert()
	default:
		return types.QueryPlan{}, fmt.Errorf("unsupported query type: %s", p.tokens[0])
	}
}

// tokenize splits SQL into tokens
func (p *Parser) tokenize(sql string) []string {
	// Simple tokenizer - split by whitespace and common delimiters
	// In a production system, you'd use a proper lexer
	
	// Replace common patterns with spaces
	sql = regexp.MustCompile(`[(),]`).ReplaceAllString(sql, " $0 ")
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")
	sql = strings.TrimSpace(sql)
	
	if sql == "" {
		return []string{}
	}
	
	tokens := strings.Split(sql, " ")
	var result []string
	
	// Clean up tokens
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token != "" {
			result = append(result, token)
		}
	}
	
	return result
}

// parseSelect parses a SELECT statement
func (p *Parser) parseSelect() (types.QueryPlan, error) {
	plan := types.QueryPlan{
		Type: types.QueryTypeSelect,
	}

	// Consume SELECT
	if !p.expect("SELECT") {
		return plan, fmt.Errorf("expected SELECT")
	}

	// Parse column list (with expression support)
	selectExprs, columns, err := p.parseSelectExpressions()
	if err != nil {
		return plan, fmt.Errorf("failed to parse column list: %w", err)
	}
	plan.SelectExprs = selectExprs
	plan.Columns = columns // Keep for backwards compatibility

	// Parse FROM clause
	if p.peek() != "" && strings.ToUpper(p.peek()) == "FROM" {
		p.consume() // consume FROM
		tableName := p.consume()
		if tableName == "" {
			return plan, fmt.Errorf("expected table name after FROM")
		}
		plan.TableName = tableName
		
		// Check for table alias
		if p.peek() != "" {
			next := strings.ToUpper(p.peek())
			// If next token is not a keyword, it's probably an alias
			if next != "JOIN" && next != "LEFT" && next != "RIGHT" && next != "INNER" && 
			   next != "OUTER" && next != "FULL" && next != "WHERE" && next != "GROUP" && 
			   next != "ORDER" && next != "LIMIT" {
				plan.TableAlias = p.consume()
			}
		}
	} else {
		return plan, fmt.Errorf("missing FROM clause")
	}

	// Parse optional JOINs
	for p.peek() != "" {
		token := strings.ToUpper(p.peek())
		if token == "JOIN" || token == "LEFT" || token == "RIGHT" || token == "INNER" || token == "OUTER" || token == "FULL" {
			join, err := p.parseJoin()
			if err != nil {
				return plan, fmt.Errorf("failed to parse JOIN: %w", err)
			}
			plan.Joins = append(plan.Joins, join)
		} else {
			break
		}
	}

	// Parse optional clauses
	for p.peek() != "" {
		switch strings.ToUpper(p.peek()) {
		case "WHERE":
			whereExpr, err := p.parseWhere()
			if err != nil {
				return plan, fmt.Errorf("failed to parse WHERE: %w", err)
			}
			plan.Where = whereExpr

		case "GROUP":
			if p.pos+1 < len(p.tokens) && strings.ToUpper(p.tokens[p.pos+1]) == "BY" {
				groupBy, err := p.parseGroupBy()
				if err != nil {
					return plan, fmt.Errorf("failed to parse GROUP BY: %w", err)
				}
				plan.GroupBy = groupBy
			} else {
				return plan, fmt.Errorf("expected BY after GROUP")
			}

		case "ORDER":
			if p.pos+1 < len(p.tokens) && strings.ToUpper(p.tokens[p.pos+1]) == "BY" {
				orderBy, err := p.parseOrderBy()
				if err != nil {
					return plan, fmt.Errorf("failed to parse ORDER BY: %w", err)
				}
				plan.OrderBy = orderBy
			} else {
				return plan, fmt.Errorf("expected BY after ORDER")
			}

		case "LIMIT":
			limit, err := p.parseLimit()
			if err != nil {
				return plan, fmt.Errorf("failed to parse LIMIT: %w", err)
			}
			plan.Limit = limit

		default:
			return plan, fmt.Errorf("unexpected token: %s", p.peek())
		}
	}

	return plan, nil
}

// parseSelectList parses the column list in SELECT
func (p *Parser) parseSelectList() ([]string, error) {
	var columns []string

	for {
		token := p.consume()
		if token == "" {
			break
		}

		// Handle special case for *
		if token == "*" {
			columns = []string{"*"}
			break
		}

		// Handle table.column format (e.g., u.name, users.email)
		if p.peek() == "." {
			p.consume() // consume dot
			columnName := p.consume()
			if columnName != "" {
				// Store as table.column format
				fullColumn := token + "." + columnName
				columns = append(columns, fullColumn)
			}
		} else {
			// Remove comma if it's attached
			token = strings.TrimSuffix(token, ",")
			if token != "" {
				columns = append(columns, token)
			}
		}

		// Check for comma separator
		if p.peek() == "," {
			p.consume() // consume comma
			continue
		}

		// If next token is not a comma, we're done with column list
		if p.peek() != "" && strings.ToUpper(p.peek()) != "FROM" {
			// Check if the next token looks like it should be part of column list
			next := strings.ToUpper(p.peek())
			if next != "FROM" && next != "WHERE" && next != "GROUP" && next != "ORDER" && next != "LIMIT" {
				continue
			}
		}
		break
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("no columns specified")
	}

	return columns, nil
}

// parseWhere parses the WHERE clause
func (p *Parser) parseWhere() (types.Expression, error) {
	p.consume() // consume WHERE
	return p.parseExpression()
}

// parseExpression parses a simple expression with proper precedence
func (p *Parser) parseExpression() (types.Expression, error) {
	return p.parseOrExpression()
}

// parseOrExpression handles OR operations (lowest precedence)
func (p *Parser) parseOrExpression() (types.Expression, error) {
	left, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for strings.ToUpper(p.peek()) == "OR" {
		op := p.peek()
		p.consume() // consume OR
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpression{
			Left:     left,
			Operator: strings.ToUpper(op),
			Right:    right,
		}
	}

	return left, nil
}

// parseAndExpression handles AND operations (medium precedence)
func (p *Parser) parseAndExpression() (types.Expression, error) {
	left, err := p.parseComparisonExpression()
	if err != nil {
		return nil, err
	}

	for strings.ToUpper(p.peek()) == "AND" {
		op := p.peek()
		p.consume() // consume AND
		right, err := p.parseComparisonExpression()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpression{
			Left:     left,
			Operator: strings.ToUpper(op),
			Right:    right,
		}
	}

	return left, nil
}

// parseComparisonExpression handles comparison operations (high precedence)
func (p *Parser) parseComparisonExpression() (types.Expression, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	op := strings.ToUpper(p.peek())
	
	// Handle IN operator
	if op == "IN" || op == "NOT" {
		if op == "NOT" {
			p.consume() // consume NOT
			if strings.ToUpper(p.peek()) == "IN" {
				p.consume() // consume IN
				return p.parseInExpression(left, true)
			} else {
				return nil, fmt.Errorf("expected IN after NOT")
			}
		} else {
			p.consume() // consume IN
			return p.parseInExpression(left, false)
		}
	}
	
	// Handle BETWEEN operator
	if op == "BETWEEN" {
		p.consume() // consume BETWEEN
		return p.parseBetweenExpression(left)
	}
	
	// Handle IS NULL / IS NOT NULL
	if op == "IS" {
		p.consume() // consume IS
		next := strings.ToUpper(p.peek())
		if next == "NULL" {
			p.consume() // consume NULL
			return &UnaryExpression{
				Operator: "IS NULL",
				Operand:  left,
			}, nil
		} else if next == "NOT" {
			p.consume() // consume NOT
			if strings.ToUpper(p.peek()) == "NULL" {
				p.consume() // consume NULL
				return &UnaryExpression{
					Operator: "IS NOT NULL",
					Operand:  left,
				}, nil
			} else {
				return nil, fmt.Errorf("expected NULL after IS NOT")
			}
		} else {
			return nil, fmt.Errorf("expected NULL after IS")
		}
	}
	
	// Handle standard comparison operators
	if op == "=" || op == "!=" || op == "<>" || op == "<" || op == "<=" || op == ">" || op == ">=" || op == "LIKE" {
		p.consume() // consume operator
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpression{
			Left:     left,
			Operator: op,
			Right:    right,
		}, nil
	}

	return left, nil
}

// parsePrimary parses primary expressions (literals, columns)
func (p *Parser) parsePrimary() (types.Expression, error) {
	token := p.peek()
	if token == "" {
		return nil, fmt.Errorf("unexpected end of expression")
	}

	// Handle parentheses
	if token == "(" {
		p.consume() // consume (
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if !p.expect(")") {
			return nil, fmt.Errorf("expected closing parenthesis")
		}
		return expr, nil
	}

	// Check if it's a function call (token followed by opening parenthesis)
	if p.pos+1 < len(p.tokens) && p.tokens[p.pos+1] == "(" {
		return p.parseFunctionCall()
	}

	p.consume() // consume token

	// Check if it's a string literal (quoted)
	if (strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'")) ||
	   (strings.HasPrefix(token, "\"") && strings.HasSuffix(token, "\"")) {
		// Remove quotes
		value := token[1 : len(token)-1]
		return &LiteralExpression{Value: value}, nil
	}

	// Check if it's a number
	if num, err := strconv.Atoi(token); err == nil {
		return &LiteralExpression{Value: num}, nil
	}
	if num, err := strconv.ParseFloat(token, 64); err == nil {
		return &LiteralExpression{Value: num}, nil
	}

	// Check for boolean literals
	switch strings.ToLower(token) {
	case "true":
		return &LiteralExpression{Value: true}, nil
	case "false":
		return &LiteralExpression{Value: false}, nil
	case "null":
		return &LiteralExpression{Value: nil}, nil
	}

	// Check if it's a table.column reference
	if strings.Contains(token, ".") {
		parts := strings.SplitN(token, ".", 2)
		if len(parts) == 2 {
			return &ColumnExpression{TableAlias: parts[0], Name: parts[1]}, nil
		}
	}
	
	// Default to column reference
	return &ColumnExpression{Name: token}, nil
}

// parseGroupBy parses GROUP BY clause
func (p *Parser) parseGroupBy() ([]string, error) {
	p.consume() // consume GROUP
	p.consume() // consume BY

	var columns []string
	for {
		token := p.consume()
		if token == "" {
			break
		}

		token = strings.TrimSuffix(token, ",")
		if token != "" {
			columns = append(columns, token)
		}

		if p.peek() == "," {
			p.consume()
			continue
		}

		// Check if next token is part of another clause
		next := strings.ToUpper(p.peek())
		if next == "ORDER" || next == "LIMIT" || next == "" {
			break
		}
	}

	return columns, nil
}

// parseOrderBy parses ORDER BY clause
func (p *Parser) parseOrderBy() ([]types.OrderByClause, error) {
	p.consume() // consume ORDER
	p.consume() // consume BY

	var clauses []types.OrderByClause
	for {
		token := p.consume()
		if token == "" {
			break
		}

		token = strings.TrimSuffix(token, ",")
		clause := types.OrderByClause{Column: token, Desc: false}

		// Check for ASC/DESC
		if p.peek() != "" {
			next := strings.ToUpper(p.peek())
			if next == "ASC" {
				p.consume()
				clause.Desc = false
			} else if next == "DESC" {
				p.consume()
				clause.Desc = true
			}
		}

		clauses = append(clauses, clause)

		if p.peek() == "," {
			p.consume()
			continue
		}

		// Check if next token is part of another clause
		next := strings.ToUpper(p.peek())
		if next == "LIMIT" || next == "" {
			break
		}
	}

	return clauses, nil
}

// parseLimit parses LIMIT clause
func (p *Parser) parseLimit() (*types.LimitClause, error) {
	p.consume() // consume LIMIT

	token := p.consume()
	if token == "" {
		return nil, fmt.Errorf("expected number after LIMIT")
	}

	// Check if it's OFFSET,COUNT format - handle both "2,3" and "2 , 3" tokenization
	if strings.Contains(token, ",") {
		// Case 1: "2,3" - comma is part of the token
		parts := strings.Split(token, ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid LIMIT format")
		}

		offset, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid offset: %w", err)
		}

		count, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid count: %w", err)
		}

		return &types.LimitClause{Offset: offset, Count: count}, nil
	} else if p.peek() == "," {
		// Case 2: "2 , 3" - comma is a separate token
		offset, err := strconv.Atoi(token)
		if err != nil {
			return nil, fmt.Errorf("invalid offset: %w", err)
		}

		p.consume() // consume comma

		countToken := p.consume()
		if countToken == "" {
			return nil, fmt.Errorf("expected count after comma in LIMIT")
		}

		count, err := strconv.Atoi(countToken)
		if err != nil {
			return nil, fmt.Errorf("invalid count: %w", err)
		}

		return &types.LimitClause{Offset: offset, Count: count}, nil
	}

	// Just COUNT format
	count, err := strconv.Atoi(token)
	if err != nil {
		return nil, fmt.Errorf("invalid limit: %w", err)
	}

	return &types.LimitClause{Offset: 0, Count: count}, nil
}

// parseJoin parses JOIN clauses
func (p *Parser) parseJoin() (types.JoinClause, error) {
	var joinType types.JoinType = types.JoinTypeInner
	
	// Determine join type
	first := strings.ToUpper(p.peek())
	switch first {
	case "LEFT":
		p.consume()
		if strings.ToUpper(p.peek()) == "JOIN" {
			joinType = types.JoinTypeLeft
		} else {
			return types.JoinClause{}, fmt.Errorf("expected JOIN after LEFT")
		}
	case "RIGHT":
		p.consume()
		if strings.ToUpper(p.peek()) == "JOIN" {
			joinType = types.JoinTypeRight
		} else {
			return types.JoinClause{}, fmt.Errorf("expected JOIN after RIGHT")
		}
	case "INNER":
		p.consume()
		if strings.ToUpper(p.peek()) == "JOIN" {
			joinType = types.JoinTypeInner
		} else {
			return types.JoinClause{}, fmt.Errorf("expected JOIN after INNER")
		}
	case "FULL":
		p.consume()
		if strings.ToUpper(p.peek()) == "JOIN" {
			joinType = types.JoinTypeFull
		} else {
			return types.JoinClause{}, fmt.Errorf("expected JOIN after FULL")
		}
	case "OUTER":
		p.consume()
		if strings.ToUpper(p.peek()) == "JOIN" {
			joinType = types.JoinTypeOuter
		} else {
			return types.JoinClause{}, fmt.Errorf("expected JOIN after OUTER")
		}
	case "JOIN":
		joinType = types.JoinTypeInner
	default:
		return types.JoinClause{}, fmt.Errorf("expected JOIN clause")
	}
	
	p.consume() // consume JOIN
	
	// Get table name
	tableName := p.consume()
	if tableName == "" {
		return types.JoinClause{}, fmt.Errorf("expected table name after JOIN")
	}
	
	// Check for table alias
	var tableAlias string
	if p.peek() != "" && strings.ToUpper(p.peek()) != "ON" {
		tableAlias = p.consume()
	}
	
	// Parse ON condition
	if !p.expect("ON") {
		return types.JoinClause{}, fmt.Errorf("expected ON after JOIN table")
	}
	
	condition, err := p.parseExpression()
	if err != nil {
		return types.JoinClause{}, fmt.Errorf("failed to parse JOIN condition: %w", err)
	}
	
	return types.JoinClause{
		Type:       joinType,
		TableName:  tableName,
		TableAlias: tableAlias,
		Condition:  condition,
	}, nil
}

// Helper methods
func (p *Parser) peek() string {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return ""
}

func (p *Parser) consume() string {
	if p.pos < len(p.tokens) {
		token := p.tokens[p.pos]
		p.pos++
		return token
	}
	return ""
}

func (p *Parser) expect(expected string) bool {
	if strings.ToUpper(p.peek()) == strings.ToUpper(expected) {
		p.consume()
		return true
	}
	return false
}

// parseInsert parses an INSERT statement
func (p *Parser) parseInsert() (types.QueryPlan, error) {
	plan := types.QueryPlan{
		Type: types.QueryTypeInsert,
	}

	// Consume INSERT
	if !p.expect("INSERT") {
		return plan, fmt.Errorf("expected INSERT")
	}

	// Optional INTO
	if strings.ToUpper(p.peek()) == "INTO" {
		p.consume()
	}

	// Parse table name
	tableName := p.consume()
	if tableName == "" {
		return plan, fmt.Errorf("expected table name after INSERT")
	}
	plan.TableName = tableName

	// Parse column list (col1, col2, ...)
	if p.peek() == "(" {
		p.consume() // consume (
		for {
			col := p.consume()
			if col == "" {
				break
			}
			col = strings.TrimSuffix(col, ",")
			if col != "" && col != ")" {
				plan.Columns = append(plan.Columns, col)
			}
			if p.peek() == ")" {
				p.consume()
				break
			}
			if p.peek() == "," {
				p.consume()
			}
		}
	}

	// Parse VALUES
	if !p.expect("VALUES") {
		return plan, fmt.Errorf("expected VALUES after column list")
	}

	// Parse value rows
	for p.peek() == "(" {
		p.consume() // consume (
		var valueRow []interface{}
		for {
			token := p.consume()
			if token == "" || token == ")" {
				break
			}
			token = strings.TrimSuffix(token, ",")
			if token != "" {
				// Parse value
				value := p.parseValue(token)
				valueRow = append(valueRow, value)
			}
			if p.peek() == ")" {
				p.consume()
				break
			}
			if p.peek() == "," {
				p.consume()
			}
		}
		plan.Values = append(plan.Values, valueRow)

		// Check for more value rows
		if p.peek() == "," {
			p.consume()
		} else {
			break
		}
	}

	return plan, nil
}

// parseUpdate parses an UPDATE statement  
func (p *Parser) parseUpdate() (types.QueryPlan, error) {
	plan := types.QueryPlan{
		Type:       types.QueryTypeUpdate,
		SetClauses: make(map[string]types.Expression),
	}

	// Consume UPDATE
	if !p.expect("UPDATE") {
		return plan, fmt.Errorf("expected UPDATE")
	}

	// Parse table name
	tableName := p.consume()
	if tableName == "" {
		return plan, fmt.Errorf("expected table name after UPDATE")
	}
	plan.TableName = tableName

	// Parse SET clause
	if !p.expect("SET") {
		return plan, fmt.Errorf("expected SET after table name")
	}

	// Parse set assignments
	for {
		column := p.consume()
		if column == "" {
			break
		}
		
		if !p.expect("=") {
			return plan, fmt.Errorf("expected = after column name")
		}

		expr, err := p.parsePrimary()
		if err != nil {
			return plan, fmt.Errorf("failed to parse SET value: %w", err)
		}

		plan.SetClauses[column] = expr

		if p.peek() == "," {
			p.consume()
		} else {
			break
		}
	}

	// Parse WHERE clause  
	if strings.ToUpper(p.peek()) == "WHERE" {
		whereExpr, err := p.parseWhere()
		if err != nil {
			return plan, fmt.Errorf("failed to parse WHERE: %w", err)
		}
		plan.Where = whereExpr
	}

	return plan, nil
}

// parseDelete parses a DELETE statement
func (p *Parser) parseDelete() (types.QueryPlan, error) {
	plan := types.QueryPlan{
		Type: types.QueryTypeDelete,
	}

	// Consume DELETE
	if !p.expect("DELETE") {
		return plan, fmt.Errorf("expected DELETE")
	}

	// Optional FROM
	if strings.ToUpper(p.peek()) == "FROM" {
		p.consume()
	}

	// Parse table name
	tableName := p.consume()
	if tableName == "" {
		return plan, fmt.Errorf("expected table name after DELETE")
	}
	plan.TableName = tableName

	// Parse WHERE clause (required for safety)
	if strings.ToUpper(p.peek()) == "WHERE" {
		whereExpr, err := p.parseWhere()
		if err != nil {
			return plan, fmt.Errorf("failed to parse WHERE: %w", err)
		}
		plan.Where = whereExpr
	}

	return plan, nil
}

// parseUpsert parses an UPSERT statement (INSERT with conflict resolution)
func (p *Parser) parseUpsert() (types.QueryPlan, error) {
	// For now, implement as INSERT
	plan, err := p.parseInsert()
	if err != nil {
		return plan, err
	}
	plan.Type = types.QueryTypeUpsert
	return plan, nil
}

// parseValue parses a literal value
func (p *Parser) parseValue(token string) interface{} {
	// Remove quotes for strings
	if (strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'")) ||
	   (strings.HasPrefix(token, "\"") && strings.HasSuffix(token, "\"")) {
		return token[1 : len(token)-1]
	}

	// Try to parse as number
	if num, err := strconv.Atoi(token); err == nil {
		return num
	}
	if num, err := strconv.ParseFloat(token, 64); err == nil {
		return num
	}

	// Boolean values
	switch strings.ToLower(token) {
	case "true":
		return true
	case "false":
		return false
	case "null":
		return nil
	}

	// Default to string
	return token
}

// parseInExpression parses IN expressions like "column IN (1, 2, 3)"
func (p *Parser) parseInExpression(left types.Expression, negated bool) (types.Expression, error) {
	// Expect opening parenthesis
	if p.peek() != "(" {
		return nil, fmt.Errorf("expected '(' after IN")
	}
	p.consume() // consume (
	
	var values []types.Expression
	for {
		if p.peek() == ")" {
			break
		}
		
		value, err := p.parsePrimary()
		if err != nil {
			return nil, fmt.Errorf("failed to parse IN value: %w", err)
		}
		values = append(values, value)
		
		if p.peek() == "," {
			p.consume() // consume comma
		} else if p.peek() == ")" {
			break
		} else {
			return nil, fmt.Errorf("expected ',' or ')' in IN expression")
		}
	}
	
	if p.peek() != ")" {
		return nil, fmt.Errorf("expected ')' to close IN expression")
	}
	p.consume() // consume )
	
	return &InExpression{
		Expression: left,
		Values:     values,
		Negated:    negated,
	}, nil
}

// parseBetweenExpression parses BETWEEN expressions like "column BETWEEN 1 AND 10"
func (p *Parser) parseBetweenExpression(left types.Expression) (types.Expression, error) {
	// Parse first value
	value1, err := p.parsePrimary()
	if err != nil {
		return nil, fmt.Errorf("failed to parse BETWEEN start value: %w", err)
	}
	
	// Expect AND
	if strings.ToUpper(p.peek()) != "AND" {
		return nil, fmt.Errorf("expected AND in BETWEEN expression")
	}
	p.consume() // consume AND
	
	// Parse second value
	value2, err := p.parsePrimary()
	if err != nil {
		return nil, fmt.Errorf("failed to parse BETWEEN end value: %w", err)
	}
	
	// Create equivalent expression: left >= value1 AND left <= value2
	leftGte := &BinaryExpression{
		Left:     left,
		Operator: ">=",
		Right:    value1,
	}
	
	leftLte := &BinaryExpression{
		Left:     left,
		Operator: "<=",
		Right:    value2,
	}
	
	return &BinaryExpression{
		Left:     leftGte,
		Operator: "AND",
		Right:    leftLte,
	}, nil
}

// parseFunctionCall parses function calls like "UPPER(name)" or "SUBSTR(text, 1, 5)"
func (p *Parser) parseFunctionCall() (types.Expression, error) {
	functionName := p.consume() // consume function name
	
	// Consume opening parenthesis
	if p.peek() != "(" {
		return nil, fmt.Errorf("expected '(' after function name %s", functionName)
	}
	p.consume() // consume (
	
	var args []types.Expression
	
	// Parse arguments
	for {
		if p.peek() == ")" {
			break
		}
		
		arg, err := p.parseExpression()
		if err != nil {
			return nil, fmt.Errorf("failed to parse function argument: %w", err)
		}
		args = append(args, arg)
		
		if p.peek() == "," {
			p.consume() // consume comma
		} else if p.peek() == ")" {
			break
		} else {
			return nil, fmt.Errorf("expected ',' or ')' in function call")
		}
	}
	
	// Consume closing parenthesis
	if p.peek() != ")" {
		return nil, fmt.Errorf("expected ')' to close function call")
	}
	p.consume() // consume )
	
	return &FunctionExpression{
		Name: functionName,
		Args: args,
	}, nil
}

// parseSelectExpressions parses SELECT expressions with optional aliases
func (p *Parser) parseSelectExpressions() ([]types.SelectExpression, []string, error) {
	var selectExprs []types.SelectExpression
	var columns []string // For backwards compatibility
	
	for {
		// Handle wildcard case
		if p.peek() == "*" {
			p.consume() // consume *
			selectExprs = append(selectExprs, types.SelectExpression{
				Expression: &LiteralExpression{Value: "*"},
				Alias:      "",
			})
			columns = []string{"*"}
			break
		}
		
		// Parse expression
		expr, err := p.parseExpression()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse SELECT expression: %w", err)
		}
		
		// Check for AS alias
		alias := ""
		if strings.ToUpper(p.peek()) == "AS" {
			p.consume() // consume AS
			alias = p.consume() // consume alias name
		} else if p.peek() != "" && p.peek() != "," && 
		           strings.ToUpper(p.peek()) != "FROM" && 
		           strings.ToUpper(p.peek()) != "WHERE" && 
		           strings.ToUpper(p.peek()) != "GROUP" && 
		           strings.ToUpper(p.peek()) != "ORDER" && 
		           strings.ToUpper(p.peek()) != "LIMIT" {
			// Implicit alias (no AS keyword)
			alias = p.consume()
		}
		
		selectExprs = append(selectExprs, types.SelectExpression{
			Expression: expr,
			Alias:      alias,
		})
		
		// For backwards compatibility, generate column name
		if alias != "" {
			columns = append(columns, alias)
		} else {
			// Try to extract column name from expression
			if colExpr, ok := expr.(*ColumnExpression); ok {
				columns = append(columns, colExpr.Name)
			} else {
				// For complex expressions, use a generated name
				columns = append(columns, fmt.Sprintf("expr_%d", len(columns)+1))
			}
		}
		
		// Check for comma or end
		if p.peek() == "," {
			p.consume() // consume comma
			continue
		} else {
			// Check if next token indicates end of SELECT list
			next := strings.ToUpper(p.peek())
			if next == "FROM" || next == "WHERE" || next == "GROUP" || 
			   next == "ORDER" || next == "LIMIT" || next == "" {
				break
			}
		}
	}
	
	return selectExprs, columns, nil
}