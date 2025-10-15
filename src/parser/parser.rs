// SQL Parser - converts tokens into AST
use crate::error::{DdbError, Result};
use crate::lexer::{Token, TokenType};
use super::ast::*;

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, position: 0 }
    }

    /// Parse tokens into a Statement
    pub fn parse(&mut self) -> Result<Statement> {
        let token = self.peek()?;

        match &token.token_type {
            TokenType::Select => self.parse_select(),
            TokenType::Insert => self.parse_insert(),
            TokenType::Update => self.parse_update(),
            TokenType::Delete => self.parse_delete(),
            TokenType::Upsert => self.parse_upsert(),
            TokenType::Create => self.parse_create(),
            TokenType::Drop => self.parse_drop(),
            TokenType::Use => self.parse_use(),
            TokenType::Show => self.parse_show(),
            TokenType::Set => self.parse_set(),
            TokenType::Begin => {
                self.advance()?;
                Ok(Statement::Begin)
            }
            TokenType::Commit => {
                self.advance()?;
                Ok(Statement::Commit)
            }
            TokenType::Rollback => {
                self.advance()?;
                Ok(Statement::Rollback)
            }
            _ => Err(DdbError::ParseError(format!(
                "Unexpected token: {:?}",
                token.token_type
            ))),
        }
    }

    /// Parse SELECT statement
    fn parse_select(&mut self) -> Result<Statement> {
        self.expect(TokenType::Select)?;

        // Check for DISTINCT
        let distinct = self.match_token(TokenType::Distinct);

        // Parse columns
        let columns = self.parse_select_columns()?;

        // Parse FROM clause
        let from = if self.match_token(TokenType::From) {
            Some(self.expect_identifier()?)
        } else {
            None
        };

        // Parse JOINs (must come after FROM, before WHERE)
        let joins = self.parse_joins()?;

        // Parse WHERE clause
        let where_clause = if self.match_token(TokenType::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse GROUP BY (must come after WHERE, before ORDER BY)
        let group_by = if self.match_token(TokenType::Group) {
            self.expect(TokenType::By)?;
            self.parse_group_by()?
        } else if self.match_token(TokenType::GroupBy) {
            self.parse_group_by()?
        } else {
            Vec::new()
        };

        // Parse HAVING (must come after GROUP BY)
        let having = if self.match_token(TokenType::Having) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        // Parse ORDER BY
        let order_by = if self.match_token(TokenType::OrderBy) {
            self.parse_order_by()?
        } else if self.match_token(TokenType::Order) {
            self.expect(TokenType::By)?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        // Parse LIMIT
        let limit = if self.match_token(TokenType::Limit) {
            Some(self.parse_limit()?)
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            distinct,
            columns,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
        }))
    }

    /// Parse select columns (*, column names, functions)
    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        let mut columns = Vec::new();

        loop {
            // Check for *
            if self.match_token(TokenType::Star) {
                columns.push(SelectColumn::Wildcard);
            } else if matches!(self.peek()?.token_type, TokenType::Identifier(_)) {
                let name = self.expect_identifier()?;

                // Check for function call
                if self.match_token(TokenType::LeftParen) {
                    let args = self.parse_function_args()?;
                    self.expect(TokenType::RightParen)?;

                    let alias = if self.match_token(TokenType::As) {
                        Some(self.expect_identifier()?)
                    } else {
                        None
                    };

                    columns.push(SelectColumn::Function { name, args, alias });
                } else {
                    // Regular column
                    let alias = if self.match_token(TokenType::As) {
                        Some(self.expect_identifier()?)
                    } else {
                        None
                    };

                    columns.push(SelectColumn::Column { name, alias });
                }
            } else {
                return Err(DdbError::ParseError(
                    "Expected column name or *".to_string()
                ));
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse ORDER BY clause
    fn parse_order_by(&mut self) -> Result<Vec<OrderByColumn>> {
        let mut order_by = Vec::new();

        loop {
            let column = self.expect_identifier()?;
            let direction = if self.match_token(TokenType::Desc) {
                OrderDirection::Desc
            } else {
                self.match_token(TokenType::Asc); // Optional ASC
                OrderDirection::Asc
            };

            order_by.push(OrderByColumn { column, direction });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(order_by)
    }

    /// Parse LIMIT clause
    fn parse_limit(&mut self) -> Result<Limit> {
        let first = self.expect_integer()?;

        if self.match_token(TokenType::Comma) {
            // LIMIT offset, count
            let count = self.expect_integer()?;
            Ok(Limit {
                offset: Some(first as usize),
                count: count as usize,
            })
        } else if self.match_token(TokenType::Offset) {
            // LIMIT count OFFSET offset
            let offset = self.expect_integer()?;
            Ok(Limit {
                offset: Some(offset as usize),
                count: first as usize,
            })
        } else {
            // LIMIT count
            Ok(Limit {
                offset: None,
                count: first as usize,
            })
        }
    }

    /// Parse JOIN clauses
    fn parse_joins(&mut self) -> Result<Vec<JoinClause>> {
        let mut joins = Vec::new();

        loop {
            // Check for join type
            let join_type = if self.match_token(TokenType::Inner) {
                self.expect(TokenType::Join)?;
                JoinType::Inner
            } else if self.match_token(TokenType::Left) {
                self.match_token(TokenType::Outer); // Optional OUTER keyword
                self.expect(TokenType::Join)?;
                JoinType::Left
            } else if self.match_token(TokenType::Right) {
                self.match_token(TokenType::Outer); // Optional OUTER keyword
                self.expect(TokenType::Join)?;
                JoinType::Right
            } else if self.match_token(TokenType::Full) {
                self.match_token(TokenType::Outer); // Optional OUTER keyword
                self.expect(TokenType::Join)?;
                JoinType::Full
            } else if self.match_token(TokenType::Join) {
                // Default to INNER JOIN
                JoinType::Inner
            } else {
                // No more joins
                break;
            };

            // Parse table name
            let table = self.expect_identifier()?;

            // Parse ON condition
            self.expect(TokenType::On)?;
            let on_condition = self.parse_expression()?;

            joins.push(JoinClause {
                join_type,
                table,
                on_condition,
            });
        }

        Ok(joins)
    }

    /// Parse GROUP BY clause (column list)
    fn parse_group_by(&mut self) -> Result<Vec<String>> {
        let mut group_by = Vec::new();

        loop {
            group_by.push(self.expect_identifier()?);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(group_by)
    }

    /// Parse INSERT statement
    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(TokenType::Insert)?;
        self.expect(TokenType::Into)?;

        let table = self.expect_identifier()?;

        // Parse column list (optional)
        let columns = if self.match_token(TokenType::LeftParen) {
            let cols = self.parse_identifier_list()?;
            self.expect(TokenType::RightParen)?;
            cols
        } else {
            Vec::new()
        };

        self.expect(TokenType::Values)?;

        // Parse values
        let mut values = Vec::new();
        loop {
            self.expect(TokenType::LeftParen)?;
            let value_row = self.parse_expression_list()?;
            self.expect(TokenType::RightParen)?;
            values.push(value_row);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    /// Parse UPSERT statement
    /// Syntax: UPSERT INTO table (columns) VALUES (...) ON key_column
    fn parse_upsert(&mut self) -> Result<Statement> {
        self.expect(TokenType::Upsert)?;
        self.expect(TokenType::Into)?;

        let table = self.expect_identifier()?;

        // Parse column list (required for UPSERT)
        self.expect(TokenType::LeftParen)?;
        let columns = self.parse_identifier_list()?;
        self.expect(TokenType::RightParen)?;

        self.expect(TokenType::Values)?;

        // Parse values
        let mut values = Vec::new();
        loop {
            self.expect(TokenType::LeftParen)?;
            let value_row = self.parse_expression_list()?;
            self.expect(TokenType::RightParen)?;
            values.push(value_row);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Parse ON key_column
        self.expect(TokenType::On)?;
        let key_column = self.expect_identifier()?;

        Ok(Statement::Upsert(UpsertStatement {
            table,
            columns,
            values,
            key_column,
        }))
    }

    /// Parse UPDATE statement
    fn parse_update(&mut self) -> Result<Statement> {
        self.expect(TokenType::Update)?;
        let table = self.expect_identifier()?;
        self.expect(TokenType::Set)?;

        // Parse assignments
        let mut assignments = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            self.expect(TokenType::Equal)?;
            let value = self.parse_expression()?;
            assignments.push((column, value));

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        // Parse WHERE clause
        let where_clause = if self.match_token(TokenType::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }))
    }

    /// Parse DELETE statement
    fn parse_delete(&mut self) -> Result<Statement> {
        self.expect(TokenType::Delete)?;
        self.expect(TokenType::From)?;
        let table = self.expect_identifier()?;

        let where_clause = if self.match_token(TokenType::Where) {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    /// Parse CREATE TABLE statement
    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(TokenType::Create)?;
        self.expect(TokenType::Table)?;

        // Check for IF NOT EXISTS
        let if_not_exists = if self.match_token(TokenType::If) {
            self.expect(TokenType::Not)?;
            self.expect(TokenType::Exists)?;
            true
        } else {
            false
        };

        let name = self.expect_identifier()?;

        self.expect(TokenType::LeftParen)?;
        let columns = self.parse_column_definitions()?;
        self.expect(TokenType::RightParen)?;

        // Parse optional FILE and DELIMITER
        let mut file_path = String::new();
        let mut delimiter = None;
        let mut data_starts_on = None;
        let mut comment_char = None;
        let mut quote_char = None;

        if self.match_token(TokenType::File) {
            file_path = self.expect_string()?;
        }

        if self.match_token(TokenType::Delimiter) {
            let delim_str = self.expect_string()?;
            delimiter = delim_str.chars().next();
        }

        // Parse additional options
        while let Ok(token) = self.peek() {
            if let TokenType::Identifier(ref id) = token.token_type {
                match id.to_uppercase().as_str() {
                    "DATA_STARTS_ON" => {
                        self.advance()?;
                        data_starts_on = Some(self.expect_integer()? as usize);
                    }
                    "COMMENT_CHAR" => {
                        self.advance()?;
                        let comment_str = self.expect_string()?;
                        comment_char = comment_str.chars().next();
                    }
                    "QUOTE_CHAR" => {
                        self.advance()?;
                        let quote_str = self.expect_string()?;
                        quote_char = quote_str.chars().next();
                    }
                    _ => break,
                }
            } else {
                break;
            }
        }

        Ok(Statement::CreateTable(CreateTableStatement {
            name,
            columns,
            file_path,
            delimiter,
            data_starts_on,
            comment_char,
            quote_char,
            if_not_exists,
        }))
    }

    /// Parse column definitions with types
    fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDefinition>> {
        let mut columns = Vec::new();

        loop {
            let col_name = self.expect_identifier()?;

            // Parse column type
            let type_str = self.expect_identifier()?;
            let data_type = self.parse_column_type(&type_str)?;

            // Check for NULL/NOT NULL
            let nullable = if self.match_token(TokenType::Not) {
                self.expect(TokenType::Null)?;
                false
            } else {
                self.match_token(TokenType::Null); // Optional NULL keyword
                true
            };

            columns.push(ColumnDefinition {
                name: col_name,
                data_type,
                nullable,
            });

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(columns)
    }

    /// Parse column type from string
    fn parse_column_type(&self, type_str: &str) -> Result<ColumnType> {
        match type_str.to_uppercase().as_str() {
            "INTEGER" | "INT" => Ok(ColumnType::Integer),
            "FLOAT" | "DOUBLE" | "REAL" => Ok(ColumnType::Float),
            "STRING" | "VARCHAR" | "TEXT" | "CHAR" => Ok(ColumnType::String),
            "BOOLEAN" | "BOOL" => Ok(ColumnType::Boolean),
            "DATE" => Ok(ColumnType::Date),
            "DATETIME" | "TIMESTAMP" => Ok(ColumnType::DateTime),
            "TIME" => Ok(ColumnType::Time),
            _ => Err(DdbError::ParseError(format!(
                "Unknown column type: {}",
                type_str
            ))),
        }
    }

    /// Parse DROP TABLE statement
    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(TokenType::Drop)?;
        self.expect(TokenType::Table)?;

        // Check for IF EXISTS
        let if_exists = if self.match_token(TokenType::If) {
            self.expect(TokenType::Exists)?;
            true
        } else {
            false
        };

        let name = self.expect_identifier()?;

        Ok(Statement::DropTable(DropTableStatement { name, if_exists }))
    }

    /// Parse USE statement
    fn parse_use(&mut self) -> Result<Statement> {
        self.expect(TokenType::Use)?;
        let database = self.expect_identifier()?;

        Ok(Statement::Use(UseStatement { database }))
    }

    /// Parse SHOW statement
    fn parse_show(&mut self) -> Result<Statement> {
        self.expect(TokenType::Show)?;

        let token = self.peek()?;
        let show_stmt = match &token.token_type {
            TokenType::Tables => {
                self.advance()?;
                ShowStatement::Tables
            }
            TokenType::Columns => {
                self.advance()?;
                self.expect(TokenType::From)?;
                let table = self.expect_identifier()?;
                ShowStatement::Columns(table)
            }
            TokenType::Variables => {
                self.advance()?;
                ShowStatement::Variables
            }
            _ => {
                return Err(DdbError::ParseError(format!(
                    "Expected TABLES, COLUMNS, or VARIABLES after SHOW, got {:?}",
                    token.token_type
                )))
            }
        };

        Ok(Statement::Show(show_stmt))
    }

    /// Parse SET statement
    fn parse_set(&mut self) -> Result<Statement> {
        self.expect(TokenType::Set)?;
        let variable = self.expect_identifier()?;
        self.expect(TokenType::Equal)?;
        let value = self.parse_expression()?;

        Ok(Statement::Set(SetStatement { variable, value }))
    }

    /// Parse expression with operator precedence
    fn parse_expression(&mut self) -> Result<Expression> {
        self.parse_or_expression()
    }

    /// Parse OR expression
    fn parse_or_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_and_expression()?;

        while self.match_token(TokenType::Or) {
            let right = self.parse_and_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_comparison_expression()?;

        while self.match_token(TokenType::And) {
            let right = self.parse_comparison_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse comparison expression (=, <>, >, >=, <, <=, LIKE)
    fn parse_comparison_expression(&mut self) -> Result<Expression> {
        let mut left = self.parse_primary_expression()?;

        while let Some(op) = self.match_comparison_op() {
            let right = self.parse_primary_expression()?;
            left = Expression::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Match comparison operators
    fn match_comparison_op(&mut self) -> Option<BinaryOperator> {
        let token = self.peek().ok()?;

        let op = match &token.token_type {
            TokenType::Equal => Some(BinaryOperator::Equal),
            TokenType::NotEqual => Some(BinaryOperator::NotEqual),
            TokenType::GreaterThan => Some(BinaryOperator::GreaterThan),
            TokenType::GreaterEqual => Some(BinaryOperator::GreaterEqual),
            TokenType::LessThan => Some(BinaryOperator::LessThan),
            TokenType::LessEqual => Some(BinaryOperator::LessEqual),
            TokenType::Like => Some(BinaryOperator::Like),
            _ => None,
        };

        if op.is_some() {
            self.advance().ok();
        }

        op
    }

    /// Parse primary expression (literals, columns, functions, parentheses)
    fn parse_primary_expression(&mut self) -> Result<Expression> {
        let token = self.peek()?;

        match &token.token_type {
            TokenType::Not => {
                self.advance()?;
                let operand = self.parse_primary_expression()?;
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Not,
                    operand: Box::new(operand),
                })
            }
            TokenType::String(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(Expression::Literal(Literal::String(s)))
            }
            TokenType::Number(n_str) => {
                let n_str = n_str.clone();
                self.advance()?;

                // Parse the number string
                if let Ok(i) = n_str.parse::<i64>() {
                    Ok(Expression::Literal(Literal::Integer(i)))
                } else if let Ok(f) = n_str.parse::<f64>() {
                    Ok(Expression::Literal(Literal::Number(f)))
                } else {
                    Err(DdbError::ParseError(format!("Invalid number: {}", n_str)))
                }
            }
            TokenType::True => {
                self.advance()?;
                Ok(Expression::Literal(Literal::Boolean(true)))
            }
            TokenType::False => {
                self.advance()?;
                Ok(Expression::Literal(Literal::Boolean(false)))
            }
            TokenType::Null => {
                self.advance()?;
                Ok(Expression::Literal(Literal::Null))
            }
            TokenType::Identifier(_) => {
                let name = self.expect_identifier()?;

                // Check for system variable (@@VARIABLE)
                if name.starts_with("@@") {
                    let var_name = name[2..].to_string();
                    return Ok(Expression::SystemVariable(var_name));
                }

                // Check for function call
                if self.match_token(TokenType::LeftParen) {
                    let args = self.parse_function_args()?;
                    self.expect(TokenType::RightParen)?;
                    Ok(Expression::Function { name, args })
                } else {
                    // Check for IS NULL
                    if self.match_token(TokenType::Is) {
                        self.expect(TokenType::Null)?;
                        Ok(Expression::UnaryOp {
                            op: UnaryOperator::IsNull,
                            operand: Box::new(Expression::Column(name)),
                        })
                    } else {
                        Ok(Expression::Column(name))
                    }
                }
            }
            TokenType::LeftParen => {
                self.advance()?;
                let expr = self.parse_expression()?;
                self.expect(TokenType::RightParen)?;
                Ok(expr)
            }
            _ => Err(DdbError::ParseError(format!(
                "Unexpected token in expression: {:?}",
                token.token_type
            ))),
        }
    }

    /// Parse function arguments
    fn parse_function_args(&mut self) -> Result<Vec<Expression>> {
        let mut args = Vec::new();

        // Check for empty argument list
        if matches!(self.peek()?.token_type, TokenType::RightParen) {
            return Ok(args);
        }

        // Special case for COUNT(*)
        if self.match_token(TokenType::Star) {
            args.push(Expression::Column("*".to_string()));
            return Ok(args);
        }

        loop {
            args.push(self.parse_expression()?);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(args)
    }

    /// Parse comma-separated list of expressions
    fn parse_expression_list(&mut self) -> Result<Vec<Expression>> {
        let mut exprs = Vec::new();

        loop {
            exprs.push(self.parse_expression()?);

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(exprs)
    }

    /// Parse comma-separated list of identifiers
    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut identifiers = Vec::new();

        loop {
            identifiers.push(self.expect_identifier()?);

            // Skip optional type specification (e.g., INTEGER, STRING, DATE, etc.)
            if self.peek().ok().map(|t| matches!(t.token_type, TokenType::Identifier(_))).unwrap_or(false) {
                self.advance()?;
            }

            if !self.match_token(TokenType::Comma) {
                break;
            }
        }

        Ok(identifiers)
    }

    // ========== Helper methods ==========

    /// Get current token without advancing
    fn peek(&self) -> Result<&Token> {
        self.tokens.get(self.position).ok_or_else(|| {
            DdbError::ParseError("Unexpected end of input".to_string())
        })
    }

    /// Advance to next token
    fn advance(&mut self) -> Result<&Token> {
        let pos = self.position;
        if pos >= self.tokens.len() {
            return Err(DdbError::ParseError("Unexpected end of input".to_string()));
        }
        self.position += 1;
        Ok(&self.tokens[pos])
    }

    /// Check if current token matches expected type, advance if it does
    fn match_token(&mut self, expected: TokenType) -> bool {
        if let Ok(token) = self.peek() {
            if std::mem::discriminant(&token.token_type) == std::mem::discriminant(&expected) {
                self.advance().ok();
                return true;
            }
        }
        false
    }

    /// Expect specific token type, return error if not found
    fn expect(&mut self, expected: TokenType) -> Result<()> {
        let token = self.peek()?;
        if std::mem::discriminant(&token.token_type) == std::mem::discriminant(&expected) {
            self.advance()?;
            Ok(())
        } else {
            Err(DdbError::ParseError(format!(
                "Expected {:?}, got {:?}",
                expected, token.token_type
            )))
        }
    }

    /// Expect identifier token
    fn expect_identifier(&mut self) -> Result<String> {
        let token = self.advance()?;
        match &token.token_type {
            TokenType::Identifier(name) => Ok(name.clone()),
            _ => Err(DdbError::ParseError(format!(
                "Expected identifier, got {:?}",
                token.token_type
            ))),
        }
    }

    /// Expect string literal
    fn expect_string(&mut self) -> Result<String> {
        let token = self.advance()?;
        match &token.token_type {
            TokenType::String(s) => Ok(s.clone()),
            _ => Err(DdbError::ParseError(format!(
                "Expected string, got {:?}",
                token.token_type
            ))),
        }
    }

    /// Expect integer literal
    fn expect_integer(&mut self) -> Result<i64> {
        let token = self.advance()?;
        match &token.token_type {
            TokenType::Number(n_str) => {
                n_str.parse::<i64>().map_err(|_| {
                    DdbError::ParseError(format!("Expected integer, got: {}", n_str))
                })
            }
            _ => Err(DdbError::ParseError(format!(
                "Expected integer, got {:?}",
                token.token_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Tokenizer;

    fn parse_sql(sql: &str) -> Result<Statement> {
        let mut tokenizer = Tokenizer::new();
        let tokens = tokenizer.tokenize(sql)?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    #[test]
    fn test_parse_simple_select() {
        let stmt = parse_sql("SELECT * FROM users").unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert!(!sel.distinct);
                assert_eq!(sel.columns.len(), 1);
                assert!(matches!(sel.columns[0], SelectColumn::Wildcard));
                assert_eq!(sel.from, Some("users".to_string()));
                assert!(sel.where_clause.is_none());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_columns() {
        let stmt = parse_sql("SELECT id, name, email FROM users").unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.columns.len(), 3);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let stmt = parse_sql("SELECT * FROM users WHERE age > 18").unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert!(sel.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_functions() {
        let stmt = parse_sql("SELECT COUNT(*), AVG(age) FROM users").unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_order_by() {
        let stmt = parse_sql("SELECT * FROM users ORDER BY name ASC, age DESC").unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 2);
                assert!(matches!(sel.order_by[0].direction, OrderDirection::Asc));
                assert!(matches!(sel.order_by[1].direction, OrderDirection::Desc));
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_limit() {
        let stmt = parse_sql("SELECT * FROM users LIMIT 10").unwrap();

        match stmt {
            Statement::Select(sel) => {
                assert!(sel.limit.is_some());
                let limit = sel.limit.unwrap();
                assert_eq!(limit.count, 10);
                assert!(limit.offset.is_none());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let stmt = parse_sql("INSERT INTO users (name, email) VALUES ('John', 'john@test.com')").unwrap();

        match stmt {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "users");
                assert_eq!(ins.columns.len(), 2);
                assert_eq!(ins.values.len(), 1);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let stmt = parse_sql("UPDATE users SET name = 'Jane' WHERE id = 1").unwrap();

        match stmt {
            Statement::Update(upd) => {
                assert_eq!(upd.table, "users");
                assert_eq!(upd.assignments.len(), 1);
                assert!(upd.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse_sql("DELETE FROM users WHERE id = 1").unwrap();

        match stmt {
            Statement::Delete(del) => {
                assert_eq!(del.table, "users");
                assert!(del.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let stmt = parse_sql(
            "CREATE TABLE users (id INTEGER NOT NULL, name STRING, age INTEGER)"
        ).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.name, "users");
                assert_eq!(create.columns.len(), 3);
                assert_eq!(create.columns[0].name, "id");
                assert!(matches!(create.columns[0].data_type, ColumnType::Integer));
                assert!(!create.columns[0].nullable);
                assert_eq!(create.columns[1].name, "name");
                assert!(matches!(create.columns[1].data_type, ColumnType::String));
                assert!(create.columns[1].nullable);
                assert_eq!(create.columns[2].name, "age");
                assert!(matches!(create.columns[2].data_type, ColumnType::Integer));
                assert!(create.columns[2].nullable);
                assert!(!create.if_not_exists);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_if_not_exists() {
        let stmt = parse_sql(
            "CREATE TABLE IF NOT EXISTS users (id INTEGER NOT NULL)"
        ).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.name, "users");
                assert!(create.if_not_exists);
                assert_eq!(create.columns.len(), 1);
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_create_table_with_options() {
        let stmt = parse_sql(
            "CREATE TABLE users (id INTEGER, name STRING) FILE '/path/to/file.csv' DELIMITER ',' DATA_STARTS_ON 2 COMMENT_CHAR '#' QUOTE_CHAR '\"'"
        ).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.name, "users");
                assert_eq!(create.file_path, "/path/to/file.csv");
                assert_eq!(create.delimiter, Some(','));
                assert_eq!(create.data_starts_on, Some(2));
                assert_eq!(create.comment_char, Some('#'));
                assert_eq!(create.quote_char, Some('"'));
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_drop_table() {
        let stmt = parse_sql("DROP TABLE users").unwrap();

        match stmt {
            Statement::DropTable(drop) => {
                assert_eq!(drop.name, "users");
                assert!(!drop.if_exists);
            }
            _ => panic!("Expected DROP TABLE statement"),
        }
    }

    #[test]
    fn test_parse_drop_table_if_exists() {
        let stmt = parse_sql("DROP TABLE IF EXISTS users").unwrap();

        match stmt {
            Statement::DropTable(drop) => {
                assert_eq!(drop.name, "users");
                assert!(drop.if_exists);
            }
            _ => panic!("Expected DROP TABLE statement"),
        }
    }

    #[test]
    fn test_parse_set() {
        let stmt = parse_sql("SET output_format = 'json'").unwrap();

        match stmt {
            Statement::Set(set) => {
                assert_eq!(set.variable, "output_format");
                match set.value {
                    Expression::Literal(Literal::String(s)) => assert_eq!(s, "json"),
                    _ => panic!("Expected string literal"),
                }
            }
            _ => panic!("Expected SET statement"),
        }
    }

    #[test]
    fn test_parse_set_with_number() {
        let stmt = parse_sql("SET max_rows = 100").unwrap();

        match stmt {
            Statement::Set(set) => {
                assert_eq!(set.variable, "max_rows");
                match set.value {
                    Expression::Literal(Literal::Integer(i)) => assert_eq!(i, 100),
                    _ => panic!("Expected integer literal"),
                }
            }
            _ => panic!("Expected SET statement"),
        }
    }
}
