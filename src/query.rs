//! SQL query parsing and execution for SELECT statements

use crate::{Error, Result, Value, Row};
use std::collections::HashMap;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{vec::Vec, string::String, format};

/// Represents a parsed SELECT query
#[derive(Debug, Clone)]
pub struct SelectQuery {
    /// Columns to select (None means SELECT *)
    pub columns: Option<Vec<String>>,
    /// Table name
    pub table: String,
    /// WHERE clause root expression
    pub where_expr: Option<Expr>,
    /// ORDER BY clause
    pub order_by: Option<OrderBy>,
    /// LIMIT clause
    pub limit: Option<usize>,
}

/// Expression for WHERE clause
#[derive(Debug, Clone)]
pub enum Expr {
    /// Comparison: column op value
    Comparison {
        column: String,
        operator: ComparisonOperator,
        value: Value,
        value2: Option<Value>, // For BETWEEN
    },
    /// Logical AND
    And(Box<Expr>, Box<Expr>),
    /// Logical OR
    Or(Box<Expr>, Box<Expr>),
    /// Logical NOT
    Not(Box<Expr>),
    /// IS NULL
    IsNull(String),
    /// IS NOT NULL
    IsNotNull(String),
    /// IN (list of values)
    In { column: String, values: Vec<Value> },
}

/// Comparison operators for WHERE clauses
#[derive(Debug, Clone, PartialEq)]
pub enum ComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    Between,
}

/// ORDER BY clause
#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub ascending: bool,
}

impl SelectQuery {
    /// Parse a SELECT SQL statement
    pub fn parse(sql: &str) -> Result<Self> {
        let sql = sql.trim();
        
        // Check if it's a SELECT statement
        if !sql.to_uppercase().starts_with("SELECT") {
            return Err(Error::QueryError("Only SELECT statements are supported".to_string()));
        }
        
        // Remove the SELECT keyword and normalize whitespace
        let sql = sql[6..].trim();
        let parts: Vec<&str> = sql.split_whitespace().collect();
        
        if parts.is_empty() {
            return Err(Error::QueryError("Invalid SELECT statement".to_string()));
        }
        
        let mut columns = None;
        let table;
        let mut where_expr = None;
        let mut order_by = None;
        let mut limit = None;
        
        let mut i = 0;
        
        // Parse column list
        let mut columns_str = String::new();
        while i < parts.len() && parts[i].to_uppercase() != "FROM" {
            if !columns_str.is_empty() {
                columns_str.push(' ');
            }
            columns_str.push_str(parts[i]);
            i += 1;
        }
        
        if columns_str.trim() == "*" {
            columns = None;
        } else {
            let column_list: Vec<String> = columns_str
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            if !column_list.is_empty() {
                columns = Some(column_list);
            }
        }
        
        // Parse FROM clause
        if i >= parts.len() || parts[i].to_uppercase() != "FROM" {
            return Err(Error::QueryError("Missing FROM clause".to_string()));
        }
        i += 1;
        
        if i >= parts.len() {
            return Err(Error::QueryError("Missing table name".to_string()));
        }
        table = parts[i].to_string();
        i += 1;
        
        // Parse optional WHERE clause
        if i < parts.len() && parts[i].to_uppercase() == "WHERE" {
            i += 1;
            let where_part = parts[i..].join(" ");
            where_expr = Some(Self::parse_where_expr(&where_part)?);
            
            // Find the end of WHERE clause (before ORDER BY or LIMIT)
            let mut where_end = parts.len();
            for (j, part) in parts[i..].iter().enumerate() {
                if part.to_uppercase() == "ORDER" || part.to_uppercase() == "LIMIT" {
                    where_end = i + j;
                    break;
                }
            }
            i = where_end;
        }
        
        // Parse optional ORDER BY clause
        if i < parts.len() - 1 && parts[i].to_uppercase() == "ORDER" && parts[i + 1].to_uppercase() == "BY" {
            i += 2;
            if i < parts.len() {
                let column_name = parts[i].to_string();
                let ascending = if i + 1 < parts.len() && parts[i + 1].to_uppercase() == "DESC" {
                    i += 1;
                    false
                } else if i + 1 < parts.len() && parts[i + 1].to_uppercase() == "ASC" {
                    i += 1;
                    true
                } else {
                    true
                };
                order_by = Some(OrderBy { column: column_name, ascending });
                i += 1;
            }
        }
        
        // Parse optional LIMIT clause
        if i < parts.len() && parts[i].to_uppercase() == "LIMIT" {
            i += 1;
            if i < parts.len() {
                limit = parts[i].parse().ok();
            }
        }
        
        Ok(SelectQuery {
            columns,
            table,
            where_expr,
            order_by,
            limit,
        })
    }
    
    /// Parse WHERE clause expression (supports AND, OR, NOT, IS NULL, IN, parentheses)
    fn parse_where_expr(where_str: &str) -> Result<Expr> {
        // Tokenize the input
        let tokens = Self::tokenize_where(where_str);
        let mut parser = ExprParser::new(tokens);
        parser.parse_expr()
    }

    /// Tokenize the WHERE clause into a vector of tokens
    fn tokenize_where(where_str: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut current = String::new();
        let mut chars = where_str.chars().peekable();
        while let Some(&ch) = chars.peek() {
            match ch {
                '(' | ')' | ',' => {
                    if !current.trim().is_empty() {
                        tokens.push(current.trim().to_string());
                        current.clear();
                    }
                    tokens.push(ch.to_string());
                    chars.next();
                }
                ' ' => {
                    if !current.trim().is_empty() {
                        tokens.push(current.trim().to_string());
                        current.clear();
                    }
                    chars.next();
                }
                _ => {
                    current.push(ch);
                    chars.next();
                }
            }
        }
        if !current.trim().is_empty() {
            tokens.push(current.trim().to_string());
        }
        tokens
    }
    
    /// Parse a value from a string (supporting basic types)
    fn parse_value(value_str: &str) -> Result<Value> {
        let value_str = value_str.trim();
        
        // Handle quoted strings
        if (value_str.starts_with('\'') && value_str.ends_with('\'')) ||
           (value_str.starts_with('"') && value_str.ends_with('"')) {
            let unquoted = &value_str[1..value_str.len()-1];
            return Ok(Value::Text(unquoted.to_string()));
        }
        
        // Handle NULL
        if value_str.to_uppercase() == "NULL" {
            return Ok(Value::Null);
        }
        
        // Try parsing as integer
        if let Ok(int_val) = value_str.parse::<i64>() {
            return Ok(Value::Integer(int_val));
        }
        
        // Try parsing as float
        if let Ok(float_val) = value_str.parse::<f64>() {
            return Ok(Value::Real(float_val));
        }
        
        // Default to text
        Ok(Value::Text(value_str.to_string()))
    }
    
    /// Execute the query against the provided rows
    pub fn execute(&self, mut rows: Vec<Row>, all_columns: &[String]) -> Result<Vec<Row>> {
        // Apply WHERE conditions
        rows = self.apply_where_conditions(rows)?;
        
        // Apply ORDER BY
        if let Some(ref order_by) = self.order_by {
            rows = self.apply_order_by(rows, order_by)?;
        }
        
        // Apply column selection
        rows = self.apply_column_selection(rows, all_columns)?;
        
        // Apply LIMIT
        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }
        
        Ok(rows)
    }
    
    /// Apply WHERE conditions to filter rows
    fn apply_where_conditions(&self, rows: Vec<Row>) -> Result<Vec<Row>> {
        if self.where_expr.is_none() {
            return Ok(rows);
        }
        
        let total_rows = rows.len();
        let filtered_rows: Vec<Row> = rows
            .into_iter()
            .filter(|row| {
                self.evaluate_expr(row, &self.where_expr.as_ref().unwrap())
            })
            .collect();
        
        // Add debug logging for WHERE clause filtering
        crate::logging::log_debug(&format!(
            "WHERE clause filtered {} rows from {} total rows", 
            filtered_rows.len(), 
            total_rows
        ));
        
        Ok(filtered_rows)
    }
    
    /// Evaluate a WHERE expression against a row
    fn evaluate_expr(&self, row: &Row, expr: &Expr) -> bool {
        match expr {
            Expr::Comparison { column, operator, value, value2 } => {
                let row_value = match row.get(column.as_str()) {
                    Some(value) => value,
                    None => return false, // Column doesn't exist
                };
                
                match operator {
                    ComparisonOperator::Equal => self.values_equal(row_value, value),
                    ComparisonOperator::NotEqual => !self.values_equal(row_value, value),
                    ComparisonOperator::LessThan => self.value_less_than(row_value, value),
                    ComparisonOperator::LessThanOrEqual => {
                        self.value_less_than(row_value, value) || 
                        self.values_equal(row_value, value)
                    },
                    ComparisonOperator::GreaterThan => {
                        !self.value_less_than(row_value, value) && 
                        !self.values_equal(row_value, value)
                    },
                    ComparisonOperator::GreaterThanOrEqual => {
                        !self.value_less_than(row_value, value)
                    },
                    ComparisonOperator::Like => self.value_like(row_value, value),
                    ComparisonOperator::Between => {
                        // BETWEEN: value >= value1 AND value <= value2
                        if let Some(value2) = value2 {
                            self.value_less_than(row_value, value) || 
                            self.values_equal(row_value, value) &&
                            (self.value_less_than(row_value, &value2) || 
                             self.values_equal(row_value, &value2))
                        } else {
                            false
                        }
                    },
                }
            },
            Expr::And(left, right) => self.evaluate_expr(row, left) && self.evaluate_expr(row, right),
            Expr::Or(left, right) => self.evaluate_expr(row, left) || self.evaluate_expr(row, right),
            Expr::Not(expr) => !self.evaluate_expr(row, expr),
            Expr::IsNull(column) => row.get(column.as_str()).is_none(),
            Expr::IsNotNull(column) => row.get(column.as_str()).is_some(),
            Expr::In { column, values } => {
                let row_value = row.get(column.as_str()).cloned().unwrap_or(Value::Null);
                values.iter().any(|v| self.values_equal(&row_value, v))
            },
        }
    }
    
    /// Compare two values for equality
    fn values_equal(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Real(a), Value::Real(b)) => (a - b).abs() < f64::EPSILON,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Blob(a), Value::Blob(b)) => a == b,
            // Type coercion
            (Value::Integer(a), Value::Real(b)) => (*a as f64 - b).abs() < f64::EPSILON,
            (Value::Real(a), Value::Integer(b)) => (a - *b as f64).abs() < f64::EPSILON,
            _ => false,
        }
    }
    
    /// Check if value a is less than value b
    fn value_less_than(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a < b,
            (Value::Real(a), Value::Real(b)) => a < b,
            (Value::Text(a), Value::Text(b)) => a < b,
            (Value::Integer(a), Value::Real(b)) => (*a as f64) < *b,
            (Value::Real(a), Value::Integer(b)) => *a < (*b as f64),
            _ => false,
        }
    }
    
    /// Check if value matches LIKE pattern (improved implementation)
    fn value_like(&self, value: &Value, pattern: &Value) -> bool {
        match (value, pattern) {
            (Value::Text(text), Value::Text(pattern)) => {
                // Improved LIKE implementation with % wildcard
                if pattern.contains('%') {
                    let pattern_parts: Vec<&str> = pattern.split('%').collect();
                    
                    // Handle simple cases like 'prefix%', '%suffix', '%middle%'
                    match pattern_parts.len() {
                        2 => {
                            let prefix = pattern_parts[0];
                            let suffix = pattern_parts[1];
                            
                            // Handle 'prefix%' case (suffix is empty)
                            if suffix.is_empty() {
                                return text.starts_with(prefix);
                            }
                            // Handle '%suffix' case (prefix is empty)
                            if prefix.is_empty() {
                                return text.ends_with(suffix);
                            }
                            // Handle 'prefix%suffix' case
                            return text.starts_with(prefix) && text.ends_with(suffix) && text.len() >= prefix.len() + suffix.len();
                        },
                        1 => {
                            // No % found, exact match
                            return text == pattern;
                        },
                        3 => {
                            // Handle '%middle%' case
                            let prefix = pattern_parts[0];
                            let middle = pattern_parts[1];
                            let suffix = pattern_parts[2];
                            
                            if prefix.is_empty() && suffix.is_empty() {
                                // Pattern is '%middle%' - check if text contains middle
                                return text.contains(middle);
                            }
                            // More complex patterns - fall back to basic matching
                            return text.starts_with(prefix) && text.contains(middle) && text.ends_with(suffix);
                        },
                        _ => {
                            // Multiple % wildcards - more complex pattern
                            // For now, do a simple contains check for each non-empty part
                            for part in pattern_parts {
                                if !part.is_empty() && !text.contains(part) {
                                    return false;
                                }
                            }
                            return true;
                        }
                    }
                } else {
                    // No wildcards, exact match
                    text == pattern
                }
            },
            _ => false,
        }
    }
    
    /// Apply ORDER BY to sort rows
    fn apply_order_by(&self, mut rows: Vec<Row>, order_by: &OrderBy) -> Result<Vec<Row>> {
        rows.sort_by(|a, b| {
            let val_a = a.get(order_by.column.as_str());
            let val_b = b.get(order_by.column.as_str());
            
            let cmp = match (val_a, val_b) {
                (Some(a), Some(b)) => self.compare_values(a, b),
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, Some(_)) => std::cmp::Ordering::Less,
                (None, None) => std::cmp::Ordering::Equal,
            };
            
            if order_by.ascending {
                cmp
            } else {
                cmp.reverse()
            }
        });
        
        Ok(rows)
    }
    
    /// Compare two values for ordering
    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => std::cmp::Ordering::Less,
            (_, Value::Null) => std::cmp::Ordering::Greater,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Integer(a), Value::Real(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Real(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
            _ => std::cmp::Ordering::Equal,
        }
    }
    
    /// Apply column selection (SELECT specific columns or *)
    fn apply_column_selection(&self, rows: Vec<Row>, all_columns: &[String]) -> Result<Vec<Row>> {
        match &self.columns {
            None => Ok(rows), // SELECT * - return all columns
            Some(selected_columns) => {
                let mut result_rows = Vec::new();
                
                for row in rows {
                    let mut new_row = HashMap::new();
                    
                    for column in selected_columns {
                        if !all_columns.contains(column) {
                            return Err(Error::ColumnNotFound(column.clone()));
                        }
                        
                        let value = row.get(column.as_str()).cloned().unwrap_or(Value::Null);
                        new_row.insert(column.clone(), value);
                    }
                    
                    result_rows.push(new_row);
                }
                
                Ok(result_rows)
            }
        }
    }
}

// Helper struct for parsing expressions
struct ExprParser {
    tokens: Vec<String>,
    pos: usize,
}

impl ExprParser {
    fn new(tokens: Vec<String>) -> Self {
        ExprParser { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&str> {
        self.tokens.get(self.pos).map(|s| s.as_str())
    }
    fn next(&mut self) -> Option<&str> {
        let tok = self.tokens.get(self.pos);
        self.pos += 1;
        tok.map(|s| s.as_str())
    }
    fn expect(&mut self, expected: &str) -> Result<()> {
        if self.peek() == Some(expected) {
            self.next();
            Ok(())
        } else {
            Err(Error::QueryError(format!("Expected '{}', found '{:?}'", expected, self.peek())))
        }
    }

    // Parse OR expressions (lowest precedence)
    fn parse_expr(&mut self) -> Result<Expr> {
        let mut expr = self.parse_and_expr()?;
        while let Some(token) = self.peek() {
            if token.to_uppercase() == "OR" {
                self.next();
                let right = self.parse_and_expr()?;
                expr = Expr::Or(Box::new(expr), Box::new(right));
            } else {
                break;
            }
        }
        Ok(expr)
    }
    // Parse AND expressions
    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut expr = self.parse_not_expr()?;
        while let Some(token) = self.peek() {
            if token.to_uppercase() == "AND" {
                self.next();
                let right = self.parse_not_expr()?;
                expr = Expr::And(Box::new(expr), Box::new(right));
            } else {
                break;
            }
        }
        Ok(expr)
    }
    // Parse NOT expressions
    fn parse_not_expr(&mut self) -> Result<Expr> {
        if let Some(token) = self.peek() {
            if token.to_uppercase() == "NOT" {
                self.next();
                let expr = self.parse_primary_expr()?;
                Ok(Expr::Not(Box::new(expr)))
            } else {
                self.parse_primary_expr()
            }
        } else {
            self.parse_primary_expr()
        }
    }
    // Parse primary expressions: parentheses, comparisons, IS NULL, IN
    fn parse_primary_expr(&mut self) -> Result<Expr> {
        if self.peek() == Some("(") {
            self.next();
            let expr = self.parse_expr()?;
            self.expect(")")?;
            return Ok(expr);
        }
        self.parse_comparison_expr()
    }
    // Parse comparison, IS NULL, IN, LIKE, BETWEEN
    fn parse_comparison_expr(&mut self) -> Result<Expr> {
        // Parse column name
        let column = self.next().ok_or_else(|| Error::QueryError("Expected column name".to_string()))?.to_string();
        if let Some(op) = self.peek() {
            match op.to_uppercase().as_str() {
                "IS" => {
                    self.next();
                    if let Some(token) = self.peek() {
                        if token.to_uppercase() == "NOT" {
                            self.next();
                            self.expect("NULL")?;
                            return Ok(Expr::IsNotNull(column));
                        } else {
                            self.expect("NULL")?;
                            return Ok(Expr::IsNull(column));
                        }
                    } else {
                        self.expect("NULL")?;
                        return Ok(Expr::IsNull(column));
                    }
                }
                "IN" => {
                    self.next();
                    self.expect("(")?;
                    let mut values = Vec::new();
                    loop {
                        let val_token = self.next().ok_or_else(|| Error::QueryError("Expected value in IN list".to_string()))?;
                        if val_token == ")" {
                            break;
                        }
                        let value = SelectQuery::parse_value(val_token)?;
                        values.push(value);
                        if self.peek() == Some(",") {
                            self.next();
                        } else if self.peek() == Some(")") {
                            self.next();
                            break;
                        }
                    }
                    return Ok(Expr::In { column, values });
                }
                "LIKE" => {
                    self.next();
                    let value_token = self.next().ok_or_else(|| Error::QueryError("Expected value after LIKE".to_string()))?;
                    let value = SelectQuery::parse_value(value_token)?;
                    return Ok(Expr::Comparison {
                        column,
                        operator: ComparisonOperator::Like,
                        value,
                        value2: None,
                    });
                }
                "BETWEEN" => {
                    self.next();
                    let value1_token = self.next().ok_or_else(|| Error::QueryError("Expected value after BETWEEN".to_string()))?;
                    let value1 = SelectQuery::parse_value(value1_token)?;
                    self.expect("AND")?;
                    let value2_token = self.next().ok_or_else(|| Error::QueryError("Expected value after AND in BETWEEN".to_string()))?;
                    let value2 = SelectQuery::parse_value(value2_token)?;
                    return Ok(Expr::Comparison {
                        column,
                        operator: ComparisonOperator::Between,
                        value: value1,
                        value2: Some(value2),
                    });
                }
                "=" => {
                    self.next();
                    let value_token = self.next().ok_or_else(|| Error::QueryError("Expected value after =".to_string()))?;
                    let value = SelectQuery::parse_value(value_token)?;
                    return Ok(Expr::Comparison {
                        column,
                        operator: ComparisonOperator::Equal,
                        value,
                        value2: None,
                    });
                }
                "!=" => {
                    self.next();
                    let value_token = self.next().ok_or_else(|| Error::QueryError("Expected value after !=".to_string()))?;
                    let value = SelectQuery::parse_value(value_token)?;
                    return Ok(Expr::Comparison {
                        column,
                        operator: ComparisonOperator::NotEqual,
                        value,
                        value2: None,
                    });
                }
                "<" => {
                    self.next();
                    let value_token = self.next().ok_or_else(|| Error::QueryError("Expected value after <".to_string()))?;
                    let value = SelectQuery::parse_value(value_token)?;
                    return Ok(Expr::Comparison {
                        column,
                        operator: ComparisonOperator::LessThan,
                        value,
                        value2: None,
                    });
                }
                "<=" => {
                    self.next();
                    let value_token = self.next().ok_or_else(|| Error::QueryError("Expected value after <=".to_string()))?;
                    let value = SelectQuery::parse_value(value_token)?;
                    return Ok(Expr::Comparison {
                        column,
                        operator: ComparisonOperator::LessThanOrEqual,
                        value,
                        value2: None,
                    });
                }
                ">" => {
                    self.next();
                    let value_token = self.next().ok_or_else(|| Error::QueryError("Expected value after >".to_string()))?;
                    let value = SelectQuery::parse_value(value_token)?;
                    return Ok(Expr::Comparison {
                        column,
                        operator: ComparisonOperator::GreaterThan,
                        value,
                        value2: None,
                    });
                }
                ">=" => {
                    self.next();
                    let value_token = self.next().ok_or_else(|| Error::QueryError("Expected value after >=".to_string()))?;
                    let value = SelectQuery::parse_value(value_token)?;
                    return Ok(Expr::Comparison {
                        column,
                        operator: ComparisonOperator::GreaterThanOrEqual,
                        value,
                        value2: None,
                    });
                }
                _ => {}
            }
        }
        Err(Error::QueryError(format!("Unsupported or invalid WHERE condition near '{}'.", column)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let query = SelectQuery::parse("SELECT * FROM users").unwrap();
        assert_eq!(query.table, "users");
        assert!(query.columns.is_none());
        assert!(query.where_expr.is_none());
    }

    #[test]
    fn test_parse_select_with_columns() {
        let query = SelectQuery::parse("SELECT name, email FROM users").unwrap();
        assert_eq!(query.table, "users");
        assert_eq!(query.columns.as_ref().unwrap(), &vec!["name".to_string(), "email".to_string()]);
    }

    #[test]
    fn test_parse_select_with_where() {
        let query = SelectQuery::parse("SELECT * FROM users WHERE age > 18").unwrap();
        assert_eq!(query.table, "users");
        assert!(query.where_expr.is_some());
        let expr = query.where_expr.as_ref().unwrap();
        if let Expr::Comparison { column, operator, .. } = expr {
            assert_eq!(column, "age");
            assert_eq!(operator, &ComparisonOperator::GreaterThan);
        } else {
            panic!("Expected Comparison expr");
        }
    }

    #[test]
    fn test_parse_select_with_order_by() {
        let query = SelectQuery::parse("SELECT * FROM users ORDER BY name ASC").unwrap();
        assert_eq!(query.table, "users");
        assert!(query.order_by.is_some());
        let order_by = query.order_by.unwrap();
        assert_eq!(order_by.column, "name");
        assert!(order_by.ascending);
    }

    #[test]
    fn test_parse_select_with_limit() {
        let query = SelectQuery::parse("SELECT * FROM users LIMIT 10").unwrap();
        assert_eq!(query.table, "users");
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_like_pattern_matching() {
        let query = SelectQuery::parse("SELECT * FROM users").unwrap();
        
        // Test prefix pattern 'f%'
        assert!(query.value_like(&Value::Text("foo".to_string()), &Value::Text("f%".to_string())));
        assert!(query.value_like(&Value::Text("ff736190-1479-4681-b9b2-78757cd55821".to_string()), &Value::Text("f%".to_string())));
        assert!(query.value_like(&Value::Text("fa18fc4d-11dc-466b-84cd-d6793ff93774".to_string()), &Value::Text("f%".to_string())));
        assert!(!query.value_like(&Value::Text("bar".to_string()), &Value::Text("f%".to_string())));
        
        // Test suffix pattern '%bar'
        assert!(query.value_like(&Value::Text("foobar".to_string()), &Value::Text("%bar".to_string())));
        assert!(!query.value_like(&Value::Text("foo".to_string()), &Value::Text("%bar".to_string())));
        
        // Test contains pattern '%middle%'
        assert!(query.value_like(&Value::Text("foo middle bar".to_string()), &Value::Text("%middle%".to_string())));
        assert!(!query.value_like(&Value::Text("foo bar".to_string()), &Value::Text("%middle%".to_string())));
        
        // Test exact match (no wildcards)
        assert!(query.value_like(&Value::Text("exact".to_string()), &Value::Text("exact".to_string())));
        assert!(!query.value_like(&Value::Text("different".to_string()), &Value::Text("exact".to_string())));
    }
}