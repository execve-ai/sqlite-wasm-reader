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
    /// WHERE clause conditions
    pub where_conditions: Vec<WhereCondition>,
    /// ORDER BY clause
    pub order_by: Option<OrderBy>,
    /// LIMIT clause
    pub limit: Option<usize>,
}

/// Represents a WHERE condition
#[derive(Debug, Clone)]
pub struct WhereCondition {
    pub column: String,
    pub operator: ComparisonOperator,
    pub value: Value,
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
        let mut where_conditions = Vec::new();
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
            where_conditions = Self::parse_where_clause(&where_part)?;
            
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
            where_conditions,
            order_by,
            limit,
        })
    }
    
    /// Parse WHERE clause conditions (simplified parser)
    fn parse_where_clause(where_str: &str) -> Result<Vec<WhereCondition>> {
        let mut conditions = Vec::new();
        
        // For now, support simple conditions like "column = value"
        // Split by AND (case insensitive)
        let and_parts: Vec<&str> = where_str.split(" AND ").collect();
        
        for part in and_parts {
            let part = part.trim();
            
            // Find the operator
            let (column, operator, value_str) = if part.contains(" = ") {
                let mut split = part.splitn(2, " = ");
                let col = split.next().unwrap_or("").trim();
                let val = split.next().unwrap_or("").trim();
                (col, ComparisonOperator::Equal, val)
            } else if part.contains(" != ") {
                let mut split = part.splitn(2, " != ");
                let col = split.next().unwrap_or("").trim();
                let val = split.next().unwrap_or("").trim();
                (col, ComparisonOperator::NotEqual, val)
            } else if part.contains(" <= ") {
                let mut split = part.splitn(2, " <= ");
                let col = split.next().unwrap_or("").trim();
                let val = split.next().unwrap_or("").trim();
                (col, ComparisonOperator::LessThanOrEqual, val)
            } else if part.contains(" >= ") {
                let mut split = part.splitn(2, " >= ");
                let col = split.next().unwrap_or("").trim();
                let val = split.next().unwrap_or("").trim();
                (col, ComparisonOperator::GreaterThanOrEqual, val)
            } else if part.contains(" < ") {
                let mut split = part.splitn(2, " < ");
                let col = split.next().unwrap_or("").trim();
                let val = split.next().unwrap_or("").trim();
                (col, ComparisonOperator::LessThan, val)
            } else if part.contains(" > ") {
                let mut split = part.splitn(2, " > ");
                let col = split.next().unwrap_or("").trim();
                let val = split.next().unwrap_or("").trim();
                (col, ComparisonOperator::GreaterThan, val)
            } else if part.to_uppercase().contains(" LIKE ") {
                let mut split = part.splitn(2, " LIKE ");
                let col = split.next().unwrap_or("").trim();
                let val = split.next().unwrap_or("").trim();
                (col, ComparisonOperator::Like, val)
            } else {
                return Err(Error::QueryError(format!("Unsupported WHERE condition: {}", part)));
            };
            
            // Parse the value
            let value = Self::parse_value(value_str)?;
            
            conditions.push(WhereCondition {
                column: column.to_string(),
                operator,
                value,
            });
        }
        
        Ok(conditions)
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
        if self.where_conditions.is_empty() {
            return Ok(rows);
        }
        
        let filtered_rows: Vec<Row> = rows
            .into_iter()
            .filter(|row| {
                for condition in &self.where_conditions {
                    if !self.evaluate_condition(row, condition) {
                        return false;
                    }
                }
                true
            })
            .collect();
        
        Ok(filtered_rows)
    }
    
    /// Evaluate a single WHERE condition against a row
    fn evaluate_condition(&self, row: &Row, condition: &WhereCondition) -> bool {
        let row_value = match row.get(&condition.column) {
            Some(value) => value,
            None => return false, // Column doesn't exist
        };
        
        match condition.operator {
            ComparisonOperator::Equal => self.values_equal(row_value, &condition.value),
            ComparisonOperator::NotEqual => !self.values_equal(row_value, &condition.value),
            ComparisonOperator::LessThan => self.value_less_than(row_value, &condition.value),
            ComparisonOperator::LessThanOrEqual => {
                self.value_less_than(row_value, &condition.value) || 
                self.values_equal(row_value, &condition.value)
            },
            ComparisonOperator::GreaterThan => {
                !self.value_less_than(row_value, &condition.value) && 
                !self.values_equal(row_value, &condition.value)
            },
            ComparisonOperator::GreaterThanOrEqual => {
                !self.value_less_than(row_value, &condition.value)
            },
            ComparisonOperator::Like => self.value_like(row_value, &condition.value),
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
    
    /// Check if value matches LIKE pattern (simplified implementation)
    fn value_like(&self, value: &Value, pattern: &Value) -> bool {
        match (value, pattern) {
            (Value::Text(text), Value::Text(pattern)) => {
                // Simple LIKE implementation with % wildcard
                if pattern.contains('%') {
                    let pattern_parts: Vec<&str> = pattern.split('%').collect();
                    if pattern_parts.len() == 2 {
                        let prefix = pattern_parts[0];
                        let suffix = pattern_parts[1];
                        return text.starts_with(prefix) && text.ends_with(suffix);
                    }
                }
                text == pattern
            },
            _ => false,
        }
    }
    
    /// Apply ORDER BY to sort rows
    fn apply_order_by(&self, mut rows: Vec<Row>, order_by: &OrderBy) -> Result<Vec<Row>> {
        rows.sort_by(|a, b| {
            let val_a = a.get(&order_by.column);
            let val_b = b.get(&order_by.column);
            
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
                        
                        let value = row.get(column).cloned().unwrap_or(Value::Null);
                        new_row.insert(column.clone(), value);
                    }
                    
                    result_rows.push(new_row);
                }
                
                Ok(result_rows)
            }
        }
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
        assert!(query.where_conditions.is_empty());
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
        assert_eq!(query.where_conditions.len(), 1);
        assert_eq!(query.where_conditions[0].column, "age");
        assert_eq!(query.where_conditions[0].operator, ComparisonOperator::GreaterThan);
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
}