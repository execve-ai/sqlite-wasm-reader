//! Example demonstrating SELECT query functionality

use sqlite_wasm_reader::{Database, Error, Value, init_default_logger, set_log_level, LogLevel, SelectQuery};
use std::env;

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <database.db>", args[0]);
        std::process::exit(1);
    }
    
    let db_path = &args[1];
    
    // Initialize logging
    init_default_logger();
    set_log_level(LogLevel::Info);
    
    println!("Opening database: {}", db_path);
    let mut db = Database::open(db_path)?;
    
    // List available tables
    println!("\nAvailable tables:");
    let tables = db.tables()?;
    for table in &tables {
        println!("  - {}", table);
    }
    
    if tables.is_empty() {
        println!("No tables found in database");
        return Ok(());
    }
    
    // Use the first table for examples
    let table_name = &tables[0];
    println!("\nRunning queries on table: {}", table_name);
    
    // Example 1: Simple SELECT *
    println!("\n=== Example 1: SELECT * FROM {} ===", table_name);
    match SelectQuery::parse(&format!("SELECT * FROM {}", table_name)).and_then(|q| db.execute_query(&q)) {
        Ok(rows) => {
            println!("Found {} rows:", rows.len());
            for (i, row) in rows.iter().take(3).enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
            if rows.len() > 3 {
                println!("... and {} more rows", rows.len() - 3);
            }
        }
        Err(e) => println!("Error: {}", e),
    }
    
    // Example 2: SELECT with specific columns
    println!("\n=== Example 2: SELECT specific columns ===");
    // Get first few column names from the table
    if let Ok(sample_rows) = db.read_table_limited(table_name, 1) {
        if !sample_rows.is_empty() {
            let columns: Vec<String> = sample_rows[0].keys().cloned().collect();
            if columns.len() >= 2 {
                let col1 = &columns[0];
                let col2 = &columns[1];
                let query = format!("SELECT {}, {} FROM {}", col1, col2, table_name);
                println!("Query: {}", query);
                
                match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                    Ok(rows) => {
                        println!("Found {} rows:", rows.len());
                        for (i, row) in rows.iter().take(3).enumerate() {
                            println!("Row {}: {:?}", i + 1, row);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
        }
    }
    
    // Example 3: SELECT with WHERE clause
    println!("\n=== Example 3: SELECT with WHERE clause ===");
    // Try to find a numeric column for the WHERE clause
    if let Ok(sample_rows) = db.read_table_limited(table_name, 10) {
        if !sample_rows.is_empty() {
            for (column, value) in sample_rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!("SELECT * FROM {} WHERE {} > {}", table_name, column, int_val - 1);
                    println!("Query: {}", query);
                    
                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows matching condition:", rows.len());
                            for (i, row) in rows.iter().take(2).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }
    
    // Example 4: SELECT with ORDER BY
    println!("\n=== Example 4: SELECT with ORDER BY ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 1) {
        if !sample_rows.is_empty() {
            let columns: Vec<String> = sample_rows[0].keys().cloned().collect();
            if !columns.is_empty() {
                let order_column = &columns[0];
                let query = format!("SELECT * FROM {} ORDER BY {} DESC", table_name, order_column);
                println!("Query: {}", query);
                
                match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                    Ok(rows) => {
                        println!("Found {} rows (ordered):", rows.len());
                        for (i, row) in rows.iter().take(3).enumerate() {
                            println!("Row {}: {:?}", i + 1, row);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
        }
    }
    
    // Example 5: SELECT with LIMIT
    println!("\n=== Example 5: SELECT with LIMIT ===");
    let query = format!("SELECT * FROM {} LIMIT 5", table_name);
    println!("Query: {}", query);
    
    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
        Ok(rows) => {
            println!("Found {} rows (limited to 5):", rows.len());
            for (i, row) in rows.iter().enumerate() {
                println!("Row {}: {:?}", i + 1, row);
            }
        }
        Err(e) => println!("Error: {}", e),
    }
    
    // Example 6: Complex query with WHERE, ORDER BY, and LIMIT
    println!("\n=== Example 6: Complex query ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 10) {
        if !sample_rows.is_empty() {
            let columns: Vec<String> = sample_rows[0].keys().cloned().collect();
            if columns.len() >= 2 {
                let col1 = &columns[0];
                let col2 = &columns[1];
                
                // Try to find a good value for WHERE clause
                for row in &sample_rows {
                    if let Some(Value::Text(text_val)) = row.get(col2) {
                        if !text_val.is_empty() {
                            let query = format!(
                                "SELECT {} FROM {} WHERE {} = '{}' ORDER BY {} LIMIT 3",
                                col1, table_name, col2, text_val, col1
                            );
                            println!("Query: {}", query);
                            
                            match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                                Ok(rows) => {
                                    println!("Found {} rows:", rows.len());
                                    for (i, row) in rows.iter().enumerate() {
                                        println!("Row {}: {:?}", i + 1, row);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
    
    // Example 7: Demonstrate LIKE operator
    println!("\n=== Example 7: SELECT with LIKE ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 10) {
        if !sample_rows.is_empty() {
            for (column, value) in sample_rows[0].iter() {
                if let Value::Text(text_val) = value {
                    if text_val.len() > 2 {
                        let prefix = &text_val[..2];
                        let query = format!("SELECT * FROM {} WHERE {} LIKE '{}%'", table_name, column, prefix);
                        println!("Query: {}", query);
                        
                        match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                            Ok(rows) => {
                                println!("Found {} rows matching pattern:", rows.len());
                                for (i, row) in rows.iter().take(2).enumerate() {
                                    println!("Row {}: {:?}", i + 1, row);
                                }
                            }
                            Err(e) => println!("Error: {}", e),
                        }
                        break;
                    }
                }
            }
        }
    }
    
    // Example 8: Demonstrate OR operator
    println!("\n=== Example 8: SELECT with OR ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 10) {
        if !sample_rows.is_empty() {
            for (column, value) in sample_rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!("SELECT * FROM {} WHERE {} = {} OR {} = {}", 
                        table_name, column, int_val, column, int_val + 1);
                    println!("Query: {}", query);
                    
                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows matching OR condition:", rows.len());
                            for (i, row) in rows.iter().take(3).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }
    
    // Example 9: Demonstrate IN operator
    println!("\n=== Example 9: SELECT with IN ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 10) {
        if !sample_rows.is_empty() {
            for (column, value) in sample_rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!("SELECT * FROM {} WHERE {} IN ({}, {}, {})", 
                        table_name, column, int_val, int_val + 1, int_val + 2);
                    println!("Query: {}", query);
                    
                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows matching IN condition:", rows.len());
                            for (i, row) in rows.iter().take(3).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }
    
    // Example 10: Demonstrate BETWEEN operator
    println!("\n=== Example 10: SELECT with BETWEEN ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 10) {
        if !sample_rows.is_empty() {
            for (column, value) in sample_rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!("SELECT * FROM {} WHERE {} BETWEEN {} AND {}", 
                        table_name, column, int_val, int_val + 5);
                    println!("Query: {}", query);
                    
                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows matching BETWEEN condition:", rows.len());
                            for (i, row) in rows.iter().take(3).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }
    
    // Example 11: Demonstrate NOT operator
    println!("\n=== Example 11: SELECT with NOT ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 10) {
        if !sample_rows.is_empty() {
            for (column, value) in sample_rows[0].iter() {
                if let Value::Integer(int_val) = value {
                    let query = format!("SELECT * FROM {} WHERE NOT ({} = {})", 
                        table_name, column, int_val);
                    println!("Query: {}", query);
                    
                    match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                        Ok(rows) => {
                            println!("Found {} rows matching NOT condition:", rows.len());
                            for (i, row) in rows.iter().take(3).enumerate() {
                                println!("Row {}: {:?}", i + 1, row);
                            }
                        }
                        Err(e) => println!("Error: {}", e),
                    }
                    break;
                }
            }
        }
    }
    
    // Example 12: Demonstrate complex expression with parentheses
    println!("\n=== Example 12: SELECT with complex expression ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 10) {
        if !sample_rows.is_empty() {
            let columns: Vec<String> = sample_rows[0].keys().cloned().collect();
            if columns.len() >= 2 {
                let col1 = &columns[0];
                let col2 = &columns[1];
                
                // Find suitable values for the complex expression
                for row in &sample_rows {
                    if let (Some(Value::Integer(val1)), Some(Value::Text(text_val))) = (row.get(col1), row.get(col2)) {
                        if !text_val.is_empty() {
                            let query = format!(
                                "SELECT * FROM {} WHERE ({} = {} OR {} = {}) AND {} LIKE '{}%'",
                                table_name, col1, val1, col1, val1 + 1, col2, &text_val[..1]
                            );
                            println!("Query: {}", query);
                            
                            match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                                Ok(rows) => {
                                    println!("Found {} rows matching complex condition:", rows.len());
                                    for (i, row) in rows.iter().take(3).enumerate() {
                                        println!("Row {}: {:?}", i + 1, row);
                                    }
                                }
                                Err(e) => println!("Error: {}", e),
                            }
                            break;
                        }
                    }
                }
            }
        }
    }
    
    // Example 13: Demonstrate IS NULL (if we can find nullable columns)
    println!("\n=== Example 13: SELECT with IS NULL ===");
    // This example might not find many results since most columns have values
    // but it demonstrates the syntax
    if let Ok(sample_rows) = db.read_table_limited(table_name, 1) {
        if !sample_rows.is_empty() {
            let columns: Vec<String> = sample_rows[0].keys().cloned().collect();
            if !columns.is_empty() {
                let column = &columns[0];
                let query = format!("SELECT * FROM {} WHERE {} IS NULL", table_name, column);
                println!("Query: {}", query);
                
                match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                    Ok(rows) => {
                        println!("Found {} rows with NULL values:", rows.len());
                        for (i, row) in rows.iter().take(3).enumerate() {
                            println!("Row {}: {:?}", i + 1, row);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
        }
    }
    
    // Example 14: Demonstrate IS NOT NULL
    println!("\n=== Example 14: SELECT with IS NOT NULL ===");
    if let Ok(sample_rows) = db.read_table_limited(table_name, 1) {
        if !sample_rows.is_empty() {
            let columns: Vec<String> = sample_rows[0].keys().cloned().collect();
            if !columns.is_empty() {
                let column = &columns[0];
                let query = format!("SELECT * FROM {} WHERE {} IS NOT NULL LIMIT 3", table_name, column);
                println!("Query: {}", query);
                
                match SelectQuery::parse(&query).and_then(|q| db.execute_query(&q)) {
                    Ok(rows) => {
                        println!("Found {} rows with non-NULL values:", rows.len());
                        for (i, row) in rows.iter().enumerate() {
                            println!("Row {}: {:?}", i + 1, row);
                        }
                    }
                    Err(e) => println!("Error: {}", e),
                }
            }
        }
    }
    
    println!("\n=== Query Examples Complete ===");
    
    Ok(())
}