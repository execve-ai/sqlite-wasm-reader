//! Example of reading a SQLite database using sqlite-wasm-reader

use sqlite_wasm_reader::{Database, Error, Value};
use std::env;

fn main() -> Result<(), Error> {
    // Get database path from command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <database.db>", args[0]);
        std::process::exit(1);
    }
    
    let db_path = &args[1];
    
    // Open the database
    println!("Opening database: {}", db_path);
    let mut db = Database::open(db_path)?;
    
    // List all tables
    println!("\nTables in the database:");
    let tables = db.tables()?;
    for table in &tables {
        println!("  - {}", table);
    }
    
    // Read and display data from each table
    for table in tables {
        println!("\n--- Table: {} ---", table);
        
        // First, count rows efficiently
        match db.count_table_rows(&table) {
            Ok(count) => {
                println!("  Total rows: {}", count);
            }
            Err(e) => {
                println!("  Error counting rows: {}", e);
                continue;
            }
        }
        
        match db.read_table(&table) {
            Ok(rows) => {
                if rows.is_empty() {
                    println!("  (empty table)");
                } else {
                    println!("  Found {} rows", rows.len());
                    
                    // For large tables, only show first 10 rows
                    let rows_to_show = if rows.len() > 10 {
                        println!("  (showing first 10 rows)");
                        &rows[..10]
                    } else {
                        &rows[..]
                    };
                    
                    display_table_data(rows_to_show);
                    
                    if rows.len() > 10 {
                        println!("  ... and {} more rows", rows.len() - 10);
                    }
                }
            }
            Err(e) => {
                println!("  Error reading table: {}", e);
            }
        }
    }
    
    Ok(())
}

/// Display table data in a formatted way
fn display_table_data(rows: &[sqlite_wasm_reader::Row]) {
    if rows.is_empty() {
        println!("  (empty table)");
        return;
    }
    
    // Print column headers from the first row
    if let Some(first_row) = rows.first() {
        let columns: Vec<&String> = first_row.keys().collect();
        println!("  Columns: {}", columns.iter()
            .map(|c| c.as_str())
            .collect::<Vec<_>>()
            .join(", "));
    }
    
    // Print each row
    for (i, row) in rows.iter().enumerate() {
        println!("\n  Row {}:", i + 1);
        for (column, value) in row {
            println!("    {}: {}", column, format_value(value));
        }
    }
}

/// Format a value for display
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Real(f) => f.to_string(),
        Value::Text(s) => format!("\"{}\"", s),
        Value::Blob(b) => format!("<BLOB {} bytes>", b.len()),
    }
} 