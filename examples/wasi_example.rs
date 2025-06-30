//! WASI-compatible example for sqlite_wasm_reader
//! 
//! This example can be compiled to WASI and run with wasmtime:
//! cargo build --example wasi_example --target wasm32-wasip1
//! wasmtime run --dir=. target/wasm32-wasip1/debug/examples/wasi_example.wasm -- test.db

use sqlite_wasm_reader::{Database, Error, init_default_logger, log_info};
use std::env;

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <database.db>", args[0]);
        return Ok(());
    }
    
    let db_path = &args[1];
    println!("Opening database: {}", db_path);
    
    // Initialize logging for WASI environment
    init_default_logger();
    log_info(&format!("Starting WASI database analysis: {}", db_path));
    
    let mut db = Database::open(db_path)?;
    log_info("Database opened successfully");
    
    // List tables
    let tables = db.tables()?;
    println!("\nFound {} tables:", tables.len());
    for table in &tables {
        println!("  - {}", table);
    }
    
    // Read first table if any exist
    if let Some(first_table) = tables.first() {
        println!("\nReading table '{}':", first_table);
        log_info(&format!("Analyzing table: {}", first_table));
        
        // Count rows efficiently first
        match db.count_table_rows(first_table) {
            Ok(count) => {
                println!("  Total rows: {}", count);
                log_info(&format!("Table {} has {} rows", first_table, count));
            }
            Err(e) => {
                eprintln!("  Error counting rows: {}", e);
                log_info(&format!("Failed to count rows in table {}: {}", first_table, e));
            }
        }
        
        match db.read_table(first_table) {
            Ok(rows) => {
                println!("  Found {} rows", rows.len());
                log_info(&format!("Successfully read {} rows from table {}", rows.len(), first_table));
                
                // Print first few rows
                let rows_to_show = if rows.len() > 5 {
                    println!("  (showing first 5 rows)");
                    &rows[..5]
                } else {
                    &rows[..]
                };
                
                for (i, row) in rows_to_show.iter().enumerate() {
                    println!("  Row {}: {:?}", i + 1, row);
                }
                
                if rows.len() > 5 {
                    println!("  ... and {} more rows", rows.len() - 5);
                }
            }
            Err(e) => {
                eprintln!("  Error reading table: {}", e);
                log_info(&format!("Failed to read table {}: {}", first_table, e));
            }
        }
    }
    
    log_info("WASI database analysis completed");
    Ok(())
} 