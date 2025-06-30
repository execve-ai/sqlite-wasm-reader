# sqlite-wasm-reader

A pure Rust SQLite reader library designed for WASI (WebAssembly System Interface) environments. This library provides read-only access to SQLite databases without any C dependencies, making it perfect for WebAssembly applications running in WasmTime or other WASI-compatible runtimes.

## Features

- **Pure Rust Implementation**: No C dependencies, fully written in Rust
- **WASI Compatible**: Designed to work in WebAssembly environments with WASI support
- **Read-Only Access**: Focused on reading SQLite databases (no write operations)
- **Minimal Dependencies**: Only depends on `byteorder` and `thiserror`
- **Configurable Logging**: Built-in logging system with multiple levels (Error, Warn, Info, Debug, Trace)
- **Robust B-tree Traversal**: Proper in-order traversal with cycle detection
- **Memory Efficient**: Designed to handle large databases with limited memory constraints
- **Simple API**: Easy-to-use interface for reading tables and data

## Why Read-Only?

This library is intentionally designed as **read-only** for several important reasons:

### 1. **WASI Environment Constraints**
- **Sandboxed Execution**: WASI environments are designed for security and isolation, often with restricted file system access
- **No Native Dependencies**: Avoiding C bindings eliminates compatibility issues in WebAssembly runtimes
- **Deterministic Behavior**: Read-only operations are more predictable and safer in sandboxed environments

### 2. **Security and Safety**
- **Immutable Data**: Prevents accidental data corruption or malicious modifications
- **Audit Trail**: Read-only access ensures data integrity for forensic and compliance purposes
- **No Lock Contention**: Eliminates complex locking mechanisms required for concurrent writes

### 3. **Performance and Reliability**
- **Simplified Architecture**: Read-only design allows for optimized, streamlined code paths
- **Reduced Complexity**: No need to handle transaction management, rollbacks, or write-ahead logging
- **Memory Efficiency**: Lower memory footprint without write buffers and transaction logs

### 4. **Use Case Alignment**
- **Data Analysis**: Perfect for reading and analyzing existing SQLite databases
- **Reporting**: Generate reports from production databases without modification risk
- **Audit and Compliance**: Safe access to sensitive data for regulatory requirements
- **Data Migration**: Read data from SQLite for migration to other systems

## Target Use Cases

This library is specifically designed for the following scenarios:

### 1. **WebAssembly Data Processing**
```rust
// Process SQLite data in a WASI environment
use sqlite_wasm_reader::{Database, Error};

fn analyze_user_data(db_path: &str) -> Result<(), Error> {
    let mut db = Database::open(db_path)?;
    let users = db.read_table("users")?;
    
    // Perform analysis without modifying the database
    for user in users {
        // Analyze user data...
    }
    Ok(())
}
```

### 2. **Sandboxed Analytics**
- **Security Scanning**: Analyze file metadata and content in isolated environments
- **Malware Detection**: Read database files for threat analysis without execution risk
- **Content Analysis**: Process user-generated content in secure containers

### 3. **Edge Computing and IoT**
- **Local Data Access**: Read configuration databases on edge devices
- **Offline Analytics**: Process data when network connectivity is limited
- **Resource-Constrained Environments**: Lightweight database access for embedded systems

### 4. **Data Pipeline Integration**
```rust
// Extract data from SQLite for ETL processes
fn extract_table_data(db_path: &str, table_name: &str) -> Result<Vec<Row>, Error> {
    let mut db = Database::open(db_path)?;
    db.read_table(table_name)
}
```

### 5. **Forensic and Compliance**
- **Data Auditing**: Safely examine databases for compliance verification
- **Digital Forensics**: Read evidence databases without contamination
- **Regulatory Reporting**: Generate reports from production systems

### 6. **Development and Testing**
- **Test Data Access**: Read test databases in CI/CD pipelines
- **Development Tools**: Build tools that analyze database schemas and content
- **Debugging**: Examine database state during development

## When to Use This Library

**Use this library when you need to:**
- ✅ Read SQLite databases in WASI/WebAssembly environments
- ✅ Analyze data without modifying the source database
- ✅ Work in sandboxed or restricted environments
- ✅ Build lightweight, dependency-free applications
- ✅ Process large databases with memory constraints
- ✅ Integrate SQLite reading into data pipelines

## Why Writing from WASM Sandboxes is Problematic

This library is intentionally read-only because writing to SQLite from WebAssembly sandboxes presents significant risks:

### **Data Corruption from Concurrent Writes**
- **Multiple WASM Instances**: When multiple WebAssembly instances write to the same SQLite database simultaneously, they can corrupt the database structure
- **No File Locking**: WASI environments often lack proper file locking mechanisms that SQLite relies on for write safety

### **Technical Limitations**
- **WAL Mode Issues**: SQLite's Write-Ahead Logging requires coordination of multiple files that may not be available in sandboxed environments
- **Shared Memory Problems**: SQLite's locking mechanisms rely on shared memory regions that may not be properly isolated in WASM
- **Partial Writes**: If a WASM instance crashes during a write operation, the database can be left in an inconsistent state

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
sqlite-wasm-reader = "0.1.0"
```

## Quick Start

```rust
use sqlite_wasm_reader::{Database, Error, LogLevel, init_default_logger};

fn main() -> Result<(), Error> {
    // Initialize logging (optional, defaults to Info level)
    init_default_logger();
    
    // Open a SQLite database
    let mut db = Database::open("example.db")?;
    
    // List all tables
    let tables = db.tables()?;
    for table in tables {
        println!("Table: {}", table);
    }
    
    // Read all rows from a table
    let rows = db.read_table("users")?;
    for row in rows {
        println!("{:?}", row);
    }
    
    Ok(())
}
```

## Logging

The library includes a configurable logging system to help with debugging and monitoring:

```rust
use sqlite_wasm_reader::{LogLevel, init_logger, set_log_level};

// Initialize with custom log level
init_logger(LogLevel::Debug);

// Change log level at runtime
set_log_level(LogLevel::Trace);

// Log levels available:
// - Error: Critical errors that prevent operation
// - Warn: Important warnings and errors
// - Info: General information about operations (default)
// - Debug: Detailed debugging information
// - Trace: Very detailed tracing information
```

## API Reference

### Database Operations

```rust
// Open a database
let mut db = Database::open("path/to/database.db")?;

// List all tables
let tables = db.tables()?;

// Read all rows from a table
let rows = db.read_table("table_name")?;

// Read limited number of rows (useful for large tables)
let rows = db.read_table_limited("table_name", 1000)?;

// Count rows in a table efficiently
let count = db.count_table_rows("table_name")?;
```

### Value Types

The library supports all basic SQLite types:

```rust
use sqlite_wasm_reader::Value;

// NULL values
Value::Null

// Integer values
Value::Integer(42)

// Floating point values
Value::Real(3.14)

// Text values
Value::Text("hello".to_string())

// BLOB values
Value::Blob(vec![0x01, 0x02, 0x03])
```

### Row Access

Rows are represented as `HashMap<String, Value>`:

```rust
for row in rows {
    // Access by column name
    if let Some(id) = row.get("id") {
        match id {
            Value::Integer(i) => println!("ID: {}", i),
            Value::Text(s) => println!("ID: {}", s),
            _ => println!("Unexpected ID type"),
        }
    }
    
    // Check if column exists
    if row.contains_key("name") {
        println!("Has name column");
    }
}
```

## Building for WASI

To build this crate for WASI target:

```bash
# Add the WASI target
rustup target add wasm32-wasip1

# Build the project
cargo build --target wasm32-wasip1 --release
```

## Running with WasmTime

```bash
# Run with wasmtime
wasmtime run --dir=. target/wasm32-wasip1/release/your_app.wasm

# Run with file access
wasmtime run --dir=. --mapdir /data:./data target/wasm32-wasip1/release/your_app.wasm
```

## Examples

The library includes several examples demonstrating different use cases:

### Basic Database Reading

```rust
use sqlite_wasm_reader::{Database, Error, Value};

fn main() -> Result<(), Error> {
    let mut db = Database::open("users.db")?;
    
    // Read user table
    let users = db.read_table("users")?;
    
    for user in users {
        let name = user.get("name").unwrap_or(&Value::Null);
        let email = user.get("email").unwrap_or(&Value::Null);
        
        println!("User: {} <{}>", name, email);
    }
    
    Ok(())
}
```

### Large Table Handling

```rust
use sqlite_wasm_reader::{Database, Error};

fn main() -> Result<(), Error> {
    let mut db = Database::open("large_database.db")?;
    
    // For large tables, use limited reading
    let batch_size = 1000;
    let mut offset = 0;
    
    loop {
        let rows = db.read_table_limited("large_table", batch_size)?;
        if rows.is_empty() {
            break;
        }
        
        println!("Processing batch of {} rows", rows.len());
        // Process rows...
        
        offset += rows.len();
    }
    
    Ok(())
}
```

### Efficient Row Counting

```rust
use sqlite_wasm_reader::{Database, Error};

fn main() -> Result<(), Error> {
    let mut db = Database::open("database.db")?;
    
    // Count rows without loading all data into memory
    let user_count = db.count_table_rows("users")?;
    let order_count = db.count_table_rows("orders")?;
    
    println!("Users: {}, Orders: {}", user_count, order_count);
    
    Ok(())
}
```

### Logging and Debugging

```rust
use sqlite_wasm_reader::{Database, Error, LogLevel, init_default_logger, set_log_level, log_debug};

fn main() -> Result<(), Error> {
    // Initialize logging with debug level
    init_default_logger();
    set_log_level(LogLevel::Debug);
    
    let mut db = Database::open("database.db")?;
    
    // Enable debug logging for troubleshooting
    log_debug("Starting database analysis");
    
    let tables = db.tables()?;
    log_debug(&format!("Found {} tables", tables.len()));
    
    for table in tables {
        let count = db.count_table_rows(&table)?;
        log_debug(&format!("Table {} has {} rows", table, count));
    }
    
    Ok(())
}
```

### Running the Examples

```bash
# Basic database reading
cargo run --example read_db -- database.db

# Logging example with custom log level
cargo run --example logging_example -- database.db debug

# Efficient row counting
cargo run --example count_rows -- database.db

# WASI-compatible example
cargo build --example wasi_example --target wasm32-wasip1
wasmtime run --dir=. target/wasm32-wasip1/debug/examples/wasi_example.wasm -- database.db
```

## Limitations

- **Read-Only**: This library only supports reading SQLite databases, not writing
- **Basic SQL Types**: Supports NULL, INTEGER, REAL, TEXT, and BLOB types
- **No Index Support**: Currently doesn't support reading from indexes
- **Simple Schema Parsing**: Basic CREATE TABLE parsing for column names
- **Memory Constraints**: Large databases may require using `read_table_limited()` to avoid memory issues

## Architecture

The library is structured into several modules:

- `format`: SQLite file format constants and structures
- `page`: Page reading and parsing
- `btree`: B-tree traversal for table data with cycle detection
- `record`: SQLite record parsing
- `value`: Value types (NULL, INTEGER, REAL, TEXT, BLOB)
- `database`: Main database interface
- `logging`: Configurable logging system
- `error`: Error types and handling

## Performance Considerations

- **Memory Usage**: For large databases, use `read_table_limited()` to control memory usage
- **B-tree Traversal**: The library uses efficient in-order traversal with cycle detection
- **Logging Overhead**: Set appropriate log levels to minimize performance impact
- **WASI Environment**: Optimized for WebAssembly environments with limited resources
- **Row Counting**: Use `count_table_rows()` for efficient row counting without loading data

## Error Handling

The library provides comprehensive error handling:

```rust
use sqlite_wasm_reader::{Database, Error};

match Database::open("database.db") {
    Ok(mut db) => {
        // Database opened successfully
    }
    Err(Error::Io(e)) => {
        eprintln!("IO error: {}", e);
    }
    Err(Error::InvalidFormat(msg)) => {
        eprintln!("Invalid SQLite format: {}", msg);
    }
    Err(Error::TableNotFound(table)) => {
        eprintln!("Table not found: {}", table);
    }
    Err(e) => {
        eprintln!("Other error: {}", e);
    }
}
```

## License

This project is licensed under Apache License, Version 2.0, ([LICENSE](LICENSE) or http://www.apache.org/licenses/LICENSE-2.0)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## Changelog

### Version 0.1.0
- Basic SQLite reading functionality
- WASI-compatible implementation