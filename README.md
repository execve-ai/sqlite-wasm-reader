# sqlite_wasm_reader

A pure Rust SQLite reader library designed for WASI (WebAssembly System Interface) environments. This library provides read-only access to SQLite databases without any C dependencies, making it perfect for WebAssembly applications running in WasmTime or other WASI-compatible runtimes.

## Version 0.3.0

This version introduces comprehensive SQL query support with enhanced WHERE clause capabilities including logical operators (`AND`, `OR`, `NOT`), null checks (`IS NULL`, `IS NOT NULL`), membership testing (`IN`), range queries (`BETWEEN`), pattern matching (`LIKE`), and complex expressions with parentheses.

See [CHANGELOG.md](CHANGELOG.md) for detailed release information.

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
use sqlite_wasm_reader::query::SelectQuery;

fn analyze_user_data(db_path: &str) -> Result<(), Error> {
    let mut db = Database::open(db_path)?;
    let users = db.execute_query(&SelectQuery::parse("SELECT * FROM users")?)?;
    
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
    db.execute_query(&SelectQuery::parse(&format!("SELECT * FROM {}", table_name))?)
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
sqlite_wasm_reader = "0.3.0"
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
    
    // Execute a query using indexes
    let query = SelectQuery::parse("SELECT * FROM users WHERE id = 1")?;
    let rows = db.execute_query(&query)?;
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

// Execute a query using indexes
let query = SelectQuery::parse("SELECT * FROM table_name WHERE column = 'value'")?;
let rows = db.execute_query(&query)?;

// Count rows in a table efficiently
let count = db.count_table_rows("table_name")?;
```

### Query Builder Helpers

For programmatic construction of `SELECT` queries without writing raw SQL, use the fluent helper API:

```rust
use sqlite_wasm_reader::{query::{SelectQuery, Expr}, Value};

let query = SelectQuery::new("users")
    .select_columns(vec!["id".into(), "name".into()])
    .with_where(
        Expr::eq("status", Value::Text("active".into()))
            .and(Expr::between("age", Value::Integer(18), Value::Integer(65)))
            .or(Expr::is_null("deleted_at"))
    )
    .with_order_by("name", true)
    .with_limit(100);

let rows = db.execute_query(&query)?;
```

### SQL Query Support

`sqlite_wasm_reader` lets you query data either by parsing raw SQL _or_ by constructing `SelectQuery` objects directly and executing them with `Database::execute_query()`.

```rust
use sqlite_wasm_reader::{Database, Error};
use sqlite_wasm_reader::query::SelectQuery;
use sqlite_wasm_reader::query::{SelectQuery, Expr};
use sqlite_wasm_reader::value::Value;

fn complex_report(db: &mut Database) -> Result<(), Error> {
    // Option 1. Parse raw SQL, then execute
    let raw = "SELECT * FROM users WHERE age > 18 AND status = 'active' ORDER BY name LIMIT 10";
    let parsed = SelectQuery::parse(raw)?;
    let rows = db.execute_query(&parsed)?;
    println!("{} rows (raw SQL): {}", rows.len(), raw);

    // Option 2. Build programmatically using helpers
    let builder = SelectQuery::new("users")
        .select_columns(vec!["id".into(), "name".into(), "age".into()])
        .with_where(
            Expr::gt("age", Value::Integer(18))
                .and(Expr::eq("status", Value::Text("active".into())))
        )
        .with_order_by("name", true)
        .with_limit(10);

    let rows = db.execute_query(&builder)?;
    println!("{} rows (builder API)", rows.len());

    Ok(())
}
```

Both paths end in a call to `execute_query`, which accepts any `SelectQuery` (parsed or manually constructed). This method uses intelligent query processing:

* **Index Acceleration**: Automatically uses available indexes for exact equality matches when suitable indexes exist
* **Table Scan Fallback**: Seamlessly falls back to full table scans when no suitable index is found, ensuring all queries work
* **WHERE filtering** with logical operators (`AND`, `OR`, `NOT`), `LIKE`, `IN`, `BETWEEN`, `IS NULL` / `IS NOT NULL`, and parentheses
* **Column projection** (`SELECT *` or explicit columns)
* **`ORDER BY` and `LIMIT`** processing in memory

Use whichever style (raw SQL vs builder) best fits your workflow.

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
    let users = db.execute_query(&SelectQuery::parse("SELECT * FROM users")?)?;
    
    for user in users {
        let name = user.get("name").unwrap_or(&Value::Null);
        let email = user.get("email").unwrap_or(&Value::Null);
        
        println!("User: {} <{}>", name, email);
    }
    
    Ok(())
}
```

### Efficient Row Counting

```rust
use sqlite_wasm_reader::{Database, Error};
use sqlite_wasm_reader::query::SelectQuery;

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
- **Partial Index Support**: Uses indexes for exact equality matches when available, falls back to table scans for complex queries or when no suitable index exists
- **Simple Schema Parsing**: Basic CREATE TABLE parsing for column names
- **Memory Constraints**: Executing `SELECT *` on very large tables can be memory-intensive. Prefer filtering with WHERE clauses and/or fetching data in smaller chunks using `LIMIT` / `OFFSET` whenever possible.

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

- **Memory Usage**: For huge datasets, process data in pages via repeated queries with `LIMIT` / `OFFSET`, or add selective WHERE conditions to minimize the rows materialized at once.
- **B-tree Traversal**: The library uses efficient in-order traversal with cycle detection
- **Logging Overhead**: Set appropriate log levels to minimize performance impact
- **WASI Environment**: Optimized for WebAssembly environments with limited resources
- **Row Counting**: Use `count_table_rows()` for efficient row counting without loading data

## Error Handling

The library provides comprehensive error handling:

```rust
use sqlite_wasm_reader::{Database, Error};
use sqlite_wasm_reader::query::SelectQuery;

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
