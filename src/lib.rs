//! SQLite WASM Reader - A pure Rust SQLite reader for WASI environments
//! 
//! This library provides a lightweight SQLite database reader that can be used
//! in WebAssembly environments, particularly WASI. It supports reading tables,
//! parsing records, and basic database operations without requiring native
//! SQLite bindings.
//!
//! # Example
//!
//! ```no_run
//! use sqlite_wasm_reader::{Database, Error};
//!
//! fn main() -> Result<(), Error> {
//!     let mut db = Database::open("example.db")?;
//!     
//!     // List all tables
//!     let tables = db.tables()?;
//!     for table in tables {
//!         println!("Table: {}", table);
//!     }
//!     
//!     // Read all rows from a table
//!     let rows = db.read_table("users")?;
//!     for row in rows {
//!         println!("{:?}", row);
//!     }
//!     
//!     Ok(())
//! }
//! ```

// Only use no_std for non-WASI WebAssembly targets
#![cfg_attr(all(target_arch = "wasm32", not(target_os = "wasi")), no_std)]

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
extern crate alloc;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{string::String, vec::Vec, format};

pub mod error;
pub mod format;
pub mod page;
pub mod btree;
pub mod record;
pub mod database;
pub mod value;
pub mod logging;
pub mod query;

pub use error::{Error, Result};
pub use database::Database;
pub use value::Value;
pub use logging::{Logger, LogLevel, init_default_logger, set_log_level, log_error, log_warn, log_info, log_debug, log_trace};
pub use query::{SelectQuery, ComparisonOperator, OrderBy};

// Re-export commonly used types
pub use format::{FileHeader, PageType};
pub use page::Page;
pub use btree::{BTreeCursor, Cell};

// Re-export key types
pub use database::Row;
