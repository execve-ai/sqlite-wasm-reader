//! Main database interface

use crate::{
    Error, Result, Value,
    format::{FileHeader, SQLITE_HEADER_MAGIC},
    page::Page,
    btree::BTreeCursor,
    record::parse_record,
    logging::{log_error, log_warn, log_info, log_debug},
    query::SelectQuery,
};
use byteorder::{BigEndian, ByteOrder};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::collections::HashMap;

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{vec::Vec, string::String, format};

/// A row of data from a table
pub type Row = HashMap<String, Value>;

/// SQLite database reader
pub struct Database {
    file: File,
    header: FileHeader,
}

impl Database {
    /// Open a SQLite database file for reading
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path)?;
        
        // Read and validate header
        let mut header_bytes = [0u8; 100];
        file.read_exact(&mut header_bytes)?;
        
        // Check magic string
        if &header_bytes[0..16] != SQLITE_HEADER_MAGIC {
            return Err(Error::InvalidFormat("Not a SQLite database".into()));
        }
        
        let header = Self::parse_header(&header_bytes)?;
        
        Ok(Database { file, header })
    }
    
    /// Parse the file header
    fn parse_header(data: &[u8]) -> Result<FileHeader> {
        let page_size = BigEndian::read_u16(&data[16..18]);
        let page_size = if page_size == 1 { 65536u32 } else { page_size as u32 };
        
        Ok(FileHeader {
            page_size,
            write_version: data[18],
            read_version: data[19],
            reserved_space: data[20],
            max_payload_fraction: data[21],
            min_payload_fraction: data[22],
            leaf_payload_fraction: data[23],
            file_change_counter: BigEndian::read_u32(&data[24..28]),
            database_size: BigEndian::read_u32(&data[28..32]),
            first_freelist_page: BigEndian::read_u32(&data[32..36]),
            freelist_pages: BigEndian::read_u32(&data[36..40]),
            schema_cookie: BigEndian::read_u32(&data[40..44]),
            schema_format: BigEndian::read_u32(&data[44..48]),
            default_cache_size: BigEndian::read_u32(&data[48..52]),
            largest_root_page: BigEndian::read_u32(&data[52..56]),
            text_encoding: BigEndian::read_u32(&data[56..60]),
            user_version: BigEndian::read_u32(&data[60..64]),
            incremental_vacuum: BigEndian::read_u32(&data[64..68]),
            application_id: BigEndian::read_u32(&data[68..72]),
            version_valid_for: BigEndian::read_u32(&data[72..92]),
            sqlite_version: BigEndian::read_u32(&data[96..100]),
        })
    }
    
    /// Read a page by number (1-indexed)
    fn read_page(&mut self, page_number: u32) -> Result<Page> {
        if page_number == 0 || page_number > self.header.database_size {
            return Err(Error::InvalidPage(page_number));
        }
        
        let offset = (page_number - 1) as u64 * self.header.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        
        let mut data = vec![0u8; self.header.page_size as usize];
        self.file.read_exact(&mut data)?;
        
        Page::parse(page_number, data, page_number == 1)
    }
    
    /// List all tables in the database
    pub fn tables(&mut self) -> Result<Vec<String>> {
        let schema = self.read_schema()?;
        Ok(schema.into_iter()
            .filter_map(|(name, info)| {
                if info.type_name == "table" && !name.starts_with("sqlite_") {
                    Some(name)
                } else {
                    None
                }
            })
            .collect())
    }
    
    /// Read all rows from a table
    pub fn read_table(&mut self, table_name: &str) -> Result<Vec<Row>> {
        let schema = self.read_schema()?;
        
        let table_info = schema.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        let columns = self.get_table_columns(table_name)?;
        let root_page = match self.read_page(table_info.root_page) {
            Ok(page) => page,
            Err(e) => {
                log_error(&format!("Failed to read table root page: {}", e));
                return Err(e);
            }
        };
        
        let mut rows = Vec::new();
        let mut cursor = BTreeCursor::new(root_page);
        
        // Find the index of the INTEGER PRIMARY KEY column, if any
        let pk_column_index = columns.iter().position(|col| col == "id");
        
        // Safety check: limit number of rows to prevent memory issues
        // For production sandbox systems, 10M rows should be sufficient
        let max_rows = 10_000_000;
        let mut row_count = 0;
        let mut error_count = 0;
        let max_errors = 100; // Allow some errors but not too many
        
        while let Some(cell) = cursor.next_cell(|page_num| self.read_page(page_num))? {
            if row_count >= max_rows {
                log_warn(&format!("Table too large, truncating at {} rows", max_rows));
                break;
            }
            
            if error_count >= max_errors {
                log_warn(&format!("Too many errors while reading table, stopping at {} rows", row_count));
                break;
            }
            
            let values = match parse_record(&cell.payload) {
                Ok(values) => values,
                Err(e) => {
                    log_warn(&format!("Failed to parse row {}: {}", row_count, e));
                    error_count += 1;
                    continue; // Skip this row and continue
                }
            };
            
            let mut row = HashMap::new();
            for (i, column) in columns.iter().enumerate() {
                let mut value = values.get(i).cloned().unwrap_or(Value::Null);
                // If this is the INTEGER PRIMARY KEY column and value is Null, use the cell's key
                if Some(i) == pk_column_index && matches!(value, Value::Null) {
                    value = Value::Integer(cell.key);
                }
                row.insert(column.clone(), value);
            }
            
            rows.push(row);
            row_count += 1;
        }
        
        log_debug(&format!("Successfully read {} rows from table {} ({} errors)", row_count, table_name, error_count));
        Ok(rows)
    }
    
    /// Count rows in a table efficiently without reading all data
    pub fn count_table_rows(&mut self, table_name: &str) -> Result<usize> {
        let schema = self.read_schema()?;
        
        let table_info = schema.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        let root_page = match self.read_page(table_info.root_page) {
            Ok(page) => page,
            Err(e) => {
                log_error(&format!("Failed to read table root page: {}", e));
                return Err(e);
            }
        };
        
        // Use the same B-tree traversal logic as read_table_limited
        // but only count cells, don't parse them
        let mut cursor = BTreeCursor::new(root_page);
        let mut row_count = 0;
        
        // Safety check: limit number of iterations to prevent infinite loops
        let max_iterations = 1_000_000;
        let mut iteration_count = 0;
        
        while let Some(cell) = cursor.next_cell(|page_num| self.read_page(page_num))? {
            iteration_count += 1;
            if iteration_count > max_iterations {
                log_warn(&format!("Row counting exceeded safety limit, stopping at {} rows", row_count));
                break;
            }
            
            // Only count cells that have actual data (non-empty payloads)
            // Empty payloads indicate deleted or empty rows
            if !cell.payload.is_empty() {
                row_count += 1;
            }
        }
        
        log_debug(&format!("Counted {} rows in table {}", row_count, table_name));
        Ok(row_count)
    }
    
    /// Estimate table rows based on database size and page information
    #[allow(dead_code)]
    fn estimate_table_rows(&mut self, _table_name: &str) -> Result<usize> {
        // Get database size in pages
        let total_pages = self.header.database_size;
        let _page_size = self.header.page_size as usize;
        
        // Estimate based on typical SQLite table density
        // Most SQLite tables use about 60-80% of available space
        let avg_cells_per_page = 50; // Conservative estimate
        let estimated_pages_for_table = total_pages / 3; // Assume table uses ~1/3 of database
        
        let estimated_rows = estimated_pages_for_table * avg_cells_per_page;
        
        // Add some variance based on actual database characteristics
        let final_estimate = if total_pages > 10000 {
            // For very large databases, be more conservative
            (estimated_rows as f64 * 0.8) as usize
        } else {
            estimated_rows as usize
        };
        
        Ok(final_estimate.max(1000)) // Ensure minimum reasonable estimate
    }
    
    /// Read the schema information
    fn read_schema(&mut self) -> Result<HashMap<String, SchemaObject>> {
        let mut schema = HashMap::new();
        
        // Read sqlite_master table (root page 1)
        let root_page = match self.read_page(1) {
            Ok(page) => page,
            Err(e) => {
                log_error(&format!("Failed to read root page: {}", e));
                return Err(e);
            }
        };
        
        let mut cursor = BTreeCursor::new(root_page);
        
        // Safety check: limit number of schema objects
        // Even large databases rarely have more than a few thousand tables/indexes
        let max_schema_objects = 10_000;
        let mut count = 0;
        
        while let Some(cell) = cursor.next_cell(|page_num| self.read_page(page_num))? {
            if count >= max_schema_objects {
                log_warn(&format!("Truncating schema objects at {} (limit: {})", count, max_schema_objects));
                break;
            }
            count += 1;
            
            let values = match parse_record(&cell.payload) {
                Ok(values) => values,
                Err(e) => {
                    log_warn(&format!("Failed to parse schema record {}: {}", count, e));
                    continue; // Skip this record and continue with the next
                }
            };
            
            if values.len() >= 5 {
                if let (Some(type_name), Some(name), _, Some(root_page), Some(sql)) = (
                    values[0].as_text(),
                    values[1].as_text(),
                    &values[2],
                    values[3].as_integer(),
                    values[4].as_text(),
                ) {
                    // Safety check: limit SQL statement size
                    // Even complex CREATE TABLE statements are rarely > 1MB
                    if sql.len() > 1_000_000 {
                        log_warn(&format!("SQL statement too large for table {} ({} bytes), skipping", name, sql.len()));
                        continue; // Skip this table
                    }
                    
                    schema.insert(name.to_string(), SchemaObject {
                        type_name: type_name.to_string(),
                        name: name.to_string(),
                        root_page: root_page as u32,
                        sql: sql.to_string(),
                    });
                }
            }
        }
        
        if schema.is_empty() {
            log_warn("No valid schema objects found");
        } else {
            log_debug(&format!("Successfully parsed {} schema objects", schema.len()));
        }
        
        Ok(schema)
    }
    
    /// Get column names for a table
    fn get_table_columns(&mut self, table_name: &str) -> Result<Vec<String>> {
        let schema = self.read_schema()?;
        let table_info = schema.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        // Parse CREATE TABLE statement to extract column names
        let sql = &table_info.sql;
        let columns = parse_create_table_columns(sql)?;
        
        Ok(columns)
    }
    
    /// Read a limited number of rows from a table
    pub fn read_table_limited(&mut self, table_name: &str, max_rows: usize) -> Result<Vec<Row>> {
        let schema = self.read_schema()?;
        
        let table_info = schema.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        let columns = self.get_table_columns(table_name)?;
        let root_page = match self.read_page(table_info.root_page) {
            Ok(page) => page,
            Err(e) => {
                log_error(&format!("Failed to read table root page: {}", e));
                return Err(e);
            }
        };
        
        let mut rows = Vec::new();
        let mut cursor = BTreeCursor::new(root_page);
        let mut row_count = 0;
        let mut error_count = 0;
        let max_errors = 50; // Allow some errors but not too many
        
        while let Some(cell) = cursor.next_cell(|page_num| self.read_page(page_num))? {
            if row_count >= max_rows {
                log_info(&format!("Reached limit of {} rows, stopping", max_rows));
                break;
            }
            
            if error_count >= max_errors {
                log_warn(&format!("Too many errors while reading table, stopping at {} rows", row_count));
                break;
            }
            
            // Debug: Print the rowid to understand traversal order
            if row_count < 10 {
                log_debug(&format!("Processing cell with rowid: {}", cell.key));
            }
            
            // Debug: Print payload information for failed rows
            if cell.payload.len() < 10 {
                log_debug(&format!("Cell {} has small payload: {} bytes", cell.key, cell.payload.len()));
            }
            
            // Handle empty payloads (0-byte cells)
            let values = if cell.payload.is_empty() {
                // Empty payload means all columns are NULL
                // We need to create NULL values for all columns
                let mut null_values = Vec::new();
                for _ in 0..columns.len() {
                    null_values.push(Value::Null);
                }
                null_values
            } else {
                match parse_record(&cell.payload) {
                    Ok(values) => values,
                    Err(e) => {
                        log_warn(&format!("Failed to parse row {}: {}", row_count, e));
                        log_debug(&format!("Cell rowid: {}, payload size: {} bytes", cell.key, cell.payload.len()));
                        if cell.payload.len() <= 100 {
                            log_debug(&format!("Payload hex: {:?}", cell.payload));
                        }
                        error_count += 1;
                        continue; // Skip this row and continue
                    }
                }
            };
            
            let mut row = HashMap::new();
            for (i, column) in columns.iter().enumerate() {
                let value = values.get(i).cloned().unwrap_or(Value::Null);
                row.insert(column.clone(), value);
            }
            
            rows.push(row);
            row_count += 1;
        }
        
        log_debug(&format!("Successfully read {} rows from table {} ({} errors)", row_count, table_name, error_count));
        Ok(rows)
    }

    /// Execute a SELECT SQL query
    pub fn execute_query(&mut self, query: &SelectQuery) -> Result<Vec<Row>> {
        log_debug(&format!("Executing SELECT query on table: {}", query.table));
        
        // First, read all rows from the specified table
        let rows = self.read_table(&query.table)?;
        log_debug(&format!("Read {} total rows from table {}", rows.len(), query.table));
        
        // Get column information for the table
        let columns = self.get_table_columns(&query.table)?;
        
        // Execute the query against the rows
        let result = query.execute(rows, &columns)?;
        
        log_debug(&format!("Query returned {} rows after filtering", result.len()));
        Ok(result)
    }
    
    /// Execute a SELECT SQL query from a string
    pub fn execute_sql(&mut self, sql: &str) -> Result<Vec<Row>> {
        let query = SelectQuery::parse(sql)?;
        self.execute_query(&query)
    }
}

/// Schema object information
struct SchemaObject {
    type_name: String,
    #[allow(dead_code)]
    name: String,
    root_page: u32,
    sql: String,
}

/// Parse column names from a CREATE TABLE statement
fn parse_create_table_columns(sql: &str) -> Result<Vec<String>> {
    // Simple parser - find content between first ( and last )
    let start = sql.find('(').ok_or_else(|| 
        Error::InvalidFormat("Invalid CREATE TABLE statement".into()))?;
    let end = sql.rfind(')').ok_or_else(|| 
        Error::InvalidFormat("Invalid CREATE TABLE statement".into()))?;
    
    let columns_str = &sql[start + 1..end];
    let mut columns = Vec::new();
    
    // Split by comma and extract column names
    for column_def in columns_str.split(',') {
        let column_def = column_def.trim();
        if let Some(space_pos) = column_def.find(|c: char| c.is_whitespace()) {
            let name = column_def[..space_pos].trim();
            // Remove quotes if present
            let name = name.trim_matches('"').trim_matches('\'').trim_matches('`');
            columns.push(name.to_string());
        }
    }
    
    if columns.is_empty() {
        return Err(Error::InvalidFormat("No columns found in CREATE TABLE".into()));
    }
    
    Ok(columns)
} 