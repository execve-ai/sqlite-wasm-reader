//! Main database interface

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use memmap2::Mmap;
use byteorder::{BigEndian, ByteOrder};

use crate::{
    btree::{BTreeCursor, Cell},
    error::{Error, Result},
    format::{FileHeader, SQLITE_HEADER_MAGIC},
    logging::{log_debug, log_error, log_warn},
    page::Page,
    query::{ComparisonOperator, Expr, SelectQuery},
    record::parse_record,
    value::Value,
};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{vec::Vec, string::String, format};

/// A row of data from a table
pub type Row = HashMap<String, Value>;

/// SQLite database reader
pub struct Database {
    file: File,
    mmap: Mmap,
    header: FileHeader,
    /// Cache of table schemas and their indexes
    schema_cache: HashMap<String, TableInfo>,
    /// Cache of recently read pages (page_number -> Page)
    /// Limited to prevent excessive memory usage
    page_cache: HashMap<u32, Page>,
    /// Maximum number of pages to cache (LRU eviction)
    max_cache_size: usize,
    /// LRU ordering for page cache - tracks access order
    page_lru_order: Vec<u32>,
    /// Interned column names to avoid string allocation during row creation
    column_name_cache: HashMap<String, String>,
}

impl Database {
    /// Open a SQLite database file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database> {
        let mut file = File::open(path)?;
        
        // Read and validate header
        let mut header_bytes = [0u8; 100];
        file.read_exact(&mut header_bytes)?;
        
        // Check magic string
        if &header_bytes[0..16] != SQLITE_HEADER_MAGIC {
            return Err(Error::InvalidFormat("Not a SQLite database".into()));
        }
        
        let header = Self::parse_header(&header_bytes)?;
        
        // Create memory-mapped file for faster access
        let mmap = unsafe { Mmap::map(&file)? };
        
        let mut db = Database { 
            file, 
            mmap,
            header,
            schema_cache: HashMap::new(),
            page_cache: HashMap::new(),
            max_cache_size: 5000, // Increased cache size significantly for table scans
            page_lru_order: Vec::new(),
            column_name_cache: HashMap::new(),
        };
        
        // Load schema information
        db.load_schema()?;
        
        Ok(db)
    }
    
    /// Load schema information for all tables and indexes
    fn load_schema(&mut self) -> Result<()> {
        let schema_objects = self.read_schema()?;
        
        let mut tables = HashMap::new();

        // First pass: process tables
        for (name, object) in &schema_objects {
            if object.type_name == "table" && !name.starts_with("sqlite_") {
                let columns = match Self::parse_create_table_columns(&object.sql) {
                    Ok(cols) => cols,
                    Err(e) => {
                        log_warn(&format!("Failed to parse CREATE TABLE statement for table '{}': {}", name, e));
                        continue;
                    }
                };
                
                let table_info = TableInfo {
                    name: name.clone(),
                    root_page: object.root_page,
                    columns,
                    indexes: Vec::new(),
                    sql: object.sql.clone(),
                };
                tables.insert(name.clone(), table_info);
            }
        }

        // Second pass: process indexes
        for (name, object) in &schema_objects {
            if object.type_name == "index" && !name.starts_with("sqlite_") {
                match Self::parse_create_index_info(&object.sql) {
                    Ok((table_name, columns)) => {
                        if let Some(table_info) = tables.get_mut(&table_name) {
                            let index_info = IndexInfo {
                                name: name.clone(),
                                table_name: table_name.clone(),
                                columns,
                                root_page: object.root_page,
                            };
                            table_info.indexes.push(index_info);
                        } else {
                            log_warn(&format!("Index '{}' references unknown table '{}'", name, table_name));
                        }
                    }
                    Err(e) => {
                        log_warn(&format!("Failed to parse CREATE INDEX for '{}': {}", name, e));
                    }
                }
            }
        }

        self.schema_cache = tables;
        Ok(())
    }

    /// Parse a CREATE TABLE statement to extract column names
    fn parse_create_table_columns(sql: &str) -> Result<Vec<String>> {
        let dialect = sqlparser::dialect::SQLiteDialect {};
        let statements = sqlparser::parser::Parser::parse_sql(&dialect, sql)
            .map_err(|e| Error::SchemaError(format!("Failed to parse SQL: {}", e)))?;

        if statements.len() != 1 {
            return Err(Error::SchemaError("Expected a single CREATE TABLE statement".into()));
        }

        if let sqlparser::ast::Statement::CreateTable(sqlparser::ast::CreateTable { name: _, columns, .. }) = &statements[0] {
            let column_names = columns.iter().map(|col| col.name.value.clone()).collect();
            Ok(column_names)
        } else {
            Err(Error::SchemaError("Expected a CREATE TABLE statement".into()))
        }
    }

    /// Parse a CREATE INDEX statement to extract the target table and column names
    /// This routine uses simple string parsing instead of relying on the full SQL parser
    /// because sqlparser's CreateIndex support is experimental and may break between versions.
    /// It supports statements of the following forms (case-insensitive):
    ///     CREATE [UNIQUE] INDEX idx_name ON table_name(col1, col2, ...);
    ///     CREATE INDEX IF NOT EXISTS idx_name ON "table" ( `col1` , `col2` );
    /// It returns the referenced table name and the list of column names in the order they
    /// appear in the index definition.
    fn parse_create_index_info(sql: &str) -> Result<(String, Vec<String>)> {
        // To keep things reasonably robust without pulling in a full SQL parser, we
        // locate the first " ON " keyword (case-insensitive) and then extract the
        // substring up to the first opening parenthesis. Everything between ON and
        // the parenthesis is considered the table name (it may include a schema
        // prefix, e.g. "main.table").
        let lowercase = sql.to_lowercase();
        let on_pos = lowercase.find(" on ")
            .ok_or_else(|| Error::SchemaError("CREATE INDEX statement missing 'ON'".into()))?;

        // Slice AFTER the " on " (preserve original casing for table name parsing)
        let after_on = &sql[on_pos + 4..];
        let after_on_trim = after_on.trim_start();

        // Parse table name (stop at whitespace or '(' )
        let mut table_name = String::new();
        for ch in after_on_trim.chars() {
            if ch.is_whitespace() || ch == '(' {
                break;
            }
            table_name.push(ch);
        }
        if table_name.is_empty() {
            return Err(Error::SchemaError("Unable to parse table name from CREATE INDEX".into()));
        }

        // Locate the first '(' which starts the column list
        let paren_start = after_on_trim.find('(')
            .ok_or_else(|| Error::SchemaError("CREATE INDEX missing column list".into()))?;
        let paren_end_rel = after_on_trim[paren_start + 1..]
            .find(')')
            .ok_or_else(|| Error::SchemaError("CREATE INDEX missing closing ')'".into()))?;
        let paren_end = paren_start + 1 + paren_end_rel;
        let cols_segment = &after_on_trim[paren_start + 1..paren_end];

        let columns: Vec<String> = cols_segment
            .split(',')
            .map(|s| s.trim().trim_matches('`').trim_matches('"').to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if columns.is_empty() {
            return Err(Error::SchemaError("CREATE INDEX has no columns".into()));
        }
        Ok((table_name, columns))
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
    
    /// Read a page with optimized caching for sequential access patterns
    fn read_page(&mut self, page_number: u32) -> Result<Page> {
        if page_number == 0 || page_number > self.header.database_size {
            return Err(Error::InvalidPage(page_number));
        }
        
        // Check cache first
        if let Some(page) = self.page_cache.get(&page_number) {
            // Update LRU order - move to end (most recently used)
            self.page_lru_order.retain(|&x| x != page_number);
            self.page_lru_order.push(page_number);
            return Ok(page.clone());
        }
        
        // Read from memory-mapped file (much faster than file I/O)
        let offset = (page_number - 1) as usize * self.header.page_size as usize;
        let page_size = self.header.page_size as usize;
        
        if offset + page_size > self.mmap.len() {
            return Err(Error::InvalidPage(page_number));
        }
        
        let data = self.mmap[offset..offset + page_size].to_vec();
        let page = Page::parse(page_number, data, page_number == 1)?;
        
        // For table scans, implement a more aggressive caching strategy
        // Cache the page with proper LRU eviction
        if self.page_cache.len() >= self.max_cache_size {
            // Remove the least recently used page
            if let Some(&oldest_page) = self.page_lru_order.first() {
                self.page_cache.remove(&oldest_page);
                self.page_lru_order.retain(|&x| x != oldest_page);
            }
        }
        
        // Insert the new page and update LRU order
        self.page_cache.insert(page_number, page.clone());
        self.page_lru_order.push(page_number);
        
        Ok(page)
    }
    
    /// Read multiple pages in a batch for better I/O efficiency
    fn read_pages_batch(&mut self, page_numbers: &[u32]) -> Result<Vec<Page>> {
        let mut pages = Vec::with_capacity(page_numbers.len());
        let mut uncached_pages = Vec::new();
        
        // First, check cache for all pages
        for &page_number in page_numbers {
            if let Some(page) = self.page_cache.get(&page_number) {
                pages.push(page.clone());
                // Update LRU order
                self.page_lru_order.retain(|&x| x != page_number);
                self.page_lru_order.push(page_number);
            } else {
                uncached_pages.push(page_number);
            }
        }
        
        // Read uncached pages from memory-mapped file
        for page_number in uncached_pages {
            if page_number == 0 || page_number > self.header.database_size {
                return Err(Error::InvalidPage(page_number));
            }
            
            let offset = (page_number - 1) as usize * self.header.page_size as usize;
            let page_size = self.header.page_size as usize;
            
            if offset + page_size > self.mmap.len() {
                return Err(Error::InvalidPage(page_number));
            }
            
            let data = self.mmap[offset..offset + page_size].to_vec();
            let page = Page::parse(page_number, data, page_number == 1)?;
            pages.push(page.clone());
            
            // Cache the page
            if self.page_cache.len() >= self.max_cache_size {
                if let Some(&oldest_page) = self.page_lru_order.first() {
                    self.page_cache.remove(&oldest_page);
                    self.page_lru_order.retain(|&x| x != oldest_page);
                }
            }
            
            self.page_cache.insert(page_number, page);
            self.page_lru_order.push(page_number);
        }
        
        Ok(pages)
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
        
        // Only count cells, don't parse them
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
    pub fn get_table_columns(&mut self, table_name: &str) -> Result<Vec<String>> {
        // Use cached schema instead of reading it again
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        Ok(table_info.columns.clone())
    }

    /// Get interned column name to avoid string allocations
    fn intern_column_name(&mut self, name: &str) -> &str {
        if !self.column_name_cache.contains_key(name) {
            self.column_name_cache.insert(name.to_string(), name.to_string());
        }
        self.column_name_cache.get(name).unwrap()
    }

    /// Perform a streaming table scan that processes rows one at a time for better memory efficiency
    fn read_table_rows_streaming<F>(&mut self, table_name: &str, limit: Option<usize>, mut row_processor: F) -> Result<usize>
    where
        F: FnMut(&Row) -> bool, // Return false to stop processing
    {
        log_debug(&format!("Performing streaming table scan for table: {}", table_name));
        
        // Get table info from cache
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        let columns = table_info.columns.clone();
        
        // Pre-intern all column names to avoid allocations during row creation
        for col in &columns {
            if !self.column_name_cache.contains_key(col) {
                self.column_name_cache.insert(col.clone(), col.clone());
            }
        }
        
        // Detect INTEGER PRIMARY KEY column by parsing the SQL
        let rowid_column = self.find_rowid_column(table_name)?;
        
        let root_page = self.read_page(table_info.root_page)?;
        let mut cursor = BTreeCursor::new(root_page);
        
        // Safety check: limit number of rows to prevent excessive memory usage
        let max_rows = limit.unwrap_or(1_000_000);
        let mut row_count = 0;
        let mut processed_count = 0;
        
        // Reuse row object to reduce allocations
        let mut row = HashMap::with_capacity(columns.len());
        
        while let Some(cell) = cursor.next_cell(|page_num| self.read_page(page_num))? {
            if row_count >= max_rows {
                if limit.is_none() {
                    log_warn(&format!("Table scan truncated at {} rows (limit: {})", row_count, max_rows));
                }
                break;
            }
            
            // Skip empty payloads (deleted rows)
            if cell.payload.is_empty() {
                continue;
            }
            
            // Parse the row data
            match parse_record(&cell.payload) {
                Ok(values) => {
                    // Clear and reuse the row HashMap
                    row.clear();
                    
                    // Convert to a row with column names
                    for (i, column_name) in columns.iter().enumerate() {
                        if let Some(ref rowid_col) = rowid_column {
                            if column_name == rowid_col {
                                // This is the INTEGER PRIMARY KEY column - use rowid from cell
                                row.insert(column_name.clone(), Value::Integer(cell.key));
                                continue;
                            }
                        }
                        
                        let value = values.get(i).cloned().unwrap_or(Value::Null);
                        row.insert(column_name.clone(), value);
                    }
                    
                    // Process the row
                    if !row_processor(&row) {
                        break; // Stop processing if processor returns false
                    }
                    
                    processed_count += 1;
                    row_count += 1;
                },
                Err(e) => {
                    log_warn(&format!("Failed to parse row {}: {}", row_count, e));
                    // Continue with next row instead of failing
                    continue;
                }
            }
        }
        
        log_debug(&format!("Streaming table scan completed: {} rows processed from {}", processed_count, table_name));
        Ok(processed_count)
    }

    /// Perform a full table scan with batch processing for better performance
    fn read_all_table_rows_batch(&mut self, table_name: &str, limit: Option<usize>) -> Result<Vec<Row>> {
        // For small limits, use streaming to reduce memory usage
        if let Some(limit_val) = limit {
            if limit_val <= 10000 {
                let mut rows = Vec::with_capacity(limit_val);
                let mut count = 0;
                
                self.read_table_rows_streaming(table_name, limit, |row| {
                    if count < limit_val {
                        rows.push(row.clone());
                        count += 1;
                        true
                    } else {
                        false
                    }
                })?;
                
                return Ok(rows);
            }
        }
        
        // Fall back to original batch processing for larger datasets
        log_debug(&format!("Performing batch table scan for table: {}", table_name));
        
        // Get table info from cache
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        let columns = table_info.columns.clone();
        
        // Pre-intern all column names to avoid allocations during row creation
        for col in &columns {
            if !self.column_name_cache.contains_key(col) {
                self.column_name_cache.insert(col.clone(), col.clone());
            }
        }
        
        // Detect INTEGER PRIMARY KEY column by parsing the SQL
        let rowid_column = self.find_rowid_column(table_name)?;
        
        let root_page = self.read_page(table_info.root_page)?;
        let mut cursor = BTreeCursor::new(root_page);
        
        // Pre-allocate with estimated capacity to reduce reallocations
        let estimated_rows = limit.unwrap_or(10000).min(100000);
        let mut rows = Vec::with_capacity(estimated_rows);
        
        // Batch processing parameters
        const BATCH_SIZE: usize = 1000;
        let mut batch_rows = Vec::with_capacity(BATCH_SIZE);
        
        // Safety check: limit number of rows to prevent excessive memory usage
        let max_rows = limit.unwrap_or(1_000_000);
        let mut row_count = 0;
        
        while let Some(cell) = cursor.next_cell(|page_num| self.read_page(page_num))? {
            if row_count >= max_rows {
                if limit.is_none() {
                    log_warn(&format!("Table scan truncated at {} rows (limit: {})", row_count, max_rows));
                }
                break;
            }
            
            // Skip empty payloads (deleted rows)
            if cell.payload.is_empty() {
                continue;
            }
            
            // Parse the row data
            match parse_record(&cell.payload) {
                Ok(values) => {
                    // Convert to a row with column names using pre-allocated capacity
                    let mut row = HashMap::with_capacity(columns.len());
                    for (i, column_name) in columns.iter().enumerate() {
                        if let Some(ref rowid_col) = rowid_column {
                            if column_name == rowid_col {
                                // This is the INTEGER PRIMARY KEY column - use rowid from cell
                                row.insert(column_name.clone(), Value::Integer(cell.key));
                                continue;
                            }
                        }
                        
                        let value = values.get(i).cloned().unwrap_or(Value::Null);
                        row.insert(column_name.clone(), value);
                    }
                    batch_rows.push(row);
                    row_count += 1;
                    
                    // Process batch when full
                    if batch_rows.len() >= BATCH_SIZE {
                        rows.extend(batch_rows.drain(..));
                        
                        // Early termination check for LIMIT queries
                        if let Some(limit_val) = limit {
                            if rows.len() >= limit_val {
                                break;
                            }
                        }
                    }
                },
                Err(e) => {
                    log_warn(&format!("Failed to parse row {}: {}", row_count, e));
                    // Continue with next row instead of failing
                    continue;
                }
            }
        }
        
        // Process remaining rows in batch
        if !batch_rows.is_empty() {
            rows.extend(batch_rows);
        }
        
        log_debug(&format!("Batch table scan completed: {} rows read from {}", rows.len(), table_name));
        Ok(rows)
    }

    /// Perform a full table scan to read all rows with optimized memory management
    fn read_all_table_rows_optimized(&mut self, table_name: &str, limit: Option<usize>) -> Result<Vec<Row>> {
        // Use batch processing for better performance
        self.read_all_table_rows_batch(table_name, limit)
    }

    /// Perform a full table scan to read all rows
    fn read_all_table_rows(&mut self, table_name: &str) -> Result<Vec<Row>> {
        // Use the optimized version with no limit
        self.read_all_table_rows_optimized(table_name, None)
    }
    
    /// Find the INTEGER PRIMARY KEY column name for a table (if any)
    fn find_rowid_column(&self, table_name: &str) -> Result<Option<String>> {
        // Get the table info from cache
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        // Parse the CREATE TABLE statement to find INTEGER PRIMARY KEY
        let sql = &table_info.sql;
        
        // Simple pattern matching for INTEGER PRIMARY KEY
        // This is a simplified approach - in a full implementation, we'd use a proper SQL parser
        // This is a basic implementation that handles common cases
        let sql_lower = sql.to_lowercase();
        
        // Look for patterns like "columnname integer primary key"
        for line in sql_lower.lines() {
            let line = line.trim();
            if line.contains("integer") && line.contains("primary") && line.contains("key") {
                // Extract column name - look for the first word before "integer"
                let words: Vec<&str> = line.split_whitespace().collect();
                for i in 0..words.len() {
                    if words[i] == "integer" && i > 0 {
                        let column_name = words[i-1].trim_matches(',').trim_matches('(').trim_matches('"').trim_matches('`');
                        return Ok(Some(column_name.to_string()));
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    /// Execute a SELECT SQL query with index acceleration and table scan fallback
    pub fn execute_query(&mut self, query: &SelectQuery) -> Result<Vec<Row>> {
        let table_name = &query.table;
        
        // Get table info once and reuse
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
        
        let table_info_clone = table_info.clone();
        
        // Try index-based search first if we have a WHERE clause
        if let Some(where_expr) = &query.where_expr {
            if let Some(index_rows) = self.try_index_lookup(query, where_expr, &table_info_clone)? {
                log_debug(&format!("Using index acceleration for query on table {}", table_name));
                // Index lookup succeeded - apply remaining operations
                return self.apply_query_operations(index_rows, query);
            }
        }
        
        // Fall back to table scan
        log_debug(&format!("Using table scan fallback for query on table {}", table_name));
        
        // Use fast path for simple queries without WHERE clauses
        let rows = if query.where_expr.is_none() && query.order_by.is_none() {
            // Fast path for simple SELECT * queries
            log_debug("Using fast table scan path");
            self.read_table_rows_fast(table_name, query.limit)?
        } else {
            // Use optimized table scan for complex queries
            self.read_all_table_rows_optimized(table_name, query.limit)?
        };
        
        // Apply query operations (WHERE, ORDER BY, LIMIT)
        self.apply_query_operations(rows, query)
    }
    
    /// Try to use index lookup for the query, returning Some(rows) if successful, None if no suitable index
    fn try_index_lookup(&mut self, query: &SelectQuery, where_expr: &Expr, table_info: &TableInfo) -> Result<Option<Vec<Row>>> {
        let table_name = &query.table;
        let columns = &table_info.columns;
        let or_branches = collect_or_branches(where_expr);
        
        // Process each OR branch to find usable indexes
        let mut all_rowids = std::collections::HashSet::new();
        let mut found_usable_index = false;
        
        for branch in or_branches.iter() {
            if let Some((index, values)) = find_best_index(table_info, branch) {
                found_usable_index = true;
                log_debug(&format!("Found usable index '{}' for query condition", index.name));
                
                // Process this index branch directly
                let index_root_page = self.read_page(index.root_page)?;
                let mut cursor = BTreeCursor::new(index_root_page);
                
                // Convert Vec<&Value> to Vec<&Value> for find_rowids_by_key
                let value_refs: Vec<&Value> = values.iter().map(|&v| v).collect();
                let page_reader = |page_num: u32| self.read_page(page_num);
                let rowids = cursor.find_rowids_by_key(&value_refs, page_reader)?;
                all_rowids.extend(rowids);
            }
        }
        
        // If no branches could use an index, return None to trigger table scan
        if !found_usable_index {
            log_debug("No suitable index found for query conditions, will use table scan");
            return Ok(None);
        }
        
        // Convert rowids to a vec for deterministic ordering
        let all_rowids: Vec<_> = all_rowids.into_iter().collect();
        let mut rows = Vec::with_capacity(all_rowids.len());
        
        // Fetch each matching row by its ROWID using targeted lookups
        for rowid in all_rowids {
            if let Some(row) = self.read_row_by_rowid(table_name, rowid, columns)? {
                rows.push(row);
            }
        }
        
        log_debug(&format!("Index lookup found {} rows", rows.len()));
        Ok(Some(rows))
    }
    
    /// Read a single row by its ROWID using targeted binary search
    fn read_row_by_rowid(&mut self, table_name: &str, rowid: i64, columns: &[String]) -> Result<Option<Row>> {
        // Get the table's root page from cache
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        // Detect INTEGER PRIMARY KEY column
        let rowid_column = self.find_rowid_column(table_name)?;
        
        let root_page = self.read_page(table_info.root_page)?;
        let mut cursor = BTreeCursor::new(root_page);

        // Try to find the cell with the matching ROWID using binary search
        match cursor.find_cell(rowid, |page_num| self.read_page(page_num)) {
            Ok(Some(cell)) => {
                // Parse the row data
                match parse_record(&cell.payload) {
                    Ok(values) => {
                        // Convert to a row with column names
                        let mut row = HashMap::new();
                        for (i, column_name) in columns.iter().enumerate() {
                            if let Some(ref rowid_col) = rowid_column {
                                if column_name == rowid_col {
                                    // This is the INTEGER PRIMARY KEY column - use rowid from cell
                                    row.insert(column_name.clone(), Value::Integer(cell.key));
                                    continue;
                                }
                            }
                            
                            let value = values.get(i).cloned().unwrap_or(Value::Null);
                            row.insert(column_name.clone(), value);
                        }
                        
                        Ok(Some(row))
                    },
                    Err(_) => {
                        // Return None instead of failing to allow processing to continue with other rows
                        Ok(None)
                    }
                }
            },
            Ok(None) => {
                Ok(None)
            },
            Err(e) => {
                Err(e)
            }
        }
    }

    /// Fast table scan for simple queries without WHERE clauses
    fn read_table_rows_fast(&mut self, table_name: &str, limit: Option<usize>) -> Result<Vec<Row>> {
        log_debug(&format!("Performing fast table scan for table: {}", table_name));
        
        // Use the high-performance optimized version
        self.read_table_rows_optimized_v2(table_name, limit)
    }

    /// Apply query operations (WHERE, ORDER BY, LIMIT) to a set of rows
    fn apply_query_operations(&self, mut rows: Vec<Row>, query: &SelectQuery) -> Result<Vec<Row>> {
        // Apply WHERE clause
        if let Some(where_expr) = &query.where_expr {
            rows.retain(|row| {
                // Use the query's evaluate_expr method
                query.evaluate_expr(row, where_expr)
            });
        }
        
        // Apply ORDER BY
        if let Some(order_by) = &query.order_by {
            rows.sort_by(|a, b| {
                let val_a = a.get(&order_by.column).unwrap_or(&Value::Null);
                let val_b = b.get(&order_by.column).unwrap_or(&Value::Null);
                
                let cmp = match (val_a, val_b) {
                    (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
                    (Value::Real(a), Value::Real(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
                    (Value::Text(a), Value::Text(b)) => a.cmp(b),
                    (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                    (Value::Null, _) => std::cmp::Ordering::Less,
                    (_, Value::Null) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                };
                
                if order_by.ascending { cmp } else { cmp.reverse() }
            });
        }
        
        // Apply LIMIT
        if let Some(limit) = query.limit {
            rows.truncate(limit);
        }
        
        // Apply column selection
        if let Some(ref columns) = query.columns {
            if !columns.is_empty() && columns != &vec!["*"] {
                for row in &mut rows {
                    row.retain(|col_name, _| columns.contains(col_name));
                }
            }
        }
        
        Ok(rows)
    }

    /// High-performance table scan that minimizes allocations
    fn read_table_rows_optimized_v2(&mut self, table_name: &str, limit: Option<usize>) -> Result<Vec<Row>> {
        log_debug(&format!("Performing high-performance table scan for table: {}", table_name));
        
        // Get table info from cache
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
        let columns = table_info.columns.clone();
        let root_page_num = table_info.root_page;
        
        // Pre-allocate with estimated capacity
        let estimated_rows = limit.unwrap_or(100_000).min(100_000);
        let mut rows = Vec::with_capacity(estimated_rows);
        
        // Pre-intern column names once
        let interned_columns: Vec<String> = columns.iter().map(|col| {
            if !self.column_name_cache.contains_key(col) {
                self.column_name_cache.insert(col.clone(), col.clone());
            }
            col.clone()
        }).collect();
        
        // Read root page
        let root_page = self.read_page(root_page_num)?;
        let mut cursor = BTreeCursor::new(root_page);
        let mut count = 0;
        
        // Safety limits
        const MAX_ROWS: usize = 1_000_000;
        const MAX_ITERATIONS: usize = 10_000_000;
        
        let row_limit = limit.unwrap_or(MAX_ROWS).min(MAX_ROWS);
        
        // Pre-allocate reusable row HashMap
        let mut reusable_row = HashMap::with_capacity(columns.len());
        
        for iteration in 0..MAX_ITERATIONS {
            if count >= row_limit {
                log_debug(&format!("Reached row limit: {}", row_limit));
                break;
            }
            
            match cursor.next_cell(|page_num| self.read_page(page_num)) {
                Ok(Some(cell)) => {
                    // Parse record with minimal allocations using optimized parser
                    if let Ok(values) = crate::record::parse_record_optimized(&cell.payload) {
                        if values.len() <= columns.len() {
                            reusable_row.clear();
                            
                            // Fill row with minimal string allocations
                            for (i, col_name) in interned_columns.iter().enumerate() {
                                let value = if i < values.len() {
                                    values[i].clone()
                                } else {
                                    Value::Null
                                };
                                reusable_row.insert(col_name.clone(), value);
                            }
                            
                            // Clone the completed row
                            rows.push(reusable_row.clone());
                            count += 1;
                        }
                    }
                }
                Ok(None) => {
                    log_debug("High-performance table scan completed - no more cells");
                    break;
                }
                Err(e) => {
                    log_warn(&format!("Error reading cell in high-perf scan (iteration {}): {}", iteration, e));
                    break;
                }
            }
        }
        
        log_debug(&format!("High-performance table scan completed: {} rows processed", count));
        Ok(rows)
    }
} // end impl Database

/// Collect all branches of an OR expression.
/// Collect all branches of an OR expression.
fn collect_or_branches(expr: &Expr) -> Vec<&Expr> {
    let mut branches = Vec::new();
    let mut stack = vec![expr];
    while let Some(e) = stack.pop() {
        match e {
            Expr::Or(left, right) => {
                stack.push(left);
                stack.push(right);
            }
            _ => branches.push(e),
        }
    }
    branches
}

/// Find the best index for a WHERE clause, supporting composite indexes.
fn find_best_index<'a, 'b>(
    table_info: &'a TableInfo,
    expr: &'b Expr,
) -> Option<(&'a IndexInfo, Vec<&'b Value>)> {
    let mut conditions = HashMap::new();
    collect_and_conditions(expr, &mut conditions);

    let mut best_index: Option<(&'a IndexInfo, Vec<&'b Value>)> = None;
    let mut max_len = 0;

    for index in &table_info.indexes {
        let mut values = Vec::new();

        // For composite indexes, we can use prefix matching
        // We need consecutive columns starting from the first column
        for col in &index.columns {
            match conditions.get(col) {
                Some(value) => {
                    values.push(*value);
                }
                None => {
                    break;
                }
            }
        }

        // We can use an index if we have at least one matching column from the prefix
        if !values.is_empty() {
            // Prefer the index that covers the most columns (ties resolved arbitrarily)
            if values.len() > max_len {
                best_index = Some((index, values.clone()));
                max_len = values.len();
            }
        }
    }



    best_index
}

/// Collect all equality conditions from an AND expression tree.
fn collect_and_conditions<'b>(expr: &'b Expr, conditions: &mut HashMap<String, &'b Value>) {
    match expr {
        Expr::And(left, right) => {
            collect_and_conditions(left, conditions);
            collect_and_conditions(right, conditions);
        }
        Expr::Or(_, _) => {
            // For OR expressions, we need to handle them at a higher level
            // Just skip them for now and let the caller handle them
        }
        Expr::Not(_) => {
            // Skip NOT expressions for now
        }
        Expr::Comparison { column, operator, value } => {
            match operator {
                ComparisonOperator::Equal => {
                    conditions.insert(column.clone(), value);
                },
                _ => {
                    // Skip non-equality conditions
                }
            }
        },
        Expr::IsNull(_) => {
            // Skip IS NULL conditions
        },
        Expr::IsNotNull(_) => {
            // Skip IS NOT NULL conditions
        },
        Expr::In { .. } => {
            // Skip IN conditions
        },
        Expr::Between { .. } => {
            // Skip BETWEEN conditions for now (they're not equality conditions)
        }
    }
}



/// Schema object information
#[derive(Debug, Clone)]
pub struct SchemaObject {
    type_name: String,
    #[allow(dead_code)]
    name: String,
    root_page: u32,
    sql: String,
}

/// Table schema information
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub indexes: Vec<IndexInfo>,
    pub root_page: u32,
    pub sql: String,
}

/// Index schema information
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub root_page: u32,
}
