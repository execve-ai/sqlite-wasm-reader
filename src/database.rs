//! Main database interface

use crate::{
    Error, Result, Value,
    format::{FileHeader, SQLITE_HEADER_MAGIC},
    page::Page,
    btree::BTreeCursor,
    record::parse_record,
    logging::{log_error, log_warn, log_debug},
    query::{ComparisonOperator, Expr, SelectQuery},
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
    /// Cache of table schemas and their indexes
    schema_cache: HashMap<String, TableInfo>,
    /// Cache of recently read pages (page_number -> Page)
    /// Limited to prevent excessive memory usage
    page_cache: HashMap<u32, Page>,
    /// Maximum number of pages to cache (LRU eviction)
    max_cache_size: usize,
    /// LRU ordering for page cache - tracks access order
    page_lru_order: Vec<u32>,
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
        
        let mut db = Database { 
            file, 
            header,
            schema_cache: HashMap::new(),
            page_cache: HashMap::new(),
            max_cache_size: 1000, // Increased cache size for better performance
            page_lru_order: Vec::new(),
        };
        
        // Preload schema information
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
    
    /// Read a page by number (1-indexed) with caching
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
        
        // Read from disk
        let offset = (page_number - 1) as u64 * self.header.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        
        let mut data = vec![0u8; self.header.page_size as usize];
        self.file.read_exact(&mut data)?;
        
        let page = Page::parse(page_number, data, page_number == 1)?;
        
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
    pub fn get_table_columns(&mut self, table_name: &str) -> Result<Vec<String>> {
        // Use cached schema instead of reading it again
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        Ok(table_info.columns.clone())
    }
    
    /// Execute a SELECT SQL query using index-based search only
    pub fn execute_query(&mut self, query: &SelectQuery) -> Result<Vec<Row>> {
        let table_name = &query.table;
        
        // Get table info once and reuse
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
        
        let columns = table_info.columns.clone();
        let table_info_clone = table_info.clone();
        let has_where = query.where_expr.is_some();

        if !has_where {
            return Err(Error::QueryError("Query requires a WHERE clause to use index-based search".to_string()));
        }

        let where_expr = query.where_expr.as_ref().unwrap();
        let or_branches = collect_or_branches(where_expr);
        
        // Process each OR branch to find usable indexes
        let mut all_rowids = std::collections::HashSet::new();
        
        for branch in or_branches.iter() {
            if let Some((index, values)) = find_best_index(&table_info_clone, branch) {
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
        
        // If no branches could use an index, return an error
        if all_rowids.is_empty() {
            return Err(Error::QueryError("No suitable index found for the query conditions".to_string()));
        }

        // If no matching rows are found, return an empty result immediately
        if all_rowids.is_empty() {
            return Ok(Vec::new());
        }

        // Convert rowids to a vec for deterministic ordering
        let all_rowids: Vec<_> = all_rowids.into_iter().collect();
        let mut rows = Vec::with_capacity(all_rowids.len());
        
        // Fetch each matching row by its ROWID using targeted lookups
        for rowid in all_rowids {
            if let Some(row) = self.read_row_by_rowid(table_name, rowid, &columns)? {
                rows.push(row);
            }
        }

        // When using an index, we trust that the index has already filtered the rows correctly.
        // We should NOT apply the WHERE clause again, only apply ORDER BY, column selection, and LIMIT.
        let mut result_query = query.clone();
        result_query.where_expr = None; // Remove WHERE clause since index already filtered
        
        let result = result_query.execute(rows, &table_info_clone.columns)?;
        Ok(result)
    }
    
    /// Read a single row by its ROWID using targeted binary search
    fn read_row_by_rowid(&mut self, table_name: &str, rowid: i64, columns: &[String]) -> Result<Option<Row>> {
        // Get the table's root page from cache
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        
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
}

/// Index schema information
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub root_page: u32,
}
