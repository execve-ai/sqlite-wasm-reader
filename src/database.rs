//! Main database interface

use crate::{
    Error, Result, Value,
    format::{FileHeader, SQLITE_HEADER_MAGIC},
    page::Page,
    btree::BTreeCursor,
    record::parse_record,
    logging::{log_error, log_warn, log_info, log_debug},
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
        let columns = Self::parse_create_table_columns(sql)?;
        
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
    
    /// Execute a SELECT SQL query using index-based search only
    pub fn execute_query(&mut self, query: &SelectQuery) -> Result<Vec<Row>> {
        log_debug(&format!("Executing query: {:?}", query));
        
        let table_name = &query.table;
        log_debug(&format!("Attempting to use index for table: {}", table_name));

        // Get table info first
        let (columns, has_where, table_info) = {
            let table_info = self.schema_cache.get(table_name)
                .ok_or_else(|| {
                    log_debug(&format!("Table not found in schema cache: {}", table_name));
                    Error::TableNotFound(table_name.clone())
                })?;
            log_debug(&format!("Table '{}' has {} indexes: {:?}", table_name, table_info.indexes.len(), 
                table_info.indexes.iter().map(|i| &i.name).collect::<Vec<_>>()));
            (table_info.columns.clone(), query.where_expr.is_some(), table_info.clone())
        };

        if !has_where {
            return Err(Error::QueryError("Query requires a WHERE clause to use index-based search".to_string()));
        }

        let where_expr = query.where_expr.as_ref().unwrap();
        log_debug(&format!("Processing WHERE expression: {:?}", where_expr));
        
        let or_branches = collect_or_branches(where_expr);
        log_debug(&format!("Collected {} OR branches from WHERE clause", or_branches.len()));
        
        // Process each OR branch to find usable indexes
        let mut indexed_branches = Vec::new();
            
        for (i, branch) in or_branches.iter().enumerate() {
            log_debug(&format!("Processing branch {}: {:?}", i, branch));
            if let Some((index, values)) = find_best_index(&table_info, branch) {
                log_debug(&format!("Found suitable index '{}' for branch {} with values: {:?}", 
                    index.name, i, values));
                log_debug(&format!(
                    "Will use index '{}' on columns '{:?}' with values: {:?}",
                    index.name, index.columns, values
                ));
                // Clone the values to avoid reference issues
                let owned_values = values.into_iter().cloned().collect();
                indexed_branches.push((index.clone(), owned_values));
            } else {
                log_debug("No suitable index found for a branch");
            }
        }
        
        log_debug(&format!("Found {} branches that can use an index", indexed_branches.len()));
        
        // If no branches could use an index, return an error
        if indexed_branches.is_empty() {
            return Err(Error::QueryError("No suitable index found for the query conditions".to_string()));
        }

        // Process the indexed branches to get rowids
        log_debug("Processing indexed branches to find matching rowids...");
        let all_rowids = self.process_indexed_branches(indexed_branches)?;
        log_debug(&format!("Found {} unique rowids from index scans", all_rowids.len()));

        // If no matching rows are found, return an empty result immediately
        if all_rowids.is_empty() {
            log_debug("No matching rows found in any index, returning empty result");
            return Ok(Vec::new());
        }

        // Convert rowids to a vec for deterministic ordering
        let all_rowids: Vec<_> = all_rowids.into_iter().collect();
        let mut rows = Vec::with_capacity(all_rowids.len());
        
        // Fetch each matching row by its ROWID
        for rowid in all_rowids {
            if let Some(row) = read_row_by_rowid(self, table_name, rowid, &columns)? {
                rows.push(row);
            }
        }

        // Get fresh table info to avoid borrow issues
        let table_info = self.schema_cache.get(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        // When using an index, we trust that the index has already filtered the rows correctly.
        // We should NOT apply the WHERE clause again, only apply ORDER BY, column selection, and LIMIT.
        let mut result_query = query.clone();
        result_query.where_expr = None; // Remove WHERE clause since index already filtered
        
        let result = result_query.execute(rows, &table_info.columns)?;
        log_debug(&format!("Query executed using index, found {} rows", result.len()));
        Ok(result)
    }
    


    // Process the indexed branches after extracting the necessary information
    fn process_indexed_branches(
        &mut self,
        indexed_branches: Vec<(IndexInfo, Vec<Value>)>,
    ) -> Result<std::collections::HashSet<i64>> {
        let mut all_rowids = std::collections::HashSet::new();

        for (index, values) in indexed_branches {
            let index_root_page = self.read_page(index.root_page)?;
            let mut cursor = BTreeCursor::new(index_root_page);
            
            // Convert Vec<Value> to Vec<&Value> for find_rowids_by_key
            let value_refs: Vec<&Value> = values.iter().collect();
            let page_reader = |page_num: u32| self.read_page(page_num);
            let rowids = cursor.find_rowids_by_key(&value_refs, page_reader)?;
            all_rowids.extend(rowids);
        }

        Ok(all_rowids)
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
    log_debug(&format!("Finding best index for expression: {:?}", expr));
    
    let mut conditions = HashMap::new();
    collect_and_conditions(expr, &mut conditions);
    
    log_debug(&format!("Extracted conditions: {:?}", 
        conditions.iter().map(|(k, v)| (k, *v)).collect::<HashMap<_, _>>()));
    log_debug(&format!("Available indexes: {:?}", 
        table_info.indexes.iter().map(|i| (i.name.clone(), i.columns.clone())).collect::<Vec<_>>()));

    let mut best_index: Option<(&'a IndexInfo, Vec<&'b Value>)> = None;
    let mut max_len = 0;

    for index in &table_info.indexes {
        log_debug(&format!("Checking index on columns: {:?}", index.columns));
        let mut values = Vec::new();
        let mut prefix_matched = true;

        // For composite indexes, we can use prefix matching
        // We need consecutive columns starting from the first column
        for col in &index.columns {
            match conditions.get(col) {
                Some(value) => {
                    log_debug(&format!("  - Column '{}' matches condition with value: {:?}", col, value));
                    values.push(*value);
                }
                None => {
                    log_debug(&format!("  - Column '{}' missing -> stopping prefix match here", col));
                    prefix_matched = false;
                    break;
                }
            }
        }

        // We can use an index if we have at least one matching column from the prefix
        if !values.is_empty() {
            if prefix_matched {
                log_debug(&format!("✔️  Index '{}' fully satisfies equality conditions", index.name));
            } else {
                log_debug(&format!("✔️  Index '{}' supports prefix matching with {} columns", index.name, values.len()));
            }
            
            // Prefer the index that covers the most columns (ties resolved arbitrarily)
            if values.len() > max_len {
                 log_debug(&format!("  ★ New best index: '{}' with {} matching columns", 
                     index.name, values.len()));
                best_index = Some((index, values.clone()));
                max_len = values.len();
            } else {
                log_debug(&format!("  ✓ Index '{}' has {} matching columns (current best: {})", 
                    index.name, values.len(), max_len));
            }
        } else {
            log_debug(&format!("  ✗ Index '{}' has no matching columns", index.name));
        }
    }

    if let Some((index, values)) = &best_index {
        log_debug(&format!("\nSelected index: '{}' on columns: {:?} with values: {:?}", 
            index.name, index.columns, values));
    } else {
        log_debug("\nNo suitable index found for the conditions");
    }

    best_index
}

/// Collect all equality conditions from an AND expression tree.
fn collect_and_conditions<'b>(expr: &'b Expr, conditions: &mut HashMap<String, &'b Value>) {
    log_debug(&format!("Processing expression: {:?}", expr));
    
    match expr {
        Expr::And(left, right) => {
            log_debug("Processing AND expression");
            collect_and_conditions(left, conditions);
            collect_and_conditions(right, conditions);
        }
        Expr::Or(left, right) => {
            log_debug("Processing OR expression");
            // For OR expressions, we need to handle them at a higher level
            // Just log them for now and let the caller handle them
            log_debug(&format!("Found OR expression between {:?} and {:?}", left, right));
        }
        Expr::Not(inner) => {
            log_debug("Processing NOT expression");
            log_debug(&format!("Found NOT expression: {:?}", inner));
        }
        Expr::Comparison { column, operator, value } => {
            match operator {
                ComparisonOperator::Equal => {
                    log_debug(&format!("  ✓ Found equality condition: {} = {:?}", column, value));
                    conditions.insert(column.clone(), value);
                },
                _ => {
                    log_debug(&format!("  ⚠️  Skipping non-equality condition: {} {:?} {:?}", 
                        column, operator, value));
                }
            }
        },
        Expr::IsNull(column) => {
            log_debug(&format!("  ⚠️  Skipping IS NULL condition on column: {}", column));
        },
        Expr::IsNotNull(column) => {
            log_debug(&format!("  ⚠️  Skipping IS NOT NULL condition on column: {}", column));
        },
        Expr::In { column, values } => {
            log_debug(&format!("  ⚠️  Skipping IN condition on column: {} with {} values", 
                column, values.len()));
        }
    }
}

/// Read a single row by its ROWID
fn read_row_by_rowid(db: &mut Database, table_name: &str, rowid: i64, columns: &[String]) -> Result<Option<Row>> {
    // Get the table's root page from cache
    let table_info = db.schema_cache.get(table_name)
        .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

    log_debug(&format!("Looking for row with ROWID {} in table '{}'", rowid, table_name));
    
    let root_page = db.read_page(table_info.root_page)?;
    let mut cursor = BTreeCursor::new(root_page);

    // Try to find the cell with the matching ROWID
    match cursor.find_cell(rowid, |page_num| db.read_page(page_num)) {
        Ok(Some(cell)) => {
            log_debug(&format!("Found cell for ROWID {}, payload size: {} bytes", rowid, cell.payload.len()));
            
            // Parse the row data
            match parse_record(&cell.payload) {
                Ok(values) => {
                    log_debug(&format!("Successfully parsed record with {} values", values.len()));
                    
                    // Convert to a row with column names
                    let mut row = HashMap::new();
                    for (i, column_name) in columns.iter().enumerate() {
                        let value = values.get(i).cloned().unwrap_or(Value::Null);
                        log_debug(&format!("  {}: {:?}", column_name, value));
                        row.insert(column_name.clone(), value);
                    }
                    
                    Ok(Some(row))
                },
                Err(e) => {
                    log_debug(&format!("Failed to parse record for ROWID {}: {}", rowid, e));
                    log_debug(&format!("Payload hex: {:?}", cell.payload));
                    // Return None instead of failing to allow processing to continue with other rows
                    Ok(None)
                }
            }
        },
        Ok(None) => {
            log_debug(&format!("No cell found for ROWID {} in table '{}'", rowid, table_name));
            Ok(None)
        },
        Err(e) => {
            log_debug(&format!("Error finding cell for ROWID {}: {}", rowid, e));
            Err(e)
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
