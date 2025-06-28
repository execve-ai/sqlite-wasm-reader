//! B-tree traversal functionality

use crate::{Error, Result, page::Page, logging::log_warn, logging::log_debug};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::vec::Vec;

/// Cell in a B-tree page
#[derive(Debug)]
#[allow(dead_code)]
pub struct Cell {
    /// Left child page number (for interior pages)
    pub left_child: Option<u32>,
    /// Key (rowid for table b-trees)
    pub key: i64,
    /// Payload data
    pub payload: Vec<u8>,
}

/// B-tree cursor for traversing pages
pub struct BTreeCursor {
    /// Stack of pages being traversed
    /// Each entry contains: (page, current_cell_index, cells_processed)
    page_stack: Vec<(Page, usize, bool)>,
    /// Track visited pages to prevent infinite loops
    visited_pages: Vec<u32>,
    /// Safety counter to prevent infinite loops
    iteration_count: usize,
}

impl BTreeCursor {
    /// Create a new cursor starting at the given page
    pub fn new(root_page: Page) -> Self {
        let page_number = root_page.page_number;
        BTreeCursor {
            page_stack: vec![(root_page, 0, false)],
            visited_pages: vec![page_number],
            iteration_count: 0,
        }
    }
    
    /// Move to the next cell in the B-tree using in-order traversal
    pub fn next_cell<F>(&mut self, mut read_page: F) -> Result<Option<Cell>>
    where
        F: FnMut(u32) -> Result<Page>,
    {
        // Safety check: prevent infinite loops
        self.iteration_count += 1;
        if self.iteration_count > 100_000 {
            return Err(Error::InvalidFormat("B-tree traversal exceeded safety limit".into()));
        }
        
        loop {
            if self.page_stack.is_empty() {
                return Ok(None);
            }
            
            let (page, cell_index, cells_processed) = self.page_stack.last_mut().unwrap();
            
            // If this is a leaf page
            if page.page_type.is_leaf() {
                // If we've processed all cells in this leaf page
                if *cell_index >= page.cell_count as usize {
                    // Pop this page and continue with parent
                    self.page_stack.pop();
                    continue;
                }
                
                // Get the current cell from leaf page
                let is_first_page = page.page_number == 1;
                let cell_pointers = match page.cell_pointers(is_first_page) {
                    Ok(pointers) => pointers,
                    Err(e) => {
                        log_warn(&format!("Failed to get cell pointers for page {}: {}", page.page_number, e));
                        // Skip this page and continue with parent
                        self.page_stack.pop();
                        continue;
                    }
                };
                
                if *cell_index >= cell_pointers.len() {
                    // Pop this page and continue with parent
                    self.page_stack.pop();
                    continue;
                }
                
                let cell_offset = cell_pointers[*cell_index];
                let cell_data = match page.cell_content(cell_offset) {
                    Ok(data) => data,
                    Err(e) => {
                        log_warn(&format!("Failed to get cell content at offset {} on page {}: {}", cell_offset, page.page_number, e));
                        // Skip this cell and move to next
                        *cell_index += 1;
                        continue;
                    }
                };
                
                // Move to next cell in current page
                *cell_index += 1;
                
                // Parse and return the leaf cell
                let cell = match parse_leaf_table_cell(cell_data) {
                    Ok(cell) => cell,
                    Err(e) => {
                        log_debug(&format!("Failed to parse leaf cell on page {}: {}", page.page_number, e));
                        // Skip this cell and continue to next iteration
                        continue;
                    }
                };
                return Ok(Some(cell));
            }
            
            // This is an interior page
            if *cell_index >= page.cell_count as usize {
                // We've processed all cells in this interior page
                // Follow the right-most pointer if it exists
                if let Some(right_ptr) = page.right_pointer {
                    // Safety check: prevent revisiting the same page
                    if self.visited_pages.contains(&right_ptr) {
                        // We're about to revisit a page, this indicates a cycle
                        // Pop this page and continue with parent instead
                        self.page_stack.pop();
                        continue;
                    }
                    
                    let right_page = read_page(right_ptr)?;
                    self.visited_pages.push(right_ptr);
                    self.page_stack.push((right_page, 0, false));
                    continue;
                }
                
                // No right pointer, pop this page and continue with parent
                self.page_stack.pop();
                continue;
            }
            
            // Process the current cell in the interior page
            let is_first_page = page.page_number == 1;
            let cell_pointers = match page.cell_pointers(is_first_page) {
                Ok(pointers) => pointers,
                Err(e) => {
                    log_warn(&format!("Failed to get cell pointers for interior page {}: {}", page.page_number, e));
                    // Skip this page and continue with parent
                    self.page_stack.pop();
                    continue;
                }
            };
            
            if *cell_index >= cell_pointers.len() {
                // Pop this page and continue with parent
                self.page_stack.pop();
                continue;
            }
            
            let cell_offset = cell_pointers[*cell_index];
            let cell_data = match page.cell_content(cell_offset) {
                Ok(data) => data,
                Err(e) => {
                    log_warn(&format!("Failed to get cell content at offset {} on interior page {}: {}", cell_offset, page.page_number, e));
                    // Skip this cell and move to next
                    *cell_index += 1;
                    continue;
                }
            };
            
            // Parse the interior cell
            let cell = match parse_interior_table_cell(cell_data) {
                Ok(cell) => cell,
                Err(e) => {
                    log_warn(&format!("Failed to parse interior cell on page {}: {}", page.page_number, e));
                    // Skip this cell and move to next
                    *cell_index += 1;
                    continue;
                }
            };
            
            // If we haven't processed the left child of this cell yet
            if !*cells_processed {
                // Mark that we've processed the left child
                *cells_processed = true;
                
                // Push the left child page onto the stack
                if let Some(left_child) = cell.left_child {
                    // Safety check: prevent revisiting the same page
                    if self.visited_pages.contains(&left_child) {
                        // We're about to revisit a page, this indicates a cycle
                        // Skip this left child and move to next cell
                        *cell_index += 1;
                        *cells_processed = false;
                        continue;
                    }
                    
                    let left_page = read_page(left_child)?;
                    self.visited_pages.push(left_child);
                    self.page_stack.push((left_page, 0, false));
                    continue;
                }
            }
            
            // We've processed the left child, now move to the next cell in this interior page
            // DO NOT return the interior cell - it's just for navigation
            *cell_index += 1;
            *cells_processed = false; // Reset for next cell
            
            // Continue to the next iteration to process the next cell
            continue;
        }
    }
}

/// Parse a leaf table cell
fn parse_leaf_table_cell(data: &[u8]) -> Result<Cell> {
    let (payload_size, offset) = read_varint(data)?;
    let (rowid, offset2) = read_varint(&data[offset..])?;
    let offset = offset + offset2;
    
    // Add bounds checking to prevent panic
    let payload_end = offset + payload_size as usize;
    if payload_end > data.len() {
        return Err(Error::InvalidFormat(format!(
            "Payload size {} exceeds available data (offset: {}, data_len: {})",
            payload_size, offset, data.len()
        )));
    }
    
    let payload = data[offset..payload_end].to_vec();
    
    Ok(Cell {
        left_child: None,
        key: rowid,
        payload,
    })
}

/// Parse an interior table cell
fn parse_interior_table_cell(data: &[u8]) -> Result<Cell> {
    // Check if we have enough data for the left child pointer
    if data.len() < 4 {
        return Err(Error::InvalidFormat(format!(
            "Interior cell data too short: {} bytes, need at least 4",
            data.len()
        )));
    }
    
    let left_child = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let (rowid, _) = read_varint(&data[4..])?;
    
    Ok(Cell {
        left_child: Some(left_child),
        key: rowid,
        payload: Vec::new(),
    })
}

/// Read a variable-length integer
pub fn read_varint(data: &[u8]) -> Result<(i64, usize)> {
    let mut value = 0i64;
    let mut offset = 0;
    
    for i in 0..9 {
        if offset >= data.len() {
            return Err(Error::InvalidVarint);
        }
        
        let byte = data[offset];
        offset += 1;
        
        if i < 8 {
            value = (value << 7) | ((byte & 0x7f) as i64);
            if byte < 0x80 {
                return Ok((value, offset));
            }
        } else {
            value = (value << 8) | (byte as i64);
            return Ok((value, offset));
        }
    }
    
    Err(Error::InvalidVarint)
} 