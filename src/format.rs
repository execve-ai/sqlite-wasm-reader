//! SQLite file format constants and structures

/// SQLite file header magic string
pub const SQLITE_HEADER_MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Size of a database page header
pub const PAGE_HEADER_SIZE: usize = 8;

/// Size of a cell pointer
pub const CELL_POINTER_SIZE: usize = 2;

/// SQLite file header structure
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FileHeader {
    /// Page size in bytes
    pub page_size: u32,
    /// File format write version
    pub write_version: u8,
    /// File format read version
    pub read_version: u8,
    /// Reserved space at end of each page
    pub reserved_space: u8,
    /// Maximum embedded payload fraction
    pub max_payload_fraction: u8,
    /// Minimum embedded payload fraction
    pub min_payload_fraction: u8,
    /// Leaf payload fraction
    pub leaf_payload_fraction: u8,
    /// File change counter
    pub file_change_counter: u32,
    /// Size of database in pages
    pub database_size: u32,
    /// Page number of first freelist page
    pub first_freelist_page: u32,
    /// Total number of freelist pages
    pub freelist_pages: u32,
    /// Schema cookie
    pub schema_cookie: u32,
    /// Schema format number
    pub schema_format: u32,
    /// Default page cache size
    pub default_cache_size: u32,
    /// Largest root page number
    pub largest_root_page: u32,
    /// Text encoding
    pub text_encoding: u32,
    /// User version
    pub user_version: u32,
    /// Incremental vacuum mode
    pub incremental_vacuum: u32,
    /// Application ID
    pub application_id: u32,
    /// Version valid for
    pub version_valid_for: u32,
    /// SQLite version number
    pub sqlite_version: u32,
}

/// Page types in SQLite
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Interior index b-tree page
    InteriorIndex = 0x02,
    /// Interior table b-tree page
    InteriorTable = 0x05,
    /// Leaf index b-tree page
    LeafIndex = 0x0a,
    /// Leaf table b-tree page
    LeafTable = 0x0d,
}

impl PageType {
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x02 => Some(PageType::InteriorIndex),
            0x05 => Some(PageType::InteriorTable),
            0x0a => Some(PageType::LeafIndex),
            0x0d => Some(PageType::LeafTable),
            _ => None,
        }
    }
    
    pub fn is_leaf(&self) -> bool {
        matches!(self, PageType::LeafIndex | PageType::LeafTable)
    }
    
    #[allow(dead_code)]
    pub fn is_table(&self) -> bool {
        matches!(self, PageType::InteriorTable | PageType::LeafTable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_header_magic() {
        assert_eq!(SQLITE_HEADER_MAGIC, b"SQLite format 3\0");
        assert_eq!(SQLITE_HEADER_MAGIC.len(), 16);
    }

    #[test]
    fn test_page_type_values() {
        assert_eq!(PageType::InteriorTable as u8, 0x05);
        assert_eq!(PageType::LeafTable as u8, 0x0d);
        assert_eq!(PageType::InteriorIndex as u8, 0x02);
        assert_eq!(PageType::LeafIndex as u8, 0x0a);
    }

    #[test]
    fn test_page_type_from_byte() {
        assert_eq!(PageType::from_byte(0x05), Some(PageType::InteriorTable));
        assert_eq!(PageType::from_byte(0x0d), Some(PageType::LeafTable));
        assert_eq!(PageType::from_byte(0x02), Some(PageType::InteriorIndex));
        assert_eq!(PageType::from_byte(0x0a), Some(PageType::LeafIndex));
        assert_eq!(PageType::from_byte(0x00), None);
        assert_eq!(PageType::from_byte(0xFF), None);
    }

    #[test]
    fn test_page_type_debug() {
        assert_eq!(format!("{:?}", PageType::InteriorTable), "InteriorTable");
        assert_eq!(format!("{:?}", PageType::LeafTable), "LeafTable");
        assert_eq!(format!("{:?}", PageType::InteriorIndex), "InteriorIndex");
        assert_eq!(format!("{:?}", PageType::LeafIndex), "LeafIndex");
    }

    #[test]
    fn test_page_type_methods() {
        assert!(PageType::InteriorTable.is_table());
        assert!(PageType::LeafTable.is_table());
        assert!(!PageType::InteriorIndex.is_table());
        assert!(!PageType::LeafIndex.is_table());

        assert!(!PageType::InteriorTable.is_leaf());
        assert!(PageType::LeafTable.is_leaf());
        assert!(!PageType::InteriorIndex.is_leaf());
        assert!(PageType::LeafIndex.is_leaf());
    }

    #[test]
    fn test_file_header_creation() {
        let header = FileHeader {
            page_size: 4096,
            write_version: 1,
            read_version: 1,
            reserved_space: 0,
            max_payload_fraction: 64,
            min_payload_fraction: 32,
            leaf_payload_fraction: 32,
            file_change_counter: 1,
            database_size: 10,
            first_freelist_page: 0,
            freelist_pages: 0,
            schema_cookie: 1,
            schema_format: 4,
            default_cache_size: 0,
            largest_root_page: 0,
            text_encoding: 1,
            user_version: 0,
            incremental_vacuum: 0,
            application_id: 0,
            version_valid_for: 1,
            sqlite_version: 3039000,
        };

        assert_eq!(header.page_size, 4096);
        assert_eq!(header.write_version, 1);
        assert_eq!(header.read_version, 1);
        assert_eq!(header.database_size, 10);
        assert_eq!(header.schema_cookie, 1);
        assert_eq!(header.text_encoding, 1);
        assert_eq!(header.sqlite_version, 3039000);
    }

    #[test]
    fn test_file_header_debug() {
        let header = FileHeader {
            page_size: 4096,
            write_version: 1,
            read_version: 1,
            reserved_space: 0,
            max_payload_fraction: 64,
            min_payload_fraction: 32,
            leaf_payload_fraction: 32,
            file_change_counter: 1,
            database_size: 10,
            first_freelist_page: 0,
            freelist_pages: 0,
            schema_cookie: 1,
            schema_format: 4,
            default_cache_size: 0,
            largest_root_page: 0,
            text_encoding: 1,
            user_version: 0,
            incremental_vacuum: 0,
            application_id: 0,
            version_valid_for: 1,
            sqlite_version: 3039000,
        };

        let debug_str = format!("{:?}", header);
        assert!(debug_str.contains("page_size: 4096"));
        assert!(debug_str.contains("database_size: 10"));
        assert!(debug_str.contains("sqlite_version: 3039000"));
    }
} 