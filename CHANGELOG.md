# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2025-07-07

### Added
- **Performance Improvements**:
  - Eliminated unnecessary memory allocations during page reading by using slices instead of `Vec<u8>` copying
  - Reduced memory footprint and improved cache locality for large databases
  - Optimized page parsing for better performance in table scans

### Improved
- **Query Determinism**: Fixed unpredictable row ordering in index-based queries by sorting rowids before fetching rows
- **Robustness**: Enhanced NaN value handling in floating-point comparisons to prevent runtime panics
- **Memory Efficiency**: Removed unused functions and optimized data structures for better memory usage
- **Error Handling**: Improved error recovery and logging for better debugging experience

### Technical Improvements
- **Value Type Safety**: Implemented robust `PartialOrd` and `Ord` traits for `Value` enum with proper NaN handling
- **Code Cleanup**: Removed unused functions and optimized internal data structures
- **Deterministic Results**: Ensured consistent query results across different runs and environments

## [0.2.0] - 2025-07-04

### Added
- **Enhanced SQL WHERE clause support** with comprehensive logical operators and expressions:
  - `OR` operator for combining conditions
  - `NOT` operator for negating expressions
  - `IS NULL` and `IS NOT NULL` for null value checks
  - `IN` operator for membership testing with value lists
  - `BETWEEN ... AND ...` for range queries
  - Parentheses support for grouping complex expressions
  - Proper operator precedence handling

### Examples
The following SQL syntax is now supported:

```sql
-- Logical operators
WHERE a = 1 AND b = 2
WHERE a = 1 OR b = 2
WHERE NOT (a = 1)

-- Null checks
WHERE column IS NULL
WHERE column IS NOT NULL

-- Membership
WHERE column IN (1, 2, 3)

-- Range queries
WHERE column BETWEEN 50 AND 100

-- Complex expressions
WHERE (a = 1 OR b = 2) AND c = 3
```

### Breaking Changes
- **API Change**: `SelectQuery::where_conditions` field renamed to `where_expr` and changed type from `Vec<WhereCondition>` to `Option<Expr>`
- **Removed**: `WhereCondition` struct (replaced with `Expr` enum)

## [0.1.0] - 2025-06-30

### Added
- Initial release with basic SQLite database reading capabilities
- WASI-compatible implementation