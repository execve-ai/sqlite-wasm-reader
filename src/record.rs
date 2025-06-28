//! SQLite record parsing

use crate::{Error, Result, Value, btree::read_varint, logging::log_warn, logging::log_debug};
use byteorder::{BigEndian, ByteOrder};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{vec::Vec, string::String};

/// Parse a record from payload data
pub fn parse_record(payload: &[u8]) -> Result<Vec<Value>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    
    // Read header size varint
    let (header_size, header_size_bytes) = read_varint(payload)?;
    if header_size as usize > payload.len() {
        return Err(Error::InvalidRecord);
    }
    
    // Safety check: limit header size to prevent memory issues
    // Record headers are typically < 1KB, 64KB is very generous
    if header_size > 65536 {
        return Err(Error::InvalidFormat(format!("Header size too large: {} bytes", header_size)));
    }
    
    // Read serial types from header
    let mut serial_types = Vec::new();
    // The header_size includes the header_size varint itself
    let header_end = header_size as usize;
    let mut offset = header_size_bytes;
    
    // Safety check: limit number of serial types
    // Most SQLite tables have < 100 columns, 10000 is very generous
    let max_serial_types = 10_000;
    
    while offset < header_end {
        if serial_types.len() >= max_serial_types {
            // If we hit this limit, something is wrong with parsing
            return Err(Error::InvalidFormat(format!(
                "Too many serial types: {} (limit: {})", 
                serial_types.len(), 
                max_serial_types
            )));
        }
        
        let (serial_type, bytes_read) = read_varint(&payload[offset..])?;
        offset += bytes_read;
        serial_types.push(serial_type);
    }
    
    // Skip to data section - data starts right after the header
    offset = header_size as usize;
    
    // Parse values based on serial types
    let mut values = Vec::new();
    
    for (i, serial_type) in serial_types.iter().enumerate() {
        // Check if we have enough data left
        if offset >= payload.len() {
            log_debug(&format!("Ran out of data while parsing value {} (serial_type: {}). Payload size: {}, offset: {}", i, serial_type, payload.len(), offset));
            // Instead of breaking, add NULL values for remaining columns
            for _ in i..serial_types.len() {
                values.push(Value::Null);
            }
            break;
        }
        
        match parse_value(*serial_type, &payload[offset..]) {
            Ok((value, bytes_read)) => {
                offset += bytes_read;
                values.push(value);
            }
            Err(e) => {
                log_warn(&format!("Failed to parse value {} (serial_type: {}): {}. Payload size: {}, offset: {}", i, serial_type, e, payload.len(), offset));
                // Add a null value as fallback and continue
                values.push(Value::Null);
                // Try to advance offset to prevent infinite loops
                if offset < payload.len() {
                    offset += 1;
                }
            }
        }
    }
    
    Ok(values)
}

/// Parse a value based on its serial type
fn parse_value(serial_type: i64, data: &[u8]) -> Result<(Value, usize)> {
    match serial_type {
        0 => Ok((Value::Null, 0)),
        1 => {
            if data.is_empty() {
                return Err(Error::InvalidRecord);
            }
            Ok((Value::Integer(data[0] as i64), 1))
        }
        2 => {
            if data.len() < 2 {
                return Err(Error::InvalidRecord);
            }
            Ok((Value::Integer(BigEndian::read_i16(data) as i64), 2))
        }
        3 => {
            if data.len() < 3 {
                return Err(Error::InvalidRecord);
            }
            let value = ((data[0] as i64) << 16) | 
                       ((data[1] as i64) << 8) | 
                       (data[2] as i64);
            // Sign extend from 24-bit
            let value = if value & 0x800000 != 0 {
                value | 0xffffffffff000000u64 as i64
            } else {
                value
            };
            Ok((Value::Integer(value), 3))
        }
        4 => {
            if data.len() < 4 {
                return Err(Error::InvalidRecord);
            }
            Ok((Value::Integer(BigEndian::read_i32(data) as i64), 4))
        }
        5 => {
            if data.len() < 6 {
                return Err(Error::InvalidRecord);
            }
            let value = ((data[0] as i64) << 40) |
                       ((data[1] as i64) << 32) |
                       ((data[2] as i64) << 24) |
                       ((data[3] as i64) << 16) |
                       ((data[4] as i64) << 8) |
                       (data[5] as i64);
            // Sign extend from 48-bit
            let value = if value & 0x800000000000 != 0 {
                value | 0xffff000000000000u64 as i64
            } else {
                value
            };
            Ok((Value::Integer(value), 6))
        }
        6 => {
            if data.len() < 8 {
                return Err(Error::InvalidRecord);
            }
            Ok((Value::Integer(BigEndian::read_i64(data)), 8))
        }
        7 => {
            if data.len() < 8 {
                return Err(Error::InvalidRecord);
            }
            Ok((Value::Real(BigEndian::read_f64(data)), 8))
        }
        8 => Ok((Value::Integer(0), 0)),
        9 => Ok((Value::Integer(1), 0)),
        10 | 11 => Err(Error::UnsupportedFeature("Reserved serial types".into())),
        n if n >= 12 && n % 2 == 0 => {
            // BLOB with length (n-12)/2
            let length = ((n - 12) / 2) as usize;
            
            // Safety check: limit BLOB size (increased significantly)
            if length > 1_000_000_000 {
                return Err(Error::InvalidFormat("BLOB too large".into()));
            }
            
            if data.len() < length {
                return Err(Error::InvalidRecord);
            }
            Ok((Value::Blob(data[..length].to_vec()), length))
        }
        n if n >= 13 && n % 2 == 1 => {
            // String with length (n-13)/2
            let length = ((n - 13) / 2) as usize;
            
            // Safety check: limit string size (increased significantly)
            if length > 100_000_000 {
                return Err(Error::InvalidFormat("String too large".into()));
            }
            
            if data.len() < length {
                return Err(Error::InvalidRecord);
            }
            let text = core::str::from_utf8(&data[..length])?;
            Ok((Value::Text(text.to_string()), length))
        }
        _ => Err(Error::InvalidFormat("Invalid serial type".into())),
    }
} 