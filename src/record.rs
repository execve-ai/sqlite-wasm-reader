//! SQLite record parsing

use crate::{Error, Result, Value, btree::read_varint};
use byteorder::{BigEndian, ByteOrder};

#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
use alloc::{vec::Vec, string::String};

/// Parse a record from payload data with optimized allocations
pub fn parse_record_optimized(payload: &[u8]) -> Result<Vec<Value>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    
    // Read header size varint
    let (header_size, header_size_bytes) = read_varint(payload)?;
    if header_size as usize > payload.len() {
        return Err(Error::InvalidRecord);
    }
    
    // Safety check: limit header size to prevent memory issues
    if header_size > 65536 {
        return Err(Error::InvalidFormat(format!("Header size too large: {} bytes", header_size)));
    }
    
    // Read serial types from header with pre-allocation
    let header_end = header_size as usize;
    let mut serial_types = Vec::new();
    let mut offset = header_size_bytes;
    
    while offset < header_end {
        let (serial_type, bytes_read) = read_varint(&payload[offset..])?;
        serial_types.push(serial_type);
        offset += bytes_read;
        
        // Safety check
        if serial_types.len() > 1000 {
            return Err(Error::InvalidFormat("Too many columns in record".into()));
        }
    }
    
    // Pre-allocate values vector
    let mut values = Vec::with_capacity(serial_types.len());
    let mut data_offset = header_end;
    
    // Parse values with minimal allocations
    for &serial_type in &serial_types {
        if data_offset >= payload.len() {
            values.push(Value::Null);
            continue;
        }
        
        let (value, bytes_consumed) = parse_value_optimized(&payload[data_offset..], serial_type)?;
        values.push(value);
        data_offset += bytes_consumed;
    }
    
    Ok(values)
}

/// Parse a single value with optimized allocations
fn parse_value_optimized(data: &[u8], serial_type: i64) -> Result<(Value, usize)> {
    match serial_type {
        0 => Ok((Value::Null, 0)),
        1 => {
            if data.is_empty() {
                Ok((Value::Integer(0), 0))
            } else {
                Ok((Value::Integer(data[0] as i8 as i64), 1))
            }
        }
        2 => {
            if data.len() < 2 {
                Ok((Value::Integer(0), 0))
            } else {
                let value = BigEndian::read_i16(data) as i64;
                Ok((Value::Integer(value), 2))
            }
        }
        3 => {
            if data.len() < 3 {
                Ok((Value::Integer(0), 0))
            } else {
                let mut bytes = [0u8; 4];
                bytes[1..4].copy_from_slice(&data[0..3]);
                let value = BigEndian::read_i32(&bytes) >> 8; // Sign extend
                Ok((Value::Integer(value as i64), 3))
            }
        }
        4 => {
            if data.len() < 4 {
                Ok((Value::Integer(0), 0))
            } else {
                let value = BigEndian::read_i32(data) as i64;
                Ok((Value::Integer(value), 4))
            }
        }
        5 => {
            if data.len() < 6 {
                Ok((Value::Integer(0), 0))
            } else {
                let mut bytes = [0u8; 8];
                bytes[2..8].copy_from_slice(&data[0..6]);
                let value = BigEndian::read_i64(&bytes) >> 16; // Sign extend
                Ok((Value::Integer(value), 6))
            }
        }
        6 => {
            if data.len() < 8 {
                Ok((Value::Integer(0), 0))
            } else {
                let value = BigEndian::read_i64(data);
                Ok((Value::Integer(value), 8))
            }
        }
        7 => {
            if data.len() < 8 {
                Ok((Value::Real(0.0), 0))
            } else {
                let bits = BigEndian::read_u64(data);
                let value = f64::from_bits(bits);
                Ok((Value::Real(value), 8))
            }
        }
        8 => Ok((Value::Integer(0), 0)),
        9 => Ok((Value::Integer(1), 0)),
        _ => {
            if serial_type >= 12 {
                if serial_type % 2 == 0 {
                    // BLOB
                    let length = ((serial_type - 12) / 2) as usize;
                    if data.len() < length {
                        Ok((Value::Blob(Vec::new()), 0))
                    } else {
                        // Use from_slice to avoid unnecessary allocation
                        Ok((Value::Blob(data[0..length].to_vec()), length))
                    }
                } else {
                    // TEXT
                    let length = ((serial_type - 13) / 2) as usize;
                    if data.len() < length {
                        Ok((Value::Text(String::new()), 0))
                    } else {
                        // Use from_utf8_lossy to handle invalid UTF-8 gracefully
                        let text = String::from_utf8_lossy(&data[0..length]).into_owned();
                        Ok((Value::Text(text), length))
                    }
                }
            } else {
                Ok((Value::Null, 0))
            }
        }
    }
}

/// Parse a value based on its serial type
pub fn parse_value(serial_type: i64, data: &[u8]) -> Result<(Value, usize)> {
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

/// Parse a record from payload data (original version for compatibility)
pub fn parse_record(payload: &[u8]) -> Result<Vec<Value>> {
    parse_record_optimized(payload)
} 