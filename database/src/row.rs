use anyhow::{Context, Result};
use common::{Data, DataType};
use common::query::{ComparisionOperator, ComparisionValue, Predicate};

use crate::schema::ColumnInfo;

pub fn decode_row(data: &[u8], mut offset: usize, schema: &[ColumnInfo]) -> Result<(Vec<Data>, usize)> {
    let mut row = Vec::with_capacity(schema.len());
    for col in schema {
        match col.data_type {
            DataType::Int32 => {
                let bytes: [u8; 4] = data[offset..offset + 4].try_into()?;
                row.push(Data::Int32(i32::from_le_bytes(bytes)));
                offset += 4;
            }
            DataType::Int64 => {
                let bytes: [u8; 8] = data[offset..offset + 8].try_into()?;
                row.push(Data::Int64(i64::from_le_bytes(bytes)));
                offset += 8;
            }
            DataType::Float32 => {
                let bytes: [u8; 4] = data[offset..offset + 4].try_into()?;
                row.push(Data::Float32(f32::from_le_bytes(bytes)));
                offset += 4;
            }
            DataType::Float64 => {
                let bytes: [u8; 8] = data[offset..offset + 8].try_into()?;
                row.push(Data::Float64(f64::from_le_bytes(bytes)));
                offset += 8;
            }
            DataType::String => {
                let end = data[offset..]
                    .iter()
                    .position(|&b| b == 0)
                    .context("Null terminator not found in string column")?;
                let s = std::str::from_utf8(&data[offset..offset + end])?.to_string();
                row.push(Data::String(s));
                offset += end + 1;
            }
        }
    }
    Ok((row, offset))
}

pub fn encode_row(row: &[Data], out: &mut Vec<u8>) {
    for val in row {
        match val {
            Data::Int32(v) => out.extend_from_slice(&v.to_le_bytes()),
            Data::Int64(v) => out.extend_from_slice(&v.to_le_bytes()),
            Data::Float32(v) => out.extend_from_slice(&v.to_le_bytes()),
            Data::Float64(v) => out.extend_from_slice(&v.to_le_bytes()),
            Data::String(v) => {
                out.extend_from_slice(v.as_bytes());
                out.push(0);
            }
        }
    }
}

pub fn encode_row_len(row: &[Data]) -> usize {
    let mut len = 0;
    for val in row {
        match val {
            Data::Int32(_) | Data::Float32(_) => len += 4,
            Data::Int64(_) | Data::Float64(_) => len += 8,
            Data::String(v) => len += v.len() + 1,
        }
        len += 40; // Data enum (32 bytes) + per-element overhead
    }
    len += 64; // Vec<Data> header + allocator overhead
    len
}

pub fn format_float(s: String) -> String {
    if s.contains('.') || s.contains('e') || s.contains('E') || s.contains("inf") || s.contains("nan") {
        s
    } else {
        s + ".0"
    }
}

pub fn format_value(val: &Data) -> String {
    match val {
        Data::Int32(v) => v.to_string(),
        Data::Int64(v) => v.to_string(),
        Data::Float32(v) => format_float(v.to_string()),
        Data::Float64(v) => format_float(v.to_string()),
        Data::String(v) => v.clone(),
    }
}

pub fn compare_data(left: &Data, op: &ComparisionOperator, right: &Data) -> bool {
    let ord = left.partial_cmp(right);
    match op {
        ComparisionOperator::EQ => left == right,
        ComparisionOperator::NE => left != right,
        ComparisionOperator::GT  => ord.map(|o| o.is_gt()).unwrap_or(false),
        ComparisionOperator::GTE => ord.map(|o| o.is_ge()).unwrap_or(false),
        ComparisionOperator::LT  => ord.map(|o| o.is_lt()).unwrap_or(false),
        ComparisionOperator::LTE => ord.map(|o| o.is_le()).unwrap_or(false),
    }
}

pub fn coerce_literal(val: &ComparisionValue, target: &DataType) -> Option<Data> {
    match (val, target) {
        (ComparisionValue::I32(v), DataType::Int32)   => Some(Data::Int32(*v)),
        (ComparisionValue::I64(v), DataType::Int64)   => Some(Data::Int64(*v)),
        (ComparisionValue::F32(v), DataType::Float32) => Some(Data::Float32(*v)),
        (ComparisionValue::F64(v), DataType::Float64) => Some(Data::Float64(*v)),
        (ComparisionValue::String(v), DataType::String) => Some(Data::String(v.clone())),
        // Cross-type numeric widening
        (ComparisionValue::I32(v), DataType::Int64)   => Some(Data::Int64(*v as i64)),
        (ComparisionValue::I64(v), DataType::Int32)   => Some(Data::Int32(*v as i32)),
        (ComparisionValue::I32(v), DataType::Float64) => Some(Data::Float64(*v as f64)),
        (ComparisionValue::I64(v), DataType::Float64) => Some(Data::Float64(*v as f64)),
        (ComparisionValue::F32(v), DataType::Float64) => Some(Data::Float64(*v as f64)),
        _ => None,
    }
}

pub fn apply_predicates(
    row: &[Data],
    schema: &[ColumnInfo],
    predicates: &[Predicate],
) -> Result<bool> {
    for pred in predicates {
        let left_idx = schema
            .iter()
            .position(|c| c.name == pred.column_name)
            .with_context(|| format!("Filter: column '{}' not found", pred.column_name))?;
        let left_val = &row[left_idx];

        let right_val: Data = match &pred.value {
            ComparisionValue::Column(col_name) => {
                let idx = schema
                    .iter()
                    .position(|c| c.name == *col_name)
                    .with_context(|| format!("Filter: rhs column '{}' not found", col_name))?;
                row[idx].clone()
            }
            literal => coerce_literal(literal, &schema[left_idx].data_type)
                .with_context(|| format!("Filter: cannot coerce value for '{}'", pred.column_name))?,
        };

        if !compare_data(left_val, &pred.operator, &right_val) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn apply_split_predicates(
    left: &[Data],
    right: &[Data],
    schema: &[ColumnInfo],
    predicates: &[Predicate],
) -> Result<bool> {
    for pred in predicates {
        let col_idx = schema
            .iter()
            .position(|c| c.name == pred.column_name)
            .with_context(|| format!("Column '{}' not found", pred.column_name))?;

        let val = if col_idx < left.len() {
            &left[col_idx]
        } else {
            &right[col_idx - left.len()]
        };

        let result = match &pred.value {
            ComparisionValue::Column(other_col) => {
                let other_idx = schema
                    .iter()
                    .position(|c| c.name == *other_col)
                    .with_context(|| format!("Column '{}' not found", other_col))?;

                let other_val = if other_idx < left.len() {
                    &left[other_idx]
                } else {
                    &right[other_idx - left.len()]
                };
                compare_data(val, &pred.operator, other_val)
            }
            ComparisionValue::I32(v) => compare_data(val, &pred.operator, &Data::Int32(*v)),
            ComparisionValue::I64(v) => compare_data(val, &pred.operator, &Data::Int64(*v)),
            ComparisionValue::F32(v) => compare_data(val, &pred.operator, &Data::Float32(*v)),
            ComparisionValue::F64(v) => compare_data(val, &pred.operator, &Data::Float64(*v)),
            ComparisionValue::String(v) => compare_data(val, &pred.operator, &Data::String(v.clone())),
        };

        if !result {
            return Ok(false);
        }
    }
    Ok(true)
}
