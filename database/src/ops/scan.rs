use anyhow::{Context, Result};
use common::{Data, DataType};
use common::query::{ComparisionValue, Predicate};
use db_config::DbContext;
use std::io::{BufRead, Read, Write};
use std::cmp;

use crate::disk::{ask_disk_line, read_blocks};
use crate::row::{decode_row, decode_row_projected, coerce_literal, compare_data};
use crate::schema::ColumnInfo;

// ── Inline predicate evaluation on raw bytes ──────────────────────────────────
// Evaluates a single predicate's LHS column directly from raw block bytes
// without building a full Vec<Data>, by seeking to the column's byte offset.
// Used to early-reject rows before any heap allocation.
fn eval_pred_raw(
    data: &[u8],
    col_offsets: &[(usize, DataType)], // byte_offset + type for each col in full schema
    schema: &[ColumnInfo],
    pred: &Predicate,
) -> Result<bool> {
    let col_idx = schema
        .iter()
        .position(|c| c.name == pred.column_name)
        .with_context(|| format!("Predicate column '{}' not found", pred.column_name))?;

    let (byte_off, ref dt) = col_offsets[col_idx];

    // Decode only this one column value
    let left_val = match dt {
        DataType::Int32 => {
            let b: [u8; 4] = data[byte_off..byte_off + 4].try_into()?;
            Data::Int32(i32::from_le_bytes(b))
        }
        DataType::Int64 => {
            let b: [u8; 8] = data[byte_off..byte_off + 8].try_into()?;
            Data::Int64(i64::from_le_bytes(b))
        }
        DataType::Float32 => {
            let b: [u8; 4] = data[byte_off..byte_off + 4].try_into()?;
            Data::Float32(f32::from_le_bytes(b))
        }
        DataType::Float64 => {
            let b: [u8; 8] = data[byte_off..byte_off + 8].try_into()?;
            Data::Float64(f64::from_le_bytes(b))
        }
        DataType::String => {
            let end = data[byte_off..]
                .iter()
                .position(|&b| b == 0)
                .context("Null terminator not found while evaluating predicate inline")?;
            Data::String(std::str::from_utf8(&data[byte_off..byte_off + end])?.to_string())
        }
    };

    let right_val = match &pred.value {
        ComparisionValue::Column(_) => {
            // Column-vs-column predicate can't be resolved inline without full decode
            return Ok(true); // conservative: don't filter, let the Filter op handle it
        }
        literal => coerce_literal(literal, dt)
            .with_context(|| format!("Inline pred: cannot coerce value for '{}'", pred.column_name))?,
    };

    Ok(compare_data(&left_val, &pred.operator, &right_val))
}

/// Compute per-column byte offsets for a fixed-width block (all offsets relative to row start).
/// For variable-length (String) columns this is not possible statically, so we return None.
/// Only used for skipping entire rows by checking leading fixed-width filter columns first.
fn make_col_offsets_if_possible(
    schema: &[ColumnInfo],
) -> Option<Vec<(usize, DataType)>> {
    // Only usable if ALL columns before any String are fixed-width.
    // We compute offsets assuming rows start at byte 0 — actual row_start offsets are tracked
    // dynamically, so this table is just relative to the start of the first column.
    let mut offsets = Vec::with_capacity(schema.len());
    let mut off = 0usize;
    for col in schema {
        offsets.push((off, col.data_type.clone()));
        match col.data_type {
            DataType::Int32 | DataType::Float32 => off += 4,
            DataType::Int64 | DataType::Float64 => off += 8,
            DataType::String => return None, // variable-width; can't pre-compute offsets
        }
    }
    Some(offsets)
}

// ── Main scan with integrated projection + filter ────────────────────────────

pub fn execute_scan<W, R>(
    table_id: &str,
    ctx: &DbContext,
    disk_out: &mut W,
    disk_buf: &mut R,
    block_size: usize,
    memory_limit_mb: u64,
    on_row: &mut dyn FnMut(&[Data]) -> Result<()>,
) -> Result<Vec<ColumnInfo>>
where
    W: Write,
    R: Read + BufRead,
{
    execute_scan_with_opts(table_id, ctx, disk_out, disk_buf, block_size, memory_limit_mb, None, None, on_row)
}

/// Scan with projection + inline filter. Called by the filter/project optimizers.
/// `keep_indices`  — If Some, only decode these column indices (sorted ascending). All others are byte-skipped.
/// `inline_preds`  — If Some, evaluate these predicates at block-decode time and skip non-matching rows entirely.
pub fn execute_scan_with_opts<W, R>(
    table_id: &str,
    ctx: &DbContext,
    disk_out: &mut W,
    disk_buf: &mut R,
    block_size: usize,
    memory_limit_mb: u64,
    keep_indices: Option<&[usize]>,        // for projection pushdown
    inline_preds: Option<&[Predicate]>,    // for filter pushdown
    on_row: &mut dyn FnMut(&[Data]) -> Result<()>,
) -> Result<Vec<ColumnInfo>>
where
    W: Write,
    R: Read + BufRead,
{
    let table_spec = ctx
        .get_table_specs()
        .iter()
        .find(|t| t.file_id == table_id)
        .with_context(|| format!("Table '{}' not found", table_id))?;

    let schema: Vec<ColumnInfo> = table_spec
        .column_specs
        .iter()
        .map(|c| ColumnInfo { name: c.column_name.clone(), data_type: c.data_type.clone() })
        .collect();

    let start: u64 = ask_disk_line(disk_out, disk_buf, &format!("get file start-block {}\n", table_id))?.parse()?;
    let num:   u64 = ask_disk_line(disk_out, disk_buf, &format!("get file num-blocks {}\n",  table_id))?.parse()?;

    let chunk_blocks = std::cmp::min(
        512,
        std::cmp::max(1, (memory_limit_mb as usize * 1024 * 1024 * 5 / 100) / block_size)
    );

    eprintln!("[scan] '{}' start={} total_blocks={} chunk_size_blocks={}", table_id, start, num, chunk_blocks);

    let use_projection = keep_indices.is_some();
    let use_inline_filter = inline_preds.map(|p| !p.is_empty()).unwrap_or(false);

    // Pre-compute the output schema based on which columns we're keeping
    let out_schema: Vec<ColumnInfo> = if let Some(ki) = keep_indices {
        ki.iter().map(|&i| schema[i].clone()).collect()
    } else {
        schema.clone()
    };

    // ── Zone Map (RangeStat) Pre-Pruning ─────────────────────────────────────
    if let Some(preds) = inline_preds {
        let mut drop_entire_table = false;
        for pred in preds {
            if let Some(col_idx) = schema.iter().position(|c| c.name == pred.column_name) {
                let col_dt = &schema[col_idx].data_type;
                if let Some(col_spec) = table_spec.column_specs.iter().find(|c| c.column_name == pred.column_name) {
                    if let Some(stats) = &col_spec.stats {
                        for stat in stats {
                            if let db_config::statistics::ColumnStat::RangeStat(range) = stat {
                                let right_val = match &pred.value {
                                    common::query::ComparisionValue::Column(_) => continue,
                                    lit => match crate::row::coerce_literal(lit, col_dt) {
                                        Some(v) => v,
                                        None => continue,
                                    }
                                };
                                match pred.operator {
                                    common::query::ComparisionOperator::GT => {
                                        if !crate::row::compare_data(&range.upper_bound, &common::query::ComparisionOperator::GT, &right_val) {
                                            drop_entire_table = true;
                                        }
                                    }
                                    common::query::ComparisionOperator::GTE => {
                                        if !crate::row::compare_data(&range.upper_bound, &common::query::ComparisionOperator::GTE, &right_val) {
                                            drop_entire_table = true;
                                        }
                                    }
                                    common::query::ComparisionOperator::LT => {
                                        if !crate::row::compare_data(&range.lower_bound, &common::query::ComparisionOperator::LT, &right_val) {
                                            drop_entire_table = true;
                                        }
                                    }
                                    common::query::ComparisionOperator::LTE => {
                                        if !crate::row::compare_data(&range.lower_bound, &common::query::ComparisionOperator::LTE, &right_val) {
                                            drop_entire_table = true;
                                        }
                                    }
                                    common::query::ComparisionOperator::EQ => {
                                        if !crate::row::compare_data(&right_val, &common::query::ComparisionOperator::GTE, &range.lower_bound) ||
                                           !crate::row::compare_data(&right_val, &common::query::ComparisionOperator::LTE, &range.upper_bound) {
                                            drop_entire_table = true;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        if drop_entire_table {
            eprintln!("[scan] Zone Map (RangeStat) completely pruned table {}", table_id);
            return Ok(out_schema); 
        }
    }

    // ── Global Bloom Filter Setup ────────────────────────────────────────────
    let mut use_blooms = Vec::new();
    crate::ops::hash_join::GLOBAL_BLOOM.with(|g| {
        for (col, bf) in g.borrow().iter() {
            if let Some(idx) = out_schema.iter().position(|c| c.name == *col) {
                use_blooms.push((idx, bf.clone()));
            }
        }
    });

    let mut remaining = num as usize;
    let mut current_block_id = start;

    while remaining > 0 {
        let blocks_to_read = cmp::min(remaining, chunk_blocks);
        let buffer = read_blocks(disk_out, disk_buf, current_block_id, blocks_to_read, block_size)?;

        for i in 0..blocks_to_read {
            let block_raw = &buffer[i * block_size..(i + 1) * block_size];
            let row_count = u16::from_le_bytes([block_raw[block_size - 2], block_raw[block_size - 1]]) as usize;
            let mut byte_offset = 0;

            for _ in 0..row_count {
                if use_inline_filter && !use_projection {
                    // Fast path: check predicates on fully-decoded row and skip early
                    let (row, new_offset) = decode_row(block_raw, byte_offset, &schema)?;
                    byte_offset = new_offset;

                    let mut pass = true;
                    for pred in inline_preds.unwrap() {
                        let col_idx = schema.iter().position(|c| c.name == pred.column_name)
                            .with_context(|| format!("Inline filter: col '{}' not found", pred.column_name))?;
                        let left = &row[col_idx];
                        let right = match &pred.value {
                            ComparisionValue::Column(other) => {
                                let oi = schema.iter().position(|c| c.name == *other)
                                    .with_context(|| format!("Inline filter: col '{}' not found", other))?;
                                row[oi].clone()
                            }
                            lit => coerce_literal(lit, &schema[col_idx].data_type)
                                .with_context(|| format!("Inline filter: coerce failed for '{}'", pred.column_name))?,
                        };
                        if !compare_data(left, &pred.operator, &right) {
                            pass = false;
                            break;
                        }
                    }
                    if pass {
                        let mut bloom_pass = true;
                        for (b_idx, b_filter) in &use_blooms {
                            if !b_filter.might_contain(&row[*b_idx]) {
                                bloom_pass = false;
                                break;
                            }
                        }
                        if !bloom_pass { continue; }
                        on_row(&row)?;
                    }
                } else if use_projection {
                    // Projection path: decode only needed columns, byte-skip all others
                    let ki = keep_indices.unwrap();
                    let (row, new_offset) = decode_row_projected(block_raw, byte_offset, &schema, ki)?;
                    byte_offset = new_offset;

                    // If also inlining predicates post-projection, apply them against out_schema
                    if use_inline_filter {
                        let mut pass = true;
                        for pred in inline_preds.unwrap() {
                            let col_idx = out_schema.iter().position(|c| c.name == pred.column_name);
                            if let Some(col_idx) = col_idx {
                                let left = &row[col_idx];
                                let right = match &pred.value {
                                    ComparisionValue::Column(other) => {
                                        let oi = out_schema.iter().position(|c| c.name == *other)
                                            .with_context(|| format!("Inline filter: col '{}' not found", other))?;
                                        row[oi].clone()
                                    }
                                    lit => coerce_literal(lit, &out_schema[col_idx].data_type)
                                        .with_context(|| format!("Inline filter: coerce failed"))?,
                                };
                                if !compare_data(left, &pred.operator, &right) {
                                    pass = false;
                                    break;
                                }
                            }
                            // Column-vs-column cross-table predicates: leave to outer Filter
                        }
                        if pass {
                            let mut bloom_pass = true;
                            for (b_idx, b_filter) in &use_blooms {
                                if !b_filter.might_contain(&row[*b_idx]) {
                                    bloom_pass = false;
                                    break;
                                }
                            }
                            if !bloom_pass { continue; }
                            on_row(&row)?;
                        }
                    } else {
                        let mut bloom_pass = true;
                        for (b_idx, b_filter) in &use_blooms {
                            if !b_filter.might_contain(&row[*b_idx]) {
                                bloom_pass = false;
                                break;
                            }
                        }
                        if !bloom_pass { continue; }
                        on_row(&row)?;
                    }
                } else {
                    // Default: full decode, no filter
                    let (row, new_offset) = decode_row(block_raw, byte_offset, &schema)?;
                    byte_offset = new_offset;
                    let mut bloom_pass = true;
                    for (b_idx, b_filter) in &use_blooms {
                        if !b_filter.might_contain(&row[*b_idx]) {
                            bloom_pass = false;
                            break;
                        }
                    }
                    if !bloom_pass { continue; }
                    on_row(&row)?;
                }
            }
        }

        current_block_id += blocks_to_read as u64;
        remaining -= blocks_to_read;
    }

    Ok(out_schema)
}
