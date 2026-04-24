use anyhow::{Context, Result};
use common::Data;
use db_config::DbContext;
use std::io::{BufRead, Read, Write};
use std::cmp;

use crate::disk::{ask_disk_line, read_blocks};
use crate::row::decode_row;
use crate::schema::ColumnInfo;

pub fn execute_scan<W, R>(
    scan_data: &common::query::ScanData,
    ctx: &DbContext,
    disk_out: &mut W,
    disk_buf: &mut R,
    block_size: usize,
    memory_limit_mb: u64,
    pushed_predicates: Option<&[common::query::Predicate]>,
    on_row: &mut dyn FnMut(&[Data]) -> Result<()>,
) -> Result<Vec<ColumnInfo>>
where
    W: Write,
    R: Read + BufRead,
{
    let table_id = &scan_data.table_id;
    let table_spec = ctx
        .get_table_specs()
        .iter()
        .find(|t| t.file_id == *table_id)
        .with_context(|| format!("Table '{}' not found", table_id))?;

    let schema: Vec<ColumnInfo> = table_spec
        .column_specs
        .iter()
        .map(|c| ColumnInfo { name: c.column_name.clone(), data_type: c.data_type.clone() })
        .collect();

    let start: u64 = ask_disk_line(disk_out, disk_buf, &format!("get file start-block {}\n", table_id))?.parse()?;
    let num:   u64 = ask_disk_line(disk_out, disk_buf, &format!("get file num-blocks {}\n",  table_id))?.parse()?;

    let preds = pushed_predicates.unwrap_or(&[]);

    // --- NEW-01: RangeStat Pruning ---
    for pred in preds {
        if let Some(col_spec) = table_spec.column_specs.iter().find(|c| c.column_name == pred.column_name) {
            if let Some(stats) = &col_spec.stats {
                for stat in stats {
                    if let db_config::statistics::ColumnStat::RangeStat(range) = stat {
                        let right_data = match crate::row::coerce_literal(&pred.value, &col_spec.data_type) {
                            Some(v) => v,
                            None => continue,
                        };
                        
                        let min_val = &range.lower_bound;
                        let max_val = &range.upper_bound;
                        
                        let can_match = match pred.operator {
                            common::query::ComparisionOperator::EQ => min_val <= &right_data && max_val >= &right_data,
                            common::query::ComparisionOperator::LT => min_val < &right_data,
                            common::query::ComparisionOperator::LTE => min_val <= &right_data,
                            common::query::ComparisionOperator::GT => max_val > &right_data,
                            common::query::ComparisionOperator::GTE => max_val >= &right_data,
                            _ => true,
                        };
                        
                        if !can_match {
                            eprintln!("[scan] range-stat skip: entire table '{}' pruned on {}", table_id, pred.column_name);
                            return Ok(schema);
                        }
                    }
                }
            }
        }
    }

    let chunk_blocks = std::cmp::min(
        512,
        std::cmp::max(1, (memory_limit_mb as usize * 1024 * 1024 * 5 / 100) / block_size)
    );

    let mut scan_start_block = start;
    let mut scan_blocks_remaining = num as usize;

    // Determine ordered columns for early aborts
    let mut ordered_cols = vec![false; preds.len()];
    for (i, pred) in preds.iter().enumerate() {
        if let Some(col_spec) = table_spec.column_specs.iter().find(|c| c.column_name == pred.column_name) {
            ordered_cols[i] = col_spec.stats.as_ref().map_or(false, |stats| {
                stats.iter().any(|s| matches!(s, db_config::statistics::ColumnStat::IsPhysicallyOrdered))
            });
        }
    }

    // --- NEW-01: Binary Search for GT/GTE on IsPhysicallyOrdered ---
    for (i, pred) in preds.iter().enumerate() {
        if ordered_cols[i] && matches!(pred.operator, common::query::ComparisionOperator::GT | common::query::ComparisionOperator::GTE | common::query::ComparisionOperator::EQ) {
            let col_idx = schema.iter().position(|c| c.name == pred.column_name).unwrap();
            
            let target_data = match crate::row::coerce_literal(&pred.value, &schema[col_idx].data_type) {
                Some(v) => v,
                None => continue,
            };
            
            let mut low = 0;
            let mut high = num;
            let mut best_start = 0;
            
            while low < high {
                let mid = low + (high - low) / 2;
                let mid_block_id = start + mid;
                let buffer = read_blocks(disk_out, disk_buf, mid_block_id, 1, block_size)?;
                
                let row_count = u16::from_le_bytes([buffer[block_size - 2], buffer[block_size - 1]]) as usize;
                if row_count == 0 {
                    low = mid + 1;
                    continue;
                }
                
                let (first_row, _) = decode_row(&buffer, 0, &schema)?;
                let cmp = first_row[col_idx].partial_cmp(&target_data);
                
                if cmp == Some(std::cmp::Ordering::Greater) || cmp == Some(std::cmp::Ordering::Equal) {
                    high = mid;
                } else {
                    best_start = mid;
                    low = mid + 1;
                }
            }
            
            scan_start_block = start + best_start;
            scan_blocks_remaining = (num - best_start) as usize;
            eprintln!("[scan] binary search on {} {:?} {:?}: skipping {} blocks", pred.column_name, pred.operator, target_data, best_start);
            break; // Apply binary search once
        }
    }

    eprintln!("[scan] '{}' start={} num={} chunk={} scan_start={} remaining={}", 
        table_id, start, num, chunk_blocks, scan_start_block, scan_blocks_remaining);

    let mut remaining = scan_blocks_remaining;
    let mut current_block_id = scan_start_block;

    while remaining > 0 {
        let blocks_to_read = cmp::min(remaining, chunk_blocks);
        
        let buffer = read_blocks(disk_out, disk_buf, current_block_id, blocks_to_read, block_size)?;

        for i in 0..blocks_to_read {
            let offset = i * block_size;
            let block_raw = &buffer[offset..offset + block_size];

            let row_count = u16::from_le_bytes([block_raw[block_size - 2], block_raw[block_size - 1]]) as usize;
            let mut byte_offset = 0;
            
            for _ in 0..row_count {
                let (row, new_offset) = decode_row(block_raw, byte_offset, &schema)?;
                byte_offset = new_offset;
                
                let res = apply_predicates(&row, &schema, preds, &ordered_cols);
                match res {
                    PredicateResult::Pass => { on_row(&row)?; },
                    PredicateResult::Fail => {},
                    PredicateResult::FailAndAbort => {
                        return Ok(schema);
                    }
                }
            }
        }

        current_block_id += blocks_to_read as u64;
        remaining -= blocks_to_read;
    }

    Ok(schema)
}

enum PredicateResult {
    Pass,
    Fail,
    FailAndAbort,
}

fn apply_predicates(row: &[Data], schema: &[ColumnInfo], preds: &[common::query::Predicate], ordered_cols: &[bool]) -> PredicateResult {
    for (i, pred) in preds.iter().enumerate() {
        if let Some(idx) = schema.iter().position(|c| c.name == pred.column_name) {
            let left = &row[idx];
            let right_data;
            let right = match &pred.value {
                common::query::ComparisionValue::Column(c) => {
                    if let Some(other_idx) = schema.iter().position(|col| col.name == *c) {
                        &row[other_idx]
                    } else {
                        continue;
                    }
                }
                literal => {
                    if let Some(coerced) = crate::row::coerce_literal(literal, &schema[idx].data_type) {
                        right_data = coerced;
                        &right_data
                    } else {
                        return PredicateResult::Fail;
                    }
                }
            };

            let ord = left.partial_cmp(right);
            let matches = match pred.operator {
                common::query::ComparisionOperator::EQ => left == right,
                common::query::ComparisionOperator::NE => left != right,
                common::query::ComparisionOperator::GT => ord == Some(std::cmp::Ordering::Greater),
                common::query::ComparisionOperator::GTE => ord == Some(std::cmp::Ordering::Greater) || left == right,
                common::query::ComparisionOperator::LT => ord == Some(std::cmp::Ordering::Less),
                common::query::ComparisionOperator::LTE => ord == Some(std::cmp::Ordering::Less) || left == right,
            };

            if !matches {
                if ordered_cols[i] {
                    if matches!(pred.operator, common::query::ComparisionOperator::EQ | common::query::ComparisionOperator::LT | common::query::ComparisionOperator::LTE) {
                        if ord == Some(std::cmp::Ordering::Greater) || (ord == Some(std::cmp::Ordering::Equal) && matches!(pred.operator, common::query::ComparisionOperator::LT)) {
                            return PredicateResult::FailAndAbort;
                        }
                    }
                }
                return PredicateResult::Fail;
            }
        }
    }
    PredicateResult::Pass
}
