use anyhow::{Context, Result};
use common::Data;
use db_config::DbContext;
use std::io::{BufRead, Read, Write};
use std::cmp;
use std::collections::HashMap;
use std::sync::{Mutex, LazyLock};

use crate::disk::read_blocks_nocache;
use crate::schema::ColumnInfo;

/// #3: Per-table metadata cache. Stores (start_block, num_blocks) looked up via
/// IPC so that repeated scans of the same table within a session skip the
/// two synchronous `get file ...` round-trips entirely.
static TABLE_META_CACHE: LazyLock<Mutex<HashMap<String, (u64, u64)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn execute_scan<W, R>(
    scan_data: &common::query::ScanData,
    ctx: &DbContext,
    disk_out: &mut W,
    disk_buf: &mut R,
    block_size: usize,
    memory_limit_mb: u64,
    pushed_predicates: Option<&[common::query::Predicate]>,
    required_cols: Option<&[bool]>,
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

    crate::disk::flush_disk_write_buffer(disk_out)?;

    // #3: serve from metadata cache when available; otherwise ask disk and cache result
    let (start, num) = {
        let cached = TABLE_META_CACHE.lock().unwrap().get(table_id).copied();
        if let Some(pair) = cached {
            pair
        } else {
            disk_out.write_all(format!("get file start-block {}\n", table_id).as_bytes())?;
            disk_out.write_all(format!("get file num-blocks {}\n",  table_id).as_bytes())?;
            disk_out.flush()?;
            let mut start_line = String::new();
            disk_buf.read_line(&mut start_line)?;
            let s: u64 = start_line.trim().parse()?;
            let mut num_line = String::new();
            disk_buf.read_line(&mut num_line)?;
            let n: u64 = num_line.trim().parse()?;
            TABLE_META_CACHE.lock().unwrap().insert(table_id.to_string(), (s, n));
            (s, n)
        }
    };

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
                    if let db_config::statistics::ColumnStat::HistogramStat(hist) = stat {
                        if matches!(pred.operator, common::query::ComparisionOperator::EQ) {
                            let right_data = match crate::row::coerce_literal(&pred.value, &col_spec.data_type) {
                                Some(v) => v,
                                None => continue,
                            };
                            
                            let mut found_bucket = false;
                            for (range, _) in &hist.frequency_points {
                                if range.lower_bound <= right_data && range.upper_bound >= right_data {
                                    found_bucket = true;
                                    break;
                                }
                            }
                            
                            if !found_bucket {
                                eprintln!("[scan] hist-stat skip: entire table '{}' pruned on {}", table_id, pred.column_name);
                                return Ok(schema);
                            }
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
                let buffer = read_blocks_nocache(disk_out, disk_buf, mid_block_id, 1, block_size)?;
                
                let row_count = u16::from_le_bytes([buffer[block_size - 2], buffer[block_size - 1]]) as usize;
                if row_count == 0 {
                    low = mid + 1;
                    continue;
                }
                
                let (first_row, _) = crate::row::decode_row_partial(&buffer, 0, &schema, required_cols)?;
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

    let compiled_preds = compile_predicates(&schema, preds, &ordered_cols);

    while remaining > 0 {
        let blocks_to_read = cmp::min(remaining, chunk_blocks);
        
        let buffer = read_blocks_nocache(disk_out, disk_buf, current_block_id, blocks_to_read, block_size)?;

        for i in 0..blocks_to_read {
            let offset = i * block_size;
            let block_raw = &buffer[offset..offset + block_size];

            let row_count = u16::from_le_bytes([block_raw[block_size - 2], block_raw[block_size - 1]]) as usize;
            let mut byte_offset = 0;
            
            for _ in 0..row_count {
                let (row, new_offset) = crate::row::decode_row_partial(block_raw, byte_offset, &schema, required_cols)?;
                byte_offset = new_offset;
                
                let res = apply_compiled_predicates(&row, &compiled_preds);
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

struct CompiledPredicate {
    left_idx: usize,
    operator: common::query::ComparisionOperator,
    right_val: Option<Data>,
    right_idx: Option<usize>,
    ordered: bool,
}

fn compile_predicates(schema: &[ColumnInfo], preds: &[common::query::Predicate], ordered_cols: &[bool]) -> Vec<CompiledPredicate> {
    let mut compiled = Vec::with_capacity(preds.len());
    for (i, pred) in preds.iter().enumerate() {
        if let Some(left_idx) = schema.iter().position(|c| c.name == pred.column_name) {
            let mut right_val = None;
            let mut right_idx = None;
            
            match &pred.value {
                common::query::ComparisionValue::Column(c) => {
                    if let Some(idx) = schema.iter().position(|col| col.name == *c) {
                        right_idx = Some(idx);
                    } else {
                        continue; // Invalid right column
                    }
                }
                literal => {
                    if let Some(coerced) = crate::row::coerce_literal(literal, &schema[left_idx].data_type) {
                        right_val = Some(coerced);
                    } else {
                        continue; // Cannot coerce
                    }
                }
            }
            
            compiled.push(CompiledPredicate {
                left_idx,
                operator: match pred.operator {
                    common::query::ComparisionOperator::EQ => common::query::ComparisionOperator::EQ,
                    common::query::ComparisionOperator::NE => common::query::ComparisionOperator::NE,
                    common::query::ComparisionOperator::GT => common::query::ComparisionOperator::GT,
                    common::query::ComparisionOperator::GTE => common::query::ComparisionOperator::GTE,
                    common::query::ComparisionOperator::LT => common::query::ComparisionOperator::LT,
                    common::query::ComparisionOperator::LTE => common::query::ComparisionOperator::LTE,
                },
                right_val,
                right_idx,
                ordered: ordered_cols[i],
            });
        }
    }
    compiled
}

fn apply_compiled_predicates(row: &[Data], compiled: &[CompiledPredicate]) -> PredicateResult {
    for pred in compiled {
        let left = &row[pred.left_idx];
        
        let right_data;
        let right = if let Some(idx) = pred.right_idx {
            &row[idx]
        } else if let Some(val) = &pred.right_val {
            right_data = val.clone(); // Keep reference valid
            &right_data
        } else {
            return PredicateResult::Fail;
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
            if pred.ordered {
                if matches!(pred.operator, common::query::ComparisionOperator::EQ | common::query::ComparisionOperator::LT | common::query::ComparisionOperator::LTE) {
                    if ord == Some(std::cmp::Ordering::Greater) || (ord == Some(std::cmp::Ordering::Equal) && matches!(pred.operator, common::query::ComparisionOperator::LT)) {
                        return PredicateResult::FailAndAbort;
                    }
                }
            }
            return PredicateResult::Fail;
        }
    }
    PredicateResult::Pass
}
