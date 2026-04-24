use anyhow::Result;
use common::query::CrossData;
use common::Data;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Read, Write};

use crate::ops::execute_op;
use crate::schema::{get_schema, ColumnInfo};
use crate::disk::{init_anon_block_allocator, read_blocks, write_blocks};
use crate::io_setup::setup_disk_io;



// ── Binary row encoding ───────────────────────────────────────────────────────

use crate::row::encode_row;

/// Pack rows into blocks securely using standard binary serialization
fn pack_rows_into_blocks(rows: &[Vec<Data>], block_size: usize) -> Vec<u8> {
    let mut all_bytes = Vec::new();
    let mut current_block = Vec::with_capacity(block_size);
    let mut row_count = 0u16;
    let mut row_bytes = Vec::new();

    for row in rows {
        row_bytes.clear();
        encode_row(row, &mut row_bytes);

        if current_block.len() + row_bytes.len() > block_size - 2 {
            current_block.resize(block_size - 2, 0);
            current_block.extend_from_slice(&row_count.to_le_bytes());
            all_bytes.extend_from_slice(&current_block);

            current_block.clear();
            row_count = 0;
        }

        current_block.extend_from_slice(&row_bytes);
        row_count += 1;
    }

    if !current_block.is_empty() {
        current_block.resize(block_size - 2, 0);
        current_block.extend_from_slice(&row_count.to_le_bytes());
        all_bytes.extend_from_slice(&current_block);
    }

    all_bytes
}

/// Decode all rows from one block
fn unpack_block(
    block_data: &[u8],
    block_size: usize,
    schema: &[ColumnInfo],
) -> Result<Vec<Vec<Data>>> {
    if block_data.len() < 2 {
        return Ok(Vec::new());
    }

    let row_count = u16::from_le_bytes([block_data[block_size - 2], block_data[block_size - 1]]) as usize;
    let mut rows = Vec::with_capacity(row_count);
    let mut offset = 0;

    for _ in 0..row_count {
        let (row, new_offset) = crate::row::decode_row(block_data, offset, schema)?;
        rows.push(row);
        offset = new_offset;
    }

    Ok(rows)
}

// ── Inner loop: one sequential pass over right scratch blocks ─────────────────

fn emit_left_chunk_against_right<W, R>(
    left_chunk: &[Vec<Data>],
    right_start_block: u64,
    total_right_blocks: usize,
    block_size: usize,
    right_schema: &[ColumnInfo],
    combined_schema: &[ColumnInfo],
    predicates: Option<&[common::query::Predicate]>,
    disk_out: &mut W,
    disk_buf: &mut R,
    memory_limit_mb: u64,
    on_row: &mut dyn FnMut(&[Data]) -> Result<()>,
) -> Result<()>
where
    W: Write,
    R: Read + BufRead,
{
    let mut block_idx = 0usize;
    let right_buffer_blocks = std::cmp::min(256, std::cmp::max(1, (memory_limit_mb as usize * 1024 * 1024 * 10 / 100) / block_size));

    let mut combined = Vec::with_capacity(combined_schema.len());

    while block_idx < total_right_blocks {
        let fetch = std::cmp::min(right_buffer_blocks, total_right_blocks - block_idx);

        let buffer = read_blocks(
            disk_out,
            disk_buf,
            right_start_block + block_idx as u64,
            fetch,
            block_size,
        )?;

        for bi in 0..fetch {
            let start = bi * block_size;
            let block_data = &buffer[start..start + block_size];
            let right_rows = unpack_block(block_data, block_size, right_schema)?;

            for left_row in left_chunk {
                for right_row in &right_rows {
                    if let Some(preds) = predicates {
                        if !crate::row::apply_split_predicates(left_row, right_row, combined_schema, preds)? {
                            continue;
                        }
                    }

                    combined.clear();
                    combined.extend_from_slice(left_row);
                    combined.extend_from_slice(right_row);
                    on_row(&combined)?;
                }
            }
        }

        block_idx += fetch;
    }

    Ok(())
}

// ── Main cross join ───────────────────────────────────────────────────────────

pub fn execute_cross<W, R>(
    cross: &CrossData,
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
    execute_cross_with_filter(cross, ctx, disk_out, disk_buf, block_size, memory_limit_mb, None, on_row)
}

pub fn execute_cross_with_filter<W, R>(
    cross: &CrossData,
    ctx: &DbContext,
    disk_out: &mut W,
    disk_buf: &mut R,
    block_size: usize,
    memory_limit_mb: u64,
    predicates: Option<&[common::query::Predicate]>,
    on_row: &mut dyn FnMut(&[Data]) -> Result<()>,
) -> Result<Vec<ColumnInfo>>
where
    W: Write,
    R: Read + BufRead,
{
    let left_schema  = get_schema(&cross.left,  ctx)?;
    let right_schema = get_schema(&cross.right, ctx)?;

    // ── Phase 1: Stream RIGHT → scratch blocks ────────────────────────────────

    let mut right_start_block = 0;
    let mut total_right_blocks = 0;
    let mut right_row_buf: Vec<Vec<Data>> = Vec::new();
    let right_chunk_bytes_limit = crate::ops::operator_budget_bytes(memory_limit_mb);
    let mut current_right_bytes = 0;

    // Bootstrap allocator safely avoiding borrow interference
    init_anon_block_allocator(disk_out, disk_buf)?;

    {
        let (_, mut inner_out) = setup_disk_io();

        execute_op(
            &cross.right,
            ctx,
            disk_out,
            disk_buf,
            block_size,
            memory_limit_mb,
            &mut |row| {
                let row_len = crate::row::encode_row_len(row);
                current_right_bytes += row_len;
                right_row_buf.push(row.to_vec());

                // Flush to scratch once buffer is full
                if current_right_bytes >= right_chunk_bytes_limit {
                    let packed = pack_rows_into_blocks(&right_row_buf, block_size);
                    let num_blocks = packed.len() / block_size;
                    
                    let allocated_start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);
                    if right_start_block == 0 {
                        right_start_block = allocated_start;
                    }

                    write_blocks(&mut inner_out, allocated_start, num_blocks, &packed)?;
                    total_right_blocks += num_blocks;
                    right_row_buf.clear();
                    current_right_bytes = 0;
                }
                Ok(())
            },
        )?;
    }

    if !right_row_buf.is_empty() {
        if total_right_blocks > 0 {
            let packed = pack_rows_into_blocks(&right_row_buf, block_size);
            let num_blocks = packed.len() / block_size;
            
            let allocated_start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);
            if right_start_block == 0 {
                right_start_block = allocated_start;
            }

            write_blocks(disk_out, allocated_start, num_blocks, &packed)?;
            total_right_blocks += num_blocks;
            right_row_buf.clear();
        } else {
            eprintln!("[cross] fast path: right side fits entirely in memory ({} rows)", right_row_buf.len());
        }
    }

    eprintln!("[cross] right materialised: {} blocks", total_right_blocks);

    let mut combined_schema = left_schema.clone();
    combined_schema.extend(right_schema.clone());

    if total_right_blocks == 0 && right_row_buf.is_empty() {
        return Ok(combined_schema);
    }

    // ── Phase 2: Stream LEFT → join against right scratch ─────────────────────

    if total_right_blocks == 0 {
        // Fast path: In-memory right side
        execute_op(
            &cross.left,
            ctx,
            disk_out,
            disk_buf,
            block_size,
            memory_limit_mb,
            &mut |left_row| {
                let mut combined = Vec::with_capacity(combined_schema.len());
                for right_row in &right_row_buf {
                    if let Some(preds) = predicates {
                        if !crate::row::apply_split_predicates(left_row, right_row, &combined_schema, preds)? {
                            continue;
                        }
                    }
                    combined.clear();
                    combined.extend_from_slice(left_row);
                    combined.extend_from_slice(right_row);
                    on_row(&combined)?;
                }
                Ok(())
            },
        )?;
        return Ok(combined_schema);
    }

    let (inner_in_raw, mut inner_out2) = setup_disk_io();
    let mut inner_buf = BufReader::new(inner_in_raw);
    let mut left_chunk: Vec<Vec<Data>> = Vec::new();
    let left_chunk_bytes_limit = crate::ops::operator_budget_bytes(memory_limit_mb);
    let mut current_left_bytes = 0;

    execute_op(
        &cross.left,
        ctx,
        disk_out,
        disk_buf,
        block_size,
        memory_limit_mb,
        &mut |left_row| {
            let row_len = crate::row::encode_row_len(left_row);
            current_left_bytes += row_len;
            left_chunk.push(left_row.to_vec());

            if current_left_bytes >= left_chunk_bytes_limit {
                emit_left_chunk_against_right(
                    &left_chunk,
                    right_start_block,
                    total_right_blocks,
                    block_size,
                    &right_schema,
                    &combined_schema,
                    predicates,
                    &mut inner_out2,
                    &mut inner_buf,
                    memory_limit_mb,
                    on_row,
                )?;
                left_chunk.clear();
                current_left_bytes = 0;
            }
            Ok(())
        },
    )?;

    // Flush last partial left chunk
    if !left_chunk.is_empty() {
        emit_left_chunk_against_right(
            &left_chunk,
            right_start_block,
            total_right_blocks,
            block_size,
            &right_schema,
            &combined_schema,
            predicates,
            &mut inner_out2,
            &mut inner_buf,
            memory_limit_mb,
            on_row,
        )?;
    }

    Ok(combined_schema)
}