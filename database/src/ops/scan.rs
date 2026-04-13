use anyhow::{Context, Result};
use common::Data;
use db_config::DbContext;
use std::io::{BufRead, Read, Write};
use std::cmp;

use crate::disk::{ask_disk_line, read_blocks};
use crate::row::decode_row;
use crate::schema::ColumnInfo;

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
        256,
        std::cmp::max(1, (memory_limit_mb as usize * 1024 * 1024 * 5 / 100) / block_size)
    );

    eprintln!("[scan] '{}' start={} total_blocks={} chunk_size_blocks={}", table_id, start, num, chunk_blocks);

    let mut remaining = num as usize;
    let mut current_block_id = start;

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
                on_row(&row)?;
            }
        }

        current_block_id += blocks_to_read as u64;
        remaining -= blocks_to_read;
    }

    Ok(schema)
}
