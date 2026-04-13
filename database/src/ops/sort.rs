use anyhow::{Context, Result};
use common::query::SortData;
use common::Data;
use db_config::DbContext;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufRead, Read, Write};

use crate::disk::{get_anon_start_block, read_blocks, write_blocks};
use crate::ops::execute_op;
use crate::schema::ColumnInfo;
use crate::row::encode_row;


#[derive(Clone)]
struct Run {
    start_block: u64,
    num_blocks: usize,
}

struct RunStreamer {
    run: Run,
    current_block_idx: usize,
    cached_rows: Vec<Vec<Data>>, // Popped from the back, so rows are reversed!
    schema: Vec<ColumnInfo>,
}

impl RunStreamer {
    fn new(run: Run, schema: Vec<ColumnInfo>) -> Self {
        RunStreamer {
            run,
            current_block_idx: 0,
            cached_rows: Vec::new(),
            schema,
        }
    }

    fn next<W: Write, R: Read + BufRead>(
        &mut self,
        disk_out: &mut W,
        disk_raw: &mut R,
        block_size: usize,
    ) -> Result<Option<Vec<Data>>> {
        if let Some(row) = self.cached_rows.pop() {
            return Ok(Some(row));
        }

        if self.current_block_idx >= self.run.num_blocks { // End of run
            return Ok(None);
        }

        // Read up to 16 blocks at once to reduce simulated seek penalties
        let fetch = std::cmp::min(16, self.run.num_blocks - self.current_block_idx);
        let block_data = read_blocks(
            disk_out,
            disk_raw,
            self.run.start_block + self.current_block_idx as u64,
            fetch,
            block_size,
        )?;
        self.current_block_idx += fetch;

        // Decode blocks in REVERSE order into cached_rows.
        // unpack_block reverses rows for tail-pop, so processing blocks
        // in reverse ensures pop() yields rows in correct forward order.
        for bi in (0..fetch).rev() {
            let start = bi * block_size;
            let rows = unpack_block(&block_data[start..start + block_size], block_size, &self.schema)?;
            self.cached_rows.extend(rows);
        }

        Ok(self.cached_rows.pop())
    }
}

#[derive(Clone)]
struct HeapItem {
    row: Vec<Data>,
    run_idx: usize,
    sort_keys: Vec<(usize, bool)>,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-Heap standard reverse wrapper
        for (idx, ascending) in &self.sort_keys {
            let ord = self.row[*idx].partial_cmp(&other.row[*idx]).unwrap_or(Ordering::Equal);
            let ord = if *ascending { ord } else { ord.reverse() };
            if ord != Ordering::Equal {
                return ord.reverse(); // Reverse outer to force Smallest at Top
            }
        }
        Ordering::Equal
    }
}

fn pack_blocks(chunk: &[Vec<Data>], block_size: usize) -> Result<Vec<u8>> {
    let mut all_bytes = Vec::new();
    let mut current_block = Vec::with_capacity(block_size);
    let mut row_count = 0u16;
    let mut row_bytes = Vec::new();

    for row in chunk {
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
    Ok(all_bytes)
}

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
    rows.reverse(); // Enable rapid tail-pop
    Ok(rows)
}

pub fn execute_sort<W, R>(
    sort: &SortData,
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
    let schema = crate::schema::get_schema(&sort.underlying, ctx)?;
    let sort_keys: Vec<(usize, bool)> = sort
        .sort_specs
        .iter()
        .map(|spec| {
            let idx = schema
                .iter()
                .position(|c| c.name == spec.column_name)
                .with_context(|| format!("Sort: column '{}' not found", spec.column_name))?;
            Ok((idx, spec.ascending))
        })
        .collect::<Result<_>>()?;

    let mut runs = Vec::new();
    let mut current_chunk: Vec<Vec<Data>> = Vec::new();
    let _anon_block_id = get_anon_start_block(disk_out, disk_buf)?;

    // Stream the data
    crate::disk::init_anon_block_allocator(disk_out, disk_buf)?;

    let chunk_limit_bytes = (memory_limit_mb as usize * 1024 * 1024 * 40) / 100;
    let mut current_chunk_bytes = 0;

    execute_op(
        &sort.underlying,
        ctx,
        disk_out,
        disk_buf,
        block_size,
        memory_limit_mb,
        &mut |row| {
            let row_len = crate::row::encode_row_len(row);
            current_chunk_bytes += row_len;
            current_chunk.push(row.to_vec());

            // If chunk reaches byte limit, sort it in memory and flush to scratch disk
            if current_chunk_bytes >= chunk_limit_bytes {
                // Sort current chunk
                current_chunk.sort_by(|a, b| {
                    for (idx, ascending) in &sort_keys {
                        let ord = a[*idx]
                            .partial_cmp(&b[*idx])
                            .unwrap_or(std::cmp::Ordering::Equal);
                        let ord = if *ascending { ord } else { ord.reverse() };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });

                let packed = pack_blocks(&current_chunk, block_size).unwrap();
                let num_blocks = packed.len() / block_size;
                
                let run_start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);
                
                // We must use a secondary writer to avoid upsetting `disk_out` borrow checker semantics during `execute_op`
                let mut chunk_out = crate::io_setup::setup_disk_io().1;
                write_blocks(&mut chunk_out, run_start, num_blocks, &packed).unwrap();

                runs.push(Run {
                    start_block: run_start,
                    num_blocks,
                });

                current_chunk.clear();
                current_chunk_bytes = 0;
            }
            Ok(())
        },
    )?;

    // If all data fit in memory (no spills to disk), sort and emit directly!
    if runs.is_empty() {
        if !current_chunk.is_empty() {
            current_chunk.sort_by(|a, b| {
                for (idx, ascending) in &sort_keys {
                    let ord = a[*idx]
                        .partial_cmp(&b[*idx])
                        .unwrap_or(std::cmp::Ordering::Equal);
                    let ord = if *ascending { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal {
                        return ord;
                    }
                }
                std::cmp::Ordering::Equal
            });
            eprintln!("[sort] in-memory: {} rows, no disk spill needed", current_chunk.len());
            for row in &current_chunk {
                on_row(row)?;
            }
        }
        return Ok(schema);
    }

    // Flush remaining rows to disk for the merge phase
    if !current_chunk.is_empty() {
        current_chunk.sort_by(|a, b| {
            for (idx, ascending) in &sort_keys {
                let ord = a[*idx]
                    .partial_cmp(&b[*idx])
                    .unwrap_or(std::cmp::Ordering::Equal);
                let ord = if *ascending { ord } else { ord.reverse() };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });

        let packed = pack_blocks(&current_chunk, block_size).unwrap();
        let num_blocks = packed.len() / block_size;
        
        let run_start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);
        write_blocks(disk_out, run_start, num_blocks, &packed).unwrap();

        runs.push(Run {
            start_block: run_start,
            num_blocks,
        });
        current_chunk.clear();
    }

    eprintln!("[sort] merging {} spilled runs", runs.len());

    let mut heap = BinaryHeap::new();
    let mut streamers: Vec<RunStreamer> = runs.into_iter().map(|run| RunStreamer::new(run, schema.clone())).collect();

    // Populate Heap with root headers
    for (i, streamer) in streamers.iter_mut().enumerate() {
        if let Some(row) = streamer.next(disk_out, disk_buf, block_size)? {
            heap.push(HeapItem {
                row,
                run_idx: i,
                sort_keys: sort_keys.clone(),
            });
        }
    }

    // N-Way Stream Merge Loop
    while let Some(min_item) = heap.pop() {
        on_row(&min_item.row)?;

        if let Some(next_row) =
            streamers[min_item.run_idx].next(disk_out, disk_buf, block_size)?
        {
            heap.push(HeapItem {
                row: next_row,
                run_idx: min_item.run_idx,
                sort_keys: sort_keys.clone(),
            });
        }
    }

    Ok(schema)
}
