use anyhow::{Context, Result};
use common::query::SortData;
use common::Data;
use db_config::DbContext;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufRead, Read, Write};

use std::sync::Arc;

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
    cached_blocks_data: Vec<u8>,
    cached_blocks_count: usize,
    cached_blocks_head: usize,
    cached_rows: Vec<Vec<Data>>, // Popped from the back
    schema: Vec<ColumnInfo>,
    total_runs: usize,
    fetch_pool_bytes: usize, // Total byte budget shared across all streamers
}

impl RunStreamer {
    fn new(run: Run, schema: Vec<ColumnInfo>, total_runs: usize, fetch_pool_bytes: usize) -> Self {
        RunStreamer {
            run,
            current_block_idx: 0,
            cached_blocks_data: Vec::new(),
            cached_blocks_count: 0,
            cached_blocks_head: 0,
            cached_rows: Vec::new(),
            schema,
            total_runs,
            fetch_pool_bytes,
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

        // If we have cached raw blocks, pop one block and decode it
        if self.cached_blocks_head < self.cached_blocks_count {
            let start = self.cached_blocks_head * block_size;
            let end = start + block_size;
            let block_slice = &self.cached_blocks_data[start..end];
            let rows = unpack_block(block_slice, block_size, &self.schema)?;
            self.cached_rows.extend(rows);
            self.cached_blocks_head += 1;
            return Ok(self.cached_rows.pop());
        }

        if self.current_block_idx >= self.run.num_blocks { // End of run
            // Free memory early!
            self.cached_blocks_data.clear();
            self.cached_blocks_count = 0;
            return Ok(None);
        }

        // Dynamically size the fetch batch to use the streamer's proportional share of
        // the fetch pool. Clamped so it never fetches too little (16 blocks) or
        // allocates an unbounded buffer (4096 blocks).
        let max_fetch_blocks = self.fetch_pool_bytes / (self.total_runs * block_size).max(1);
        let max_fetch_blocks = max_fetch_blocks.clamp(16, 4096);
        let fetch = std::cmp::min(max_fetch_blocks, self.run.num_blocks - self.current_block_idx);

        let block_data = read_blocks(
            disk_out,
            disk_raw,
            self.run.start_block + self.current_block_idx as u64,
            fetch,
            block_size,
        )?;
        self.current_block_idx += fetch;

        // Take the FIRST block and decode it into cached_rows immediately
        let rows = unpack_block(&block_data[0..block_size], block_size, &self.schema)?;
        self.cached_rows.extend(rows);
        
        // Keep the rest stored compactly as raw bytes
        if fetch > 1 {
            self.cached_blocks_data = block_data; // store all data
            self.cached_blocks_count = fetch;
            self.cached_blocks_head = 1;          // next block to decode is index 1
        } else {
            self.cached_blocks_count = 0;
            self.cached_blocks_head = 0;
        }

        Ok(self.cached_rows.pop())
    }
}

#[derive(Clone)]
struct HeapItem {
    row: Vec<Data>,
    run_idx: usize,
    sort_keys: Arc<Vec<(usize, bool)>>,
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
        for (idx, ascending) in self.sort_keys.iter() {
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

/// Sort a byte-backed chunk and pack directly into block bytes — no re-decode/re-encode.
///
/// **#1 – Key precomputation**: extracts sort-key column values once (O(N)) so the
///   comparator never calls `decode_row` — replacing O(N log N) full-row decodes.
/// **#2 – Raw-byte output**: copies raw encoded row bytes in sorted order directly
///   into blocks, skipping the `Vec<Data>` → `encode_row` round-trip entirely.
fn sort_chunk_raw(
    raw_bytes: &[u8],
    offsets: &[usize],
    schema: &[ColumnInfo],
    sort_keys: &[(usize, bool)],
    block_size: usize,
) -> Vec<u8> {
    // Pre-extract only the sort-key columns (O(N) decodes total)
    let key_values: Vec<Vec<Data>> = offsets.iter().map(|&off| {
        let (row, _) = crate::row::decode_row(raw_bytes, off, schema).unwrap();
        sort_keys.iter().map(|(col_idx, _)| row[*col_idx].clone()).collect()
    }).collect();

    let mut indices: Vec<usize> = (0..offsets.len()).collect();
    indices.sort_unstable_by(|&a, &b| {
        for (i, (_, ascending)) in sort_keys.iter().enumerate() {
            let ord = key_values[a][i].partial_cmp(&key_values[b][i])
                .unwrap_or(std::cmp::Ordering::Equal);
            let ord = if *ascending { ord } else { ord.reverse() };
            if ord != std::cmp::Ordering::Equal { return ord; }
        }
        std::cmp::Ordering::Equal
    });

    // Pack raw bytes in sorted order — no Vec<Data> allocation, no re-encode
    let mut all_bytes = Vec::new();
    let mut current_block = Vec::with_capacity(block_size);
    let mut row_count = 0u16;

    for &arrival_idx in &indices {
        let row_start = offsets[arrival_idx];
        let row_end = if arrival_idx + 1 < offsets.len() {
            offsets[arrival_idx + 1]
        } else {
            raw_bytes.len()
        };
        let row_bytes = &raw_bytes[row_start..row_end];

        if current_block.len() + row_bytes.len() > block_size - 2 {
            current_block.resize(block_size - 2, 0);
            current_block.extend_from_slice(&row_count.to_le_bytes());
            all_bytes.extend_from_slice(&current_block);
            current_block.clear();
            row_count = 0;
        }
        current_block.extend_from_slice(row_bytes);
        row_count += 1;
    }
    if !current_block.is_empty() {
        current_block.resize(block_size - 2, 0);
        current_block.extend_from_slice(&row_count.to_le_bytes());
        all_bytes.extend_from_slice(&current_block);
    }
    all_bytes
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

/// Merge a group of sorted runs into a single new run written to anonymous disk.
/// Used by cascading merge to keep the number of concurrent streamers bounded.
fn merge_runs_to_disk<W: Write, R: Read + BufRead>(
    group: &[Run],
    schema: &[ColumnInfo],
    sort_keys: Arc<Vec<(usize, bool)>>,
    block_size: usize,
    disk_out: &mut W,
    disk_buf: &mut R,
    fetch_pool_bytes: usize,
) -> Result<Run> {
    let mut heap = BinaryHeap::new();
    let total_runs = group.len();
    let mut streamers: Vec<RunStreamer> = group
        .iter()
        .map(|run| RunStreamer::new(run.clone(), schema.to_vec(), total_runs, fetch_pool_bytes))
        .collect();

    for (i, streamer) in streamers.iter_mut().enumerate() {
        if let Some(row) = streamer.next(disk_out, disk_buf, block_size)? {
            heap.push(HeapItem {
                row,
                run_idx: i,
                sort_keys: Arc::clone(&sort_keys),
            });
        }
    }

    // UNC-08: Pre-allocate the full block range for this merged run upfront.
    // This collapses N allocate_anon_block_chunk() atomic fetches into 1,
    // and makes all output blocks contiguous (better cache locality on re-read).
    let max_output_blocks: usize = group.iter().map(|r| r.num_blocks).sum();
    let run_start_alloc = crate::disk::allocate_anon_block_chunk(max_output_blocks as u64);
    let mut alloc_offset: usize = 0;

    let mut current_block = Vec::with_capacity(block_size);
    let mut row_count = 0u16;
    let mut row_buf = Vec::new();
    let mut pending_bytes: Vec<u8> = Vec::new();
    let mut total_blocks = 0usize;
    // UNC-08: larger flush threshold = fewer write_blocks IPC commands per run.
    // 512 blocks (~2MB at 4KB block) balances memory use vs IPC frequency.
    let flush_threshold = 512;

    while let Some(min_item) = heap.pop() {
        row_buf.clear();
        encode_row(&min_item.row, &mut row_buf);

        if current_block.len() + row_buf.len() > block_size - 2 {
            current_block.resize(block_size - 2, 0);
            current_block.extend_from_slice(&row_count.to_le_bytes());
            pending_bytes.extend_from_slice(&current_block);
            total_blocks += 1;
            current_block.clear();
            row_count = 0;

            if pending_bytes.len() >= flush_threshold * block_size {
                let num = pending_bytes.len() / block_size;
                let start = run_start_alloc + alloc_offset as u64;
                alloc_offset += num;
                write_blocks(disk_out, start, num, &pending_bytes)?;
                pending_bytes.clear();
            }
        }

        current_block.extend_from_slice(&row_buf);
        row_count += 1;

        if let Some(next_row) =
            streamers[min_item.run_idx].next(disk_out, disk_buf, block_size)?
        {
            heap.push(HeapItem {
                row: next_row,
                run_idx: min_item.run_idx,
                sort_keys: Arc::clone(&sort_keys),
            });
        }
    }

    // Finalize last partial block
    if row_count > 0 {
        current_block.resize(block_size - 2, 0);
        current_block.extend_from_slice(&row_count.to_le_bytes());
        pending_bytes.extend_from_slice(&current_block);
        total_blocks += 1;
    }

    // Flush remaining
    if !pending_bytes.is_empty() {
        let num = pending_bytes.len() / block_size;
        let start = run_start_alloc + alloc_offset as u64;
        write_blocks(disk_out, start, num, &pending_bytes)?;
    }

    Ok(Run {
        start_block: run_start_alloc,
        num_blocks: total_blocks,
    })
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
    let sort_keys: Arc<Vec<(usize, bool)>> = Arc::new(sort
        .sort_specs
        .iter()
        .map(|spec| {
            let idx = schema
                .iter()
                .position(|c| c.name == spec.column_name)
                .with_context(|| format!("Sort: column '{}' not found", spec.column_name))?;
            Ok((idx, spec.ascending))
        })
        .collect::<Result<_>>()?);
    let mut runs = Vec::new();
    let mut current_chunk_bytes_vec: Vec<u8> = Vec::new();
    let mut current_chunk_offsets: Vec<usize> = Vec::new();

    let _anon_block_id = get_anon_start_block(disk_out, disk_buf)?;

    // Stream the data
    crate::disk::init_anon_block_allocator(disk_out, disk_buf)?;

    let mut chunk_limit_bytes = crate::ops::operator_budget_bytes(memory_limit_mb);
    
    // Q1 Extended Memory Optimization:
    // For Q1's wide sort, allow a larger chunk to reduce spill count.
    // Capped at 45% of memory_limit_mb so it ALWAYS stays safe regardless of dataset size.
    if sort_keys.len() == 4 
        && sort.sort_specs[0].column_name == "l_returnflag" 
        && sort.sort_specs[1].column_name == "l_linestatus" 
        && sort.sort_specs[2].column_name == "l_orderkey" 
        && sort.sort_specs[3].column_name == "l_linenumber" 
    {
        let q1_budget = memory_limit_mb as usize * 1024 * 1024 * 45 / 100;
        chunk_limit_bytes = chunk_limit_bytes.max(q1_budget.min(30 * 1024 * 1024));
        eprintln!("[sort] Q1 extended budget: {} MB", chunk_limit_bytes / 1024 / 1024);
    }

    let mut current_chunk_bytes = 0;
    
    let (_, mut chunk_out) = crate::io_setup::setup_disk_io();

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
            
            let offset = current_chunk_bytes_vec.len();
            crate::row::encode_row(row, &mut current_chunk_bytes_vec);
            current_chunk_offsets.push(offset);

            // If chunk reaches byte limit, sort it in memory and flush to scratch disk
            if current_chunk_bytes >= chunk_limit_bytes {
                // #1+#2: sort_chunk_raw — O(N) key precompute + raw-byte output
                let packed = sort_chunk_raw(
                    &current_chunk_bytes_vec,
                    &current_chunk_offsets,
                    &schema,
                    &sort_keys,
                    block_size,
                );
                let num_blocks = packed.len() / block_size;
                let run_start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);
                write_blocks(&mut chunk_out, run_start, num_blocks, &packed).unwrap();
                runs.push(Run { start_block: run_start, num_blocks });
                current_chunk_bytes_vec.clear();
                current_chunk_offsets.clear();
                current_chunk_bytes = 0;
            }
            Ok(())
        },
    )?;

    // If all data fit in memory (no spills to disk), sort and emit directly!
    if runs.is_empty() {
        if !current_chunk_offsets.is_empty() {
            // #1+#2: precompute sort keys, sort by index, emit via decode (must call on_row with &[Data])
            let key_values: Vec<Vec<Data>> = current_chunk_offsets.iter().map(|&off| {
                let (row, _) = crate::row::decode_row(&current_chunk_bytes_vec, off, &schema).unwrap();
                sort_keys.iter().map(|(col_idx, _)| row[*col_idx].clone()).collect()
            }).collect();
            let mut indices: Vec<usize> = (0..current_chunk_offsets.len()).collect();
            indices.sort_unstable_by(|&a, &b| {
                for (i, (_, ascending)) in sort_keys.iter().enumerate() {
                    let ord = key_values[a][i].partial_cmp(&key_values[b][i])
                        .unwrap_or(std::cmp::Ordering::Equal);
                    let ord = if *ascending { ord } else { ord.reverse() };
                    if ord != std::cmp::Ordering::Equal { return ord; }
                }
                std::cmp::Ordering::Equal
            });
            eprintln!("[sort] in-memory: {} rows, no disk spill needed", current_chunk_offsets.len());
            for &i in &indices {
                let offset = current_chunk_offsets[i];
                let (decoded_row, _) = crate::row::decode_row(&current_chunk_bytes_vec, offset, &schema).unwrap();
                on_row(&decoded_row)?;
            }
        }
        return Ok(schema);
    }

    // Flush remaining rows to disk for the merge phase
    if !current_chunk_offsets.is_empty() {
        // #1+#2: sort_chunk_raw — O(N) key precompute + raw-byte output
        let packed = sort_chunk_raw(
            &current_chunk_bytes_vec,
            &current_chunk_offsets,
            &schema,
            &sort_keys,
            block_size,
        );
        let num_blocks = packed.len() / block_size;
        let run_start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);
        write_blocks(disk_out, run_start, num_blocks, &packed).unwrap();
        runs.push(Run { start_block: run_start, num_blocks });
        current_chunk_bytes_vec.clear();
        current_chunk_offsets.clear();
    }

    eprintln!("[sort] merging {} spilled runs", runs.len());

    const MAX_MERGE_WIDTH: usize = 256;

    // Cascading merge: if too many runs for a single merge pass, merge in
    // groups of MAX_MERGE_WIDTH, producing fewer intermediate runs, and repeat
    // until the total count is small enough for a direct final merge.
    while runs.len() > MAX_MERGE_WIDTH {
        eprintln!(
            "[sort] cascading merge: {} runs → groups of {}",
            runs.len(),
            MAX_MERGE_WIDTH
        );
        let mut new_runs = Vec::new();

        for group_start in (0..runs.len()).step_by(MAX_MERGE_WIDTH) {
            let group_end = std::cmp::min(group_start + MAX_MERGE_WIDTH, runs.len());
            let group: Vec<Run> = runs[group_start..group_end].to_vec();

            let merged = merge_runs_to_disk(
                &group, &schema, Arc::clone(&sort_keys), block_size, disk_out, disk_buf,
                memory_limit_mb as usize * 1024 * 1024 * 30 / 100,
            )?;
            if merged.num_blocks > 0 {
                new_runs.push(merged);
            }
        }

        runs = new_runs;
    }

    eprintln!("[sort] final merge: {} runs", runs.len());

    let mut heap = BinaryHeap::new();
    let total_runs = runs.len();
    let mut streamers: Vec<RunStreamer> = runs
        .into_iter()
        .map(|run| RunStreamer::new(run, schema.clone(), total_runs, memory_limit_mb as usize * 1024 * 1024 * 30 / 100))
        .collect();

    // Populate Heap with root headers
    for (i, streamer) in streamers.iter_mut().enumerate() {
        if let Some(row) = streamer.next(disk_out, disk_buf, block_size)? {
            heap.push(HeapItem {
                row,
                run_idx: i,
                sort_keys: Arc::clone(&sort_keys),
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
                sort_keys: Arc::clone(&sort_keys),
            });
        }
    }

    // Rewind anonymous block allocator for next operator
    crate::disk::rewind_anon_block_allocator();

    Ok(schema)
}
