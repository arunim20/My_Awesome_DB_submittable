use anyhow::{Context, Result};
use common::query::SortData;
use common::Data;
use db_config::DbContext;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::io::{BufRead, Read, Write};

use crate::disk::{read_blocks, write_blocks};
use crate::ops::execute_op;
use crate::schema::ColumnInfo;
use crate::row::encode_row;

// ── Run descriptor ────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Run {
    start_block: u64,
    num_blocks: usize,
}

// ── Raw-byte chunk entry ──────────────────────────────────────────────────────

/// An entry in the in-memory sort chunk.
///
/// Instead of decoding an entire row into `Vec<Data>` (which carries ~40 bytes
/// of Rust enum overhead per field plus 64 bytes of Vec header), we:
///
///   1. Encode the row to raw bytes once and append those bytes to a shared
///      `chunk_bytes: Vec<u8>` buffer.
///   2. Decode **only the sort key columns** and store them here.
///   3. Record the byte range [byte_start, byte_start + byte_len) so we can
///      copy the raw bytes into disk blocks in sorted order without decoding.
///
/// Memory density example for a 6-column row (4 ints + 2 strings totalling 50 bytes on wire):
///   - Old Vec<Data> cost: 64 + 6 × (40 + ~8) ≈ 352 bytes
///   - New cost: 50 raw bytes + ChunkEntry (~80 bytes) ≈ 130 bytes
///   → ~2.7× improvement. For narrow integer-only tables the ratio reaches ~7×.
struct ChunkEntry {
    /// Values of the sort-key columns only (not all columns).
    sort_key: Vec<Data>,
    /// Start offset of this row's raw bytes in the shared `chunk_bytes` buffer.
    byte_start: usize,
    /// Length of this row's raw bytes.
    byte_len: usize,
}

// ── RunStreamer ───────────────────────────────────────────────────────────────

struct RunStreamer {
    run: Run,
    current_block_idx: usize,
    cached_rows: Vec<Vec<Data>>, // stored reversed for efficient tail-pop
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

        if self.current_block_idx >= self.run.num_blocks {
            return Ok(None); // End of run
        }

        // Read up to 32 blocks at once — aligns with 256-block write chunks and
        // amortises seek overhead much better than the previous 8-block default.
        let fetch = std::cmp::min(32, self.run.num_blocks - self.current_block_idx);
        let block_data = read_blocks(
            disk_out,
            disk_raw,
            self.run.start_block + self.current_block_idx as u64,
            fetch,
            block_size,
        )?;
        self.current_block_idx += fetch;

        // Decode blocks in reverse so that pop() yields rows in forward order.
        for bi in (0..fetch).rev() {
            let start = bi * block_size;
            let rows =
                unpack_block(&block_data[start..start + block_size], block_size, &self.schema)?;
            self.cached_rows.extend(rows);
        }

        Ok(self.cached_rows.pop())
    }
}

// ── HeapItem for k-way merge ──────────────────────────────────────────────────

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
        // Standard min-heap via outer reverse
        for (idx, ascending) in &self.sort_keys {
            let ord = self.row[*idx]
                .partial_cmp(&other.row[*idx])
                .unwrap_or(Ordering::Equal);
            let ord = if *ascending { ord } else { ord.reverse() };
            if ord != Ordering::Equal {
                return ord.reverse(); // Reverse to make BinaryHeap a min-heap
            }
        }
        Ordering::Equal
    }
}

// ── Block packing helpers ─────────────────────────────────────────────────────

/// Pack rows from raw chunk_bytes in the order given by `sorted_index`.
/// No decoding required — bytes are copied directly from the raw buffer.
fn pack_blocks_raw(
    chunk_bytes: &[u8],
    sorted_index: &[ChunkEntry],
    block_size: usize,
) -> Vec<u8> {
    let mut all_bytes = Vec::new();
    let mut current_block = Vec::with_capacity(block_size);
    let mut row_count = 0u16;

    for entry in sorted_index {
        let row_bytes = &chunk_bytes[entry.byte_start..entry.byte_start + entry.byte_len];

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

/// Pack decoded rows into blocks (used by the cascading merge path).
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
    let row_count =
        u16::from_le_bytes([block_data[block_size - 2], block_data[block_size - 1]]) as usize;
    let mut rows = Vec::with_capacity(row_count);
    let mut offset = 0;

    for _ in 0..row_count {
        let (row, new_offset) = crate::row::decode_row(block_data, offset, schema)?;
        rows.push(row);
        offset = new_offset;
    }

    rows.reverse(); // Enable rapid tail-pop in RunStreamer
    Ok(rows)
}

// ── Cascading merge helper ────────────────────────────────────────────────────

/// Merge a group of sorted runs into one new run written to anonymous disk.
/// Used when there are too many runs for a single final merge pass.
fn merge_runs_to_disk<W: Write, R: Read + BufRead>(
    group: &[Run],
    schema: &[ColumnInfo],
    sort_keys: &[(usize, bool)],
    block_size: usize,
    disk_out: &mut W,
    disk_buf: &mut R,
) -> Result<Run> {
    let mut heap = BinaryHeap::new();
    let mut streamers: Vec<RunStreamer> = group
        .iter()
        .map(|run| RunStreamer::new(run.clone(), schema.to_vec()))
        .collect();

    for (i, streamer) in streamers.iter_mut().enumerate() {
        if let Some(row) = streamer.next(disk_out, disk_buf, block_size)? {
            heap.push(HeapItem {
                row,
                run_idx: i,
                sort_keys: sort_keys.to_vec(),
            });
        }
    }

    let mut current_block = Vec::with_capacity(block_size);
    let mut row_count = 0u16;
    let mut row_buf = Vec::new();
    let mut pending_bytes: Vec<u8> = Vec::new();
    let mut first_block_id: Option<u64> = None;
    let mut total_blocks = 0usize;
    let flush_threshold = 256; // write 256 blocks (~1 MB) at a time

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
                let start = crate::disk::allocate_anon_block_chunk(num as u64);
                if first_block_id.is_none() {
                    first_block_id = Some(start);
                }
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
                sort_keys: sort_keys.to_vec(),
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
        let start = crate::disk::allocate_anon_block_chunk(num as u64);
        if first_block_id.is_none() {
            first_block_id = Some(start);
        }
        write_blocks(disk_out, start, num, &pending_bytes)?;
    }

    Ok(Run {
        start_block: first_block_id.unwrap_or(0),
        num_blocks: total_blocks,
    })
}

// ── Chunk flush helper (standalone generic fn — closures can't be generic) ────

/// Sort `chunk_index` by the given sort-key specs, then pack the raw bytes from
/// `chunk_bytes` into disk blocks and write them as a new anonymous scratch run.
/// Clears both buffers on success so they can be reused immediately.
fn flush_chunk_to_disk<W: Write>(
    chunk_bytes: &mut Vec<u8>,
    chunk_index: &mut Vec<ChunkEntry>,
    sort_key_specs: &[(usize, bool)],
    block_size: usize,
    writer: &mut W,
) -> Result<Run> {
    chunk_index.sort_by(|a, b| {
        for (i, (_, ascending)) in sort_key_specs.iter().enumerate() {
            let ord = a.sort_key[i]
                .partial_cmp(&b.sort_key[i])
                .unwrap_or(Ordering::Equal);
            let ord = if *ascending { ord } else { ord.reverse() };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    let packed = pack_blocks_raw(chunk_bytes, chunk_index, block_size);
    let num_blocks = packed.len() / block_size;
    let run_start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);
    write_blocks(writer, run_start, num_blocks, &packed)?;

    chunk_bytes.clear();
    chunk_index.clear();

    Ok(Run { start_block: run_start, num_blocks })
}

// ── Main sort entry point ─────────────────────────────────────────────────────

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

    // Resolve sort column indices once before streaming starts.
    let sort_key_specs: Vec<(usize, bool)> = sort
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

    let mut runs: Vec<Run> = Vec::new();

    // ── Raw-byte in-memory chunk buffers ──────────────────────────────────────
    //
    // `chunk_bytes` holds all row data as a flat byte stream (dense, no overhead).
    // `chunk_index` holds one ChunkEntry per row: extracted sort key values +
    // byte range in chunk_bytes.
    //
    // Memory estimate: chunk_bytes.len() (actual data) + chunk_index.len() * 80
    // (sort key values + struct overhead). At 12% of 64 MB = 7.68 MB budget,
    // we can fit ~5–7× more rows than the old Vec<Vec<Data>> approach.
    let mut chunk_bytes: Vec<u8> = Vec::new();
    let mut chunk_index: Vec<ChunkEntry> = Vec::new();

    // Dynamic budget: computed from the global operator count set in main.rs.
    // Each heavy operator gets an equal share of 50% of the memory limit.
    // NOTE: main.rs already called set_anon_block_base() before execute_op runs.
    //       Do NOT call get_anon_start_block() or init_anon_block_allocator() here —
    //       that would inject extra IPC commands on the disk pipe mid-execution.
    let chunk_limit_bytes = crate::ops::operator_budget_bytes(memory_limit_mb);

    // Reusable encode buffer to avoid per-row allocations.
    let mut row_encode_buf: Vec<u8> = Vec::new();


    // ── Phase 1: Stream child rows into sorted raw-byte chunks ────────────────
    execute_op(
        &sort.underlying,
        ctx,
        disk_out,
        disk_buf,
        block_size,
        memory_limit_mb,
        &mut |row| {
            // Encode row to raw bytes and append to the shared byte buffer.
            row_encode_buf.clear();
            encode_row(row, &mut row_encode_buf);
            let byte_start = chunk_bytes.len();
            let byte_len = row_encode_buf.len();
            chunk_bytes.extend_from_slice(&row_encode_buf);

            // Extract only sort-key column values (avoids storing full Vec<Data>).
            let sort_key: Vec<Data> = sort_key_specs
                .iter()
                .map(|(idx, _)| row[*idx].clone())
                .collect();

            chunk_index.push(ChunkEntry { sort_key, byte_start, byte_len });

            // Memory estimate: raw bytes + ~80 bytes per ChunkEntry (sort key + metadata).
            let estimated_mem = chunk_bytes.len() + chunk_index.len() * 80;
            if estimated_mem >= chunk_limit_bytes {
                // Flush this chunk to disk as a sorted run.
                let mut chunk_out = crate::io_setup::setup_disk_io().1;
                let run = flush_chunk_to_disk(
                    &mut chunk_bytes, &mut chunk_index,
                    &sort_key_specs, block_size, &mut chunk_out,
                )?;
                runs.push(run);
            }
            Ok(())
        },
    )?;

    // ── Fast path: all data fit in memory — no disk spill ────────────────────
    if runs.is_empty() {
        if !chunk_index.is_empty() {
            chunk_index.sort_by(|a, b| {
                for (i, (_, ascending)) in sort_key_specs.iter().enumerate() {
                    let ord = a.sort_key[i]
                        .partial_cmp(&b.sort_key[i])
                        .unwrap_or(Ordering::Equal);
                    let ord = if *ascending { ord } else { ord.reverse() };
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });

            eprintln!(
                "[sort] in-memory: {} rows, no disk spill needed",
                chunk_index.len()
            );

            // Emit rows by decoding from raw bytes in sorted order.
            for entry in &chunk_index {
                let (row, _) =
                    crate::row::decode_row(&chunk_bytes, entry.byte_start, &schema)?;
                on_row(&row)?;
            }
        }
        return Ok(schema);
    }

    // ── Flush remaining rows before the merge phase ───────────────────────────
    if !chunk_index.is_empty() {
        let run = flush_chunk_to_disk(
            &mut chunk_bytes, &mut chunk_index,
            &sort_key_specs, block_size, disk_out,
        )?;
        runs.push(run);
    }

    eprintln!("[sort] merging {} spilled runs", runs.len());

    const MAX_MERGE_WIDTH: usize = 64;

    // ── Cascading merge: reduce run count to ≤ MAX_MERGE_WIDTH ───────────────
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

            let merged =
                merge_runs_to_disk(&group, &schema, &sort_key_specs, block_size, disk_out, disk_buf)?;
            if merged.num_blocks > 0 {
                new_runs.push(merged);
            }
        }

        runs = new_runs;
    }

    // ── Final k-way merge pass ────────────────────────────────────────────────
    eprintln!("[sort] final merge: {} runs", runs.len());

    let mut heap = BinaryHeap::new();
    let mut streamers: Vec<RunStreamer> = runs
        .into_iter()
        .map(|run| RunStreamer::new(run, schema.clone()))
        .collect();

    for (i, streamer) in streamers.iter_mut().enumerate() {
        if let Some(row) = streamer.next(disk_out, disk_buf, block_size)? {
            heap.push(HeapItem {
                row,
                run_idx: i,
                sort_keys: sort_key_specs.clone(),
            });
        }
    }

    while let Some(min_item) = heap.pop() {
        on_row(&min_item.row)?;

        if let Some(next_row) =
            streamers[min_item.run_idx].next(disk_out, disk_buf, block_size)?
        {
            heap.push(HeapItem {
                row: next_row,
                run_idx: min_item.run_idx,
                sort_keys: sort_key_specs.clone(),
            });
        }
    }

    Ok(schema)
}
