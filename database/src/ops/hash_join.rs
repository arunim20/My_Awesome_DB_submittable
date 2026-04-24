use anyhow::{anyhow, Result};
use common::query::HashJoinData;
use common::Data;
use db_config::DbContext;
use std::collections::{HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::io::{BufRead, Read, Write};

use crate::ops::execute_op;
use crate::schema::{get_schema, ColumnInfo};
use crate::row::{encode_row, decode_row};

const NUM_BUCKETS: usize = 8;

// ── Bloom Filter (512 KB) ─────────────────────────────────────────────────────
const BLOOM_BITS: usize = 4_194_304;           // 4M bits = 512 KB
const BLOOM_WORDS: usize = BLOOM_BITS / 64;    // 65 536 u64s

struct BloomFilter {
    bits: Vec<u64>,
}

impl BloomFilter {
    fn new() -> Self {
        BloomFilter { bits: vec![0u64; BLOOM_WORDS] }
    }

    fn bit_positions(key: &Data) -> [usize; 3] {
        let mut h = DefaultHasher::new();
        hash_data(key, &mut h);
        let h1 = h.finish();
        let h2 = h1.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(0x6c62272e07bb0142);
        let h3 = h2.wrapping_mul(0x517cc1b727220a95).wrapping_add(0xd2a98b26625eee7b);
        [
            (h1 % BLOOM_BITS as u64) as usize,
            (h2 % BLOOM_BITS as u64) as usize,
            (h3 % BLOOM_BITS as u64) as usize,
        ]
    }

    fn insert(&mut self, key: &Data) {
        for pos in Self::bit_positions(key) {
            self.bits[pos >> 6] |= 1u64 << (pos & 63);
        }
    }

    #[inline]
    fn might_contain(&self, key: &Data) -> bool {
        for pos in Self::bit_positions(key) {
            if self.bits[pos >> 6] & (1u64 << (pos & 63)) == 0 {
                return false;
            }
        }
        true
    }
}

// ── Binary row encoding ───────────────────────────────────────────────────────

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

struct GraceBuffer {
    bytes: Vec<u8>,
    current_block: Vec<u8>,
    row_count: u16,
    pub total_bytes: usize,
}

impl GraceBuffer {
    fn new(block_size: usize) -> Self {
        GraceBuffer {
            bytes: Vec::new(),
            current_block: Vec::with_capacity(block_size),
            row_count: 0,
            total_bytes: 0,
        }
    }

    fn push_row(&mut self, row: &[Data], block_size: usize) {
        let mut row_bytes = Vec::new();
        encode_row(row, &mut row_bytes);
        self.total_bytes += row_bytes.len();

        if self.current_block.len() + row_bytes.len() > block_size - 2 {
            self.current_block.resize(block_size - 2, 0);
            self.current_block.extend_from_slice(&self.row_count.to_le_bytes());
            self.bytes.extend_from_slice(&self.current_block);
            self.current_block.clear();
            self.row_count = 0;
        }

        self.current_block.extend_from_slice(&row_bytes);
        self.row_count += 1;
    }

    fn finish_block(&mut self, block_size: usize) {
        if !self.current_block.is_empty() {
            self.current_block.resize(block_size - 2, 0);
            self.current_block.extend_from_slice(&self.row_count.to_le_bytes());
            self.bytes.extend_from_slice(&self.current_block);
            self.current_block.clear();
            self.row_count = 0;
        }
    }
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
        let (row, new_offset) = decode_row(block_data, offset, schema)?;
        rows.push(row);
        offset = new_offset;
    }

    Ok(rows)
}

// ── Hashing  ──────────────────────────────────────────────────────────────────

fn hash_data(data: &Data, hasher: &mut impl Hasher) {
    match data {
        Data::Int32(v) => v.hash(hasher),
        Data::Int64(v) => v.hash(hasher),
        Data::Float32(v) => v.to_bits().hash(hasher),
        Data::Float64(v) => v.to_bits().hash(hasher),
        Data::String(v) => v.hash(hasher),
    }
}

#[derive(Debug, Clone)]
struct HashKey(Data);

impl PartialEq for HashKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for HashKey {}

impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_data(&self.0, state);
    }
}

// ── Grace Hash Join ───────────────────────────────────────────────────────────

#[derive(Clone)]
struct BucketRun {
    start_block: u64,
    num_blocks: usize,
}

/// Flush a bucket's in-memory buffer to disk using the canonical disk_out.
/// IMPORTANT: Always use the same disk_out handle — never create a second
/// FD wrapper (setup_disk_io().1), as that would interleave bytes on the
/// underlying FD and corrupt the disk protocol stream.
fn flush_bucket<W: Write>(
    buffer: &mut GraceBuffer,
    runs: &mut Vec<BucketRun>,
    disk_out: &mut W,
    block_size: usize
) -> Result<()> {
    buffer.finish_block(block_size);
    if buffer.bytes.is_empty() { return Ok(()); }

    let num_blocks = buffer.bytes.len() / block_size;
    let start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);

    let max_write_blocks = 256;
    let mut written_blocks = 0;
    while written_blocks < num_blocks {
        let to_write = std::cmp::min(max_write_blocks, num_blocks - written_blocks);
        let byte_start = written_blocks * block_size;
        let byte_end = (written_blocks + to_write) * block_size;

        crate::disk::write_blocks(
            disk_out,
            start + written_blocks as u64,
            to_write,
            &buffer.bytes[byte_start..byte_end]
        )?;
        written_blocks += to_write;
    }

    runs.push(BucketRun { start_block: start, num_blocks });
    buffer.bytes.clear();
    buffer.total_bytes = 0;
    Ok(())
}

pub fn execute_hash_join<W, R>(
    join: &HashJoinData,
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
    let left_schema = get_schema(&join.left, ctx)?;
    let right_schema = get_schema(&join.right, ctx)?;

    let left_join_idx = left_schema.iter().position(|c| c.name == join.left_join_col)
        .ok_or_else(|| anyhow!("Join column {} not found in left child", join.left_join_col))?;

    let right_join_idx = right_schema.iter().position(|c| c.name == join.right_join_col)
        .ok_or_else(|| anyhow!("Join column {} not found in right child", join.right_join_col))?;

    let mut combined_schema = left_schema.clone();
    combined_schema.extend(right_schema.clone());

    crate::disk::init_anon_block_allocator(disk_out, disk_buf)?;

    // Dynamic budget
    let mem_budget = crate::ops::operator_budget_bytes(memory_limit_mb);
    let bucket_byte_limit = std::cmp::max(1, mem_budget / NUM_BUCKETS);

    // Bloom filter: built during right-side collection
    let mut bloom = BloomFilter::new();

    let mut right_rows: Vec<Vec<Data>> = Vec::new();
    let mut right_total_bytes: usize = 0;
    let mut budget_exceeded = false;

    // Grace-mode per-bucket in-memory accumulators.
    // These are only filled AFTER execute_op returns (we do NOT flush inside
    // the closure) to avoid a second concurrent FdWrapper writing to FD 4.
    let mut right_runs: Vec<Vec<BucketRun>> = vec![Vec::new(); NUM_BUCKETS];
    let mut grace_buffers: Vec<GraceBuffer> = (0..NUM_BUCKETS).map(|_| GraceBuffer::new(block_size)).collect();

    let (_, mut chunk_out) = crate::io_setup::setup_disk_io();

    // ── Collect right side ────────────────────────────────────────────────
    // NOTE: We intentionally do NOT write to disk inside this closure.
    // Any disk write here would use a second WriteFdWrapper on FD 4,
    // interleaving with execute_op's own disk_out traffic → corruption.
    // Instead we accumulate everything in grace_buffers and flush afterward.
    execute_op(
        &join.right, ctx, disk_out, disk_buf, block_size,
        memory_limit_mb,
        &mut |row| {
            let row_len = crate::row::encode_row_len(row);
            right_total_bytes += row_len;

            // Always insert into bloom filter
            bloom.insert(&row[right_join_idx]);

            if !budget_exceeded && right_total_bytes <= mem_budget {
                right_rows.push(row.to_vec());
                return Ok(());
            }

            if !budget_exceeded {
                budget_exceeded = true;
                eprintln!(
                    "[hash_join] Grace switch at {} bytes (budget {})",
                    right_total_bytes, mem_budget
                );

                // Redistribute already-collected rows into grace buckets
                for old_row in right_rows.drain(..) {
                    let mut hasher = DefaultHasher::new();
                    hash_data(&old_row[right_join_idx], &mut hasher);
                    let bucket = (hasher.finish() as usize) % NUM_BUCKETS;
                    grace_buffers[bucket].push_row(&old_row, block_size);
                    if grace_buffers[bucket].total_bytes >= bucket_byte_limit {
                        flush_bucket(&mut grace_buffers[bucket], &mut right_runs[bucket], &mut chunk_out, block_size)?;
                    }
                }
                right_rows.shrink_to_fit();
            }

            let mut hasher = DefaultHasher::new();
            hash_data(&row[right_join_idx], &mut hasher);
            let bucket = (hasher.finish() as usize) % NUM_BUCKETS;
            grace_buffers[bucket].push_row(row, block_size);
            
            if grace_buffers[bucket].total_bytes >= bucket_byte_limit {
                flush_bucket(&mut grace_buffers[bucket], &mut right_runs[bucket], &mut chunk_out, block_size)?;
            }

            Ok(())
        }
    )?;

    // ── Flush all grace buffers to disk now that execute_op has returned ──
    // disk_out is exclusively ours again; no interleaving possible.
    if budget_exceeded {
        eprintln!(
            "[hash_join] Grace mode: right {} bytes, {} buckets",
            right_total_bytes, NUM_BUCKETS
        );
        for b in 0..NUM_BUCKETS {
            flush_bucket(&mut grace_buffers[b], &mut right_runs[b], disk_out, block_size)?;
        }
    }
    drop(grace_buffers);

    // ── FAST PATH: In-memory hash join ────────────────────────────────────
    if !budget_exceeded {
        eprintln!(
            "[hash_join] In-memory: {} right rows ({} bytes, budget {})",
            right_rows.len(), right_total_bytes, mem_budget
        );

        let mut build_hash: HashMap<HashKey, Vec<Vec<Data>>> = HashMap::new();
        for row in right_rows {
            let key = HashKey(row[right_join_idx].clone());
            build_hash.entry(key).or_default().push(row);
        }

        execute_op(
            &join.left, ctx, disk_out, disk_buf, block_size,
            memory_limit_mb,
            &mut |left_row| {
                // Bloom filter: skip if definitely no match
                if !bloom.might_contain(&left_row[left_join_idx]) {
                    return Ok(());
                }
                let key = HashKey(left_row[left_join_idx].clone());
                if let Some(matches) = build_hash.get(&key) {
                    for right_row in matches {
                        let mut combined = left_row.to_vec();
                        combined.extend(right_row.clone());
                        on_row(&combined)?;
                    }
                }
                Ok(())
            }
        )?;

        return Ok(combined_schema);
    }

    // ── GRACE PATH: partition left side & probe ──────────────────────────
    let mut left_runs: Vec<Vec<BucketRun>> = vec![Vec::new(); NUM_BUCKETS];

    {
        // Accumulate left side into per-bucket in-memory buffers.
        // Again: no disk writes inside the closure — flush afterward.
        let mut left_buffers: Vec<GraceBuffer> = (0..NUM_BUCKETS).map(|_| GraceBuffer::new(block_size)).collect();

        let (_, mut left_chunk_out) = crate::io_setup::setup_disk_io();

        execute_op(
            &join.left, ctx, disk_out, disk_buf, block_size,
            memory_limit_mb,
            &mut |row| {
                // Bloom filter: skip rows that definitely have no match
                if !bloom.might_contain(&row[left_join_idx]) {
                    return Ok(());
                }

                let mut hasher = DefaultHasher::new();
                hash_data(&row[left_join_idx], &mut hasher);
                let bucket = (hasher.finish() as usize) % NUM_BUCKETS;

                left_buffers[bucket].push_row(row, block_size);
                if left_buffers[bucket].total_bytes >= bucket_byte_limit {
                    flush_bucket(&mut left_buffers[bucket], &mut left_runs[bucket], &mut left_chunk_out, block_size)?;
                }
                Ok(())
            }
        )?;

        // Flush all left buckets after execute_op returns.
        for b in 0..NUM_BUCKETS {
            flush_bucket(&mut left_buffers[b], &mut left_runs[b], disk_out, block_size)?;
        }
    }

    drop(bloom);

    // Phase 3: Multi-pass probe — load right side in memory-safe chunks
    // Budget dynamically set to prevent allocator fragmentation
    // or HashMap virtual memory resizing from blowing the strict 64MB total limit.
    let phase3_budget: usize = mem_budget;

    eprintln!("[hash_join] Phase 3: probing {} buckets", NUM_BUCKETS);

    for b in 0..NUM_BUCKETS {
        if right_runs[b].is_empty() { continue; }

        // Flatten right-side runs into a sequential block list
        let right_segs: Vec<(u64, usize)> = right_runs[b]
            .iter()
            .map(|r| (r.start_block, r.num_blocks))
            .collect();

        // State: track position across segments for multi-pass
        let mut seg_idx: usize = 0;
        let mut blk_off: usize = 0;

        loop {
            let mut build_hash: HashMap<HashKey, Vec<Vec<Data>>> = HashMap::new();
            let mut loaded_bytes: usize = 0;
            let mut any_loaded = false;

            // ── Load right-side rows until budget exhausted or data done ──
            'load_right: while seg_idx < right_segs.len() {
                let (seg_start, seg_blocks) = right_segs[seg_idx];
                while blk_off < seg_blocks {
                    let fetch = std::cmp::min(32, seg_blocks - blk_off);
                    let buffer = crate::disk::read_blocks(
                        disk_out,
                        disk_buf,
                        seg_start + blk_off as u64,
                        fetch,
                        block_size,
                    )?;
                    for bi in 0..fetch {
                        let start = bi * block_size;
                        let rows = unpack_block(
                            &buffer[start..start + block_size],
                            block_size,
                            &right_schema,
                        )?;
                        for row in rows {
                            loaded_bytes += crate::row::encode_row_len(&row);
                            let key = HashKey(row[right_join_idx].clone());
                            build_hash.entry(key).or_default().push(row);
                            any_loaded = true;
                        }
                    }
                    blk_off += fetch;
                    if loaded_bytes >= phase3_budget {
                        break 'load_right;
                    }
                }
                // Finished this segment, move to next
                seg_idx += 1;
                blk_off = 0;
            }

            if !any_loaded {
                break;
            }

            // ── Probe entire left side against this partial right HashMap ──
            for run in &left_runs[b] {
                let mut rb = 0;
                while rb < run.num_blocks {
                    let fetch = std::cmp::min(256, run.num_blocks - rb);
                    let buffer = crate::disk::read_blocks(
                        disk_out,
                        disk_buf,
                        run.start_block + rb as u64,
                        fetch,
                        block_size,
                    )?;
                    for bi in 0..fetch {
                        let start = bi * block_size;
                        let rows = unpack_block(
                            &buffer[start..start + block_size],
                            block_size,
                            &left_schema,
                        )?;
                        for left_row in rows {
                            let key = HashKey(left_row[left_join_idx].clone());
                            if let Some(right_matches) = build_hash.get(&key) {
                                for right_row in right_matches {
                                    let mut combined = left_row.clone();
                                    combined.extend(right_row.clone());
                                    on_row(&combined)?;
                                }
                            }
                        }
                    }
                    rb += fetch;
                }
            }

            // If we've exhausted all right segments, done with this bucket
            if seg_idx >= right_segs.len() {
                break;
            }
        }
    }


    Ok(combined_schema)
}