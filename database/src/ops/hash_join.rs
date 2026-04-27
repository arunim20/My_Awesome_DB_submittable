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

const NUM_BUCKETS: usize = 16;

// ── Bloom Filter (512 KB) ─────────────────────────────────────────────────────
const BLOOM_BITS: usize = 524_288;             // 512K bits = 64 KB
const BLOOM_WORDS: usize = BLOOM_BITS / 64;    // 8192 u64s

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
    /// UNC-16: Reusable encode scratch — eliminates one Vec alloc per push_row call.
    row_scratch: Vec<u8>,
}

impl GraceBuffer {
    fn new(block_size: usize) -> Self {
        GraceBuffer {
            bytes: Vec::new(),
            current_block: Vec::with_capacity(block_size),
            row_count: 0,
            total_bytes: 0,
            row_scratch: Vec::new(),
        }
    }

    fn push_row(&mut self, row: &[Data], block_size: usize) {
        // UNC-16: reuse scratch buffer instead of allocating a new Vec each call
        self.row_scratch.clear();
        encode_row(row, &mut self.row_scratch);
        self.total_bytes += self.row_scratch.len();

        if self.current_block.len() + self.row_scratch.len() > block_size - 2 {
            self.current_block.resize(block_size - 2, 0);
            self.current_block.extend_from_slice(&self.row_count.to_le_bytes());
            self.bytes.extend_from_slice(&self.current_block);
            self.current_block.clear();
            self.row_count = 0;
        }

        self.current_block.extend_from_slice(&self.row_scratch);
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
    let mut blooms: Vec<BloomFilter> = (0..NUM_BUCKETS).map(|_| BloomFilter::new()).collect();

    let mut spilled_buckets = vec![false; NUM_BUCKETS];
    let mut in_memory_buckets: Vec<Vec<Vec<Data>>> = vec![Vec::new(); NUM_BUCKETS];
    let mut bucket_bytes = vec![0usize; NUM_BUCKETS];
    let mut right_total_bytes: usize = 0;

    // Grace-mode per-bucket in-memory accumulators.
    // These are only filled AFTER execute_op returns (we do NOT flush inside
    // the closure) to avoid a second concurrent FdWrapper writing to FD 4.
    let mut right_runs: Vec<Vec<BucketRun>> = vec![Vec::new(); NUM_BUCKETS];
    let mut grace_buffers: Vec<GraceBuffer> = (0..NUM_BUCKETS).map(|_| GraceBuffer::new(block_size)).collect();

    let (_, mut chunk_out) = crate::io_setup::setup_disk_io();

    // ── Collect right side ────────────────────────────────────────────────
    execute_op(
        &join.right, ctx, disk_out, disk_buf, block_size,
        memory_limit_mb,
        &mut |row| {
            let row_len = crate::row::encode_row_len(row);

            let mut hasher = DefaultHasher::new();
            hash_data(&row[right_join_idx], &mut hasher);
            let bucket = (hasher.finish() as usize) % NUM_BUCKETS;

            // Always insert into bloom filter
            blooms[bucket].insert(&row[right_join_idx]);

            if spilled_buckets[bucket] {
                grace_buffers[bucket].push_row(row, block_size);
                if grace_buffers[bucket].total_bytes >= bucket_byte_limit {
                    flush_bucket(&mut grace_buffers[bucket], &mut right_runs[bucket], &mut chunk_out, block_size)?;
                }
            } else {
                in_memory_buckets[bucket].push(row.to_vec());
                bucket_bytes[bucket] += row_len;
                right_total_bytes += row_len;

                if right_total_bytes > mem_budget {
                    // Need to spill a bucket! Find the largest in-memory bucket
                    let mut largest_bucket = 0;
                    let mut max_bytes = 0;
                    for b in 0..NUM_BUCKETS {
                        if !spilled_buckets[b] && bucket_bytes[b] > max_bytes {
                            max_bytes = bucket_bytes[b];
                            largest_bucket = b;
                        }
                    }

                    if max_bytes > 0 {
                        spilled_buckets[largest_bucket] = true;
                        right_total_bytes -= bucket_bytes[largest_bucket];
                        bucket_bytes[largest_bucket] = 0;

                        eprintln!("[hash_join] Hybrid spill: bucket {} ({} bytes spilled)", largest_bucket, max_bytes);

                        for old_row in in_memory_buckets[largest_bucket].drain(..) {
                            grace_buffers[largest_bucket].push_row(&old_row, block_size);
                            if grace_buffers[largest_bucket].total_bytes >= bucket_byte_limit {
                                flush_bucket(&mut grace_buffers[largest_bucket], &mut right_runs[largest_bucket], &mut chunk_out, block_size)?;
                            }
                        }
                        in_memory_buckets[largest_bucket].shrink_to_fit();
                    }
                }
            }

            Ok(())
        }
    )?;

    // ── Flush all grace buffers to disk now that execute_op has returned ──
    for b in 0..NUM_BUCKETS {
        if spilled_buckets[b] {
            flush_bucket(&mut grace_buffers[b], &mut right_runs[b], disk_out, block_size)?;
        }
    }
    drop(grace_buffers);

    // Build HashMap for in-memory buckets
    let mut in_memory_hashes: Vec<HashMap<HashKey, Vec<Vec<Data>>>> = vec![HashMap::new(); NUM_BUCKETS];
    for b in 0..NUM_BUCKETS {
        if !spilled_buckets[b] {
            for row in in_memory_buckets[b].drain(..) {
                let key = HashKey(row[right_join_idx].clone());
                in_memory_hashes[b].entry(key).or_default().push(row);
            }
            in_memory_buckets[b].shrink_to_fit();
        }
    }

    // ── Phase 2: Stream left side and probe in-memory buckets OR spill ────
    let mut left_runs: Vec<Vec<BucketRun>> = vec![Vec::new(); NUM_BUCKETS];

    {
        let mut left_buffers: Vec<GraceBuffer> = (0..NUM_BUCKETS).map(|_| GraceBuffer::new(block_size)).collect();
        let (_, mut left_chunk_out) = crate::io_setup::setup_disk_io();

        execute_op(
            &join.left, ctx, disk_out, disk_buf, block_size,
            memory_limit_mb,
            &mut |row| {
                let mut hasher = DefaultHasher::new();
                hash_data(&row[left_join_idx], &mut hasher);
                let bucket = (hasher.finish() as usize) % NUM_BUCKETS;

                // Bloom filter: skip rows that definitely have no match
                if !blooms[bucket].might_contain(&row[left_join_idx]) {
                    return Ok(());
                }

                if spilled_buckets[bucket] {
                    // UNC-05: if right side for this bucket is entirely empty,
                    // no left row can ever produce a match — skip writing to disk.
                    if right_runs[bucket].is_empty() {
                        return Ok(());
                    }
                    left_buffers[bucket].push_row(row, block_size);
                    if left_buffers[bucket].total_bytes >= bucket_byte_limit {
                        flush_bucket(&mut left_buffers[bucket], &mut left_runs[bucket], &mut left_chunk_out, block_size)?;
                    }
                } else {
                    // Probe immediately!
                    let key = HashKey(row[left_join_idx].clone());
                    if let Some(matches) = in_memory_hashes[bucket].get(&key) {
                        for right_row in matches {
                            let mut combined = row.to_vec();
                            combined.extend(right_row.clone());
                            on_row(&combined)?;
                        }
                    }
                }
                Ok(())
            }
        )?;

        // Flush all left buckets
        for b in 0..NUM_BUCKETS {
            if spilled_buckets[b] {
                flush_bucket(&mut left_buffers[b], &mut left_runs[b], disk_out, block_size)?;
            }
        }
    }

    drop(blooms);
    drop(in_memory_hashes);

    // Phase 3: Multi-pass probe — load right side in memory-safe chunks
    // Budget dynamically set to prevent allocator fragmentation
    // or HashMap virtual memory resizing from blowing the strict 64MB total limit.
    let phase3_budget: usize = mem_budget;

    eprintln!("[hash_join] Phase 3: probing {} buckets", NUM_BUCKETS);

    // UNC-15: declare build_hash ONCE outside the bucket loop.
    // HashMap::clear() retains the internal allocation, avoiding repeated
    // multi-MB backing-array allocations across buckets.
    let mut build_hash: HashMap<HashKey, Vec<Vec<Data>>> = HashMap::new();

    for b in 0..NUM_BUCKETS {
        // #5: skip if either side has no spilled rows — no output possible
        if !spilled_buckets[b] || right_runs[b].is_empty() { continue; }
        if left_runs[b].is_empty() {
            eprintln!("[hash_join] Phase 3 bucket {}: no left runs, skipping all right reads", b);
            continue;
        }

        let left_blocks: usize = left_runs[b].iter().map(|r| r.num_blocks).sum();
        let right_blocks: usize = right_runs[b].iter().map(|r| r.num_blocks).sum();
        
        let (build_runs, probe_runs, build_schema, probe_schema, build_join_idx, probe_join_idx, left_is_build) = if left_blocks < right_blocks {
            (&left_runs[b], &right_runs[b], &left_schema, &right_schema, left_join_idx, right_join_idx, true)
        } else {
            (&right_runs[b], &left_runs[b], &right_schema, &left_schema, right_join_idx, left_join_idx, false)
        };

        // Flatten build-side runs into a sequential block list
        let build_segs: Vec<(u64, usize)> = build_runs
            .iter()
            .map(|r| (r.start_block, r.num_blocks))
            .collect();

        // State: track position across segments for multi-pass
        let mut seg_idx: usize = 0;
        let mut blk_off: usize = 0;

        loop {
            // UNC-15: reuse existing HashMap allocation (clear retains capacity)
            build_hash.clear();
            let mut loaded_bytes: usize = 0;
            let mut any_loaded = false;

            // ── Load build-side rows until budget exhausted or data done ──
            'load_build: while seg_idx < build_segs.len() {
                let (seg_start, seg_blocks) = build_segs[seg_idx];
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
                            build_schema,
                        )?;
                        for row in rows {
                            loaded_bytes += crate::row::encode_row_len(&row);
                            let key = HashKey(row[build_join_idx].clone());
                            build_hash.entry(key).or_default().push(row);
                            any_loaded = true;
                        }
                    }
                    blk_off += fetch;
                    if loaded_bytes >= phase3_budget {
                        break 'load_build;
                    }
                }
                // Finished this segment, move to next
                seg_idx += 1;
                blk_off = 0;
            }

            if !any_loaded {
                break;
            }

            // ── Probe entire probe side against this partial build HashMap ──
            for run in probe_runs {
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
                            probe_schema,
                        )?;
                        for probe_row in rows {
                            let key = HashKey(probe_row[probe_join_idx].clone());
                            if let Some(build_matches) = build_hash.get(&key) {
                                for build_row in build_matches {
                                    let mut combined;
                                    if left_is_build {
                                        combined = build_row.clone();
                                        combined.extend(probe_row.clone());
                                    } else {
                                        combined = probe_row.clone();
                                        combined.extend(build_row.clone());
                                    }
                                    on_row(&combined)?;
                                }
                            }
                        }
                    }
                    rb += fetch;
                }
            }

            // If we've exhausted all build segments, done with this bucket
            if seg_idx >= build_segs.len() {
                break;
            }
        }
    }


    Ok(combined_schema)
}