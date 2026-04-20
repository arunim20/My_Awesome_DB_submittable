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

const NUM_BUCKETS: usize = 64;

// ── Bloom Filter ──────────────────────────────────────────────────────────────

/// Bloom filter size: 1 MB = 8 388 608 bits (131 072 × u64).
///
/// Used to pre-filter the left (probe) side of a Grace Hash Join: any left row
/// whose join key provably does not exist on the right side is discarded
/// immediately, without being written to a grace bucket on disk.
///
/// Uses 3 independent hash probes derived from a single `DefaultHasher` call.
/// False-positive rate for an 8M-bit filter with 3 probes:
///   - At  50 000 right rows: ≈0.004%  (virtually zero false positives)
///   - At 500 000 right rows: ≈0.4%    (still excellent)
///
/// Memory cost: 1 MB — fits comfortably in the 64 MB budget and prevents
/// millions of unnecessary grace partition disk writes/reads.
const BLOOM_BITS: usize = 8_388_608;
const BLOOM_WORDS: usize = BLOOM_BITS / 64; // 131 072 u64s = 1 MB

struct BloomFilter {
    bits: Vec<u64>,
}

impl BloomFilter {
    fn new() -> Self {
        BloomFilter { bits: vec![0u64; BLOOM_WORDS] }
    }

    /// Derive 3 independent bit positions from a join key via bit-mixing.
    fn bit_positions(key: &Data) -> [usize; 3] {
        let mut h = DefaultHasher::new();
        hash_data(key, &mut h);
        let h1 = h.finish();

        // Two additional hashes via multiplicative mixing (fast, good avalanche).
        let h2 = h1
            .wrapping_mul(0x9e3779b97f4a7c15)
            .wrapping_add(0x6c62272e07bb0142);
        let h3 = h2
            .wrapping_mul(0x517cc1b727220a95)
            .wrapping_add(0xd2a98b26625eee7b);

        [
            (h1 % BLOOM_BITS as u64) as usize,
            (h2 % BLOOM_BITS as u64) as usize,
            (h3 % BLOOM_BITS as u64) as usize,
        ]
    }

    /// Insert a join key into the filter.
    fn insert(&mut self, key: &Data) {
        for pos in Self::bit_positions(key) {
            self.bits[pos >> 6] |= 1u64 << (pos & 63);
        }
    }

    /// Return `false` if the key is *definitely not* in the set.
    /// Return `true` if the key *might* be in the set (could be a false positive).
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

/// Pack rows into disk blocks using binary row serialisation.
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

/// Decode all rows from a single block.
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
        let (row, new_offset) = decode_row(block_data, offset, schema)?;
        rows.push(row);
        offset = new_offset;
    }

    Ok(rows)
}

// ── Hashing helpers ───────────────────────────────────────────────────────────

fn hash_data(data: &Data, hasher: &mut impl Hasher) {
    match data {
        Data::Int32(v)  => v.hash(hasher),
        Data::Int64(v)  => v.hash(hasher),
        Data::Float32(v) => v.to_bits().hash(hasher),
        Data::Float64(v) => v.to_bits().hash(hasher),
        Data::String(v)  => v.hash(hasher),
    }
}

#[derive(Debug, Clone)]
struct HashKey(Data);

impl PartialEq for HashKey {
    fn eq(&self, other: &Self) -> bool { self.0 == other.0 }
}
impl Eq for HashKey {}

impl Hash for HashKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        hash_data(&self.0, state);
    }
}

// ── Grace bucket structures ───────────────────────────────────────────────────

#[derive(Clone)]
struct BucketRun {
    start_block: u64,
    num_blocks: usize,
}

/// Flush a grace bucket buffer to disk and record the resulting run.
fn flush_bucket<W: Write>(
    buffer: &mut Vec<Vec<Data>>,
    runs: &mut Vec<BucketRun>,
    chunk_out: &mut W,
    block_size: usize,
) -> Result<()> {
    if buffer.is_empty() {
        return Ok(());
    }

    let packed = pack_rows_into_blocks(buffer, block_size);
    let num_blocks = packed.len() / block_size;
    let start = crate::disk::allocate_anon_block_chunk(num_blocks as u64);

    // Write in 256-block batches to avoid huge single IPC messages.
    let max_write_blocks = 256;
    let mut written_blocks = 0;
    while written_blocks < num_blocks {
        let to_write = std::cmp::min(max_write_blocks, num_blocks - written_blocks);
        let byte_start = written_blocks * block_size;
        let byte_end = (written_blocks + to_write) * block_size;

        crate::disk::write_blocks(
            chunk_out,
            start + written_blocks as u64,
            to_write,
            &packed[byte_start..byte_end],
        )?;
        written_blocks += to_write;
    }

    runs.push(BucketRun { start_block: start, num_blocks });
    buffer.clear();
    Ok(())
}

// ── Main hash join entry point ────────────────────────────────────────────────

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
    let left_schema  = get_schema(&join.left,  ctx)?;
    let right_schema = get_schema(&join.right, ctx)?;

    let left_join_idx = left_schema
        .iter()
        .position(|c| c.name == join.left_join_col)
        .ok_or_else(|| anyhow!("Join column {} not found in left schema",  join.left_join_col))?;

    let right_join_idx = right_schema
        .iter()
        .position(|c| c.name == join.right_join_col)
        .ok_or_else(|| anyhow!("Join column {} not found in right schema", join.right_join_col))?;

    let mut combined_schema = left_schema.clone();
    combined_schema.extend(right_schema.clone());

    // Idempotent: only queries disk if block base hasn't been set yet.
    crate::disk::init_anon_block_allocator(disk_out, disk_buf)?;

    // 15% of memory limit for the right-side collection budget.
    // 10% of memory limit — reduced from 15% to allow 4+ nested hash joins
    // within the 64 MB budget without OOM.
    let mem_budget       = (memory_limit_mb as usize * 1024 * 1024 * 10) / 100;
    let bucket_byte_limit = std::cmp::max(1, mem_budget / NUM_BUCKETS);

    // ── Bloom Filter: built during right-side collection ─────────────────────
    // Any left row whose join key is absent from this filter is guaranteed to
    // produce zero join matches and can be discarded without touching disk.
    let mut bloom = BloomFilter::new();

    // ── Right-side collection: in-memory first, Grace if too large ────────────
    let mut right_rows: Vec<Vec<Data>> = Vec::new();
    let mut right_total_bytes: usize   = 0;
    let mut budget_exceeded            = false;

    // Grace-mode state (allocated lazily only if the right side overflows).
    // Using Vec<Vec<…>> here is fine at NUM_BUCKETS = 64 (small constant).
    let mut right_runs:         Vec<Vec<BucketRun>>   = vec![Vec::new(); NUM_BUCKETS];
    let mut grace_buffers:      Vec<Vec<Vec<Data>>>   = vec![Vec::new(); NUM_BUCKETS];
    let mut grace_bucket_bytes: Vec<usize>            = vec![0;          NUM_BUCKETS];

    execute_op(
        &join.right, ctx, disk_out, disk_buf, block_size, memory_limit_mb,
        &mut |row| {
            let row_len = crate::row::encode_row_len(row);
            right_total_bytes += row_len;

            // Always insert into the bloom filter regardless of in-memory/grace mode.
            bloom.insert(&row[right_join_idx]);

            // ── In-memory fast path ──────────────────────────────────────────
            if !budget_exceeded && right_total_bytes <= mem_budget {
                right_rows.push(row.to_vec());
                return Ok(());
            }

            // ── First overflow: switch to Grace Hash Join ─────────────────────
            if !budget_exceeded {
                budget_exceeded = true;
                eprintln!(
                    "[hash_join] Adaptive switch to Grace at {} bytes (budget {})",
                    right_total_bytes, mem_budget
                );

                // Drain previously collected in-memory rows into grace buckets.
                let mut chunk_out = crate::io_setup::setup_disk_io().1;
                for old_row in right_rows.drain(..) {
                    let mut hasher = DefaultHasher::new();
                    hash_data(&old_row[right_join_idx], &mut hasher);
                    let bucket = (hasher.finish() as usize) % NUM_BUCKETS;
                    grace_bucket_bytes[bucket] += crate::row::encode_row_len(&old_row);
                    grace_buffers[bucket].push(old_row);
                    if grace_bucket_bytes[bucket] >= bucket_byte_limit {
                        flush_bucket(&mut grace_buffers[bucket], &mut right_runs[bucket], &mut chunk_out, block_size)?;
                        grace_bucket_bytes[bucket] = 0;
                    }
                }
                right_rows.shrink_to_fit(); // Free backing memory
            }

            // ── Grace mode: partition this row into the appropriate bucket ────
            let mut hasher = DefaultHasher::new();
            hash_data(&row[right_join_idx], &mut hasher);
            let bucket = (hasher.finish() as usize) % NUM_BUCKETS;
            grace_bucket_bytes[bucket] += row_len;
            grace_buffers[bucket].push(row.to_vec());
            if grace_bucket_bytes[bucket] >= bucket_byte_limit {
                let mut chunk_out = crate::io_setup::setup_disk_io().1;
                flush_bucket(&mut grace_buffers[bucket], &mut right_runs[bucket], &mut chunk_out, block_size)?;
                grace_bucket_bytes[bucket] = 0;
            }

            Ok(())
        },
    )?;

    // Flush any remaining grace right-side buffers.
    if budget_exceeded {
        let mut chunk_out = crate::io_setup::setup_disk_io().1;
        for b in 0..NUM_BUCKETS {
            flush_bucket(&mut grace_buffers[b], &mut right_runs[b], &mut chunk_out, block_size)?;
        }
    }

    // Free grace-mode buffers immediately — they are fully flushed and no
    // longer referenced.  Reclaims capacity overhead (~few KB) early.
    drop(grace_buffers);
    drop(grace_bucket_bytes);

    // ── FAST PATH: entire right side fits in memory ───────────────────────────
    if !budget_exceeded {
        eprintln!(
            "[hash_join] In-memory mode: {} right rows ({} bytes, budget {})",
            right_rows.len(), right_total_bytes, mem_budget
        );

        let mut build_hash: HashMap<HashKey, Vec<Vec<Data>>> = HashMap::new();
        for row in right_rows {
            let key = HashKey(row[right_join_idx].clone());
            build_hash.entry(key).or_default().push(row);
        }

        execute_op(
            &join.left, ctx, disk_out, disk_buf, block_size, memory_limit_mb,
            &mut |left_row| {
                // Bloom filter: skip if the join key definitely isn't on the right side.
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
            },
        )?;

        return Ok(combined_schema);
    }

    // ── GRACE PATH: partition left side & probe bucket by bucket ─────────────
    eprintln!(
        "[hash_join] Grace mode: right side {} bytes, {} buckets",
        right_total_bytes, NUM_BUCKETS
    );

    let mut left_runs: Vec<Vec<BucketRun>> = vec![Vec::new(); NUM_BUCKETS];

    {
        let mut left_buffers:      Vec<Vec<Vec<Data>>> = vec![Vec::new(); NUM_BUCKETS];
        let mut left_bucket_bytes: Vec<usize>          = vec![0;          NUM_BUCKETS];

        execute_op(
            &join.left, ctx, disk_out, disk_buf, block_size, memory_limit_mb,
            &mut |row| {
                // ── Bloom filter pushdown ──────────────────────────────────
                // If the row's join key is definitely absent from the right side,
                // skip it entirely — no disk write needed for this row.
                if !bloom.might_contain(&row[left_join_idx]) {
                    return Ok(());
                }

                let mut hasher = DefaultHasher::new();
                hash_data(&row[left_join_idx], &mut hasher);
                let bucket = (hasher.finish() as usize) % NUM_BUCKETS;

                left_bucket_bytes[bucket] += crate::row::encode_row_len(row);
                left_buffers[bucket].push(row.to_vec());

                if left_bucket_bytes[bucket] >= bucket_byte_limit {
                    let mut chunk_out = crate::io_setup::setup_disk_io().1;
                    flush_bucket(&mut left_buffers[bucket], &mut left_runs[bucket], &mut chunk_out, block_size)?;
                    left_bucket_bytes[bucket] = 0;
                }
                Ok(())
            },
        )?;

        let mut chunk_out = crate::io_setup::setup_disk_io().1;
        for b in 0..NUM_BUCKETS {
            flush_bucket(&mut left_buffers[b], &mut left_runs[b], &mut chunk_out, block_size)?;
        }
    }

    // Free the 1 MB bloom filter — no longer needed after left-side partitioning.
    drop(bloom);

    // ── Phase 3: probe bucket by bucket ──────────────────────────────────────
    eprintln!("[hash_join] Phase 3: probing buckets via in-memory hashing");

    let mut inner_out = crate::io_setup::setup_disk_io().1;
    let mut inner_buf = crate::io_setup::setup_disk_io().0;

    for b in 0..NUM_BUCKETS {
        if right_runs[b].is_empty() {
            continue;
        }

        // Build hash table from right-side bucket.
        let mut build_hash: HashMap<HashKey, Vec<Vec<Data>>> = HashMap::new();

        for run in &right_runs[b] {
            let mut rb = 0;
            while rb < run.num_blocks {
                let fetch = std::cmp::min(256, run.num_blocks - rb);
                let buffer = crate::disk::read_blocks(
                    &mut inner_out, &mut inner_buf,
                    run.start_block + rb as u64, fetch, block_size,
                )?;
                for bi in 0..fetch {
                    let start = bi * block_size;
                    let rows = unpack_block(
                        &buffer[start..start + block_size], block_size, &right_schema,
                    )?;
                    for row in rows {
                        let key = HashKey(row[right_join_idx].clone());
                        build_hash.entry(key).or_default().push(row);
                    }
                }
                rb += fetch;
            }
        }

        if build_hash.is_empty() {
            continue;
        }

        // Probe with left-side bucket.
        for run in &left_runs[b] {
            let mut rb = 0;
            while rb < run.num_blocks {
                let fetch = std::cmp::min(256, run.num_blocks - rb);
                let buffer = crate::disk::read_blocks(
                    &mut inner_out, &mut inner_buf,
                    run.start_block + rb as u64, fetch, block_size,
                )?;
                for bi in 0..fetch {
                    let start = bi * block_size;
                    let rows = unpack_block(
                        &buffer[start..start + block_size], block_size, &left_schema,
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
    }

    Ok(combined_schema)
}
