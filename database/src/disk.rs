use anyhow::Result;
use std::io::{BufRead, Read, Write};

/// Send a text command to disk and read back the one-line text response.
pub fn ask_disk_line<R: BufRead>(
    disk_out: &mut impl Write,
    disk_buf: &mut R,
    cmd: &str,
) -> Result<String> {
    flush_disk_write_buffer(disk_out)?;
    disk_out.write_all(cmd.as_bytes())?;
    disk_out.flush()?;
    let mut line = String::new();
    disk_buf.read_line(&mut line)?;
    Ok(line.trim().to_string())
}

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

static DISK_WRITE_BUFFER: Mutex<Vec<u8>> = Mutex::new(Vec::new());
static DISK_BUFFER_LIMIT: AtomicUsize = AtomicUsize::new(4 * 1024 * 1024); // Default 4MB

pub fn flush_disk_write_buffer(disk_out: &mut impl Write) -> Result<()> {
    let mut buf = DISK_WRITE_BUFFER.lock().unwrap();
    if !buf.is_empty() {
        disk_out.write_all(&buf)?;
        disk_out.flush()?;
        buf.clear();
    }
    Ok(())
}

pub fn set_global_budgets(memory_limit_mb: u64) {
    let total_bytes = memory_limit_mb as usize * 1024 * 1024;
    // Cache: ~18% of memory (was 12MB for 64MB)
    let cache_limit = total_bytes * 18 / 100;
    CACHE_LIMIT_BYTES.store(cache_limit, Ordering::SeqCst);
    A1_IN_LIMIT.store(cache_limit / 4, Ordering::SeqCst);
    
    // Write buffer: ~6% of memory (was 4MB for 64MB)
    let write_limit = total_bytes * 6 / 100;
    DISK_BUFFER_LIMIT.store(write_limit, Ordering::SeqCst);
    
    eprintln!("[disk] Dynamic budgets: cache={}MB, write_buf={}MB", cache_limit/1024/1024, write_limit/1024/1024);
}

static NEXT_ANON_BLOCK: AtomicU64 = AtomicU64::new(0);
static BASE_ANON_BLOCK: AtomicU64 = AtomicU64::new(0);

pub fn set_anon_block_base(base: u64) {
    BASE_ANON_BLOCK.store(base, Ordering::SeqCst);
    NEXT_ANON_BLOCK.store(base, Ordering::SeqCst);
}
struct CacheEntry {
    start_block_id: u64,
    num_blocks: usize,
    data: Vec<u8>,
}

struct TwoQueueCache {
    am: Vec<CacheEntry>,
    a1_in: Vec<CacheEntry>,
    a1_out: Vec<(u64, usize)>,
    size_bytes: usize,
}

impl TwoQueueCache {
    const fn new() -> Self {
        TwoQueueCache {
            am: Vec::new(),
            a1_in: Vec::new(),
            a1_out: Vec::new(),
            size_bytes: 0,
        }
    }

    fn remove_overlap(&mut self, start: u64, num: usize) -> Option<(CacheEntry, bool)> {
        if let Some(idx) = self.am.iter().position(|e| {
            start >= e.start_block_id && (start + num as u64) <= (e.start_block_id + e.num_blocks as u64)
        }) {
            return Some((self.am.remove(idx), true));
        }
        if let Some(idx) = self.a1_in.iter().position(|e| {
            start >= e.start_block_id && (start + num as u64) <= (e.start_block_id + e.num_blocks as u64)
        }) {
            return Some((self.a1_in.remove(idx), false));
        }
        None
    }

    fn retain_anon_blocks(&mut self, base: u64) {
        self.am.retain(|e| {
            if e.start_block_id >= base {
                self.size_bytes -= e.data.len();
                false
            } else { true }
        });
        self.a1_in.retain(|e| {
            if e.start_block_id >= base {
                self.size_bytes -= e.data.len();
                false
            } else { true }
        });
        self.a1_out.retain(|&(s, _)| s < base);
    }

    fn insert(&mut self, entry: CacheEntry, target_limit: usize, a1_in_limit: usize) {
        if entry.data.len() > target_limit { return; }

        let mut is_ghost_hit = false;
        let start = entry.start_block_id;
        let num = entry.num_blocks;
        
        if let Some(idx) = self.a1_out.iter().position(|&(g_start, g_num)| {
            let end = start + num as u64;
            let g_end = g_start + g_num as u64;
            start < g_end && end > g_start
        }) {
            self.a1_out.remove(idx);
            is_ghost_hit = true;
        }

        self.size_bytes += entry.data.len();
        
        if is_ghost_hit {
            self.am.insert(0, entry);
        } else {
            self.a1_in.insert(0, entry);
        }

        while self.size_bytes > target_limit && (!self.am.is_empty() || !self.a1_in.is_empty()) {
            let a1_in_bytes: usize = self.a1_in.iter().map(|e| e.data.len()).sum();
            if a1_in_bytes > a1_in_limit || self.am.is_empty() {
                if let Some(evicted) = self.a1_in.pop() {
                    self.size_bytes -= evicted.data.len();
                    self.a1_out.insert(0, (evicted.start_block_id, evicted.num_blocks));
                    if self.a1_out.len() > 1000 {
                        self.a1_out.pop();
                    }
                }
            } else {
                // UNC-07: Evict the LARGEST entry from am instead of always
                // the LRU tail. This keeps small dimension-table blocks
                // (nation=1 block, region=1 block) hot while large sort-run
                // fetches are displaced first.
                if let Some(largest_idx) = self.am
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, e)| e.data.len())
                    .map(|(i, _)| i)
                {
                    let evicted = self.am.remove(largest_idx);
                    self.size_bytes -= evicted.data.len();
                } else if let Some(evicted) = self.am.pop() {
                    self.size_bytes -= evicted.data.len();
                }
            }
        }
    }
}

static CACHE: Mutex<TwoQueueCache> = Mutex::new(TwoQueueCache::new());
static CACHE_LIMIT_BYTES: AtomicUsize = AtomicUsize::new(12 * 1024 * 1024); // Default 12MB
static A1_IN_LIMIT: AtomicUsize = AtomicUsize::new(3 * 1024 * 1024); // Default 3MB

pub fn read_blocks<R: Read>(
    disk_out: &mut impl Write,
    disk_raw: &mut R,
    start_block_id: u64,
    num_blocks: usize,
    block_size: usize,
) -> Result<Vec<u8>> {
    {
        let mut cache = CACHE.lock().unwrap();
        if let Some((entry, in_am)) = cache.remove_overlap(start_block_id, num_blocks) {
            let offset_blocks = (start_block_id - entry.start_block_id) as usize;
            let byte_start = offset_blocks * block_size;
            let byte_end = byte_start + num_blocks * block_size;
            let data = entry.data[byte_start..byte_end].to_vec();
            
            if in_am {
                cache.am.insert(0, entry);
            } else {
                cache.a1_in.insert(0, entry);
            }
            return Ok(data);
        }
    }

    flush_disk_write_buffer(disk_out)?;

    disk_out.write_all(format!("get block {} {}\n", start_block_id, num_blocks).as_bytes())?;
    disk_out.flush()?;
    let mut buf = vec![0u8; num_blocks * block_size];
    disk_raw.read_exact(&mut buf)?;

    {
        let mut cache = CACHE.lock().unwrap();
        cache.insert(CacheEntry {
            start_block_id,
            num_blocks,
            data: buf.clone(),
        }, CACHE_LIMIT_BYTES.load(Ordering::SeqCst), A1_IN_LIMIT.load(Ordering::SeqCst));
    }

    Ok(buf)
}

pub fn read_blocks_nocache<R: Read>(
    disk_out: &mut impl Write,
    disk_raw: &mut R,
    start_block_id: u64,
    num_blocks: usize,
    block_size: usize,
) -> Result<Vec<u8>> {
    {
        let mut cache = CACHE.lock().unwrap();
        if let Some((entry, in_am)) = cache.remove_overlap(start_block_id, num_blocks) {
            let offset_blocks = (start_block_id - entry.start_block_id) as usize;
            let byte_start = offset_blocks * block_size;
            let byte_end = byte_start + num_blocks * block_size;
            let data = entry.data[byte_start..byte_end].to_vec();
            
            if in_am {
                cache.am.insert(0, entry);
            } else {
                cache.a1_in.insert(0, entry);
            }
            return Ok(data);
        }
    }

    flush_disk_write_buffer(disk_out)?;

    disk_out.write_all(format!("get block {} {}\n", start_block_id, num_blocks).as_bytes())?;
    disk_out.flush()?;
    let mut buf = vec![0u8; num_blocks * block_size];
    disk_raw.read_exact(&mut buf)?;

    // We purposely do NOT insert into CACHE to avoid polluting it with massive sequential scans.
    Ok(buf)
}

pub fn init_anon_block_allocator(
    disk_out: &mut impl Write,
    disk_buf: &mut impl BufRead,
) -> Result<()> {
    let mut base = NEXT_ANON_BLOCK.load(Ordering::SeqCst);
    if base == 0 {
        base = ask_disk_line(disk_out, disk_buf, "get anon-start-block\n")?.parse::<u64>()?;
        NEXT_ANON_BLOCK.store(base, Ordering::SeqCst);
        BASE_ANON_BLOCK.store(base, Ordering::SeqCst);
    }
    Ok(())
}

pub fn get_anon_start_block(
    disk_out: &mut impl Write,
    disk_buf: &mut impl BufRead,
) -> Result<u64> {
    ask_disk_line(disk_out, disk_buf, "get anon-start-block\n")?.parse::<u64>().map_err(|e| e.into())
}

pub fn allocate_anon_block_chunk(num_blocks: u64) -> u64 {
    NEXT_ANON_BLOCK.fetch_add(num_blocks, Ordering::SeqCst)
}

pub fn rewind_anon_block_allocator() {
    let base = BASE_ANON_BLOCK.load(Ordering::SeqCst);
    if base > 0 {
        NEXT_ANON_BLOCK.store(base, Ordering::SeqCst);

        // Bulletproof Cache Correctness: Remove cached anonymous blocks so that they don't accidentally
        // serve stale mismatched-chunk data to the next operator reusing this space.
        let mut cache = CACHE.lock().unwrap();
        cache.retain_anon_blocks(base);
    }
}

pub fn write_blocks(
    disk_out: &mut impl Write,
    start_block_id: u64,
    num_blocks: usize,
    data: &[u8],
) -> Result<()> {
    {
        let mut buf = DISK_WRITE_BUFFER.lock().unwrap();
        buf.extend_from_slice(format!("put block {} {}\n", start_block_id, num_blocks).as_bytes());
        buf.extend_from_slice(data);
        if buf.len() >= DISK_BUFFER_LIMIT.load(Ordering::SeqCst) {
            disk_out.write_all(&buf)?;
            disk_out.flush()?;
            buf.clear();
        }
    }

    {
        let mut cache = CACHE.lock().unwrap();
        
        if let Some(idx) = cache.am.iter().position(|e| e.start_block_id == start_block_id && e.num_blocks == num_blocks) {
            let removed = cache.am.remove(idx);
            cache.size_bytes -= removed.data.len();
        } else if let Some(idx) = cache.a1_in.iter().position(|e| e.start_block_id == start_block_id && e.num_blocks == num_blocks) {
            let removed = cache.a1_in.remove(idx);
            cache.size_bytes -= removed.data.len();
        }

        cache.insert(CacheEntry {
            start_block_id,
            num_blocks,
            data: data.to_vec(),
        }, CACHE_LIMIT_BYTES.load(Ordering::SeqCst), A1_IN_LIMIT.load(Ordering::SeqCst));
    }

    Ok(())
}
