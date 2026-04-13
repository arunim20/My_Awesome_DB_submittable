use anyhow::Result;
use std::io::{BufRead, Read, Write};

/// Send a text command to disk and read back the one-line text response.
pub fn ask_disk_line<R: BufRead>(
    disk_out: &mut impl Write,
    disk_buf: &mut R,
    cmd: &str,
) -> Result<String> {
    disk_out.write_all(cmd.as_bytes())?;
    disk_out.flush()?;
    let mut line = String::new();
    disk_buf.read_line(&mut line)?;
    Ok(line.trim().to_string())
}

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

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

static CACHE: Mutex<Vec<CacheEntry>> = Mutex::new(Vec::new());
static CACHE_SIZE_BYTES: Mutex<usize> = Mutex::new(0);
const CACHE_LIMIT_BYTES: usize = 30 * 1024 * 1024; // 30MB

pub fn read_blocks<R: Read>(
    disk_out: &mut impl Write,
    disk_raw: &mut R,
    start_block_id: u64,
    num_blocks: usize,
    block_size: usize,
) -> Result<Vec<u8>> {
    {
        let mut cache = CACHE.lock().unwrap();
        if let Some(idx) = cache.iter().position(|e| e.start_block_id == start_block_id && e.num_blocks == num_blocks) {
            let entry = cache.remove(idx);
            let data = entry.data.clone();
            cache.insert(0, entry);
            return Ok(data);
        }
    }

    disk_out.write_all(format!("get block {} {}\n", start_block_id, num_blocks).as_bytes())?;
    disk_out.flush()?;
    let mut buf = vec![0u8; num_blocks * block_size];
    disk_raw.read_exact(&mut buf)?;

    {
        let mut cache = CACHE.lock().unwrap();
        let mut size = CACHE_SIZE_BYTES.lock().unwrap();
        
        while *size + buf.len() > CACHE_LIMIT_BYTES && !cache.is_empty() {
            let removed = cache.pop().unwrap();
            *size -= removed.data.len();
        }
        
        if buf.len() <= CACHE_LIMIT_BYTES {
            cache.insert(0, CacheEntry {
                start_block_id,
                num_blocks,
                data: buf.clone(),
            });
            *size += buf.len();
        }
    }

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
        let mut size = CACHE_SIZE_BYTES.lock().unwrap();
        
        cache.retain(|e| {
            if e.start_block_id >= base {
                *size -= e.data.len();
                false // remove
            } else {
                true // keep base table chunks safely!
            }
        });
    }
}

pub fn write_blocks(
    disk_out: &mut impl Write,
    start_block_id: u64,
    num_blocks: usize,
    data: &[u8],
) -> Result<()> {
    disk_out.write_all(format!("put block {} {}\n", start_block_id, num_blocks).as_bytes())?;
    disk_out.write_all(data)?;
    disk_out.flush()?;

    {
        let mut cache = CACHE.lock().unwrap();
        let mut size = CACHE_SIZE_BYTES.lock().unwrap();
        
        if let Some(idx) = cache.iter().position(|e| e.start_block_id == start_block_id && e.num_blocks == num_blocks) {
            let removed = cache.remove(idx);
            *size -= removed.data.len();
        }

        while *size + data.len() > CACHE_LIMIT_BYTES && !cache.is_empty() {
            let removed = cache.pop().unwrap();
            *size -= removed.data.len();
        }
        
        if data.len() <= CACHE_LIMIT_BYTES {
            cache.insert(0, CacheEntry {
                start_block_id,
                num_blocks,
                data: data.to_vec(),
            });
            *size += data.len();
        }
    }

    Ok(())
}
