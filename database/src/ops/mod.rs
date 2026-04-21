use anyhow::Result;
use common::{Data, query::QueryOp};
use db_config::DbContext;
use std::io::{BufRead, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::schema::ColumnInfo;

// ── Dynamic memory budget ─────────────────────────────────────────────────────
static HEAVY_OP_COUNT: AtomicUsize = AtomicUsize::new(1);

pub fn set_heavy_op_count(count: usize) {
    HEAVY_OP_COUNT.store(count.max(1), Ordering::SeqCst);
}

/// Per-operator memory budget in bytes.
/// encode_row_len already accounts for Vec<Data> heap overhead
/// (40 bytes/element + 64 bytes/Vec), overestimating by ~50%.
/// At 12% = 7.68MB, actual heap at budget = ~5.1MB.
/// Grace switch peak = 2 × 5.1MB + 23MB runtime = 33MB < 64MB.
pub fn operator_budget_bytes(memory_limit_mb: u64) -> usize {
    let count = HEAVY_OP_COUNT.load(Ordering::SeqCst).max(1);
    let total_bytes = memory_limit_mb as usize * 1024 * 1024;
    let fixed_overhead = 19 * 1024 * 1024;
    let available = total_bytes.saturating_sub(fixed_overhead);
    let dynamic = available / (count + 1);
    let max_budget = total_bytes * 12 / 100;
    std::cmp::min(dynamic, max_budget)
}
pub mod cross;
pub mod filter;
pub mod hash_join;
pub mod project;
pub mod scan;
pub mod sort;

pub fn execute_op<W, R>(
    op: &QueryOp,
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
    match op {
        QueryOp::Scan(d)    => scan::execute_scan(&d.table_id, ctx, disk_out, disk_buf, block_size, memory_limit_mb, on_row),
        QueryOp::Filter(d)  => filter::execute_filter(d, ctx, disk_out, disk_buf, block_size, memory_limit_mb, on_row),
        QueryOp::Project(d) => project::execute_project(d, ctx, disk_out, disk_buf, block_size, memory_limit_mb, on_row),
        QueryOp::Cross(d)   => cross::execute_cross(d, ctx, disk_out, disk_buf, block_size, memory_limit_mb, on_row),
        QueryOp::HashJoin(d)=> hash_join::execute_hash_join(d, ctx, disk_out, disk_buf, block_size, memory_limit_mb, on_row),
        QueryOp::Sort(d)    => sort::execute_sort(d, ctx, disk_out, disk_buf, block_size, memory_limit_mb, on_row),
    }
}
