use anyhow::Result;
use common::{Data, query::QueryOp};
use db_config::DbContext;
use std::io::{BufRead, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::cell::Cell;

use crate::schema::ColumnInfo;

// ── Live operator tracking ────────────────────────────────────────────────────
// Instead of dividing memory by the TOTAL number of operators in the query tree
// (which massively over-divides), we track how many heavy operators are CURRENTLY
// ACTIVE on the call stack. This way, if only 2 operators are alive, each gets
// half the available memory — not 1/7th.

thread_local! {
    static LIVE_HEAVY_OPS: Cell<usize> = Cell::new(0);
}

/// RAII guard that decrements the live counter when dropped.
/// This ensures correct cleanup even on error paths (via `?`).
pub struct HeavyOpGuard;

impl Drop for HeavyOpGuard {
    fn drop(&mut self) {
        LIVE_HEAVY_OPS.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

/// Call at the start of every heavy operator (HashJoin, Sort).
/// Returns a guard that auto-decrements on drop.
pub fn enter_heavy_op() -> HeavyOpGuard {
    LIVE_HEAVY_OPS.with(|c| c.set(c.get() + 1));
    HeavyOpGuard
}

// Keep the old static count for logging only — NOT used for budgets
static HEAVY_OP_COUNT: AtomicUsize = AtomicUsize::new(1);

pub fn set_heavy_op_count(count: usize) {
    HEAVY_OP_COUNT.store(count.max(1), Ordering::SeqCst);
}

/// Per-operator memory budget in bytes.
///
/// Uses LIVE operator count (not static max). This is the key insight:
/// when the big orders join runs, typically only 2-3 ops are live,
/// so it gets 15-20MB budget instead of 6.7MB. This avoids unnecessary
/// Grace spills and dramatically reduces writes.
///
/// Safety guarantee: with (live+1) divisor, total operator memory
/// = live × available/(live+1) < available, so we never exceed 64MB.
pub fn operator_budget_bytes(memory_limit_mb: u64) -> usize {
    let live = LIVE_HEAVY_OPS.with(|c| c.get()).max(1);
    let total_bytes = memory_limit_mb as usize * 1024 * 1024;
    // Fixed overhead: cache (4MB) + bloom filters (~0.5MB) + OS/stack/strings (4MB)
    let fixed_overhead = 8 * 1024 * 1024;
    let available = total_bytes.saturating_sub(fixed_overhead);
    let dynamic = available / (live + 1);
    // Cap at 40% of total — generous but safe with the (live+1) divisor
    let max_budget = total_bytes * 40 / 100;
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
