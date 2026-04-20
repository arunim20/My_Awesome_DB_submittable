use anyhow::Result;
use common::{Data, query::QueryOp};
use db_config::DbContext;
use std::io::{BufRead, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::schema::ColumnInfo;

// ── Dynamic memory budget ─────────────────────────────────────────────────────
//
// Before query execution, `main.rs` walks the optimised query tree and counts
// the maximum number of memory-heavy operators (HashJoin, Sort, Cross) that
// can be alive simultaneously on the call stack.  This count is stored here
// and read by each operator to compute its fair share of the memory budget.
//
// Budget model (accounting for 2× peak during packing/flush):
//   • Fixed overhead: ~19 MB (6 MB disk cache + ~13 MB runtime/stack/code)
//     + N × 128 KB for bloom filters.
//   • Available = 64 MB − fixed overhead.
//   • During flush, ONE operator peaks at 2× its budget while the remaining
//     N−1 hold 1× each.  Total = (N+1) × budget.
//   • ⇒ budget = available / (N+1), capped at 15%.
//
// Result matrix (64 MB limit):
//   1 op  → min(available/2, 15%) = 15% = 9.6 MB   ← max in-memory hash join
//   3 ops → min(available/4, 15%) = 15% = 9.6 MB   ← all get full budget
//   4 ops → min(8.8 MB, 9.6 MB)  = 8.8 MB          ← dynamic kicks in
//   5 ops → 7.4 MB each
//   6 ops → 6.3 MB each

static HEAVY_OP_COUNT: AtomicUsize = AtomicUsize::new(1);

/// Called once from `main.rs` after optimisation to set the operator count.
pub fn set_heavy_op_count(count: usize) {
    HEAVY_OP_COUNT.store(count.max(1), Ordering::SeqCst);
}

/// Returns the memory budget (in bytes) that a single heavy operator may use
/// for its in-memory working set (e.g. right-side rows, sort chunks).
pub fn operator_budget_bytes(memory_limit_mb: u64) -> usize {
    let count = HEAVY_OP_COUNT.load(Ordering::SeqCst).max(1);
    let total_bytes = memory_limit_mb as usize * 1024 * 1024;

    // Fixed overhead: disk cache (6 MB) + runtime/stack/code (~13 MB) + bloom filters
    let fixed_overhead = 19 * 1024 * 1024 + count * 131072;
    let available = total_bytes.saturating_sub(fixed_overhead);

    // During flush, one operator peaks at 2× while others hold 1×
    // total_peak = (N+1) × budget  →  budget = available / (N+1)
    let dynamic = available / (count + 1);

    // Cap at 15% — the original hash join budget that maximises in-memory joins
    let max_budget = total_bytes * 15 / 100;
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
