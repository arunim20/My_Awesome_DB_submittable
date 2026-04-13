use anyhow::Result;
use common::{Data, query::QueryOp};
use db_config::DbContext;
use std::io::{BufRead, Read, Write};

use crate::schema::ColumnInfo;

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
