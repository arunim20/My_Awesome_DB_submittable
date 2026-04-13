use anyhow::Result;
use common::query::FilterData;
use common::Data;
use db_config::DbContext;
use std::io::{BufRead, Read, Write};

use crate::ops::execute_op;
use crate::row::apply_predicates;
use crate::schema::{get_schema, ColumnInfo};

pub fn execute_filter<W, R>(
    filter: &FilterData,
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
    let schema = get_schema(&filter.underlying, ctx)?;
    let predicates = &filter.predicates;

    // Intercept Cross Joins to perform Filter condition evaluations BEFORE row materialization (O(NxM) allocation dodge)
    if let common::query::QueryOp::Cross(cross) = &*filter.underlying {
        return crate::ops::cross::execute_cross_with_filter(
            cross, ctx, disk_out, disk_buf, block_size, memory_limit_mb, Some(predicates), on_row
        );
    }

    execute_op(
        &filter.underlying,
        ctx,
        disk_out,
        disk_buf,
        block_size,
        memory_limit_mb,
        &mut |row| {
            if apply_predicates(row, &schema, predicates)? {
                on_row(row)?;
            }
            Ok(())
        },
    )
}
