use anyhow::{Context, Result};
use common::query::{ProjectData, QueryOp};
use common::{Data, DataType};
use db_config::DbContext;
use std::io::{BufRead, Read, Write};

use crate::ops::execute_op;
use crate::schema::{get_schema, ColumnInfo};

pub fn execute_project<W, R>(
    project: &ProjectData,
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
    let child_schema = get_schema(&project.underlying, ctx)?;

    // Pre-compute (source_index, output_name) for each projected column
    let projections: Vec<(usize, String, DataType)> = project
        .column_name_map
        .iter()
        .map(|(from, to)| {
            let idx = child_schema
                .iter()
                .position(|c| c.name == *from)
                .with_context(|| format!("Project: column '{}' not found", from))?;
            Ok((idx, to.clone(), child_schema[idx].data_type.clone()))
        })
        .collect::<Result<_>>()?;

    // ── Optimization: Project(Scan) fusion ───────────────────────────────────
    // When the direct child is a Scan, we can skip decoding unused columns
    // entirely at the block level using decode_row_projected, avoiding all
    // heap allocations for dropped columns.
    if let QueryOp::Scan(scan_data) = &*project.underlying {
        let mut keep_indices: Vec<usize> = projections.iter().map(|(i, _, _)| *i).collect();
        keep_indices.sort_unstable();
        keep_indices.dedup();

        // Map original index -> position in keep_indices for output reordering
        let pos_map: Vec<usize> = projections
            .iter()
            .map(|(orig_idx, _, _)| keep_indices.iter().position(|&k| k == *orig_idx).unwrap())
            .collect();

        let out_schema: Vec<ColumnInfo> = projections
            .iter()
            .map(|(_, name, dt)| ColumnInfo { name: name.clone(), data_type: dt.clone() })
            .collect();

        crate::ops::scan::execute_scan_with_opts(
            &scan_data.table_id,
            ctx,
            disk_out,
            disk_buf,
            block_size,
            memory_limit_mb,
            Some(&keep_indices),
            None,
            &mut |decoded_row| {
                // decoded_row has keep_indices.len() elements in keep_indices order
                // We need to reorder to match projections order
                let reordered: Vec<Data> = pos_map.iter().map(|&p| decoded_row[p].clone()).collect();
                on_row(&reordered)
            },
        )?;

        return Ok(out_schema);
    }

    // ── Default path: Project over arbitrary child ────────────────────────────
    execute_op(
        &project.underlying,
        ctx,
        disk_out,
        disk_buf,
        block_size,
        memory_limit_mb,
        &mut |row| {
            let projected: Vec<Data> =
                projections.iter().map(|(idx, _, _)| row[*idx].clone()).collect();
            on_row(&projected)
        },
    )?;

    let out_schema = projections
        .into_iter()
        .map(|(_, name, dt)| ColumnInfo { name, data_type: dt })
        .collect();

    Ok(out_schema)
}
