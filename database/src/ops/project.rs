use anyhow::{Context, Result};
use common::query::ProjectData;
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

    let out_schema: Vec<ColumnInfo> = projections
        .iter()
        .map(|(_, name, dt)| ColumnInfo { name: name.clone(), data_type: dt.clone() })
        .collect();

    // Intercept Scan to perform Physical Projection
    if let common::query::QueryOp::Scan(scan) = &*project.underlying {
        let mut required = vec![false; child_schema.len()];
        for (idx, _, _) in &projections {
            required[*idx] = true;
        }
        crate::ops::scan::execute_scan(
            scan, ctx, disk_out, disk_buf, block_size, memory_limit_mb, None, Some(&required), &mut |row| {
                let projected: Vec<Data> =
                    projections.iter().map(|(idx, _, _)| row[*idx].clone()).collect();
                on_row(&projected)
            }
        )?;
        return Ok(out_schema);
    }

    // Intercept Filter(Scan) to perform Physical Projection + Filter Pushdown
    if let common::query::QueryOp::Filter(filter) = &*project.underlying {
        if let common::query::QueryOp::Scan(scan) = &*filter.underlying {
            let mut required = vec![false; child_schema.len()];
            for (idx, _, _) in &projections {
                required[*idx] = true;
            }
            for p in &filter.predicates {
                if let Some(pos) = child_schema.iter().position(|c| c.name == p.column_name) {
                    required[pos] = true;
                }
                if let common::query::ComparisionValue::Column(c) = &p.value {
                    if let Some(pos) = child_schema.iter().position(|col| col.name == *c) {
                        required[pos] = true;
                    }
                }
            }
            crate::ops::scan::execute_scan(
                scan, ctx, disk_out, disk_buf, block_size, memory_limit_mb, Some(&filter.predicates), Some(&required), &mut |row| {
                    let projected: Vec<Data> =
                        projections.iter().map(|(idx, _, _)| row[*idx].clone()).collect();
                    on_row(&projected)
                }
            )?;
            return Ok(out_schema);
        }
    }

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

    Ok(out_schema)
}
