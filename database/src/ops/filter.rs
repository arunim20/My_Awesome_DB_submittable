use anyhow::Result;
use common::query::{FilterData, QueryOp};
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
    let predicates = &filter.predicates;

    // ── Optimization 1: Filter(Scan) fusion ──────────────────────────────────
    // Push filter predicates directly into the scan loop. Rows are discarded
    // at decode time — no heap allocation is done for filtered-out rows.
    if let QueryOp::Scan(scan_data) = &*filter.underlying {
        return crate::ops::scan::execute_scan_with_opts(
            &scan_data.table_id,
            ctx,
            disk_out,
            disk_buf,
            block_size,
            memory_limit_mb,
            None,           // no column projection (handled by parent Project)
            Some(predicates),
            on_row,
        );
    }

    // ── Optimization 2: Filter(Project(Scan)) fusion ──────────────────────────
    // When Filter sits directly above a Project that sits above a Scan, fuse all
    // three into a single scan pass: decode only needed columns AND inline filter.
    if let QueryOp::Project(proj) = &*filter.underlying {
        if let QueryOp::Scan(scan_data) = &*proj.underlying {
            let child_schema = get_schema(&proj.underlying, ctx)?;

            let projections: Vec<(usize, String)> = proj.column_name_map
                .iter()
                .map(|(from, to)| {
                    let idx = child_schema.iter().position(|c| c.name == *from)
                        .ok_or_else(|| anyhow::anyhow!("Project column '{}' not found", from))?;
                    Ok((idx, to.clone()))
                })
                .collect::<Result<_>>()?;

            let mut keep_indices: Vec<usize> = projections.iter().map(|(i, _)| *i).collect();
            keep_indices.sort_unstable();
            keep_indices.dedup();

            let pos_map: Vec<usize> = projections
                .iter()
                .map(|(orig_idx, _)| keep_indices.iter().position(|&k| k == *orig_idx).unwrap())
                .collect();

            let proj_schema = get_schema(&*filter.underlying, ctx)?;

            // Only inline predicates that reference columns inside the projection
            let proj_col_names: std::collections::HashSet<&str> =
                proj_schema.iter().map(|c| c.name.as_str()).collect();
            // Partition predicates: those resolvable within the projection vs cross-column
            let all_inline = predicates.iter().all(|p| {
                proj_col_names.contains(p.column_name.as_str())
                    && match &p.value {
                        common::query::ComparisionValue::Column(other) => proj_col_names.contains(other.as_str()),
                        _ => true,
                    }
            });

            if all_inline {
                // Full three-way fusion: Project+Filter both absorbed into Scan
                return crate::ops::scan::execute_scan_with_opts(
                    &scan_data.table_id,
                    ctx,
                    disk_out,
                    disk_buf,
                    block_size,
                    memory_limit_mb,
                    Some(&keep_indices),
                    Some(predicates),
                    &mut |decoded_row| {
                        let reordered: Vec<Data> = pos_map.iter().map(|&p| decoded_row[p].clone()).collect();
                        on_row(&reordered)
                    },
                );
            }
            // If there are outer predicates (cross-column) we can't fully inline,
            // fall through to default path below.
        }
    }

    // ── Optimization 3: Filter(Cross) — push predicates into cross join eval ──
    if let QueryOp::Cross(cross) = &*filter.underlying {
        return crate::ops::cross::execute_cross_with_filter(
            cross, ctx, disk_out, disk_buf, block_size, memory_limit_mb, Some(predicates), on_row
        );
    }

    // ── Default path ──────────────────────────────────────────────────────────
    let schema = get_schema(&filter.underlying, ctx)?;
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
