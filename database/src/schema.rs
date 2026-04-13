use anyhow::{Context, Result};
use common::query::QueryOp;
use common::DataType;
use db_config::DbContext;

#[derive(Clone, Debug)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: DataType,
}

/// Compute the output schema for a query op without executing it.
pub fn get_schema(op: &QueryOp, ctx: &DbContext) -> Result<Vec<ColumnInfo>> {
    match op {
        QueryOp::Scan(scan_data) => {
            let table_spec = ctx
                .get_table_specs()
                .iter()
                .find(|t| t.file_id == scan_data.table_id)
                .with_context(|| format!("Table '{}' not found in db config", scan_data.table_id))?;
            Ok(table_spec
                .column_specs
                .iter()
                .map(|c| ColumnInfo { name: c.column_name.clone(), data_type: c.data_type.clone() })
                .collect())
        }
        QueryOp::Filter(f) => get_schema(&f.underlying, ctx),
        QueryOp::Sort(s) => get_schema(&s.underlying, ctx),
        QueryOp::Project(p) => {
            let child = get_schema(&p.underlying, ctx)?;
            p.column_name_map
                .iter()
                .map(|(from, to)| {
                    let col = child
                        .iter()
                        .find(|c| c.name == *from)
                        .with_context(|| format!("Project: column '{}' not found", from))?;
                    Ok(ColumnInfo { name: to.clone(), data_type: col.data_type.clone() })
                })
                .collect()
        }
        QueryOp::Cross(c) => {
            let mut left = get_schema(&c.left, ctx)?;
            let right = get_schema(&c.right, ctx)?;
            left.extend(right);
            Ok(left)
        }
        QueryOp::HashJoin(c) => {
            let mut left = get_schema(&c.left, ctx)?;
            let right = get_schema(&c.right, ctx)?;
            left.extend(right);
            Ok(left)
        }
    }
}
