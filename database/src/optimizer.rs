use anyhow::Result;
use common::query::{QueryOp, FilterData, ComparisionValue};
use db_config::DbContext;

use crate::schema::get_schema;

pub fn optimize(op: QueryOp, ctx: &DbContext) -> Result<QueryOp> {
    // Step 1: Reorder joins so filtered tables are deepest in the tree
    let reordered = reorder_joins(op, ctx)?;
    // Step 2: Filter pushdown + cross→hash-join conversion
    let rewritten = optimize_rewrites(reordered, ctx)?;
    // Step 3: Projection pushdown
    let pushed = pushdown_projections(rewritten, None, ctx)?;
    // Step 4: Left/right swap within each join (smaller side as build)
    let ordered = optimize_join_order(pushed, ctx)?;
    Ok(ordered)
}

// ── Join reordering ───────────────────────────────────────────────────────────
// Flattens a Filter(Cross(Cross(...))) tree, scores each base table by its
// estimated post-filter cardinality, and rebuilds a left-deep join tree with
// smallest (most-filtered) tables deepest. This ensures subsequent filter
// pushdown pushes predicates all the way to the scans.

fn reorder_joins(op: QueryOp, ctx: &DbContext) -> Result<QueryOp> {
    match op {
        QueryOp::Filter(f) => {
            let underlying = reorder_joins(*f.underlying, ctx)?;

            // Only reorder if the child is a pure tree of Cross joins over Scans
            // and has more than 2 leaves. Check BEFORE consuming ownership.
            if is_pure_cross_tree(&underlying) && count_cross_leaves(&underlying) > 2 {
                let mut tables = Vec::new();
                flatten_crosses(underlying, &mut tables);
                let n = tables.len();

                eprintln!("[optimizer] join-reorder: flattened {} base tables", n);

                // ── 1. Compute schemas for every table ──────────────────
                let schemas: Vec<std::collections::HashSet<String>> = {
                    let mut v = Vec::with_capacity(n);
                    for t in tables.iter() {
                        let s = get_schema(t, ctx)?;
                        v.push(s.iter().map(|c| c.name.clone()).collect());
                    }
                    v
                };

                // ── 2. Build join-edge graph from equi-join predicates ──
                // join_edges[i][j] = true means tables i and j share an
                // equality predicate (e.g. A.col = B.col).
                let mut join_edges = vec![vec![false; n]; n];
                for pred in &f.predicates {
                    if matches!(pred.operator, common::query::ComparisionOperator::EQ) {
                        if let ComparisionValue::Column(other_col) = &pred.value {
                            let mut has_main = Vec::new();
                            let mut has_other = Vec::new();
                            for (i, schema) in schemas.iter().enumerate() {
                                if schema.contains(&pred.column_name) { has_main.push(i); }
                                if schema.contains(other_col) { has_other.push(i); }
                            }
                            for &m in &has_main {
                                for &o in &has_other {
                                    if m != o {
                                        join_edges[m][o] = true;
                                        join_edges[o][m] = true;
                                    }
                                }
                            }
                        }
                    }
                }

                // ── 3. Effective cardinality per table ──────────────────
                let effective_cards: Vec<f64> = (0..n).map(|i| {
                    let filter_count = f.predicates.iter().filter(|p| {
                        if !schemas[i].contains(&p.column_name) { return false; }
                        match &p.value {
                            ComparisionValue::Column(c) => schemas[i].contains(c),
                            _ => true,
                        }
                    }).count();
                    let raw = estimate_cardinality(&tables[i], ctx) as f64;
                    raw * (0.1_f64).powi(filter_count as i32)
                }).collect();

                // ── 4. Greedy join-graph-aware ordering ─────────────────
                // Always prefer a table that is CONNECTED (has a join edge)
                // to any table already in the tree. Among candidates, pick
                // the one with the smallest effective cardinality.
                let mut used = vec![false; n];
                let mut order = Vec::with_capacity(n);

                // Seed: smallest effective cardinality
                let start = (0..n)
                    .min_by(|&a, &b| effective_cards[a].partial_cmp(&effective_cards[b])
                        .unwrap_or(std::cmp::Ordering::Equal))
                    .unwrap();
                used[start] = true;
                order.push(start);

                for _ in 1..n {
                    let mut best_connected: Option<(usize, f64)> = None;
                    let mut best_unconnected: Option<(usize, f64)> = None;

                    for j in 0..n {
                        if used[j] { continue; }
                        let connected = order.iter().any(|&k| join_edges[j][k]);
                        let score = effective_cards[j];

                        if connected {
                            if best_connected.is_none() || score < best_connected.unwrap().1 {
                                best_connected = Some((j, score));
                            }
                        } else {
                            if best_unconnected.is_none() || score < best_unconnected.unwrap().1 {
                                best_unconnected = Some((j, score));
                            }
                        }
                    }

                    let next = if let Some((j, _)) = best_connected { j }
                               else { best_unconnected.unwrap().0 };
                    used[next] = true;
                    order.push(next);
                }

                // ── 5. Logging ──────────────────────────────────────────
                for &idx in &order {
                    eprintln!(
                        "[optimizer]   join-order: {:?} (effective_card={:.0})",
                        match &tables[idx] { QueryOp::Scan(s) => s.table_id.as_str(), _ => "??" },
                        effective_cards[idx]
                    );
                }

                // ── 6. Rebuild left-deep tree in the chosen order ───────
                let mut slots: Vec<Option<QueryOp>> =
                    tables.into_iter().map(Some).collect();
                let mut ordered_tables = Vec::new();
                for &idx in &order {
                    ordered_tables.push(slots[idx].take().unwrap());
                }

                let mut tree = ordered_tables.remove(0);
                for t in ordered_tables {
                    tree = QueryOp::Cross(common::query::CrossData {
                        left: Box::new(tree),
                        right: Box::new(t),
                    });
                }

                return Ok(QueryOp::Filter(FilterData {
                    predicates: f.predicates,
                    underlying: Box::new(tree),
                }));
            }

            Ok(QueryOp::Filter(FilterData {
                predicates: f.predicates,
                underlying: Box::new(underlying),
            }))
        }
        QueryOp::Sort(mut s) => {
            s.underlying = Box::new(reorder_joins(*s.underlying, ctx)?);
            Ok(QueryOp::Sort(s))
        }
        QueryOp::Project(mut p) => {
            p.underlying = Box::new(reorder_joins(*p.underlying, ctx)?);
            Ok(QueryOp::Project(p))
        }
        QueryOp::Cross(mut c) => {
            c.left = Box::new(reorder_joins(*c.left, ctx)?);
            c.right = Box::new(reorder_joins(*c.right, ctx)?);
            Ok(QueryOp::Cross(c))
        }
        QueryOp::HashJoin(mut h) => {
            h.left = Box::new(reorder_joins(*h.left, ctx)?);
            h.right = Box::new(reorder_joins(*h.right, ctx)?);
            Ok(QueryOp::HashJoin(h))
        }
        QueryOp::Scan(s) => Ok(QueryOp::Scan(s)),
    }
}

/// Returns true if `op` is a tree consisting only of Cross and Scan nodes.
fn is_pure_cross_tree(op: &QueryOp) -> bool {
    match op {
        QueryOp::Cross(c) => is_pure_cross_tree(&c.left) && is_pure_cross_tree(&c.right),
        QueryOp::Scan(_) => true,
        _ => false,
    }
}

/// Consumes a Cross-join tree and collects all leaf (Scan) nodes in order.
fn flatten_crosses(op: QueryOp, tables: &mut Vec<QueryOp>) {
    match op {
        QueryOp::Cross(c) => {
            flatten_crosses(*c.left, tables);
            flatten_crosses(*c.right, tables);
        }
        other => tables.push(other),
    }
}

/// Counts the number of leaf nodes in a Cross-join tree (borrows only).
fn count_cross_leaves(op: &QueryOp) -> usize {
    match op {
        QueryOp::Cross(c) => count_cross_leaves(&c.left) + count_cross_leaves(&c.right),
        _ => 1,
    }
}

fn optimize_join_order(op: QueryOp, ctx: &DbContext) -> Result<QueryOp> {
    match op {
        QueryOp::Filter(mut f) => {
            f.underlying = Box::new(optimize_join_order(*f.underlying, ctx)?);
            Ok(QueryOp::Filter(f))
        }
        QueryOp::Project(mut p) => {
            p.underlying = Box::new(optimize_join_order(*p.underlying, ctx)?);
            Ok(QueryOp::Project(p))
        }
        QueryOp::Sort(mut s) => {
            s.underlying = Box::new(optimize_join_order(*s.underlying, ctx)?);
            Ok(QueryOp::Sort(s))
        }
        QueryOp::Cross(mut c) => {
            c.left = Box::new(optimize_join_order(*c.left, ctx)?);
            c.right = Box::new(optimize_join_order(*c.right, ctx)?);
            
            let est_l = estimate_cardinality(&c.left, ctx);
            let est_r = estimate_cardinality(&c.right, ctx);
            
            if est_l < est_r {
                let left_schema = crate::schema::get_schema(&*c.left, ctx)?;
                let right_schema = crate::schema::get_schema(&*c.right, ctx)?;
                
                std::mem::swap(&mut c.left, &mut c.right);
                
                let mut map = Vec::new();
                for col in left_schema {
                    map.push((col.name.clone(), col.name.clone()));
                }
                for col in right_schema {
                    map.push((col.name.clone(), col.name.clone()));
                }
                
                return Ok(QueryOp::Project(common::query::ProjectData {
                    column_name_map: map,
                    underlying: Box::new(QueryOp::Cross(c))
                }));
            }
            Ok(QueryOp::Cross(c))
        }
        QueryOp::HashJoin(mut h) => {
            h.left = Box::new(optimize_join_order(*h.left, ctx)?);
            h.right = Box::new(optimize_join_order(*h.right, ctx)?);

            let est_l = estimate_cardinality(&h.left, ctx);
            let est_r = estimate_cardinality(&h.right, ctx);
            
            if est_l < est_r {
                let left_schema = crate::schema::get_schema(&*h.left, ctx)?;
                let right_schema = crate::schema::get_schema(&*h.right, ctx)?;
                
                std::mem::swap(&mut h.left, &mut h.right);
                std::mem::swap(&mut h.left_join_col, &mut h.right_join_col);

                let mut map = Vec::new();
                for col in left_schema {
                    map.push((col.name.clone(), col.name.clone()));
                }
                for col in right_schema {
                    map.push((col.name.clone(), col.name.clone()));
                }

                return Ok(QueryOp::Project(common::query::ProjectData {
                    column_name_map: map,
                    underlying: Box::new(QueryOp::HashJoin(h))
                }));
            }
            Ok(QueryOp::HashJoin(h))
        }
        QueryOp::Scan(s) => Ok(QueryOp::Scan(s)),
    }
}

fn optimize_rewrites(op: QueryOp, ctx: &DbContext) -> Result<QueryOp> {
    match op {
        QueryOp::Filter(mut f) => {
            f.underlying = Box::new(optimize_rewrites(*f.underlying, ctx)?);

            // Attempt Filter Push-Down
            if let QueryOp::Cross(mut c) = *f.underlying {
                let left_schema = get_schema(&c.left, ctx)?;
                let left_column_names: std::collections::HashSet<&String> = left_schema.iter().map(|c| &c.name).collect();

                let right_schema = get_schema(&c.right, ctx)?;
                let right_column_names: std::collections::HashSet<&String> = right_schema.iter().map(|c| &c.name).collect();

                let mut left_preds = Vec::new();
                let mut right_preds = Vec::new();
                let mut keeping_preds = Vec::new();

                for pred in f.predicates {
                    let mut needs_left = false;
                    let mut needs_right = false;

                    if left_column_names.contains(&pred.column_name) {
                        needs_left = true;
                    } else if right_column_names.contains(&pred.column_name) {
                        needs_right = true;
                    }

                    if let ComparisionValue::Column(other_col) = &pred.value {
                        if left_column_names.contains(other_col) {
                            needs_left = true;
                        } else if right_column_names.contains(other_col) {
                            needs_right = true;
                        }
                    }

                    if needs_left && !needs_right {
                        left_preds.push(pred);
                    } else if needs_right && !needs_left {
                        right_preds.push(pred);
                    } else {
                        // Needs both (e.g., A.id = B.id) or external
                        keeping_preds.push(pred);
                    }
                }

                if !left_preds.is_empty() {
                    let new_left = QueryOp::Filter(FilterData {
                        predicates: left_preds,
                        underlying: c.left,
                    });
                    c.left = Box::new(optimize_rewrites(new_left, ctx)?);
                }

                if !right_preds.is_empty() {
                    let new_right = QueryOp::Filter(FilterData {
                        predicates: right_preds,
                        underlying: c.right,
                    });
                    c.right = Box::new(optimize_rewrites(new_right, ctx)?);
                }

                let mut equi_join_pred_idx = None;
                for (i, pred) in keeping_preds.iter().enumerate() {
                    if matches!(pred.operator, common::query::ComparisionOperator::EQ) {
                        if let ComparisionValue::Column(other_col) = &pred.value {
                            let left_has_main = left_column_names.contains(&pred.column_name);
                            let right_has_other = right_column_names.contains(other_col);
                            let right_has_main = right_column_names.contains(&pred.column_name);
                            let left_has_other = left_column_names.contains(other_col);

                            if (left_has_main && right_has_other) || (right_has_main && left_has_other) {
                                equi_join_pred_idx = Some(i);
                                break;
                            }
                        }
                    }
                }

                if let Some(idx) = equi_join_pred_idx {
                    let pred = keeping_preds.remove(idx);
                    let (left_col, right_col) = if left_column_names.contains(&pred.column_name) {
                        (pred.column_name, match pred.value { ComparisionValue::Column(c) => c, _ => unreachable!() })
                    } else {
                        (match pred.value { ComparisionValue::Column(c) => c, _ => unreachable!() }, pred.column_name)
                    };

                    let hash_join = QueryOp::HashJoin(common::query::HashJoinData {
                        left: c.left,
                        right: c.right,
                        left_join_col: left_col,
                        right_join_col: right_col,
                    });

                    if keeping_preds.is_empty() {
                        return Ok(hash_join);
                    } else {
                        let new_filter = QueryOp::Filter(FilterData {
                            predicates: keeping_preds,
                            underlying: Box::new(hash_join),
                        });
                        return optimize_rewrites(new_filter, ctx);
                    }
                }

                if keeping_preds.is_empty() {
                    return Ok(QueryOp::Cross(c));
                } else {
                    return Ok(QueryOp::Filter(FilterData {
                        predicates: keeping_preds,
                        underlying: Box::new(QueryOp::Cross(c)),
                    }));
                }
            }

            // Normal fallback (maybe wrapped over scan/sort)
            Ok(QueryOp::Filter(f))
        }
        QueryOp::Sort(mut s) => {
            s.underlying = Box::new(optimize_rewrites(*s.underlying, ctx)?);

            // Sort elision (Sort -> Sort) => Outer Sort wins
            if let QueryOp::Sort(inner_sort) = *s.underlying {
                s.underlying = inner_sort.underlying; // drop the inner sort completely
            }

            Ok(QueryOp::Sort(s))
        }
        QueryOp::Project(mut p) => {
            p.underlying = Box::new(optimize_rewrites(*p.underlying, ctx)?);
            Ok(QueryOp::Project(p))
        }
        QueryOp::Cross(mut c) => {
            c.left = Box::new(optimize_rewrites(*c.left, ctx)?);
            c.right = Box::new(optimize_rewrites(*c.right, ctx)?);
            Ok(QueryOp::Cross(c))
        }
        QueryOp::HashJoin(mut h) => {
            h.left = Box::new(optimize_rewrites(*h.left, ctx)?);
            h.right = Box::new(optimize_rewrites(*h.right, ctx)?);
            Ok(QueryOp::HashJoin(h))
        }
        QueryOp::Scan(s) => Ok(QueryOp::Scan(s)),
    }
}

pub fn pushdown_projections(
    op: QueryOp,
    required_cols: Option<std::collections::HashSet<String>>,
    ctx: &DbContext,
) -> Result<QueryOp> {
    match op {
        QueryOp::Project(mut p) => {
            let mut new_map = Vec::new();
            let mut child_req = std::collections::HashSet::new();

            for (from, to) in &p.column_name_map {
                if let Some(req_cols) = &required_cols {
                    if req_cols.contains(to) {
                        new_map.push((from.clone(), to.clone()));
                        child_req.insert(from.clone());
                    }
                } else {
                    new_map.push((from.clone(), to.clone()));
                    child_req.insert(from.clone());
                }
            }
            p.column_name_map = new_map;
            p.underlying = Box::new(pushdown_projections(*p.underlying, Some(child_req), ctx)?);
            Ok(QueryOp::Project(p))
        }
        QueryOp::Filter(mut f) => {
            let next_req = if let Some(mut req) = required_cols {
                for pred in &f.predicates {
                    req.insert(pred.column_name.clone());
                    if let ComparisionValue::Column(c) = &pred.value {
                        req.insert(c.clone());
                    }
                }
                Some(req)
            } else {
                None
            };
            f.underlying = Box::new(pushdown_projections(*f.underlying, next_req, ctx)?);
            Ok(QueryOp::Filter(f))
        }
        QueryOp::Sort(mut s) => {
            let next_req = if let Some(mut req) = required_cols {
                for spec in &s.sort_specs {
                    req.insert(spec.column_name.clone());
                }
                Some(req)
            } else {
                None
            };
            s.underlying = Box::new(pushdown_projections(*s.underlying, next_req, ctx)?);
            Ok(QueryOp::Sort(s))
        }
        QueryOp::Cross(mut c) => {
            let next_left;
            let next_right;

            if let Some(req) = required_cols {
                let left_schema = crate::schema::get_schema(&*c.left, ctx)?;
                let right_schema = crate::schema::get_schema(&*c.right, ctx)?;
                
                let left_names: std::collections::HashSet<String> = left_schema.into_iter().map(|col| col.name).collect();
                let right_names: std::collections::HashSet<String> = right_schema.into_iter().map(|col| col.name).collect();

                let mut l_req = std::collections::HashSet::new();
                let mut r_req = std::collections::HashSet::new();

                for col_name in req {
                    if left_names.contains(&col_name) {
                        l_req.insert(col_name.clone());
                    }
                    if right_names.contains(&col_name) {
                        r_req.insert(col_name.clone());
                    }
                }

                next_left = Some(l_req);
                next_right = Some(r_req);
            } else {
                next_left = None;
                next_right = None;
            }

            c.left = Box::new(pushdown_projections(*c.left, next_left, ctx)?);
            c.right = Box::new(pushdown_projections(*c.right, next_right, ctx)?);
            Ok(QueryOp::Cross(c))
        }
        QueryOp::HashJoin(mut h) => {
            let next_left;
            let next_right;

            if let Some(req) = required_cols {
                let left_schema = crate::schema::get_schema(&*h.left, ctx)?;
                let right_schema = crate::schema::get_schema(&*h.right, ctx)?;
                
                let left_names: std::collections::HashSet<String> = left_schema.into_iter().map(|col| col.name).collect();
                let right_names: std::collections::HashSet<String> = right_schema.into_iter().map(|col| col.name).collect();

                let mut l_req = std::collections::HashSet::new();
                let mut r_req = std::collections::HashSet::new();

                for col_name in req {
                    if left_names.contains(&col_name) {
                        l_req.insert(col_name.clone());
                    }
                    if right_names.contains(&col_name) {
                        r_req.insert(col_name.clone());
                    }
                }
                
                l_req.insert(h.left_join_col.clone());
                r_req.insert(h.right_join_col.clone());

                next_left = Some(l_req);
                next_right = Some(r_req);
            } else {
                next_left = None;
                next_right = None;
            }

            h.left = Box::new(pushdown_projections(*h.left, next_left, ctx)?);
            h.right = Box::new(pushdown_projections(*h.right, next_right, ctx)?);
            Ok(QueryOp::HashJoin(h))
        }
        QueryOp::Scan(s) => {
            if let Some(req) = required_cols {
                let temp_scan = QueryOp::Scan(common::query::ScanData { table_id: s.table_id.clone() });
                let full_schema = crate::schema::get_schema(&temp_scan, ctx)?;
                let fs_len = full_schema.len();
                
                let used_cols: Vec<_> = full_schema.into_iter()
                    .filter(|col| req.contains(&col.name))
                    .collect();
                    
                if used_cols.len() < fs_len && !used_cols.is_empty() {
                    let mut column_name_map = Vec::new();
                    for col in used_cols {
                        column_name_map.push((col.name.clone(), col.name.clone()));
                    }
                    return Ok(QueryOp::Project(common::query::ProjectData {
                        column_name_map,
                        underlying: Box::new(QueryOp::Scan(s)),
                    }));
                }
            }
            Ok(QueryOp::Scan(s))
        }
    }
}

pub fn estimate_cardinality(op: &QueryOp, ctx: &DbContext) -> usize {
    match op {
        QueryOp::Scan(s) => {
            if let Some(spec) = ctx.get_table_specs().iter().find(|t| t.file_id == s.table_id) {
                if let Some(col) = spec.column_specs.first() {
                    if let Some(stats) = &col.stats {
                        let mut card = None;
                        let mut dens = None;
                        for s in stats {
                            match s {
                                db_config::statistics::ColumnStat::CardinalityStat(c) => card = Some(c.0),
                                db_config::statistics::ColumnStat::DensityStat(d) => dens = Some(d.0),
                                _ => {}
                            }
                        }
                        if let (Some(c), Some(d)) = (card, dens) {
                            if d > 0.0 {
                                return (c as f64 / d as f64) as usize;
                            }
                        }
                        if let Some(c) = card {
                            return c as usize;
                        }
                    }
                }
            }
            1000
        }
        QueryOp::Filter(f) => {
            let base = estimate_cardinality(&f.underlying, ctx) as f64;
            (base * 0.1).max(1.0) as usize
        }
        QueryOp::Sort(s) => estimate_cardinality(&s.underlying, ctx),
        QueryOp::Project(p) => estimate_cardinality(&p.underlying, ctx),
        QueryOp::Cross(c) => {
            estimate_cardinality(&c.left, ctx) * estimate_cardinality(&c.right, ctx)
        }
        QueryOp::HashJoin(h) => {
            std::cmp::max(estimate_cardinality(&h.left, ctx), estimate_cardinality(&h.right, ctx))
        }
    }
}

/// Compute the maximum number of memory-heavy operators (HashJoin, Sort, Cross)
/// that can be alive simultaneously on the Rust call stack during execution.
///
/// For binary operators (HashJoin, Cross), the right child is fully executed
/// first, then the left child is streamed.  At any moment only ONE child is
/// active alongside the parent, so we take `max(left_depth, right_depth)`.
///
/// This drives the dynamic per-operator memory budget in `ops::mod.rs`.
pub fn max_concurrent_heavy_ops(op: &QueryOp) -> usize {
    match op {
        QueryOp::HashJoin(h) => {
            let l = max_concurrent_heavy_ops(&h.left);
            let r = max_concurrent_heavy_ops(&h.right);
            1 + std::cmp::max(l, r)
        }
        QueryOp::Sort(s) => {
            1 + max_concurrent_heavy_ops(&s.underlying)
        }
        QueryOp::Cross(c) => {
            let l = max_concurrent_heavy_ops(&c.left);
            let r = max_concurrent_heavy_ops(&c.right);
            1 + std::cmp::max(l, r)
        }
        QueryOp::Filter(f) => max_concurrent_heavy_ops(&f.underlying),
        QueryOp::Project(p) => max_concurrent_heavy_ops(&p.underlying),
        QueryOp::Scan(_) => 0,
    }
}
