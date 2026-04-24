# Database Optimizations Review and Ranking Plan

This document summarizes the optimizations already present in `database/`, identifies the current bottlenecks that are most likely limiting benchmark rank, and proposes a prioritized roadmap for improving throughput and memory efficiency.

The goal is not just to list database ideas, but to tie each optimization directly to this codebase.

## 1. What the current engine already does well

The current implementation already contains several meaningful execution-time and optimizer-time improvements:

1. Join reordering with DP over filtered base tables.
   Code: [optimizer.rs](./src/optimizer.rs)
   `reorder_joins` flattens pure cross trees, estimates effective cardinalities, builds a join-edge graph from equi-join predicates, and uses `find_best_join_order` to avoid expensive Cartesian blowups.

2. Filter pushdown and `Cross -> HashJoin` rewrite.
   Code: [optimizer.rs](./src/optimizer.rs)
   `optimize_rewrites` pushes single-side predicates below joins and rewrites a cross plus equi-predicate into a `HashJoin`.

3. Projection pushdown.
   Code: [optimizer.rs](./src/optimizer.rs)
   `pushdown_projections` reduces row width flowing through joins and sorts by pushing required-column sets down the tree.

4. Sort elision on physically ordered scans.
   Code: [optimizer.rs](./src/optimizer.rs)
   `sort_is_physically_ordered` detects when a requested ascending sort already matches the table’s physical order and removes the sort operator.

5. Scan-time pruning from statistics.
   Code: [ops/scan.rs](./src/ops/scan.rs)
   The scan operator already uses `RangeStat` for whole-table pruning and uses `IsPhysicallyOrdered` to do a binary-search-style skip for `GT`, `GTE`, and `EQ`.

6. Early predicate evaluation during scan and cross join.
   Code: [ops/filter.rs](./src/ops/filter.rs), [ops/cross.rs](./src/ops/cross.rs)
   Filter execution avoids unnecessary materialization by evaluating scan predicates during table scan and split predicates during cross join.

7. External sort with cascading merge.
   Code: [ops/sort.rs](./src/ops/sort.rs)
   The sort operator supports in-memory sort, run generation, bounded-width merge, and multi-pass cascading merge.

8. Anonymous scratch-space reuse and a small block cache.
   Code: [disk.rs](./src/disk.rs)
   Scratch block allocation is reused across operators, and the cache helps repeated block accesses.

9. Memory budgeting by heavy-operator depth.
   Code: [main.rs](./src/main.rs), [ops/mod.rs](./src/ops/mod.rs), [optimizer.rs](./src/optimizer.rs)
   The engine computes `max_concurrent_heavy_ops` and divides memory budget across expensive operators.

This is already beyond a naive iterator pipeline. The ranking gains now are likely to come from reducing constant factors, avoiding repeated decoding work, and improving estimation quality.

## 2. Highest-impact bottlenecks

These are the areas most likely holding back performance now.

### P0. Missing `hash_join.rs` is a correctness and ranking blocker

Code: [ops/mod.rs](./src/ops/mod.rs)

`mod.rs` still declares `pub mod hash_join;` and dispatches `QueryOp::HashJoin(...)`, but the file `database/src/ops/hash_join.rs` is missing in the current worktree. Since the optimizer aggressively rewrites cross joins into hash joins, this is not a minor cleanup issue. It is a critical blocker for both correctness and benchmark performance.

If this file was deleted accidentally, restoring it is the first priority.

### P1. Scan predicate evaluation is doing repeated schema lookups per row

Code: [ops/scan.rs](./src/ops/scan.rs)

Inside `apply_predicates`, the engine repeatedly runs:

- `schema.iter().position(...)` to find the left column index
- another `schema.iter().position(...)` for RHS column references

This happens for every row and every predicate. For wide tables or long scans, this creates avoidable `O(num_preds * num_cols)` overhead per row before the actual comparison work even begins.

Best fix:

1. Precompile pushed predicates once before the scan loop.
2. Store direct column indexes and pre-coerced literal values.
3. Evaluate predicates using integer indexes only.

Expected impact:

- Lower CPU time on all filtered scans
- Bigger wins on selective queries over large tables
- Better benchmark stability because predicate overhead becomes predictable

### P1. Projection pushdown is only logical, not physical

Code: [optimizer.rs](./src/optimizer.rs), [row.rs](./src/row.rs), [ops/scan.rs](./src/ops/scan.rs)

`pushdown_projections` inserts a `Project` above `Scan`, which is still useful because it narrows tuples before sort/join/output. But `decode_row` always decodes the full row, including all columns, before projection happens.

That means scan cost is still proportional to full row width, not required column width.

Best fix:

1. Add a scan-path API that receives an optional vector of required column indexes.
2. Decode only the referenced columns.
3. For skipped string columns, advance offsets without allocating `String`.

Expected impact:

- Large improvement when queries touch a small subset of columns
- Lower heap allocation pressure
- Better sort/join speed because rows become smaller earlier

This is one of the strongest ranking opportunities in the current design.

### P1. Disk cache has expensive lookup and clone behavior

Code: [disk.rs](./src/disk.rs)

The cache currently uses:

- `Vec<CacheEntry>`
- linear search via `cache.iter().position(...)`
- front insertion/removal for LRU behavior
- full `Vec<u8>` cloning on cache hit

This creates three problems:

1. Lookup is `O(cache_size)`.
2. LRU maintenance shifts entries in a vector.
3. Cache hits still copy the entire block buffer.

Best fix:

1. Replace the cache with `HashMap<(start_block, num_blocks), entry>` plus an LRU list.
2. Store block data in `Arc<[u8]>` or equivalent shared backing storage.
3. Return borrowed/shared data instead of cloning the whole buffer.

Expected impact:

- Better performance for merge-heavy sort and repeated scratch reads
- Lower memory-copy overhead
- More predictable latency on cache hits

### P1. Sort spends too much time cloning metadata and decoding row-by-row

Code: [ops/sort.rs](./src/ops/sort.rs)

The sort implementation is solid structurally, but several constant-factor inefficiencies remain:

1. `sort_keys.clone()` and `sort_keys.to_vec()` are repeated when pushing heap items.
2. `schema.clone()` and `schema.to_vec()` are copied into many streamers.
3. Merge paths decode rows into `Vec<Vec<Data>>`, which causes high allocation churn.
4. Sorting compares full `Data` rows repeatedly instead of comparing precomputed keys where possible.

Best fix:

1. Share sort metadata by reference instead of cloning it per heap item.
2. Precompute compact sort-key descriptors once.
3. Consider storing `(sort_key, row)` during run generation.
4. Reuse row buffers or decode blocks into reusable arenas.

Expected impact:

- Lower CPU cost in spill-heavy workloads
- Fewer heap allocations
- Stronger performance on large sorts where merge dominates

### P1. Cardinality estimation is still very coarse

Code: [optimizer.rs](./src/optimizer.rs)

The optimizer currently uses:

- a fixed `0.1` multiplier per filter
- `left * right` for cross join
- `max(left, right)` for hash join

This is reasonable as a first heuristic, but ranking-sensitive workloads often depend on choosing the best of multiple plausible orders. The current model can mis-rank plans when:

- one predicate is much more selective than another
- column correlations matter
- join selectivity differs across tables

Best fix:

1. Use available stats per predicate instead of a uniform `0.1`.
2. Estimate equality selectivity from cardinality and density stats.
3. Combine multiple predicates more carefully, possibly with caps/floors.
4. Penalize plans by estimated output width, not just row count.

Expected impact:

- Better join order choices
- Smaller intermediates
- Better use of the existing optimizer architecture

## 3. Medium-impact improvements

### P2. Scan pruning can be made tighter

Code: [ops/scan.rs](./src/ops/scan.rs)

The current ordered-scan optimization is helpful, but still conservative:

1. Binary search reads the first row of a block only.
2. It handles one qualifying predicate and then stops.
3. It does not use end-of-block fence values.
4. It does not combine lower-bound and upper-bound pruning into a tighter range.

Best fix:

1. Store or derive per-block min/max fence keys for ordered columns.
2. Binary search to the first relevant block and stop at the last relevant block.
3. Support both lower-bound and upper-bound narrowing.

Expected impact:

- Fewer block reads on range filters
- Bigger advantage on ordered tables

### P2. Cross join still decodes right blocks repeatedly

Code: [ops/cross.rs](./src/ops/cross.rs)

The cross operator uses a blocked algorithm and is already much better than naive nested loops. But each fetched right block is unpacked into fresh row vectors, and the right side is fully re-scanned for every left chunk.

This is acceptable for unavoidable cross joins, but still expensive.

Best fix:

1. Reuse decoded right-block buffers across left chunks when memory allows.
2. Pre-evaluate join predicate metadata once.
3. Keep decoded scratch rows in a more compact representation than `Vec<Vec<Data>>`.

Expected impact:

- Lower CPU overhead when cross joins cannot be rewritten
- Better performance on wide tuples

### P2. Sort run generation can avoid full-row comparisons

Code: [ops/sort.rs](./src/ops/sort.rs)

During in-memory chunk sort, the comparator repeatedly touches `Data` values inside rows. For wide rows and multi-column sorts, repeated comparator calls become expensive.

Best fix:

1. Decorate rows with extracted sort keys before `sort_by`.
2. Sort on compact keys, then emit the original row payload.

Expected impact:

- Faster in-memory sort phase
- Better scaling with wide rows and many sort keys

## 4. Architectural upgrades most likely to improve rank

If the goal is to move up the leaderboard rather than just polish the code, the strongest next upgrades are:

### A. Reinstate and harden hash join

Because the optimizer already rewrites joins into `HashJoin`, a good hash join gives immediate benefit across many query shapes. The best version here would:

1. Build on the smaller input.
2. Use precomputed join-column indexes.
3. Spill by partition if the build side exceeds budget.
4. Avoid cloning joined rows until a match is confirmed.

If your earlier `hash_join.rs` already did some of this, restoring and tuning it is likely the fastest path to real benchmark gains.

### B. Physical late materialization or partial decoding

This codebase is row-oriented, so full late materialization may be too large a redesign for the assignment. But partial decoding is realistic and high-return.

Minimum viable version:

1. Track required column indexes at `Scan`.
2. Decode only predicate, join, sort, and projected columns.
3. Keep a compact row layout internally for downstream operators.

This could produce a bigger gain than yet another optimizer heuristic.

### C. Precompiled execution metadata

Several operators repeatedly rediscover schema positions and rebuild small metadata structures at runtime.

Examples:

- scan predicate column indexes
- filter predicate column indexes
- sort key indexes
- join key indexes

Best fix:

1. Add a lightweight physical-plan preparation pass after logical optimization.
2. Resolve all repeated schema/name lookups into integer positions once.

Expected impact:

- Lower CPU overhead across the board
- Cleaner operator implementations
- Easier future optimizations

## 5. Recommended implementation order

If the objective is maximum ranking gain per unit time, the implementation order should be:

1. Restore and verify `hash_join.rs`.
2. Precompile predicate indexes for scan and filter.
3. Implement physical partial decoding in scan.
4. Improve sort metadata sharing and reduce merge allocations.
5. Replace the vector-based disk cache with a lookup-friendly LRU structure.
6. Upgrade cardinality estimation using existing stats.
7. Tighten ordered-scan block pruning using lower and upper bounds.

This order prioritizes changes that should affect the largest number of benchmark queries.

## 6. Assignment-ready summary

A strong way to present this project is:

1. The engine already performs logical optimization:
   join reordering, predicate pushdown, projection pushdown, sort elimination, and cross-to-hash-join rewriting.

2. The engine already performs physical optimization:
   blocked scans, range-stat pruning, ordered-scan skipping, external sort, scratch-space reuse, and dynamic memory budgeting.

3. The next ranking gains are mostly in reducing per-row overhead:
   precompiled predicate metadata, physical projection/partial decoding, better sort merge efficiency, and a faster cache.

4. The single most important immediate issue is the missing hash join implementation in the current worktree, because the optimizer depends on it.

## 7. Practical next-step checklist

- Restore `database/src/ops/hash_join.rs` and make sure the current tree builds.
- Refactor scan predicates into a compiled representation with column indexes.
- Extend scan to decode only needed columns.
- Remove repeated `clone()`/`to_vec()` metadata churn from sort merge.
- Replace cache linear search with direct lookup.
- Upgrade estimator quality using stats already present in config.

If we implement only the top three items well, the engine should become materially more competitive on the assignment benchmarks.
