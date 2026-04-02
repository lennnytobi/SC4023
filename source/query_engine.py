"""
query_engine.py - General-purpose (x, y) grid scan over a column store.

Provides two implementations:
1. run_query_naive(): Uses the generic Query API for each (x,y) — simple
   but slow. Works with any column store and any set of filters.
2. run_query(): Optimized with incremental range accumulation and sweep.
   Uses raw column access for performance.

Both produce identical results. The naive version shows the generic API
in action; the optimized version shows what custom optimization can achieve.

The query pattern is:
    For each (x, y) in a grid:
        SELECT MIN(agg_col) FROM store
        WHERE <pre_filters>
          AND range_col >= range_start
          AND range_col <= range_start + x - 1
          AND sweep_col >= y
    Keep results where rounded MIN <= threshold.
"""

from collections import defaultdict


def run_query_naive(store, pre_filters,
                    range_col, range_start, x_min, x_max,
                    sweep_col, y_min, y_max,
                    agg_col, threshold=None, range_cap=None):
    """
    Naive baseline using the generic store.query() API.

    For every (x, y) pair, builds a fresh query with all filters and
    calls .min() — the simplest possible approach.

    Parameters
    ----------
    store : ColumnStore
    pre_filters : list of (col_name, op, value)
        Static filters applied to every query (e.g. year==2020, town in [...]).
    range_col : str
        Column for the x-dimension expanding range.
    range_start : int
        Lower bound for range_col.
    x_min, x_max : int
        Grid bounds for x (range expansion steps).
    sweep_col : str
        Column for the y-dimension (>= threshold sweep).
    y_min, y_max : int
        Grid bounds for y.
    agg_col : str
        Column to compute MIN on.
    threshold : number or None
        If set, only keep results where round(min) <= threshold.
    range_cap : int or None
        If set, cap range_start + x - 1 at this value (e.g. 12 for months).
    """
    results = {}

    for x in range(x_min, x_max + 1):
        upper = range_start + x - 1
        if range_cap is not None:
            upper = min(upper, range_cap)

        for y in range(y_min, y_max + 1):
            q = store.query()
            for col, op, val in pre_filters:
                q = q.filter(col, op, val)
            q = q.filter(range_col, ">=", range_start)
            q = q.filter(range_col, "<=", upper)
            q = q.filter(sweep_col, ">=", y)

            row_idx, min_val = q.min(agg_col)

            if row_idx is not None:
                rounded = round(min_val)
                if threshold is None or rounded <= threshold:
                    results[(x, y)] = (row_idx, rounded)

    print(f"  Naive: found {len(results)} valid (x, y) pairs")
    return results


def run_query(store, pre_filters,
              range_col, range_start, x_min, x_max,
              sweep_col, y_min, y_max,
              agg_col, threshold=None, range_cap=None):
    """
    Optimized query using raw column access for performance.

    Two key optimizations over the naive approach:
    1. Incremental range accumulation: x=2 reuses x=1 candidates.
    2. Sweep with running min: answers all y values in O(max_sweep).

    Pre-filters are applied via the Query API to get candidate rows,
    then the optimized x/y loop uses raw column arrays.

    Parameters are the same as run_query_naive.
    Note: sweep_col must be an integer column (values used as array indices).
    """
    # Phase 1: Pre-filter candidates using the Query API
    q = store.query()
    for col, op, val in pre_filters:
        q = q.filter(col, op, val)
    candidate_rows = q.execute()

    print(f"  Pre-filtered to {len(candidate_rows)} candidate rows")

    if not candidate_rows:
        print(f"  Found 0 valid (x, y) pairs")
        return {}

    # Get raw column arrays for direct access
    range_vals = store.get_column(range_col)
    sweep_vals = store.get_column(sweep_col)
    agg_vals = store.get_column(agg_col)

    # Group candidates by range_col value
    candidates_by_range = defaultdict(list)
    for i in candidate_rows:
        candidates_by_range[range_vals[i]].append(i)

    # Determine max sweep value for array sizing
    max_sweep = max(sweep_vals[i] for i in candidate_rows)
    max_sweep = max(max_sweep, y_max)

    # Phase 2: Optimized (x, y) loop
    best_agg = [float("inf")] * (max_sweep + 1)
    best_row = [-1] * (max_sweep + 1)
    results = {}

    last_added_upper = range_start - 1

    for x in range(x_min, x_max + 1):
        upper = range_start + x - 1
        if range_cap is not None:
            upper = min(upper, range_cap)

        # Incrementally add newly included range values only.
        # If upper is capped (e.g. x=7 and x=8 both map to month=12),
        # this loop naturally adds nothing for later x values.
        for r in range(last_added_upper + 1, upper + 1):
            for idx in candidates_by_range.get(r, []):
                sv = sweep_vals[idx]
                av = agg_vals[idx]
                if av < best_agg[sv]:
                    best_agg[sv] = av
                    best_row[sv] = idx

        if upper > last_added_upper:
            last_added_upper = upper

        # Sweep from high to low, tracking running min
        run_min = float("inf")
        run_min_row = -1

        for s in range(max_sweep, y_min - 1, -1):
            if best_agg[s] < run_min:
                run_min = best_agg[s]
                run_min_row = best_row[s]

            if y_min <= s <= y_max and run_min_row != -1:
                rounded = round(run_min)
                if threshold is None or rounded <= threshold:
                    results[(x, s)] = (run_min_row, rounded)

    print(f"  Found {len(results)} valid (x, y) pairs")
    return results
