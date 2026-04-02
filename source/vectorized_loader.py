"""
vectorized_loader.py - Generic vectorized out-of-core CSV loader.

Implements the vector-at-a-time execution model used by modern column-store
databases (MonetDB, Vertica, ClickHouse). Processes the CSV in fixed-size
vectors (batches of rows) and applies filter predicates during loading.

Key benefits:
- Memory bounded: only one vector + accumulated candidates in memory.
- CPU cache friendly: processes contiguous arrays of same-type values.
- Predicate short-circuiting: skip entire vectors if first predicate fails.
- Works with any CSV schema — filter predicates are passed as parameters.
"""

import csv
from column_store import ColumnStore
from csv_loader import _cast_value, _detect_schema


# Default vector size — number of rows processed per batch.
VECTOR_SIZE = 4096


def iter_load_vectors(filepath, schema=None, vector_size=VECTOR_SIZE):
    """
    Yield ColumnStore batches of up to vector_size rows (no filtering).

    This is used to run the same full pipeline per vector and merge results,
    enabling an apples-to-apples comparison against the full-load path.
    """
    if schema is None:
        header, schema = _detect_schema(filepath)
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = [col.strip() for col in next(reader)]

    min_cols = len(header)

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        while True:
            store = ColumnStore()
            for name in header:
                store.add_column(name, schema.get(name, "str"))

            rows_in_vector = 0
            for _ in range(vector_size):
                row = next(reader, None)
                if row is None:
                    break
                if len(row) < min_cols:
                    continue

                row_dict = {}
                for j, name in enumerate(header):
                    raw = row[j].strip()
                    row_dict[name] = _cast_value(raw, schema.get(name, "str"))
                store.append_row(row_dict)
                rows_in_vector += 1

            if rows_in_vector == 0:
                break

            yield store


def load_and_filter_vectorized(filepath, predicates, schema=None,
                                vector_size=VECTOR_SIZE):
    """
    Load CSV data using vectorized execution with predicate pushdown.

    Reads the CSV in fixed-size vectors. For each vector, filter predicates
    are applied column-at-a-time in the order given (put the most selective
    predicate first for best short-circuiting). Only rows passing ALL
    predicates are kept.

    Args:
        filepath: Path to the input CSV file.
        predicates: List of (col_name, test_func) tuples. Each test_func
                    takes a raw (cast) value and returns True to keep.
                    Predicates are applied in order — put the most
                    selective one first for best short-circuit performance.
                    Example:
                        [("year", lambda v: v == 2020),
                         ("town", lambda v: v in {"BEDOK", "CLEMENTI"})]
        schema: Optional dict mapping column_name -> type. If None,
                types are auto-detected.
        vector_size: Number of rows per vector batch (default 4096).

    Returns:
        A ColumnStore containing ONLY the rows passing all predicates.
    """
    # Detect or use provided schema
    if schema is None:
        header, schema = _detect_schema(filepath)
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = [col.strip() for col in next(reader)]

    # Validate that predicate columns exist in header
    header_set = set(header)
    for col_name, _ in predicates:
        if col_name not in header_set:
            raise ValueError(
                f"Predicate column '{col_name}' not found in CSV header. "
                f"Available columns: {header}")

    # Build column index map for fast access
    col_idx = {name: i for i, name in enumerate(header)}
    min_cols = len(header)

    # Output store
    candidates = ColumnStore()
    for name in header:
        candidates.add_column(name, schema.get(name, "str"))

    # Statistics
    total_rows = 0
    vectors_processed = 0
    vectors_skipped = 0

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        while True:
            # === LOAD ONE VECTOR OF RAW DATA ===
            # Store raw parsed values per column for this vector
            vec_data = {name: [] for name in header}
            rows_in_vector = 0

            for _ in range(vector_size):
                row = next(reader, None)
                if row is None:
                    break
                if len(row) < min_cols:
                    continue

                for j, name in enumerate(header):
                    raw = row[j].strip()
                    vec_data[name].append(
                        _cast_value(raw, schema.get(name, "str")))

                rows_in_vector += 1

            if rows_in_vector == 0:
                break

            total_rows += rows_in_vector
            vectors_processed += 1

            # === VECTORIZED FILTERING — COLUMN AT A TIME ===
            mask = [True] * rows_in_vector

            skipped = False
            for pred_col, pred_func in predicates:
                col_data = vec_data[pred_col]

                # Apply predicate only to rows still passing
                mask = [mask[i] and pred_func(col_data[i])
                        for i in range(rows_in_vector)]

                # SHORT-CIRCUIT: if no rows survive, discard entire vector
                if not any(mask):
                    vectors_skipped += 1
                    skipped = True
                    break

            if skipped:
                continue

            # === EXTRACT SURVIVORS INTO CANDIDATE STORE ===
            for i in range(rows_in_vector):
                if mask[i]:
                    row_dict = {name: vec_data[name][i] for name in header}
                    candidates.append_row(row_dict)

            # Temporary vector arrays go out of scope and are freed here

    print(f"  Vectorized loading complete (vector_size={vector_size})")
    print(f"  Total rows scanned: {total_rows}")
    print(f"  Vectors processed: {vectors_processed}")
    print(f"  Vectors skipped (short-circuit): {vectors_skipped} "
          f"({100 * vectors_skipped / max(vectors_processed, 1):.1f}%)")
    print(f"  Candidates in memory: {candidates.num_rows} "
          f"({100 * candidates.num_rows / max(total_rows, 1):.1f}% of total)")

    return candidates
