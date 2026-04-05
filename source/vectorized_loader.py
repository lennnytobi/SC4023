"""
vectorized_loader.py - Vectorized out-of-core CSV loader.

Reads the CSV in fixed-size batches (vectors) and applies filters
column-at-a-time, skipping entire batches early when possible.
"""

import csv
from column_store import ColumnStore
from csv_loader import _cast_value, _detect_schema


# Number of rows per batch.
VECTOR_SIZE = 4096


def iter_load_vectors(filepath, schema=None, vector_size=VECTOR_SIZE):
    """Yield ColumnStore batches of up to vector_size rows with no filtering."""
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
    Load CSV with predicate pushdown using vectorized execution.

    Applies predicates column-at-a-time per batch. Put the most selective
    predicate first for best short-circuit performance. Returns a ColumnStore
    containing only rows passing all predicates.

    predicates: list of (col_name, test_func) where test_func(value) -> bool
    """
    if schema is None:
        header, schema = _detect_schema(filepath)
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = [col.strip() for col in next(reader)]

    header_set = set(header)
    for col_name, _ in predicates:
        if col_name not in header_set:
            raise ValueError(
                f"Predicate column '{col_name}' not found in CSV header. "
                f"Available columns: {header}")

    col_idx = {name: i for i, name in enumerate(header)}
    min_cols = len(header)

    candidates = ColumnStore()
    for name in header:
        candidates.add_column(name, schema.get(name, "str"))

    total_rows = 0
    vectors_processed = 0
    vectors_skipped = 0

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        while True:
            # Load one vector of raw data
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

            # Filter column-at-a-time; short-circuit if no rows survive
            mask = [True] * rows_in_vector

            skipped = False
            for pred_col, pred_func in predicates:
                col_data = vec_data[pred_col]
                mask = [mask[i] and pred_func(col_data[i])
                        for i in range(rows_in_vector)]

                if not any(mask):
                    vectors_skipped += 1
                    skipped = True
                    break

            if skipped:
                continue

            # Add surviving rows to output
            for i in range(rows_in_vector):
                if mask[i]:
                    row_dict = {name: vec_data[name][i] for name in header}
                    candidates.append_row(row_dict)

    print(f"  Vectorized loading complete (vector_size={vector_size})")
    print(f"  Total rows scanned: {total_rows}")
    print(f"  Vectors processed: {vectors_processed}")
    print(f"  Vectors skipped (short-circuit): {vectors_skipped} "
          f"({100 * vectors_skipped / max(vectors_processed, 1):.1f}%)")
    print(f"  Candidates in memory: {candidates.num_rows} "
          f"({100 * candidates.num_rows / max(total_rows, 1):.1f}% of total)")

    return candidates
