"""
result_writer.py - Generic CSV writer for query results from a ColumnStore.

Writes matching rows to a CSV file using late materialization — string
columns are decoded only for the rows being written, not for the entire
dataset.
"""


def write_query_results(filepath, store, row_indices, columns=None):
    """
    Write rows from a ColumnStore to a CSV file.

    Args:
        filepath: Output file path.
        store: ColumnStore containing the data.
        row_indices: List of row indices to write.
        columns: List of column names to include. If None, all columns.
    """
    if columns is None:
        columns = store.column_names()

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(",".join(columns) + "\n")
        for idx in row_indices:
            values = [str(store.get_decoded(c, idx)) for c in columns]
            f.write(",".join(values) + "\n")

    print(f"Written {len(row_indices)} rows to {filepath}")
