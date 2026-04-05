"""
result_writer.py - Write ColumnStore query results to a CSV file.
"""


def write_query_results(filepath, store, row_indices, columns=None):
    """Write selected rows from a ColumnStore to a CSV file."""
    if columns is None:
        columns = store.column_names()

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(",".join(columns) + "\n")
        for idx in row_indices:
            values = [str(store.get_decoded(c, idx)) for c in columns]
            f.write(",".join(values) + "\n")

    print(f"Written {len(row_indices)} rows to {filepath}")
