"""
csv_loader.py - Generic CSV file parser for column-oriented data loading.

Loads any CSV file into a ColumnStore. Column types can be specified via
a schema dict, or auto-detected from the data. Works with any CSV
regardless of column names, order, or count.
"""

import csv
from column_store import ColumnStore


def _auto_detect_type(value):
    """
    Attempt to detect whether a string value is int, float, or str.

    Tries int first, then float, falls back to str.
    """
    try:
        int(value)
        return "int"
    except ValueError:
        pass
    try:
        float(value)
        return "float"
    except ValueError:
        return "str"


def _detect_schema(filepath, sample_rows=100):
    """
    Auto-detect column types by sampling the first N rows of a CSV.

    For each column, checks if all sampled values are int, float, or str.
    If mixed numeric types exist, promotes to float. If any non-numeric
    value is found, the column becomes str.

    Args:
        filepath: Path to the CSV file.
        sample_rows: Number of rows to sample for type detection.

    Returns:
        Tuple of (header_list, schema_dict) where schema maps
        column_name -> "int" | "float" | "str".
    """
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = [col.strip() for col in next(reader)]

        # Track detected types per column
        col_types = {name: set() for name in header}

        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            for j, name in enumerate(header):
                if j < len(row) and row[j].strip():
                    col_types[name].add(_auto_detect_type(row[j].strip()))

    # Resolve final type per column
    schema = {}
    for name in header:
        types = col_types[name]
        if not types or "str" in types:
            schema[name] = "str"
        elif "float" in types:
            schema[name] = "float"
        else:
            schema[name] = "int"

    return header, schema


def _cast_value(raw, col_type):
    """Cast a raw string value to the target column type."""
    raw = raw.strip()
    if col_type == "int":
        return int(float(raw))  # handles "123.0" -> 123
    elif col_type == "float":
        return float(raw)
    else:
        return raw  # str — will be dictionary-encoded by ColumnStore


def load_csv(filepath, schema=None):
    """
    Load any CSV file into a generic ColumnStore.

    If a schema is provided, it maps column_name -> type ("int", "float",
    "str"). If not provided, types are auto-detected from the data.

    Args:
        filepath: Path to the CSV file.
        schema: Optional dict mapping column_name -> "int"|"float"|"str".
                If None, types are auto-detected.

    Returns:
        A populated ColumnStore instance.
    """
    # Detect or use provided schema
    if schema is None:
        header, schema = _detect_schema(filepath)
    else:
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            header = [col.strip() for col in next(reader)]

    # Initialize the column store with the detected/provided schema
    store = ColumnStore()
    for name in header:
        col_type = schema.get(name, "str")  # default to str if not in schema
        store.add_column(name, col_type)

    min_cols = len(header)
    col_types = [schema.get(name, "str") for name in header]
    type_codes = [0 if t == "str" else 1 if t == "int" else 2 for t in col_types]
    col_arrays = [store._columns[name] for name in header]
    dicts = [store._dictionaries.get(name) for name in header]

    # Load all rows (fast path: append directly to column arrays)
    with open(filepath, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header

        for row in reader:
            if len(row) < min_cols:
                continue  # skip malformed rows

            for j in range(min_cols):
                raw = row[j].strip()
                type_code = type_codes[j]

                if type_code == 0:
                    col_arrays[j].append(dicts[j].encode(raw))
                elif type_code == 1:
                    try:
                        col_arrays[j].append(int(raw))
                    except ValueError:
                        col_arrays[j].append(int(float(raw)))
                else:
                    col_arrays[j].append(float(raw))

            store.num_rows += 1

    print(f"Loaded {store.num_rows} rows, {len(header)} columns from {filepath}")
    for name in header:
        if store.get_type(name) == "str":
            d = store.get_dictionary(name)
            print(f"  {name} ({store.get_type(name)}): "
                  f"{d.size()} distinct values")

    return store
