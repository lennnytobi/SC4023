# HDB Resale Flat Column-Store Query Engine

**Course:** CE/CZ4123 / SC4023 - Big Data Management
**Institution:** Nanyang Technological University, College of Computing and Data Science
**Language:** Python 3 (no external dependencies)

A column-oriented database engine built from scratch for analyzing Singapore HDB (Housing and Development Board) resale flat transaction records. The system demonstrates core column-store principles including dictionary encoding, late materialization, vectorized execution, and predicate pushdown.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Module Reference](#module-reference)
   - [column_store.py](#column_storepy---column-oriented-database-engine)
   - [csv_loader.py](#csv_loaderpy---generic-csv-parser)
   - [vectorized_loader.py](#vectorized_loaderpy---out-of-core-vectorized-loader)
   - [query_engine.py](#query_enginepy---hdb-query-implementations)
   - [result_writer.py](#result_writerpy---csv-output-writer)
   - [main.py](#mainpy---entry-point--orchestration)
4. [Data Flow](#data-flow)
5. [Query Logic](#query-logic)
6. [Optimizations](#optimizations)
7. [Usage](#usage)
8. [Output Format](#output-format)
9. [Project Structure](#project-structure)

---

## Project Overview

The engine processes a CSV dataset of HDB resale flat transactions (Jan 2015 - Dec 2025) to find valid `(x, y)` coordinate pairs where:

- **x** = number of consecutive months (range: 1-8)
- **y** = minimum floor area in square meters (range: 80-150)

For each `(x, y)` pair, the system finds the record with the **minimum price per square meter** among all transactions matching the student's query conditions (derived from their matriculation number). Only pairs where the rounded minimum price per sqm is <= 4725 are included in the output.

The query conditions (target year, start month, and towns) are deterministically derived from the student's NTU matriculation number.

---

## Architecture

The system follows a layered architecture that separates generic, reusable database components from HDB-specific application logic:

```
+------------------------------------------------------------------+
|                      main.py (Application Layer)                 |
|  - Matriculation number parsing                                  |
|  - HDB-specific configuration (town map, schema, thresholds)    |
|  - Post-load transformations (parse dates, derive price/sqm)    |
|  - Output formatting                                             |
+------------------------------------------------------------------+
        |                    |                       |
        v                    v                       v
+----------------+  +------------------+  +---------------------+
| csv_loader.py  |  | query_engine.py  |  | vectorized_loader.py|
| Generic CSV    |  | HDB-specific     |  | Out-of-core         |
| parsing with   |  | optimized query  |  | vectorized CSV      |
| schema         |  | strategies       |  | loading with        |
| inference      |  | (naive + fast)   |  | predicate pushdown  |
+----------------+  +------------------+  +---------------------+
        |                    |                       |
        v                    v                       v
+------------------------------------------------------------------+
|                 column_store.py (Storage Engine)                  |
|  - ColumnStore: column-oriented storage with schema              |
|  - Dictionary: string dictionary encoding                        |
|  - Query: chainable query builder with filtering & aggregation   |
+------------------------------------------------------------------+

+------------------------------------------------------------------+
|                   result_writer.py (Output Layer)                |
|  - Generic CSV writer with late materialization                  |
+------------------------------------------------------------------+
```

### Design Principles

- **Generic components:** `ColumnStore`, `csv_loader`, `vectorized_loader`, and `result_writer` are fully generic and can work with any CSV dataset.
- **Column-oriented storage:** Data is stored as arrays of column values, not arrays of rows. This enables column-at-a-time processing with better cache locality.
- **Dictionary encoding:** String columns are encoded as integer codes, reducing memory usage and enabling fast integer comparisons instead of string comparisons.
- **Late materialization:** Full row reconstruction (including string decoding) is deferred until results are needed for output, avoiding unnecessary work during filtering.

---

## Module Reference

### `column_store.py` - Column-Oriented Database Engine

The core storage engine providing column-oriented data storage, dictionary encoding, and a chainable query API.

#### Class: `Dictionary`

Implements dictionary encoding for string columns. Maps string values to sequential integer codes for compact, fast storage and comparison.

| Method | Signature | Description |
|--------|-----------|-------------|
| `encode` | `encode(value: str) -> int` | Encode a string to an integer code. Adds to dictionary if new. |
| `decode` | `decode(code: int) -> str` | Decode an integer code back to the original string. |
| `get_code` | `get_code(value: str) -> int` | Get code for a value, or `-1` if not found. |
| `size` | `size() -> int` | Return number of distinct values in the dictionary. |

**Example:**
```python
d = Dictionary()
code = d.encode("BEDOK")      # Returns 0 (first entry)
code = d.encode("CLEMENTI")   # Returns 1
d.decode(0)                   # Returns "BEDOK"
d.get_code("UNKNOWN")         # Returns -1
```

#### Class: `ColumnStore`

Generic column-oriented storage engine. Each column is stored as a Python list. String columns are automatically dictionary-encoded.

**Schema & Loading Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `add_column` | `add_column(name, col_type)` | Register a column. `col_type` is `"int"`, `"float"`, or `"str"`. |
| `append_value` | `append_value(col_name, raw_value)` | Append a value to a column. Strings are auto dictionary-encoded. |
| `append_row` | `append_row(row_dict)` | Append a full row as a dict of `{col_name: value}`. |
| `add_derived_column` | `add_derived_column(name, col_type, values)` | Add a pre-computed derived column (e.g., `price/area`). |

**Access Methods:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `get_column` | `get_column(name) -> list` | Get raw column array (encoded integers for string columns). |
| `get_value` | `get_value(col_name, row_idx) -> any` | Get a single raw value at a given row index. |
| `get_decoded` | `get_decoded(col_name, row_idx) -> any` | Get a value, decoding strings from dictionary encoding. |
| `get_dictionary` | `get_dictionary(col_name) -> Dictionary` | Get the `Dictionary` object for a string column, or `None`. |
| `get_type` | `get_type(col_name) -> str` | Get the type of a column (`"int"`, `"float"`, or `"str"`). |
| `column_names` | `column_names() -> list` | Get list of column names in insertion order. |
| `has_column` | `has_column(name) -> bool` | Check if a column exists. |
| `materialize_row` | `materialize_row(row_idx) -> dict` | Late materialization: reconstruct a full row with decoded strings. |

**Query Method:**

| Method | Signature | Description |
|--------|-----------|-------------|
| `query` | `query() -> Query` | Start a chainable query. Returns a `Query` object. |

**Example:**
```python
store = ColumnStore()
store.add_column("town", "str")
store.add_column("price", "float")
store.append_row({"town": "BEDOK", "price": 350000.0})

# Late materialization
row = store.materialize_row(0)  # {"town": "BEDOK", "price": 350000.0}
```

#### Class: `Query`

Chainable query builder for `ColumnStore`. Supports filtering, aggregation, and result retrieval, all operating on column arrays.

**Filter Method:**

```python
query.filter(col_name, op, value)
```

Supported operators:

| Operator | Description |
|----------|-------------|
| `"=="` / `"="` | Equals |
| `"!="` / `"<>"` | Not equals |
| `">"` | Greater than |
| `">="` | Greater than or equal |
| `"<"` | Less than |
| `"<="` | Less than or equal |
| `"in"` | Value is in a collection |
| `"not_in"` | Value is not in a collection |

For string columns, comparisons use dictionary codes internally for speed. The caller passes human-readable values; the query engine resolves them to codes automatically.

**Aggregation Methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `min(col_name)` | `(row_index, min_value)` or `(None, None)` | Find row with minimum value. |
| `max(col_name)` | `(row_index, max_value)` or `(None, None)` | Find row with maximum value. |
| `sum(col_name)` | `float` | Sum of values among matching rows. |
| `avg(col_name)` | `float` or `None` | Average of values among matching rows. |
| `count()` | `int` | Number of matching rows. |

**Result Methods:**

| Method | Returns | Description |
|--------|---------|-------------|
| `execute()` | `list[int]` | List of matching row indices. |
| `select(columns=None)` | `list[dict]` | Matching rows as a list of dicts (with decoded strings). |
| `to_column_store()` | `ColumnStore` | Matching rows as a new `ColumnStore` (filtered subset). |

**Example:**
```python
# Find the cheapest flat in BEDOK with area >= 85 sqm in 2020
row_idx, min_price = (store.query()
    .filter("year", "==", 2020)
    .filter("town", "in", ["BEDOK", "CLEMENTI"])
    .filter("floor_area_sqm", ">=", 85)
    .min("price_per_sqm"))

# Count records
count = store.query().filter("year", "==", 2020).count()

# Get matching rows as dicts
rows = (store.query()
    .filter("town", "==", "BEDOK")
    .select(["town", "price", "area"]))
```

---

### `csv_loader.py` - Generic CSV Parser

Loads any CSV file into a `ColumnStore` with automatic or explicit type detection.

#### Functions

| Function | Description |
|----------|-------------|
| `load_csv(filepath, schema=None)` | Main entry point. Loads a CSV file into a `ColumnStore`. |
| `_auto_detect_type(value)` | Detects if a string value is `int`, `float`, or `str`. |
| `_detect_schema(filepath, sample_rows=100)` | Samples the first N rows to infer column types. |
| `_cast_value(raw, col_type)` | Converts a raw string to the target type. |

**`load_csv` Details:**

```python
def load_csv(filepath, schema=None) -> ColumnStore
```

- If `schema` is provided (dict of `column_name -> "int"|"float"|"str"`), uses it directly.
- If `schema` is `None`, auto-detects types by sampling the first 100 rows.
- Prints a summary after loading: row count, column count, and distinct values for string columns.

**Schema Auto-Detection Logic:**
1. Samples the first 100 rows of the CSV.
2. For each column, tests all sampled values:
   - If any value is non-numeric -> `"str"`
   - If all values are numeric but some have decimals -> `"float"`
   - If all values are integers -> `"int"`

**Example:**
```python
# With explicit schema
schema = {"town": "str", "price": "float", "area": "int"}
store = load_csv("data.csv", schema=schema)

# With auto-detection
store = load_csv("data.csv")
```

---

### `vectorized_loader.py` - Out-of-Core Vectorized Loader

Implements the vector-at-a-time execution model used by modern column-store databases (MonetDB, Vertica, ClickHouse). Processes the CSV in fixed-size vectors and applies filter predicates during loading.

#### Function

```python
def load_and_filter_vectorized(filepath, predicates, schema=None,
                                vector_size=4096) -> ColumnStore
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filepath` | `str` | required | Path to the input CSV file. |
| `predicates` | `list[tuple]` | required | List of `(col_name, test_func)` tuples. Each `test_func` takes a cast value and returns `True` to keep the row. |
| `schema` | `dict` | `None` | Optional column type schema. Auto-detected if `None`. |
| `vector_size` | `int` | `4096` | Number of rows per vector batch. |

**Key Features:**

| Feature | Description |
|---------|-------------|
| **Vector Processing** | Loads CSV in fixed-size batches (default 4096 rows). |
| **Predicate Pushdown** | Filters are applied during loading, not after. Only qualifying rows enter the `ColumnStore`. |
| **Short-Circuit** | If no rows survive a predicate within a vector, remaining predicates are skipped for that vector. |
| **Memory Bounded** | Only one vector plus accumulated candidates are held in memory at any time. |
| **Cache Friendly** | Processes contiguous arrays of same-type values. |

**Predicate Ordering:** Put the most selective predicate first for best short-circuit performance.

**Example:**
```python
predicates = [
    ("month", lambda v: v.startswith("2020")),
    ("town",  lambda v: v in {"BEDOK", "CLEMENTI"}),
]
store = load_and_filter_vectorized("data.csv", predicates, schema=HDB_SCHEMA)
```

**Console Output:**
```
  Vectorized loading complete (vector_size=4096)
  Total rows scanned: 318402
  Vectors processed: 78
  Vectors skipped (short-circuit): 52 (66.7%)
  Candidates in memory: 4231 (1.3% of total)
```

---

### `query_engine.py` - HDB Query Implementations

Provides two query strategies for finding valid `(x, y)` pairs, both producing identical results.

#### `run_query_naive` - Baseline Implementation

```python
def run_query_naive(store, target_year, start_month, matched_towns, **kwargs) -> dict
```

Uses the generic `Query` API for each `(x, y)` pair. Equivalent to running this SQL for every combination:

```sql
SELECT MIN(price_per_sqm) FROM data
WHERE year = target_year
  AND month >= start_month
  AND month <= start_month + x - 1
  AND town IN (matched_towns)
  AND floor_area_sqm >= y
```

Simple but slow due to redundant filtering across overlapping `(x, y)` ranges.

#### `run_query` - Optimized Implementation

```python
def run_query(store, target_year, start_month, matched_towns, **kwargs) -> dict
```

Uses raw column access with two key optimizations:

1. **Incremental Month Accumulation:** For x=2, reuses the candidate set from x=1 and only adds records from the new month. Avoids re-scanning the entire dataset.

2. **Area Sweep with Running Minimum:** Instead of iterating over every `y` value independently, performs a single backward sweep from `max_area` down to `y_min`, maintaining a running minimum. This answers all `y` values in O(max_area) time instead of O(x * y) time.

**Common Parameters:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `store` | required | The `ColumnStore` containing the data. |
| `target_year` | required | Year to filter on. |
| `start_month` | required | Starting month for the consecutive month range. |
| `matched_towns` | required | List of town names to filter on. |
| `col_year` | `"year"` | Column name for year. |
| `col_month` | `"month_num"` | Column name for month. |
| `col_town` | `"town"` | Column name for town. |
| `col_area` | `"floor_area_sqm"` | Column name for floor area. |
| `col_ppsm` | `"price_per_sqm"` | Column name for price per square meter. |
| `x_min` / `x_max` | `1` / `8` | Range for x (consecutive months). |
| `y_min` / `y_max` | `80` / `150` | Range for y (minimum floor area). |
| `threshold` | `4725` | Maximum rounded price per sqm to include. |

**Return Value:** `dict` mapping `(x, y) -> (row_index, rounded_price)`.

---

### `result_writer.py` - CSV Output Writer

Generic CSV writer that uses late materialization to decode strings only for the rows being written.

```python
def write_query_results(filepath, store, row_indices, columns=None)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filepath` | `str` | required | Output file path. |
| `store` | `ColumnStore` | required | Data source. |
| `row_indices` | `list[int]` | required | Row indices to write. |
| `columns` | `list[str]` | `None` | Columns to include. `None` = all columns. |

---

### `main.py` - Entry Point & Orchestration

Contains HDB-specific configuration and orchestrates the full pipeline.

#### Matriculation Number Parsing

```python
def parse_matric(matric_num) -> (target_year, start_month, towns)
```

Derives query parameters from an NTU matriculation number:

| Component | Rule | Example (`A5656567B`) |
|-----------|------|----------------------|
| **Target Year** | Last digit `d`: if `d >= 5` then `2010 + d`, else `2020 + d` | `7` -> `2017` |
| **Start Month** | Second-to-last digit: `0` maps to `10`, else used directly | `6` -> `06` |
| **Towns** | All unique digits mapped through `TOWN_MAP` | `{5,6,7}` -> JURONG WEST, PASIR RIS, TAMPINES |

#### Town Mapping

| Digit | Town |
|-------|------|
| 0 | BEDOK |
| 1 | BUKIT PANJANG |
| 2 | CLEMENTI |
| 3 | CHOA CHU KANG |
| 4 | HOUGANG |
| 5 | JURONG WEST |
| 6 | PASIR RIS |
| 7 | TAMPINES |
| 8 | WOODLANDS |
| 9 | YISHUN |

#### HDB Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `month` | `str` | Transaction month in `"YYYY-MM"` format |
| `town` | `str` | HDB town name |
| `block` | `str` | Block number |
| `street_name` | `str` | Street name |
| `flat_type` | `str` | Flat type (e.g., "4 ROOM") |
| `flat_model` | `str` | Flat model (e.g., "Model A") |
| `storey_range` | `str` | Storey range (e.g., "07 TO 09") |
| `floor_area_sqm` | `int` | Floor area in square meters |
| `lease_commence_date` | `int` | Year lease commenced |
| `resale_price` | `float` | Resale transaction price in SGD |

**Derived Columns** (added by `post_load_transform`):

| Column | Type | Derived From |
|--------|------|-------------|
| `year` | `int` | Parsed from `month` (e.g., `"2020-06"` -> `2020`) |
| `month_num` | `int` | Parsed from `month` (e.g., `"2020-06"` -> `6`) |
| `price_per_sqm` | `float` | `resale_price / floor_area_sqm` |

#### Execution Modes

The program runs three modes sequentially for comparison:

| Mode | Description | Purpose |
|------|-------------|---------|
| **Demo** | Generic Query API examples (min, count, avg) | Demonstrates the query API is general-purpose |
| **Mode 1** | Full load + optimized query (`run_query`) | Primary mode for generating results |
| **Mode 2** | Naive baseline (`run_query_naive`) | Validates correctness by comparing against Mode 1 |
| **Mode 3** | Vectorized out-of-core loading + optimized query | Demonstrates memory-efficient processing |

---

## Data Flow

```
INPUT: ResalePricesSingapore.csv (~318,000 rows)
  |
  v
[CSV LOADER] ---- Parse CSV, infer/apply types, dictionary-encode strings
  |
  v
[COLUMN STORE] -- Column-oriented storage with dictionary-encoded strings
  |
  v
[POST-LOAD TRANSFORM]
  |   - Parse "YYYY-MM" -> separate year (int) and month_num (int) columns
  |   - Compute price_per_sqm = resale_price / floor_area_sqm
  |
  v
[QUERY ENGINE]
  |   Phase 1: Pre-filter candidates (year + town match)
  |   Phase 2: For each x (consecutive months):
  |     - Incrementally add new month's candidates
  |     - Backward area sweep to find min price for all y values
  |     - Keep (x, y) pairs where rounded min price <= threshold
  |
  v
[RESULT WRITER]
  |   - Late materialization: decode strings only for result rows
  |   - Write to CSV with all required fields
  |
  v
OUTPUT: ScanResult_<MatricNum>.csv
```

---

## Query Logic

For each `(x, y)` pair where `x` in `[1, 8]` and `y` in `[80, 150]`:

1. **Filter** all transactions matching:
   - `year == target_year`
   - `month_num` in `[start_month, start_month + x - 1]` (capped at 12)
   - `town` in `matched_towns`
   - `floor_area_sqm >= y`

2. **Find** the minimum `price_per_sqm` among all matching records.

3. **Round** the minimum price to the nearest integer.

4. **Include** the `(x, y)` pair in results only if `rounded_price <= 4725`.

5. **Output** the full record details for the row with the minimum price.

---

## Optimizations

### 1. Dictionary Encoding
String columns (town, block, street_name, etc.) are stored as integer codes. Filtering by `town == "BEDOK"` becomes an integer comparison rather than a string comparison, providing both memory savings and faster execution.

### 2. Late Materialization
During query processing, only raw column values (integers/floats) are accessed. String decoding via the dictionary is deferred until results are written to CSV. This avoids reconstructing full rows for the thousands of records that get filtered out.

### 3. Column-at-a-Time Processing
Each filter predicate iterates over a single column array, not full rows. This improves CPU cache locality since contiguous memory for a single column stays in cache during the scan.

### 4. Incremental Month Accumulation
The optimized query avoids redundant work across x values:
- x=1: scans records for `month == start_month`
- x=2: reuses x=1's candidates, adds only `month == start_month + 1`
- x=3: reuses x=2's candidates, adds only `month == start_month + 2`
- ...and so on

### 5. Area Sweep with Running Minimum
Instead of independently finding the minimum for each y value (O(y_range) per x value), the optimized query performs a single backward sweep from `max_area` down to `y_min`. A running minimum tracks the best price seen so far at areas >= current position, answering all y values in a single pass.

### 6. Vectorized Out-of-Core Execution
The vectorized loader processes the CSV in batches of 4096 rows:
- **Predicate pushdown:** Filters are applied during loading, so non-matching rows never enter the `ColumnStore`.
- **Short-circuit:** If an entire vector fails the first predicate, remaining predicates and the extraction step are skipped entirely.
- **Memory bounded:** Only one vector plus accumulated results are in memory at any time.

---

## Usage

### Prerequisites

- Python 3.6+
- No external libraries required (uses only `csv`, `sys`, `os`, `time`, `collections` from the standard library)

### Running

```bash
cd /mnt/e/Desktop/SC4023/project/source

# Run with default matriculation number (U2331760J)
python3 main.py

# Run with a specific matriculation number
python3 main.py A5656567B

# Run with a custom CSV file path
python3 main.py U2331760J /path/to/ResalePricesSingapore.csv
```

### Input Data

Place the `ResalePricesSingapore.csv` file in the project root directory (one level above `source/`). The file can be downloaded from NTU Learn.

### Expected Console Output

```
=== HDB Resale Flat Column-Store Query Engine ===
Matriculation: U2331760J
Target year:   2020
Start month:   06
Matched towns: BEDOK, BUKIT PANJANG, CLEMENTI, ...
x range:       1 to 8
y range:       80 to 150
Threshold:     4725

Loaded 318402 rows, 10 columns from .../ResalePricesSingapore.csv
  month (str): 130 distinct values
  town (str): 26 distinct values
  ...

==================================================
MODE 1: Full Load + Optimized Query
==================================================
  Pre-filtered to 4231 candidate rows (year=2020, 7 towns)
  Found 284 valid (x, y) pairs
  Optimized query time: 0.012s

==================================================
MODE 2: Naive Baseline (Generic Query API)
==================================================
  Naive: found 284 valid (x, y) pairs
  Naive query time: 1.423s
  Verification: naive and optimized results MATCH
  Speedup: 118.6x faster with optimized

==================================================
TIMING SUMMARY
==================================================
  Full load:           2.341s
  Naive query:         1.423s
  Optimized query:     0.012s
  Vectorized load+qry: 0.987s
  Output write:        0.003s
```

---

## Output Format

The output CSV file (`ScanResult_<MatricNum>.csv`) has the following columns:

| Column | Description | Example |
|--------|-------------|---------|
| `(x, y)` | The valid coordinate pair | `(3, 85)` |
| `Year` | Transaction year | `2020` |
| `Month` | Transaction month (zero-padded) | `06` |
| `Town` | HDB town name | `BEDOK` |
| `Block` | Block number | `123` |
| `Floor_Area` | Floor area in sqm | `92` |
| `Flat_Model` | HDB flat model | `Model A` |
| `Lease_Commence_Date` | Year lease commenced | `1998` |
| `Price_Per_Square_Meter` | Rounded minimum price/sqm | `4523` |

Results are sorted by `(x, y)` in ascending order.

---

## Project Structure

```
project/
|-- Semester_Group_Project.pdf    # Assignment specification
|-- ResalePricesSingapore.csv     # Input dataset (not included, download from NTU Learn)
|-- ScanResult_*.csv              # Generated output files
|-- README.md                     # This documentation
|
|-- source/
    |-- main.py                   # Entry point, HDB-specific logic, orchestration
    |-- column_store.py           # Generic column-oriented database engine (385 lines)
    |-- csv_loader.py             # Generic CSV parser with schema inference (141 lines)
    |-- vectorized_loader.py      # Out-of-core vectorized loader (147 lines)
    |-- query_engine.py           # HDB-specific query optimizations (134 lines)
    |-- result_writer.py          # Generic CSV output writer (30 lines)
```

**Total source code:** ~1,121 lines of Python (no external dependencies).
