"""
column_store.py - Generic column-oriented database engine.

A self-contained column store with a chainable query API, similar to
pandas or SQL but implemented from scratch using column-oriented storage.

Usage:
    store = ColumnStore()
    store.add_column("price", "float")
    store.add_column("town", "str")
    ...
    # Chainable queries:
    result = (store.query()
        .filter("year", "==", 2020)
        .filter("town", "in", ["BEDOK", "CLEMENTI"])
        .filter("area", ">=", 85)
        .min("price_per_sqm"))
"""

class Dictionary:
    """
    Dictionary encoding for string columns.
    Maps string values to integer codes for compact, fast storage.
    """

    def __init__(self):
        self._str_to_code = {}
        self._code_to_str = []

    def encode(self, value):
        """Encode a string to an integer code. Adds to dict if new."""
        if value not in self._str_to_code:
            code = len(self._code_to_str)
            self._str_to_code[value] = code
            self._code_to_str.append(value)
        return self._str_to_code[value]

    def decode(self, code):
        """Decode an integer code back to the original string."""
        return self._code_to_str[code]

    def get_code(self, value):
        """Get code for a value, or -1 if not found."""
        return self._str_to_code.get(value, -1)

    def size(self):
        return len(self._code_to_str)


class ColumnStore:
    """
    Generic column-oriented storage and query engine.

    Each column is a Python list stored by name. String columns are
    dictionary-encoded automatically. Supports derived columns,
    late materialization, and a chainable query API.
    """

    TYPE_INT = "int"
    TYPE_FLOAT = "float"
    TYPE_STR = "str"

    def __init__(self):
        self.num_rows = 0
        self._columns = {}
        self._col_types = {}
        self._dictionaries = {}
        self._col_order = []
        self._zone_maps = {}
        self._zone_size = None

    # ---- Schema & loading ----

    def _invalidate_zone_maps(self):
        self._zone_maps = {}
        self._zone_size = None

    def add_column(self, name, col_type):
        """Register a column with a name and type ("int", "float", "str")."""
        if name in self._columns:
            return
        self._columns[name] = []
        self._col_types[name] = col_type
        self._col_order.append(name)
        if col_type == self.TYPE_STR:
            self._dictionaries[name] = Dictionary()

    def append_value(self, col_name, raw_value):
        """Append a value to a column (auto dictionary-encodes strings)."""
        if self._zone_maps:
            self._invalidate_zone_maps()
        if self._col_types[col_name] == self.TYPE_STR:
            self._columns[col_name].append(
                self._dictionaries[col_name].encode(raw_value))
        else:
            self._columns[col_name].append(raw_value)

    def append_row(self, row_dict):
        """Append a full row as a dict of {col_name: value}."""
        for name in self._col_order:
            if name in row_dict:
                self.append_value(name, row_dict[name])
        self.num_rows += 1

    def add_derived_column(self, name, col_type, values):
        """Add a pre-computed derived column (e.g. price/area)."""
        if len(values) != self.num_rows:
            raise ValueError(
                f"Derived column '{name}' has {len(values)} values, "
                f"expected {self.num_rows}")
        if self._zone_maps:
            self._invalidate_zone_maps()
        self._columns[name] = values
        self._col_types[name] = col_type
        if name not in self._col_order:
            self._col_order.append(name)

    def sort_by(self, columns):
        """Sort all columns by the given key columns."""
        if not columns or self.num_rows <= 1:
            return
        for name in columns:
            if name not in self._columns:
                raise ValueError(f"Unknown sort column: '{name}'")

        order = sorted(
            range(self.num_rows),
            key=lambda i: tuple(self.get_value(c, i) for c in columns),
        )

        for name in self._col_order:
            reordered = [self.get_value(name, i) for i in order]
            self._columns[name] = reordered

        if self._zone_maps:
            self._invalidate_zone_maps()

    def build_zone_maps(self, zone_size=4096, columns=None):
        """
        Build min-max zone maps for selected columns.
        """
        if zone_size <= 0:
            raise ValueError("zone_size must be > 0")

        if columns is None:
            columns = list(self._col_order)
        else:
            for name in columns:
                if name not in self._columns:
                    raise ValueError(f"Unknown zone-map column: '{name}'")

        n = self.num_rows
        num_zones = (n + zone_size - 1) // zone_size if n else 0
        zone_maps = {}

        for name in columns:
            col = self._columns[name]
            bounds = []
            for z in range(num_zones):
                start = z * zone_size
                end = min(start + zone_size, n)
                vals = col[start:end]
                bounds.append((min(vals), max(vals)))
            zone_maps[name] = bounds

        self._zone_maps = zone_maps
        self._zone_size = zone_size

    def has_zone_maps(self):
        return bool(self._zone_maps) and self._zone_size is not None

    def zone_count(self):
        if not self._zone_size or self.num_rows == 0:
            return 0
        return (self.num_rows + self._zone_size - 1) // self._zone_size

    def candidate_rows_from_zone_mask(self, zone_mask):
        rows = []
        for z, keep in enumerate(zone_mask):
            if not keep:
                continue
            start = z * self._zone_size
            end = min(start + self._zone_size, self.num_rows)
            rows.extend(range(start, end))
        return rows

    def zone_mask_for_predicate(self, col_name, op, value):
        """
        Return a bool mask of zones that may contain matches.
        """
        if col_name not in self._zone_maps:
            return None

        if op in ("in", "not_in") and not isinstance(value, set):
            value = set(value)

        mask = []
        for lo, hi in self._zone_maps[col_name]:
            if op in ("==", "="):
                keep = lo <= value <= hi
            elif op in ("!=", "<>"):
                keep = not (lo == hi == value)
            elif op == ">":
                keep = hi > value
            elif op == ">=":
                keep = hi >= value
            elif op == "<":
                keep = lo < value
            elif op == "<=":
                keep = lo <= value
            elif op == "in":
                keep = any(lo <= v <= hi for v in value)
            elif op == "not_in":
                keep = not (lo == hi and lo in value)
            else:
                raise ValueError(f"Unknown operator: '{op}'")
            mask.append(keep)
        return mask

    # ---- Access ----

    def get_column(self, name):
        """Get raw column list by name (encoded ints for str columns)."""
        return self._columns[name]

    def get_value(self, col_name, row_idx):
        """Get a single raw value."""
        col = self._columns[col_name]
        return col[row_idx]

    def get_decoded(self, col_name, row_idx):
        """Get a value, decoding strings from dictionary encoding."""
        val = self.get_value(col_name, row_idx)
        if col_name in self._dictionaries:
            return self._dictionaries[col_name].decode(val)
        return val

    def get_dictionary(self, col_name):
        """Get Dictionary object for a string column (or None)."""
        return self._dictionaries.get(col_name)

    def get_type(self, col_name):
        return self._col_types.get(col_name)

    def column_names(self):
        return list(self._col_order)

    def has_column(self, name):
        return name in self._columns

    def materialize_row(self, row_idx):
        """Late materialization: reconstruct a full row with decoded strings."""
        return {name: self.get_decoded(name, row_idx)
                for name in self._col_order}

    # ---- Query API ----

    def query(self):
        """Start a chainable query. Returns a Query object."""
        return Query(self)


class Query:
    """
    Chainable query builder for ColumnStore.

    Supports filtering, selection, aggregation (min/max/sum/avg/count),
    and result retrieval — all operating on column arrays.

    Example:
        store.query()
            .filter("year", "==", 2020)
            .filter("town", "in", ["BEDOK", "CLEMENTI"])
            .filter("area", ">=", 85)
            .min("price_per_sqm")
    """

    def __init__(self, store):
        self._store = store
        self._predicates = []  # list of (col_name, op, value)

    def filter(self, col_name, op, value):
        """
        Add a filter predicate. Returns self for chaining.

        Supported operators:
            "==" / "="    : equals
            "!=" / "<>"   : not equals
            ">"           : greater than
            ">="          : greater than or equal
            "<"           : less than
            "<="          : less than or equal
            "in"          : value is in a collection
            "not_in"      : value is not in a collection

        For string columns, comparisons use dictionary codes internally
        for speed. The caller passes human-readable values — the query
        engine resolves them to codes automatically.

        Args:
            col_name: Column name to filter on.
            op: Comparison operator string.
            value: Value to compare against (or collection for "in"/"not_in").

        Returns:
            self (for chaining).
        """
        self._predicates.append((col_name, op, value))
        return self

    def _build_mask(self):
        """
        Build a boolean mask by applying all predicates column-at-a-time.

        For each predicate, iterates only the relevant column (not full rows).
        String columns are compared using dictionary codes for speed.

        Returns:
            List of row indices that pass all predicates.
        """
        store = self._store
        n = store.num_rows

        # Zone-map pruning first (if available), then row-level filtering.
        if store.has_zone_maps() and n > 0:
            zone_mask = [True] * store.zone_count()

            for col_name, op, value in self._predicates:
                dictionary = store.get_dictionary(col_name)
                zone_value = value

                if dictionary and op in ("==", "="):
                    code = dictionary.get_code(value)
                    if code == -1:
                        return []
                    zone_value = code
                elif dictionary and op in ("!=", "<>"):
                    code = dictionary.get_code(value)
                    if code == -1:
                        continue
                    zone_value = code
                elif dictionary and op == "in":
                    codes = {c for v in value
                             for c in [dictionary.get_code(v)] if c != -1}
                    if not codes:
                        return []
                    zone_value = codes
                elif dictionary and op == "not_in":
                    zone_value = {c for v in value
                                  for c in [dictionary.get_code(v)] if c != -1}
                elif op in ("in", "not_in"):
                    zone_value = set(value)

                pred_zone_mask = store.zone_mask_for_predicate(
                    col_name, op, zone_value)
                if pred_zone_mask is None:
                    continue

                zone_mask = [zone_mask[i] and pred_zone_mask[i]
                             for i in range(len(zone_mask))]
                if not any(zone_mask):
                    return []

            surviving = store.candidate_rows_from_zone_mask(zone_mask)
        else:
            # Start with all rows
            surviving = list(range(n))

        for col_name, op, value in self._predicates:
            col = store.get_column(col_name)
            dictionary = store.get_dictionary(col_name)

            # For string columns, resolve values to dictionary codes
            if dictionary and op in ("==", "="):
                code = dictionary.get_code(value)
                if code == -1:
                    return []  # value not in dictionary — no matches
                surviving = [i for i in surviving if col[i] == code]
                continue

            if dictionary and op in ("!=", "<>"):
                code = dictionary.get_code(value)
                if code == -1:
                    continue  # value not in dict — all rows pass
                surviving = [i for i in surviving if col[i] != code]
                continue

            if dictionary and op == "in":
                codes = set()
                for v in value:
                    c = dictionary.get_code(v)
                    if c != -1:
                        codes.add(c)
                if not codes:
                    return []
                surviving = [i for i in surviving if col[i] in codes]
                continue

            if dictionary and op == "not_in":
                codes = set()
                for v in value:
                    c = dictionary.get_code(v)
                    if c != -1:
                        codes.add(c)
                surviving = [i for i in surviving if col[i] not in codes]
                continue

            # Numeric / non-encoded comparisons
            if op in ("==", "="):
                surviving = [i for i in surviving if col[i] == value]
            elif op in ("!=", "<>"):
                surviving = [i for i in surviving if col[i] != value]
            elif op == ">":
                surviving = [i for i in surviving if col[i] > value]
            elif op == ">=":
                surviving = [i for i in surviving if col[i] >= value]
            elif op == "<":
                surviving = [i for i in surviving if col[i] < value]
            elif op == "<=":
                surviving = [i for i in surviving if col[i] <= value]
            elif op == "in":
                value_set = set(value)
                surviving = [i for i in surviving if col[i] in value_set]
            elif op == "not_in":
                value_set = set(value)
                surviving = [i for i in surviving
                             if col[i] not in value_set]
            else:
                raise ValueError(f"Unknown operator: '{op}'")

        return surviving

    def execute(self):
        """
        Execute the query. Returns list of matching row indices.

        These indices can be used with store.materialize_row(idx) or
        store.get_decoded(col, idx) to retrieve values.
        """
        return self._build_mask()

    def count(self):
        """Return the number of rows matching all predicates."""
        return len(self._build_mask())

    def min(self, col_name):
        """
        Find the row with the minimum value in col_name among matches.

        Returns:
            (row_index, min_value) or (None, None) if no matches.
        """
        indices = self._build_mask()
        if not indices:
            return None, None
        col = self._store.get_column(col_name)
        best_idx = indices[0]
        best_val = col[best_idx]
        for i in indices[1:]:
            if col[i] < best_val:
                best_val = col[i]
                best_idx = i
        return best_idx, best_val

    def max(self, col_name):
        """
        Find the row with the maximum value in col_name among matches.

        Returns:
            (row_index, max_value) or (None, None) if no matches.
        """
        indices = self._build_mask()
        if not indices:
            return None, None
        col = self._store.get_column(col_name)
        best_idx = indices[0]
        best_val = col[best_idx]
        for i in indices[1:]:
            if col[i] > best_val:
                best_val = col[i]
                best_idx = i
        return best_idx, best_val

    def sum(self, col_name):
        """Sum values in col_name among matching rows."""
        indices = self._build_mask()
        if not indices:
            return 0
        col = self._store.get_column(col_name)
        return sum(col[i] for i in indices)

    def avg(self, col_name):
        """Average of values in col_name among matching rows."""
        indices = self._build_mask()
        if not indices:
            return None
        col = self._store.get_column(col_name)
        total = sum(col[i] for i in indices)
        return total / len(indices)

    def select(self, columns=None):
        """
        Return matching rows as a list of dicts (with decoded strings).

        Args:
            columns: List of column names to include. If None, all columns.

        Returns:
            List of dicts, one per matching row.
        """
        indices = self._build_mask()
        store = self._store
        if columns is None:
            columns = store.column_names()
        return [
            {c: store.get_decoded(c, i) for c in columns}
            for i in indices
        ]

    def to_column_store(self):
        """
        Return matching rows as a new ColumnStore.

        Useful for creating a filtered subset for further processing.
        """
        indices = self._build_mask()
        store = self._store
        new_store = ColumnStore()

        for name in store.column_names():
            new_store.add_column(name, store.get_type(name))

        for i in indices:
            row = store.materialize_row(i)
            new_store.append_row(row)

        return new_store
