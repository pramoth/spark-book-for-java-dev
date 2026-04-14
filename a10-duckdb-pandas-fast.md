# Appendix 10 — Why DuckDB ↔ pandas Is Suddenly Fast

## Setup

Parquet file, 10 million rows. Run a DuckDB SQL query, get results into pandas.

## Before Arrow

DuckDB and pandas had completely different memory layouts:
- **DuckDB** — tightly packed C++ arrays (`int32_t*`, `double*`, length-prefixed strings).
- **Legacy pandas** — NumPy arrays with quirks:
  - Integer columns with nulls → `float64` (NumPy ints don't have NaN).
  - Strings → arrays of **Python objects** — each string a separate `PyObject` with header, refcount.
  - Dates sometimes wrapped in Python objects.

Conversion required looping over every value:

```
 DuckDB result (C++ columnar)
           │
           ▼  for every row, every column:
   Allocate Python int / str object
   Increment refcount
   Place in NumPy object array
           │
           ▼
 pandas DataFrame (NumPy arrays)
```

For 10M rows × 10 columns = **100 million Python object allocations**. Each one: CPython allocator + GIL + refcount bookkeeping. Python is slow at this.

**Query itself:** 0.3 seconds. **Handover to pandas:** 15 seconds. The serialization was 50× more expensive than the work.

Every Python data tool had this disease — Spark→pandas, Postgres→pandas, BigQuery→pandas.

## After Arrow

Both DuckDB and modern pandas (2.x) can store columns in the **Arrow memory format**.

```python
import duckdb
df = duckdb.sql("SELECT * FROM 'sales.parquet'").df()
```

Internally:

```
 DuckDB result (already Arrow-native)
   ┌─────────────────────────────────┐
   │ Buffer A: [int32, int32, ...]   │
   │ Buffer B: [validity bitmap]     │
   │ Buffer C: [string offsets]      │
   │ Buffer D: [string bytes]        │
   └─────────────────────────────────┘
           │
           │ pandas wraps the SAME pointers
           │ — no copy, no allocation
           ▼
 pandas DataFrame (thin wrapper on same Arrow buffers)
```

**Nothing copied.** pandas receives pointers and lengths, wraps them in a DataFrame.

15-second handover → **~5 milliseconds**. A **3000× speedup** on that step.

## Two ingredients

1. **Shared memory layout (Arrow)** — byte-for-byte agreement on int32 columns, string columns, etc.
2. **pandas can use Arrow buffers** — opt in via `dtype_backend="pyarrow"` or via DuckDB's `.df()` which now uses Arrow internally.

## "Zero-copy" — what it means

> Bytes are written to RAM **once**. Multiple tools read or wrap those same bytes without moving or duplicating them.

DuckDB allocates the buffer. pandas receives a pointer and treats it as its own column. Both read the same memory.

## Concrete benchmark

10M rows × 10 columns:

| Step | Old (pre-Arrow) | New (Arrow) |
|---|---|---|
| DuckDB query execution | 0.3 s | 0.3 s |
| Convert to pandas | **14.7 s** | **0.005 s** |
| **Total** | **15.0 s** | **0.305 s** |

Same query, same engine, ~50× total speedup — entirely from eliminating serialization.

## Generalizes everywhere

Same pattern works for any pair of Arrow-aware tools:
- **Spark → pandas** — `spark.sql.execution.arrow.pyspark.enabled`, 10–50× faster.
- **BigQuery → Python** — Storage API uses Arrow.
- **Snowflake → Python** — same.
- **Postgres → Python** via ADBC drivers.
- **Polars ↔ DuckDB ↔ pandas** — all zero-copy.
- **R ↔ Python** — trivial now.

## Mental model

> **DuckDB ↔ pandas is fast now because they stopped translating between languages and started speaking the same one.** Arrow is that shared language. The 15-second copy step disappears because there's nothing to copy — both tools point at the same bytes in RAM.

## Client lesson

When a client says *"our Python pipeline is slow"*, ask:
1. Where is time going? Often "moving data between tools", not "the work".
2. Are the tools Arrow-aware?
3. Stuck on legacy pandas + ODBC + JSON? Switching to Arrow-aware equivalents is often 10× without any algorithm change.
