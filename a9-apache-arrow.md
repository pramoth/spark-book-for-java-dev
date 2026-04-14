# Appendix 9 — Apache Arrow

## One-line definition

> **Apache Arrow is a standard for representing columnar data in memory** — plus libraries implementing that standard in many languages.

It's not a file format. Not a database. Not a query engine. A **memory layout specification** — a precise agreement on *how columns of typed data should be laid out in RAM*.

## The problem Arrow solves

Before Arrow, every tool had its own in-memory representation:
- **pandas** → NumPy arrays (with object overhead).
- **Spark** → Tungsten format.
- **R data.frame** → its own layout.
- **Postgres** → another.

Moving data between tools meant **serialize + deserialize**. Often **80–90% of the time** in cross-tool pipelines.

```
   Spark (Tungsten)  →  serialize  →  bytes  →  deserialize  →  pandas (NumPy)
                          [SLOW]                   [SLOW]
```

Arrow's bet: *what if every tool agreed on one in-memory format?* Then tools share the same memory pages. Zero-copy exchange.

```
   Spark (Arrow)    ─────────  same memory  ─────────→   pandas (Arrow)
                          [INSTANT — no copy at all]
```

## What Arrow looks like

A column of integers `[10, 20, 30, NULL, 50]` in Arrow:

```
 ┌─────────────────────────────────┐
 │ Validity bitmap: 11101          │  ← which values are non-null
 ├─────────────────────────────────┤
 │ Values: [10][20][30][??][50]    │  ← contiguous typed array
 └─────────────────────────────────┘
```

A string column is two buffers — offsets + concatenated bytes:

```
 Offsets:  [0, 5, 10, 13]
 Bytes:    "Alice""Bob  ""Carol"
```

Properties:
- **Columnar** — one column = one contiguous memory chunk.
- **Typed** — int32 / string / float64 — no boxing.
- **Cache-friendly** — CPUs love sequential reads.
- **SIMD-friendly** — CPU vector instructions.
- **Identical across languages** — bytes are bytes.

## Arrow ≠ Parquet

| | Arrow | Parquet |
|---|---|---|
| **Lives in** | RAM | Disk |
| **Optimized for** | Fast computation | Storage + retrieval |
| **Compression** | None / minimal | Heavy (zstd, snappy) |
| **Random access** | Cheap | Cheaper than CSV, not free |

> **Parquet is for the disk. Arrow is for the RAM.** Sibling Apache projects, intentionally aligned.

## What you actually use Arrow for

### 1. Cross-tool data exchange
```python
import pyarrow as pa, pandas as pd

table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
df = table.to_pandas()           # zero-copy
back = pa.Table.from_pandas(df)  # zero-copy

# Spark → Python
df = spark.range(1000000).toPandas()  # uses Arrow under the hood
```

### 2. Pandas 2.x Arrow backend
```python
df = pd.read_parquet("sales.parquet", dtype_backend="pyarrow")
```
Real string types, real null support, faster, less memory.

### 3. DuckDB ↔ everything
```python
result = duckdb.sql("SELECT * FROM 'sales.parquet'").arrow()  # zero-copy
```

### 4. Arrow Flight (network transport)
gRPC-based protocol shipping Arrow at line speed. Used by Dremio, Snowflake, BigQuery clients to deliver results to Python with no serialization tax. Replaces ODBC/JDBC for analytics.

### 5. Spark vectorized Python UDFs
Spark hands data to Python as Arrow batches — pandas/NumPy processes them — hands back. No per-row JVM↔Python serialization.

## Arrow also has a compute engine

- **Arrow Compute** — kernel library (sum, filter, join on Arrow arrays).
- **DataFusion** — full SQL engine in Rust, built on Arrow.
- **Acero** — streaming execution engine on Arrow.

Arrow is increasingly the **foundation other engines build on**: DuckDB, Polars, DataFusion, InfluxDB IOx, Snowflake client libs, the new Spark Comet plugin.

## Why this matters

> Tools used to be **vertical silos** — each had its own storage, memory, execution. Switching tools meant rewriting and copying data.
>
> Tools are becoming **layers** — they share Arrow as memory, Parquet as storage, Iceberg/Delta as tables. Swap query engines without touching data.

## The naming chain

```
   ON DISK              IN MEMORY            ON THE WIRE
 ┌─────────┐         ┌─────────┐          ┌──────────────┐
 │ Parquet │ ◄────► │  Arrow   │ ◄────►  │ Arrow Flight │
 └─────────┘         └─────────┘          └──────────────┘
   "files"            "RAM layout"          "network protocol"
```

## Mental model

> **Arrow is the universal in-memory language for columnar data.** Tools that speak Arrow hand data to each other without conversion. Combined with Parquet (universal on-disk language), it eliminates the serialization tax that dominated the previous decade of data engineering.
