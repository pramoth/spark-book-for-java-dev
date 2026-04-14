# Appendix 11 — Why Convert DuckDB to pandas At All?

**For many tasks, you don't need to.** People used to convert out of habit; increasingly they're stopping.

## Why people converted to pandas historically

### 1. pandas was the lingua franca

For ~10 years pandas was the *only* serious Python DataFrame. Everything plugged into pandas:
- scikit-learn, matplotlib, seaborn, plotly, statsmodels.
- Jupyter renders DataFrames nicely.
- Stack Overflow, tutorials, books.

Your query ran in DuckDB, but downstream code *demanded* pandas.

### 2. pandas has things SQL can't easily express

SQL is great for: filter, join, group, aggregate, window functions.
SQL is awkward for:
- Row-by-row Python logic.
- Calling a Python function per value (regex from a library, API call, ML inference).
- Reshaping (pivot/unpivot).
- Custom time-series resampling.
- Plotting.

### 3. Mutability and exploration

```python
df["new_col"] = df["a"] * df["b"]
df.loc[df.x > 10, "flag"] = True
```
DuckDB tables are immutable query results. For interactive exploration, mutating in-place felt natural.

## Why people are now skipping the conversion

### Change 1: DuckDB became powerful enough

Modern DuckDB has: window functions, CTEs, PIVOT/UNPIVOT, LIST/STRUCT/MAP types, JSON, regex, dates, stats functions, Python UDFs, `SUMMARIZE`, direct Parquet/CSV/JSON/Excel/Iceberg/Delta I/O.

**DuckDB SQL covers ~95% of what you used pandas for.** When it doesn't, mix — Python UDFs in SQL, or call DuckDB from Python.

### Change 2: ML and plotting accept Arrow / DuckDB directly

- scikit-learn accepts Arrow tables and Arrow-backed pandas.
- Plotly Express accepts DuckDB relations.
- Altair accepts Arrow tables.
- PyTorch/TensorFlow can ingest from Arrow batches.

The forcing function for converting is weakening every year.

### Change 3: pandas isn't the obvious choice anymore

Modern picks:
- **Polars** — Rust-based, faster, nicer API, native Arrow.
- **pandas 2.x with Arrow backend** — pandas without legacy pain.
- **Ibis** — pandas-like code that compiles to SQL and runs on DuckDB / BigQuery / Snowflake / Spark.

## When to stay in DuckDB

```python
duckdb.sql("""
  SELECT region, COUNT(*) AS orders, SUM(amount) AS revenue
  FROM 'sales.parquet'
  WHERE order_date >= '2026-01-01'
  GROUP BY region
  ORDER BY revenue DESC
""").show()
```

You got your answer. No conversion needed.

Chain into another query:
```python
duckdb.sql("CREATE VIEW recent AS SELECT * FROM 'sales.parquet' WHERE order_date >= '2026-01-01'")
duckdb.sql("SELECT region, SUM(amount) FROM recent GROUP BY region").show()
```

## When to leave DuckDB (legitimate)

1. **Feeding sklearn / PyTorch** — most still want a DataFrame or array.
2. **Row-wise Python function** — calling an API, regex library, custom logic.
3. **Plotting library that doesn't speak Arrow yet.**
4. **Sharing code with pandas-only colleagues** — pragmatic.
5. **Genuinely iterative mutable analysis.**

In those cases, keep the conversion — but use Arrow so it's free:
```python
df = duckdb.sql("...").df()   # Arrow-backed since DuckDB ~0.7
```

## Modern best-practice pattern

```
 ┌────────────────────────────────────────┐
 │ Heavy lifting: DuckDB SQL              │  ← GBs to TBs
 │  - reads Parquet/Iceberg directly      │
 │  - vectorized C++, fast                │
 └────────────────┬───────────────────────┘
                  │  hand off final ~thousands of rows
                  ▼  (Arrow zero-copy)
 ┌────────────────────────────────────────┐
 │ Last-mile: pandas / Polars / sklearn   │  ← KBs
 │  matplotlib / plotly                   │
 └────────────────────────────────────────┘
```

**Push heavy work into DuckDB, keep last-mile Python work in pandas/Polars.** Arrow handoff is free.

## Concrete example

**Old way — everything in pandas:**
```python
df = pd.read_parquet("s3://bucket/sales/")     # loads all 50 GB. Maybe crashes.
df = df[df.order_date >= "2026-01-01"]
result = df.groupby("region")["amount"].sum().reset_index()
result.plot.bar(x="region", y="amount")
```

**Modern way — DuckDB does the work:**
```python
result = duckdb.sql("""
  SELECT region, SUM(amount) AS amount
  FROM 's3://bucket/sales/*.parquet'
  WHERE order_date >= '2026-01-01'
  GROUP BY region
""").df()                                       # tiny result
result.plot.bar(x="region", y="amount")
```

DuckDB reads 50 GB, pandas sees 10 rows. **pandas never touches the big data.**

## Mental model

> Converting DuckDB to pandas isn't *required* — it's a habit from when pandas was the only Python DataFrame. Modern best practice: **stay in DuckDB as long as possible**, convert only at the end when you need a Python library that genuinely needs a DataFrame. Thanks to Arrow, that conversion is essentially free.

## Client lesson

*"Should we use DuckDB or pandas?"* → **Both, in different places:**
- **DuckDB** for the query layer — gigabytes to terabytes.
- **Polars or pandas** for in-memory manipulation — megabytes, post-filtering.
- **Arrow** as the glue — invisible, free.

Stop thinking *"which tool?"*, start thinking *"what role does each play?"*
