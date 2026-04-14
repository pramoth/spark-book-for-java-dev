# Appendix 7 — What is Parquet Really?

**Parquet is a file format** — like CSV or JSON, but designed for analytics on big data. A `.parquet` file is just a file on disk (or S3). What makes it special is **how data is stored inside**.

## Row-oriented vs column-oriented

### CSV (row-oriented)

```
order_id,region,amount,timestamp
1,EU,99.50,2026-04-14
2,US,150.00,2026-04-14
3,EU,45.25,2026-04-14
```

Data stored **row by row**. To read the `amount` column, you must read every line.

### Parquet (column-oriented)

```
order_id column:  [1, 2, 3]
region column:    [EU, US, EU]
amount column:    [99.50, 150.00, 45.25]
timestamp column: [2026-04-14, 2026-04-14, 2026-04-14]
```

All values of one column sit together. Three massive wins:

1. **Column pruning** — `SELECT amount FROM sales` reads **only the amount column's bytes**. On a 100-column table, you might read 1% of the file.
2. **Compression** — values in the same column are similar → compress extremely well. Parquet is typically 5–10× smaller than CSV.
3. **Vectorized reads** — CPUs process arrays of same-typed values very fast (SIMD). Row formats can't.

## What's inside a Parquet file

```
 ┌─────────────────────────────────┐
 │          Parquet file            │
 │                                  │
 │  ┌──────────────────────────┐    │
 │  │ Row group 1 (~128 MB)    │    │
 │  │  ├─ column: order_id     │    │
 │  │  ├─ column: region       │    │
 │  │  ├─ column: amount       │    │
 │  │  └─ column: timestamp    │    │
 │  └──────────────────────────┘    │
 │  ┌──────────────────────────┐    │
 │  │ Row group 2              │    │
 │  │  ├─ column: order_id     │    │
 │  │  ├─ ...                  │    │
 │  └──────────────────────────┘    │
 │  ┌──────────────────────────┐    │
 │  │ Footer (metadata)        │    │
 │  │  - schema                │    │
 │  │  - min/max per col/group │    │
 │  │  - row counts            │    │
 │  └──────────────────────────┘    │
 └─────────────────────────────────┘
```

**Row groups** = chunks of rows (e.g., 1M rows per group). Inside each, data is stored column-by-column. Lets readers skip entire row groups based on stats *and* process chunks in parallel.

**Footer** = schema and per-column stats (min, max, null count). Readers open the footer **first** to plan the read.

## Parquet vs table format — key distinction

| | Parquet | Delta / Iceberg |
|---|---|---|
| **What it is** | A file format | A *table* format (metadata on top of files) |
| **Scope** | One file | A collection of files = one "table" |
| **Knows about** | Rows and columns in itself | Versions, transactions, which files belong |
| **UPDATE/DELETE?** | ❌ immutable | ✅ via the log |

> **Parquet is the floor. Delta/Iceberg are the building on top.** Delta/Iceberg never reinvent storage — they always use Parquet underneath.

## The critical insight: Parquet files are immutable

Once written, you never modify a Parquet file. Every write operation creates **new** Parquet files.

### `INSERT INTO sales VALUES (...)`

1. Spark takes new rows.
2. Executors write **one or more new Parquet files**:
   ```
   s3://bucket/sales/
   ├── part-00000-abc.parquet   ← existed before
   ├── part-00001-def.parquet   ← existed before
   └── part-00002-xyz.parquet   ← NEW
   ```
3. Delta writes a log entry: `{"add": {"path": "part-00002-xyz.parquet"}}`

### `UPDATE sales SET amount = amount * 1.1 WHERE region = 'EU'`

Parquet is immutable, so:

1. Spark reads `part-00000-abc.parquet`, finds rows where `region='EU'`.
2. Writes a **new file** `part-00003-new.parquet` containing:
   - All rows from `abc` **except** EU rows
   - The EU rows with `amount * 1.1` applied
3. Log entry:
   ```json
   {"remove": {"path": "part-00000-abc.parquet"}}
   {"add":    {"path": "part-00003-new.parquet"}}
   ```
4. Old file physically stays until `VACUUM`.

This pattern — *never modify, always rewrite and log* — is called **copy-on-write**. It's how lakehouses get ACID on immutable cloud storage.

(Delta and Iceberg also have **merge-on-read** mode for faster updates — small "delete files" instead of rewriting whole files. Ignore for now.)

### `DELETE FROM sales WHERE amount < 10`

Same pattern: read affected files, write new ones with only rows to *keep*, log `remove` + `add`.

## The "aha" moment

> You never edit a Parquet file. You only **add** or **stop referencing** Parquet files. The table format's log is the single source of truth for "which files make up the table right now."

This is why:
- Cloud storage (bad at updates) works fine as lakehouse storage.
- Time travel is free — old files are still there, just unreferenced.
- Concurrent writes work — each writer creates new files, only the log commit races.

## Mental model

> **Parquet** = a single compressed, columnar file. Immutable. Fast to read.
> **Table format** = a directory of Parquet files + a log that says *"here's the current set and here's the history of changes."*
> **Spark** = the engine that reads/writes both, driven by your SQL or DataFrame code.
