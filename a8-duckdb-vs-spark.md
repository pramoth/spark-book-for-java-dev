# Appendix 8 — DuckDB vs Spark

**DuckDB and Spark solve the same problem (analytics on tabular data) at completely different scales.**

## What is DuckDB?

> SQLite, but optimized for analytics instead of transactions.

- **In-process** — runs *inside* your application, not as a separate server. `pip install duckdb` and you have a database.
- **Analytical** — column-oriented storage, vectorized execution, fast SQL aggregations.
- **Single machine** — one process, no distribution, no cluster manager.

Created at CWI Amsterdam in 2019. Open source (MIT).

## What it looks like

```python
import duckdb

# Query a Parquet file directly — no loading, no server
duckdb.sql("SELECT region, SUM(amount) FROM 'sales.parquet' GROUP BY region").show()

# Query 100 Parquet files at once
duckdb.sql("SELECT COUNT(*) FROM 's3://bucket/sales/*.parquet'").show()

# Join Parquet + CSV + Postgres
duckdb.sql("""
  SELECT c.name, SUM(s.amount)
  FROM 'sales.parquet' s
  JOIN read_csv('customers.csv') c USING (customer_id)
  GROUP BY c.name
""").show()
```

Two things to notice:
1. **No data loading step** — queries files in place.
2. **No server** — just a library call.

## Side-by-side: DuckDB vs Spark

| | DuckDB | Spark |
|---|---|---|
| **Runs as** | Library inside your app | Cluster of JVM processes |
| **Machines** | Always one | One to thousands |
| **Setup** | `pip install duckdb` | Cluster manager + config |
| **Startup time** | Milliseconds | 30 sec – minutes |
| **Data sweet spot** | 1 MB – 1 TB | 100 GB – petabytes |
| **Distributed** | ❌ | ✅ |
| **Fault tolerance** | ❌ | ✅ |
| **Streaming** | ❌ | ✅ |
| **ML library** | ❌ | ✅ (MLlib) |
| **Cost** | Free | Cluster cost ($$$) |
| **Ops complexity** | Zero | High |

## A single machine is now huge

In 2010, "big data" started at 100 GB — bigger than any reasonable machine. In 2026:

- A modest cloud VM has **64 cores / 512 GB RAM**.
- AWS rents `r6id.32xlarge` with **128 cores / 1 TB RAM** for ~$8/hr.
- NVMe SSDs hit 7 GB/sec.
- DuckDB exploits all of that.

So one machine running DuckDB can handle **hundreds of GB to a few TB** comfortably — territory that used to require Spark.

## Where DuckDB beats Spark (surprising)

Benchmarks show DuckDB **faster than Spark** for queries on data up to ~500 GB. Why?

1. **No network shuffles** — everything is RAM + local disk. Spark spends most of its time on shuffles.
2. **No JVM overhead** — C++, no GC, no serialization.
3. **No cluster startup** — 5-second query vs 30 seconds waiting for Spark to wake up.
4. **Hand-tuned vectorized C++** — SIMD-friendly. Spark's Tungsten is good, DuckDB is better.

Crossover where Spark starts winning: **~500 GB to a few TB**, depending on workload.

## Where Spark wins

1. **Data bigger than one machine** (>2 TB).
2. **Distributed fault tolerance** — a 6-hour query failing on hour 5.
3. **Streaming** (Structured Streaming).
4. **Distributed ML** (MLlib).
5. **Multi-user concurrent jobs sharing one cluster.**
6. **Heavy ecosystems** — Delta, Iceberg, Unity Catalog, MLflow.

## Mental picture

```
   Data size →

   1 MB        1 GB        100 GB        1 TB        10 TB        1 PB
   ├───────────┼────────────┼─────────────┼────────────┼────────────┤
   │  pandas / Polars / DuckDB                │            │
   │                                          │  DuckDB    │
   │                                          │   ↕        │
   │                                          │  overlap   │  Spark
   └────────────────────────────── Spark (overkill below ~50 GB) ───┘
```

## Why "DuckDB for 100 GB" is the right call

For a 2-engineer team with 100 GB:

- **DuckDB:** `pip install`, write SQL, done. $50/month server. Zero ops.
- **Spark:** stand up a cluster, learn YARN/K8s, debug shuffles, pay for idle, hire platform engineers.

The Spark route consumes **months of engineering time** for a workload DuckDB handles in an afternoon.

## Related single-machine engines

- **Polars** — Rust DataFrame library, pandas-like API, blazing fast.
- **Pandas 2.x with Arrow backend** — modern pandas is dramatically faster.
- **Dask** — distributed pandas, lighter than Spark, Python-native.
- **Ray Data** — distributed data processing for ML pipelines.

## Mental model

> **DuckDB = SQLite for analytics.** Install a library, point at Parquet/S3, write SQL. For data up to ~1 TB on modern hardware, it's often *faster and cheaper* than Spark — and the operational difference is night and day.
>
> **Spark = a distributed engine for when your data or workload genuinely doesn't fit on one machine.**

## Client conversation questions

1. **"How big is your biggest dataset?"** — under 500 GB → DuckDB first.
2. **"Does it grow fast?"** — slow → single-machine. Doubling quarterly → Spark.
3. **"Streaming or distributed ML?"** — yes → Spark.
4. **"Engineers who like infra?"** — no → managed Spark or skip Spark.
