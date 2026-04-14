# Chapter 4 — Spark Use Cases

This is the chapter for client conversations. Spark is a **unified engine** — the same cluster can do all of these, which is its biggest selling point.

## 1. Batch ETL (the #1 use case, ~70% of real-world Spark)

**ETL** = Extract, Transform, Load. Read, clean/reshape, write.

**Typical scenario:**
> "Every night at 2 AM, read yesterday's 500 GB of raw JSON logs from S3, parse them, join with the customer table from Postgres, aggregate by region, and write Parquet files to our data lake."

**Why Spark wins:**
- Reads from S3, HDFS, Kafka, JDBC, Parquet, ORC, JSON, CSV, Delta, Iceberg.
- Handles hundreds of GB to TB routinely.
- Runs on a schedule (Airflow, cron, Databricks Jobs).

**Module:** Spark Core + Spark SQL.

**Client red flag:** "Our nightly ETL takes 8 hours and it's getting worse." → Usually skewed joins or too many small files.

## 2. Interactive SQL Analytics / Data Warehousing

**Scenario:**
> "Analysts want to run SQL on 50 TB of Parquet files in S3 without loading it into a traditional warehouse."

```sql
SELECT region, SUM(revenue) FROM 's3://bucket/sales/*.parquet' GROUP BY region;
```

**Module:** Spark SQL + **Thrift Server** (JDBC endpoint for Tableau, Power BI, Superset).

**Competitor alert:** **Trino/Presto, DuckDB, Dremio** compete hard here.

## 3. Structured Streaming (real-time / near-real-time)

**Scenario:**
> "Process events from Kafka as they arrive, compute rolling 5-minute counts per user, write alerts to another Kafka topic."

```python
df = spark.readStream.format("kafka").load()
df.groupBy(window("timestamp", "5 minutes"), "user").count() \
  .writeStream.format("kafka").start()
```

**Mind-blowing part:** same DataFrame API as batch — **unified batch and streaming**.

**Important nuance — micro-batching:**
- **Latency floor:** ~100 ms – 1 second. Not microseconds.
- **Throughput:** excellent.
- **Not suitable for:** ultra-low-latency trading, sub-100ms fraud detection.

**Competitor alert:** **Apache Flink** is true event-at-a-time, lower latency.

**Client red flag:** "we need real-time" → ask *how* real-time. If <1s → Spark fine. If <50ms → Flink.

## 4. Machine Learning at Scale (MLlib)

**Scenario:**
> "Train a recommendation model on 200 million user-product interactions."

Spark ships with **MLlib** — distributed classical ML: regression, trees, random forests, GBT, k-means, ALS, PCA.

**When to use MLlib:**
- Training data too big for one machine.
- Classical ML, not deep learning.
- Want ML inside your existing Spark ETL pipeline.

**When NOT:**
- Deep learning (use PyTorch/TensorFlow + Ray/Horovod).
- Small data (use scikit-learn — simpler, better algorithms).
- Cutting-edge research (MLlib is conservative).

**Client red flag:** "Using Spark MLlib for deep learning" → wrong tool.

## 5. Graph Processing (GraphX / GraphFrames)

Least used Spark module. Serious graph work → **Neo4j** or specialized engines. GraphX only if graph work is a *small step inside a bigger Spark pipeline*.

## 6. Data Lakehouse (the modern big-ticket use case)

**Scenario:**
> "Cheap S3 storage like a lake, plus ACID transactions, UPDATE/DELETE, time travel like a warehouse."

Spark + a **table format**:
- **Delta Lake** (Databricks, most popular)
- **Apache Iceberg** (Netflix-origin, vendor-neutral, rising fast)
- **Apache Hudi** (Uber, upsert-focused)

These add a transaction log on top of Parquet files.

**Why clients care:** replace expensive warehouses (Snowflake, BigQuery, Redshift) with a lakehouse at a fraction of the cost — if they have the engineering team.

## Decision table for clients

| Client need | Spark module | Alternative |
|---|---|---|
| Nightly batch ETL | Spark SQL / DataFrame | dbt + warehouse |
| Interactive SQL on data lake | Spark SQL + Thrift | Trino, DuckDB, Dremio |
| Near-real-time streaming (≥1s) | Structured Streaming | Flink, Kafka Streams |
| Ultra-low-latency streaming | ❌ not Spark | Flink, Materialize |
| Distributed classical ML | MLlib | Dask-ML, Ray |
| Deep learning | ❌ not Spark | PyTorch, TF + Ray/Horovod |
| Graph analytics | GraphFrames (meh) | Neo4j, TigerGraph |
| Lakehouse | Spark + Delta/Iceberg | Snowflake, BigQuery |
| Small data (<50 GB) | ❌ overkill | pandas, DuckDB, Polars |

## The killer selling point

> "With Spark, your batch ETL, SQL analytics, streaming, and ML all run on **one engine**, **one cluster**, **one codebase**."

## The most important client warning

> **Spark is overkill for small data.** If a client's biggest dataset is under ~50 GB, Spark will be *slower* than pandas, DuckDB, or Polars, and way more operationally painful.

## Quiz (with answers)

**Q1. Client needs Kafka events with 20ms latency — use Spark?**
A: No. Spark's floor is ~100 ms micro-batching. 20 ms needs Flink or Kafka Streams.

**Q2. Client has 30 GB of CSVs, wants SQL — use Spark?**
A: No — too small. DuckDB, Polars, or pandas on one big machine is faster and simpler.

**Q3. What does "unified batch and streaming" mean?**
A: The same DataFrame code works for batch and streaming. Spark re-runs the query incrementally as new data arrives. One codebase for both — huge operational win.

**Q4. Client says "training a deep NN on Spark MLlib":**
A: Wrong tool. MLlib does classical ML. For deep learning, use PyTorch/TensorFlow, distributed with Ray or Horovod. Spark can still do the data prep upstream.
