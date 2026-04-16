# Chapter 8 — Client-Conversation Cheat Sheet

The reference you keep open during client meetings. Everything from Lessons 1–7 distilled into quick-lookup format.

## The Five Questions to Ask Every Client

### Q1. "How big is your biggest dataset today?"

| Answer | Direction |
|---|---|
| Under 50 GB | **Don't use Spark.** DuckDB, Polars, or pandas. |
| 50 – 500 GB | **Maybe Spark.** DuckDB on a beefy VM handles this. Spark if also need streaming/ML. |
| 500 GB – 10 TB | **Spark territory.** The sweet spot. |
| 10 TB+ | **Spark, no question.** Possibly Spark + Trino for queries. |

### Q2. "How fast does it need to be?"

| Answer | Direction |
|---|---|
| Nightly batch (hours OK) | Spark, or DuckDB/Polars if small |
| Interactive SQL (seconds) | Trino on Spark-written tables |
| Near-real-time (100ms – seconds) | Spark Structured Streaming |
| True real-time (< 100ms) | **Flink** or Kafka Streams |

### Q3. "What does your team know?"

| Answer | Direction |
|---|---|
| Python data scientists | PySpark, Polars, DuckDB |
| Java/Scala engineers | Spark (Dataset API), Flink, Kafka Streams |
| SQL analysts | Spark SQL, Trino, DuckDB |
| "We'll hire" | Default to Spark — largest talent pool |

### Q4. "What infrastructure do you have?"

| Answer | Direction |
|---|---|
| Hadoop / Cloudera / HDFS | Spark on YARN |
| Kubernetes | Spark on K8s |
| AWS | EMR, EMR Serverless, or Databricks |
| GCP | Dataproc or Databricks |
| Azure | Synapse, HDInsight, or Databricks |
| Nothing yet | Start with DuckDB. Add managed Spark when needed. |

### Q5. "What's the workload mix?"

| Answer | Direction |
|---|---|
| Pure batch ETL | Spark (or DuckDB if small) |
| Batch + interactive SQL | Spark + Trino |
| Batch + streaming | Spark (both) or Spark + Flink |
| Streaming-heavy | Flink |
| Batch + ML training | Spark + Ray (deep learning) or Spark + MLlib (classical) |
| "A bit of everything" | Spark as center, specialists at edges |

## One-Liner for Each Tool

| Tool | One-liner |
|---|---|
| **Spark** | "The Swiss army knife — batch, SQL, streaming, ML in one engine on one cluster." |
| **Flink** | "True event-at-a-time streaming with millisecond latency." |
| **Trino** | "Interactive SQL engine that queries data where it lives. Sub-second dashboards." |
| **DuckDB** | "SQLite for analytics. One pip install, queries terabytes of Parquet." |
| **Polars** | "pandas but 10x faster. Rust under the hood, Arrow-native." |
| **Dask** | "Distributed pandas. Scale to ~1 TB without leaving the pandas API." |
| **Ray** | "The framework for distributed ML. PyTorch on 100 GPUs." |
| **Kafka Streams** | "Stream processing as a Java library. No cluster needed." |
| **Beam** | "Write once, run on Spark/Flink/Dataflow. Best on Google Cloud." |
| **Delta Lake** | "ACID transactions on your S3 Parquet files. Lake → lakehouse." |
| **Iceberg** | "Same as Delta but vendor-neutral. Netflix origin. Rising fast." |
| **Arrow** | "The universal in-memory format. Why moving data between tools is free." |
| **Parquet** | "The universal on-disk format. Columnar, compressed, fast to scan." |
| **Databricks** | "Premium managed platform by Spark's creators. Fastest but most expensive." |

## Architecture Patterns

### Pattern 1: Small team, small data

```
   S3
    │
    ▼
  DuckDB  ──→  pandas/Polars  ──→  matplotlib / sklearn
```

No Spark. No cluster. One VM. Cost: < $100/month.

### Pattern 2: Medium data, batch ETL + analytics

```
   S3 (Parquet + Iceberg)
    │              │
    ▼              ▼
  Spark          Trino
 (writes)       (reads, dashboards)
    │
    ▼
  Airflow (orchestration)
```

Most common production pattern 2025–2026.

### Pattern 3: Streaming + batch

```
   Kafka
    │
    ├──→  Flink (real-time, < 100ms)
    │        │
    │        ▼
    │     Iceberg on S3
    │        │
    └──→  Spark (batch, hourly/nightly)
             │
             ▼
          Iceberg on S3
             │
             ▼
           Trino (analyst queries)
```

### Pattern 4: ML platform

```
   S3 (Iceberg)
    │
    ▼
  Spark (data prep, features)
    │
    ▼
  Ray (distributed PyTorch training)
    │
    ▼
  Ray Serve (model serving)
```

### Pattern 5: Databricks everything

```
   Cloud Storage
    │
    ▼
  Databricks
    ├── Spark + Photon (ETL, 2-3x faster)
    ├── Delta Lake (storage)
    ├── Unity Catalog (governance)
    ├── SQL Warehouse (analyst queries)
    ├── MLflow (ML)
    ├── Workflows (scheduling)
    └── Notebooks (collaboration)
```

One vendor, one bill. Premium path.

## Red Flags

| Client says | Problem | Suggest |
|---|---|---|
| "We need Spark for 50 GB" | Overkill | DuckDB / Polars |
| "Training deep learning on MLlib" | MLlib = classical ML only | PyTorch + Ray |
| "Real-time with Spark" (< 50ms) | Spark floor is ~100ms | Flink / Kafka Streams |
| "Databricks is official Spark" | Databricks is a company, Spark is Apache OSS | Clarify, evaluate cost |
| "Building a data lake" (no table format) | Future data swamp | Add Iceberg or Delta from day 1 |
| "We need Hadoop" | Hadoop is legacy in 2026 | S3 + Spark on K8s or managed |
| "One tool for everything" | No tool does everything best | Compose stack from specialists + open formats |

## Cost Heuristics (rough monthly)

| Setup | Cost |
|---|---|
| DuckDB on one VM | $50–200 |
| Spark on K8s (self-managed, small) | $1,000–5,000 |
| EMR / Dataproc (small cluster) | $2,000–10,000 |
| Databricks (small-medium) | $5,000–30,000 |
| Databricks (enterprise) | $50,000–500,000+ |

**Hidden cost:** engineer time. A $5K/month self-managed cluster needing 2 engineers costs more than a $15K/month managed service needing zero.

## Vocabulary Quick Reference

| Term | What it means |
|---|---|
| **Data Lake** | Folder of files in S3. No transactions. |
| **Data Lakehouse** | Lake + table format (Delta/Iceberg). ACID, time travel. |
| **Data Warehouse** | Managed analytics DB (Snowflake, BigQuery). Expensive, reliable. |
| **ELT vs ETL** | ETL: transform then load. ELT: load raw, transform inside. ELT is the modern trend. |
| **Data Mesh** | Org pattern: each team owns their data as a "product". Not a technology. |
| **Data Fabric** | Vendor marketing for "integrated data management". Ask what they mean. |
| **Medallion Architecture** | Bronze (raw) → Silver (cleaned) → Gold (business-ready). Naming convention for ETL layers. |
| **dbt** | SQL transformation tool running inside warehouse/lakehouse. Often replaces Spark for SQL-only transforms. |
| **Airflow** | Dominant workflow orchestrator. Schedules Spark/Flink/dbt jobs. Not a processing engine. |
| **Kafka** | Distributed event platform. The "highway" streaming engines read from. |
| **OLAP vs OLTP** | OLTP: transactional (Postgres — fast writes, row-oriented). OLAP: analytical (Spark, Trino — fast reads, columnar). |

## The Closing Template

When a client asks *"so what should we use?"*:

> *"It depends on three things: how big your data is, how fast you need answers, and how much engineering effort you want to spend. For [their data size], I'd start with [tool] for [their primary workload]. If you also need [secondary need], add [specialist tool]. Connect them through [Iceberg/Delta] on [S3/GCS] so you're never locked in and can swap components later."*

Fill in the blanks with Lessons 1–7. You now have the knowledge to fill every blank correctly.
