# Appendix 4 — Data Lake vs Data Warehouse vs Lakehouse

Clients throw these three terms around interchangeably. They are not the same.

## 1. Data Warehouse (1990s–today)

A specialized database built for analytics. Think **Snowflake, BigQuery, Redshift, Teradata**.

- Stores **structured tables** with strict schemas.
- **ACID transactions** — safe `UPDATE`, `DELETE`, `MERGE`.
- **Fast SQL** — indexes, statistics, tuned optimizer.
- **Expensive** — proprietary storage + compute, tightly coupled.
- **Schema-on-write** — data must be modeled before loading.

**Good at:** BI dashboards, financial reports, defined business metrics.
**Bad at:** raw logs, images, JSON blobs, ML training data.

## 2. Data Lake (2010s, Hadoop era)

Just **a pile of files in cheap storage** — HDFS originally, now S3 / GCS / Azure Blob.

- Stores **anything**: CSV, JSON, Parquet, images, video, PDFs.
- **Schema-on-read** — dump raw, figure out structure when querying.
- **Dirt cheap** — S3 is ~$0.02/GB/month.
- **Storage and compute decoupled**.
- ❌ **No transactions.** No safe `UPDATE`/`DELETE`.
- ❌ **No schema enforcement.** Garbage data breaks downstream.
- ❌ **Slow for SQL** — must scan files.

**Nickname it earned:** *"data swamp"* — without discipline, lakes become unusable dumping grounds.

**Good at:** bulk storage, raw data, ML training sets, ingestion landing.
**Bad at:** reliable analytics, concurrent writes, BI.

## 3. Data Lakehouse (2020+, the synthesis)

*"What if we keep the cheap S3 storage of a lake, but add the reliability of a warehouse on top?"*

The magic trick is a **table format** layered over Parquet files:
- **Delta Lake** (Databricks)
- **Apache Iceberg** (Netflix/Apple/AWS)
- **Apache Hudi** (Uber)

These formats add a **transaction log** — small metadata files tracking *"which Parquet files make up version 47 of this table"*.

### What the transaction log gives you

1. **ACID transactions** — concurrent writes are safe.
2. **`UPDATE` / `DELETE` / `MERGE`** — even though S3 objects are immutable.
3. **Time travel** — query the table *as it was yesterday*.
4. **Schema enforcement and evolution**.
5. **Performance** — file-level stats enable skipping.

### Mental picture

```
   DATA LAKE              LAKEHOUSE
 ┌───────────┐          ┌───────────┐
 │ S3 bucket │          │ S3 bucket │
 │           │          │           │
 │ file1.pq  │          │ file1.pq  │
 │ file2.pq  │          │ file2.pq  │
 │ file3.pq  │          │ file3.pq  │
 │ ...       │          │ ...       │
 │           │          │           │
 │           │          │ _delta_log/  ← the magic
 │           │          │  0001.json │
 │           │          │  0002.json │
 │           │          │  0003.json │
 └───────────┘          └───────────┘
  (dumb pile)          (pile + version log)
```

Physically, it's still just files in S3. The lakehouse is what the table format **makes of** those files.

## 4. Data Mart (a subset, not a separate thing)

A **data mart** is a focused slice of a warehouse (or lakehouse) scoped to **one business unit** — Sales, Finance, Marketing, HR.

- **Narrow scope** — one domain, not the whole enterprise.
- **Smaller & faster** — less data, simpler queries.
- **Business-user shaped** — modeled around how that team thinks (e.g. *"monthly revenue by region"*).
- **Usually dimensional** — star/snowflake schemas (fact + dimensions).

### Warehouse vs Mart

| | Warehouse | Data Mart |
|---|---|---|
| **Scope** | Enterprise-wide | Single department |
| **Users** | Analysts across org | Business users in one team |
| **Size** | TB | GB–low TB |
| **Built from** | Source systems | Warehouse (or sources directly) |

### Two build styles

1. **Dependent mart** — carved out of an existing warehouse. *Top-down, Inmon style.*
2. **Independent mart** — built straight from sources, integrated into a warehouse later. *Bottom-up, Kimball style.*

### Where it fits in the lakehouse / medallion model

In the **bronze → silver → gold** pattern used in later labs:

- **Bronze** — raw ingestion
- **Silver** — cleaned, conformed
- **Gold** — business-level aggregates → **this *is* the data mart**

In a modern lakehouse, a data mart is usually just a **curated gold table** (Delta/Iceberg) exposed to a specific BI tool or team. No separate physical system needed — Trino or Spark queries the same storage.

> **The shift:** marts used to be copied into their own databases. In a lakehouse, a mart is just a *view of curated tables* on the same cheap S3 storage.

## Side-by-side

| | Warehouse | Data Lake | Lakehouse |
|---|---|---|---|
| **Storage** | Proprietary, expensive | S3/HDFS, cheap | S3/HDFS, cheap |
| **Formats** | Structured only | Anything | Parquet + log |
| **ACID** | ✅ | ❌ | ✅ |
| **UPDATE/DELETE** | ✅ | ❌ | ✅ |
| **Schema enforcement** | ✅ | ❌ | ✅ |
| **Time travel** | Rare | ❌ | ✅ |
| **SQL performance** | 🚀 | 🐢 | 🏃 |
| **Unstructured data** | ❌ | ✅ | ✅ |
| **Cost** | $$$ | $ | $ |

## The key insight

> **A lakehouse = a data lake with a transaction log bolted on.** Not new storage technology — new *metadata* technology on top of Parquet files in S3.

Delta/Iceberg can adopt existing Parquet files in place — clients rarely "migrate", they *add* a table format.

## Client heuristics

- "We have a data swamp" → lakehouse.
- "Snowflake too expensive at our scale" → lakehouse candidate.
- "We need BI on clean business data" → warehouse is still fine.
- "Raw data for ML + clean for BI" → lakehouse shines.
- "No data platform team" → warn them: lakehouses need engineering.
