# Chapter 7 — Open-Source Alternatives to Spark

The "honest broker" chapter. Clients ask *"why Spark and not X?"* — you need the landscape to give a real answer.

## The landscape

```
                        Batch                    Streaming
                    ┌──────────────────────┬──────────────────────┐
  Distributed       │  Spark               │  Spark Structured    │
  (cluster)         │  Trino / Presto      │    Streaming         │
                    │  Dask                 │  Flink ★             │
                    │  Ray Data             │  Kafka Streams       │
                    │  Apache Beam          │  Apache Beam         │
                    ├──────────────────────┼──────────────────────┤
  Single-machine    │  DuckDB ★            │  Benthos / Redpanda  │
                    │  Polars ★            │                      │
                    │  pandas              │                      │
                    └──────────────────────┴──────────────────────┘

★ = rising fast, worth knowing well
```

## 1. Apache Flink — Spark's biggest rival

A **distributed stream processing engine** with batch as a special case. Born in Berlin (TU Berlin), Apache since 2014. Core philosophy is the opposite of Spark: **Spark treats streaming as micro-batched batch; Flink treats batch as bounded streaming**.

| | Spark | Flink |
|---|---|---|
| **Core model** | Batch-first, streaming bolted on | Streaming-first, batch is "bounded stream" |
| **Processing** | Micro-batch (~100ms–seconds) | **True event-at-a-time** |
| **Latency** | 100ms – seconds | **Milliseconds** |
| **State management** | Limited | **First-class** (RocksDB, exactly-once, checkpointed) |
| **API** | DataFrame/SQL/RDD (Python, Scala, Java, R) | DataStream/Table/SQL (Java, Scala, Python growing) |
| **Ecosystem** | Massive | Smaller but growing |

**Where Flink beats Spark:**
1. True low-latency streaming (sub-100ms).
2. Stateful stream processing with exactly-once guarantees.
3. Event-time processing, watermarks, out-of-order handling.

**Where Spark beats Flink:**
1. Batch analytics (more mature optimizer, more connectors).
2. Ecosystem breadth (MLlib, Delta/Iceberg, Databricks, notebooks).
3. Python support (PySpark >> PyFlink).
4. Talent pool (far more Spark engineers exist).

**When to suggest Flink:**
- Sub-100ms latency needed → Flink.
- Complex stateful event processing → Flink.
- Batch-only → Spark.
- Both batch and streaming → Spark if batch-heavy, Flink if streaming-heavy. Many teams run both.

**Users:** Alibaba (biggest Flink user), Uber, Netflix, Airbnb, Lyft, Stripe, LinkedIn.

## 2. Trino (formerly Presto) — interactive SQL engine

A **distributed SQL query engine** that doesn't store data — it queries data *where it lives* (S3, HDFS, Postgres, MySQL, Cassandra, Kafka, Delta, Iceberg, MongoDB, Elasticsearch, etc.).

Originally created at **Facebook** as Presto (2012). Core team left, forked to **Trino** (2019). Facebook's version is **PrestoDB**. Trino is the community-backed one.

| | Spark SQL | Trino |
|---|---|---|
| **Primary use** | Batch ETL + analytics | Interactive ad-hoc SQL |
| **Query latency** | Seconds to hours | **Sub-second to minutes** |
| **Data processing** | Read → transform → write | **Read-only query** (no native write) |
| **Fault tolerance** | Yes (DAG recompute) | **No** (query fails → re-run) |
| **Concurrency** | Low (few big queries) | **High** (many small queries) |

**Where Trino beats Spark:**
1. Interactive SQL speed — sub-second for analyst dashboards.
2. Federated queries across sources (Postgres + S3 + Cassandra in one SQL).
3. No cluster warm-up — always-on service, instant query.

**Where Spark beats Trino:**
1. ETL / write workloads (Trino is read-mostly).
2. Fault tolerance (8-hour queries can't fail and restart in Trino).
3. ML, streaming, graph — Trino does SQL only.

**The classic pattern:** **Spark writes Iceberg tables, Trino reads them.** Best of both worlds.

**Users:** Netflix, LinkedIn, Lyft, Airbnb, Shopify. Engine behind **Amazon Athena** and **Starburst**.

## 3. DuckDB — embedded analytics

Already covered in depth (Appendix 8). Quick recap:

- **SQLite for analytics.** `pip install duckdb`, query Parquet/CSV/S3 directly.
- Beats Spark for data under ~500 GB on one machine: faster, cheaper, zero ops.
- Can't distribute, no streaming, no ML.
- Exploding adoption: MotherDuck, dbt, Airflow, Observable embed it.

## 4. Polars — modern DataFrame library

A **Rust-based DataFrame library** for Python. "pandas but 10x faster."

| | Spark | Polars |
|---|---|---|
| **Runs on** | Cluster | Single machine |
| **Language** | Python/Scala/Java + JVM | Python + Rust (no JVM) |
| **Lazy evaluation** | Yes | **Yes** (own optimizer) |
| **Memory format** | Tungsten/Arrow | **Arrow natively** |
| **Speed (1 machine)** | Slow (JVM, shuffles) | **Much faster** (Rust, SIMD) |

Beats Spark on single-machine performance. Cleaner API than PySpark. Can't distribute (yet — distributed version in early development as of 2026).

**Suggest when:** data scientists switching from pandas, Python ETL under 500 GB.

## 5. Dask — distributed pandas

A **Python-native distributed library** that scales pandas, NumPy, and scikit-learn. API is almost identical to pandas with `.compute()` as the action trigger.

**Where Dask beats Spark:** Python-native (no JVM), parallelizes arbitrary Python code, lighter weight.
**Where Spark beats Dask:** Performance at scale (Catalyst optimizer), ecosystem, maturity above a few TB.

**Suggest when:** pandas users needing to scale to ~1 TB without learning Spark.

## 6. Ray — distributed compute Swiss army knife

A **general-purpose distributed framework** from UC Berkeley (same lab as Spark). Primary focus: **distributed ML training and inference**.

| | Spark | Ray |
|---|---|---|
| **Primary purpose** | Data processing | **Distributed Python / ML** |
| **ML training** | MLlib (classical only) | **PyTorch/TF/JAX at scale** |
| **Model serving** | Not built in | **Ray Serve** |
| **Data processing** | Excellent | Decent (Ray Data) |

**Suggest when:** distributed deep learning training, ML platforms needing data prep + training + serving in one framework.

**Users:** OpenAI, Anthropic, Uber, Spotify, Netflix, ByteDance. The dominant framework for large-scale ML training.

## 7. Apache Beam — write-once-run-anywhere

A **unified programming model** for batch and streaming that runs on multiple engines (Spark, Flink, Google Dataflow). Created by Google.

Write pipeline once in Beam's API, choose a **runner** at deployment. In practice: the Dataflow runner (Google Cloud) is excellent, the Flink runner is good, the Spark runner is mediocre.

**Suggest when:** Google Cloud + managed streaming (Beam + Dataflow). Otherwise use native APIs.

## 8. Kafka Streams — lightweight stream processing

A **Java library** (not a cluster) for processing Kafka data. Runs inside your regular Java application.

| | Spark Structured Streaming | Kafka Streams |
|---|---|---|
| **Deployment** | Needs a Spark cluster | **Runs in your Java app** |
| **Input/Output** | Any source → any sink | **Kafka → Kafka only** |
| **Latency** | 100ms+ | **Milliseconds** |
| **Scaling** | Spark executors | **Just add app instances** |

**Suggest when:** simple Kafka-to-Kafka transformations with low latency, no separate cluster wanted.

## Decision matrix

| Client situation | Recommend |
|---|---|
| Batch ETL, 100 GB – petabytes | **Spark** |
| Batch ETL, under 500 GB | **DuckDB** or **Polars** |
| Interactive SQL on data lake | **Trino** (+ Spark for ETL) |
| Streaming, latency > 100ms | **Spark Structured Streaming** |
| Streaming, latency < 100ms | **Flink** |
| Streaming, Kafka-to-Kafka only | **Kafka Streams** |
| Distributed ML training | **Ray** |
| Classical ML at scale | **Spark MLlib** (or Ray) |
| Python data science, < 500 GB | **Polars** or **DuckDB** |
| Python data science, > 1 TB | **Spark** (PySpark) or **Dask** |
| Google Cloud managed streaming | **Beam + Dataflow** |
| "We don't know what we need" | Start with **DuckDB**, graduate to **Spark** when data outgrows one machine |

## The big picture

The modern data stack is **composable**:

```
Storage:       S3 / GCS / Azure Blob
Table format:  Iceberg / Delta / Hudi
Memory:        Arrow
ETL:           Spark (or DuckDB for small)
SQL:           Trino (or Spark SQL)
Streaming:     Flink (or Spark Structured Streaming)
ML:            Ray + PyTorch
Orchestration: Airflow / Dagster / Prefect
```

Tools share data through Parquet + Arrow + Iceberg/Delta. You don't pick one — you pick the right tool for each layer. Spark's unique value: it **covers more layers** than any single alternative, which is why it remains the center of gravity.

## Quiz (with answers)

**Q1. Client has Kafka, 1M events/sec, needs 10ms latency. On Spark Structured Streaming, too slow. What do you suggest?**
A: **Flink** if processing is complex (stateful, windowed joins). **Kafka Streams** if it's simple Kafka-to-Kafka transformations. Both handle sub-10ms. Kafka Streams is simpler (no separate cluster); Flink handles more complexity.

**Q2. Client analysts complain Spark SQL dashboards take 30s. 5 TB Iceberg on S3.**
A: Add **Trino** for the analyst-facing query layer. Keep Spark for ETL writes. Classic pattern: Spark writes Iceberg, Trino reads it.

**Q3. Client choosing between Spark and Flink for new platform. What questions to ask?**
A: (1) Acceptable latency? >100ms → Spark, <100ms → Flink. (2) Batch-heavy or streaming-heavy? (3) Need stateful stream processing? (4) Team knows Python or Java? (5) Need ML/notebooks/SQL analytics? (6) Existing infra? Many teams run both: Spark for batch, Flink for streaming, connected through Iceberg.

**Q4. Startup: 100 GB, 3 Python devs, no infra, needs batch ETL + PyTorch training.**
A: **DuckDB or Polars** for ETL (100 GB fits one machine, zero ops). **PyTorch directly** on a single GPU VM for training (100 GB doesn't need distributed training). No Spark, no Ray, no Dask — simplicity wins at this scale. Add Spark + Ray later if data grows past 1 TB.
