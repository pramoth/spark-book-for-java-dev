# Learning Apache Spark — A Java Developer's Guide

A conversational walkthrough of Apache Spark for a Java developer who needs conceptual breadth and client-conversation fluency.

## Table of Contents

1. [Chapter 1 — Why Spark Exists](01-why-spark-exists.md)
2. [Chapter 2 — Spark Architecture](02-architecture.md)
3. [Chapter 3 — RDD, DataFrame, Dataset](03-rdd-dataframe-dataset.md)
4. [Chapter 4 — Spark Use Cases](04-use-cases.md)
5. [Chapter 5 — Ecosystem & Deployment](05-ecosystem-and-deployment.md)
6. [Chapter 6 — Docker Hands-On](06-docker-hands-on.md)
7. [Chapter 7 — Open-Source Alternatives](07-open-source-alternatives.md)
8. [Chapter 8 — Client-Conversation Cheat Sheet](08-client-cheat-sheet.md)

## Appendices (side discussions)

- [A1 — Does Spark spawn Docker containers?](a1-spark-and-docker.md)
- [A2 — Cluster Managers: Standalone, YARN, Kubernetes](a2-cluster-managers.md)
- [A3 — What is a DAG?](a3-what-is-dag.md)
- [A4 — Data Lake vs Data Warehouse vs Lakehouse](a4-lake-warehouse-lakehouse.md)
- [A5 — Table Formats (Delta, Iceberg) on disk](a5-table-formats.md)
- [A6 — How Spark works with Delta and Iceberg](a6-spark-with-delta-iceberg.md)
- [A7 — What is Parquet really?](a7-parquet-explained.md)
- [A8 — DuckDB vs Spark](a8-duckdb-vs-spark.md)
- [A9 — Apache Arrow](a9-apache-arrow.md)
- [A10 — Why DuckDB ↔ pandas is suddenly fast](a10-duckdb-pandas-fast.md)
- [A11 — Why convert DuckDB to pandas at all?](a11-stay-in-duckdb.md)
- [A12 — Job, Stage, Task: the three-level hierarchy](a12-job-stage-task.md)
- [A13 — Mapping code to stages](a13-code-to-stage.md)
- [A14 — Shuffle reuse, skipped stages, and cache()](a14-shuffle-reuse-cache.md)
- [A15 — Data Lake vs Delta Lake (the naming confusion)](a15-data-lake-vs-delta-lake.md)

## Hands-on labs

- [Lab 1 — spark-lab/](spark-lab/) — Docker cluster + wordcount (Chapter 6)
- [Lab 2 — Delta Lake basics](lab2-delta-lake-basics.md) — CRUD, time travel, schema enforcement, VACUUM
  - Script: [spark-lab/jobs/lab2_delta.py](spark-lab/jobs/lab2_delta.py)
- [Lab 3 — Medallion architecture](lab3-medallion-architecture.md) — Bronze → Silver → Gold pipeline with messy data
  - Script: [spark-lab/jobs/lab3_medallion.py](spark-lab/jobs/lab3_medallion.py)
- [Lab 4 — Iceberg side-by-side](lab4-iceberg-side-by-side.md) — same ops as Lab 2, Iceberg vs Delta comparison
  - Script: [spark-lab/jobs/lab4_iceberg.py](spark-lab/jobs/lab4_iceberg.py)
- [Playground — Interactive PySpark REPL](lab-playground.md) — explore all lab tables with `./repl.sh`
  - Script: [spark-lab/repl.sh](spark-lab/repl.sh) — one command, loads both Delta + Iceberg

## Planned labs

- Lab 5 — Streaming into the lakehouse (Kafka → Spark → Delta)
- Lab 6 — Trino reads the lakehouse (Spark writes, Trino queries)
