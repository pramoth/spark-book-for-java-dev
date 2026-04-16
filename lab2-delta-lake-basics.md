# Lab 2 — Delta Lake Basics

Hands-on: CRUD operations, time travel, schema enforcement, and VACUUM on a real Delta table running on your Docker cluster.

## Prerequisites

- Docker cluster from Lab 1 (Chapter 6) running.
- `delta-data/` volume mounted on all containers (master + workers).

## Setup

### docker-compose.yml must include shared volume

Every service needs this volume mount so executors can read/write Delta data:
```yaml
volumes:
  - ./delta-data:/delta-data
```

Without this, the driver can write but executors fail with `Mkdirs failed` — because `/delta-data` only exists inside the client container, not on the worker nodes. In production, this is solved by S3/HDFS (shared storage). In Docker, we use a shared host mount.

### Run command

```bash
docker run --rm -it \
  --network spark-lab_spark-net \
  -v $(pwd)/jobs:/jobs \
  -v $(pwd)/delta-data:/delta-data \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /jobs/lab2_delta.py
```

Key flags:
- **`--packages io.delta:delta-spark_2.12:3.1.0`** — downloads the Delta JAR from Maven. Version must match Spark: 3.1.0 works with Spark 3.5.1. (3.2.1 does NOT — `NoSuchMethodError`.)
- **`--conf spark.sql.extensions=...`** — registers Delta's SQL keywords (`MERGE`, `VACUUM`, etc.).
- **`--conf spark.sql.catalog.spark_catalog=...`** — lets Spark resolve table names via Delta's catalog.
- **`--conf spark.jars.ivy=/tmp/.ivy2`** — fixes Ivy cache permission error in the apache/spark image (the default `~/.ivy2` isn't writable).

## What the lab covers (9 parts)

### Part 1 — CREATE a Delta table
```python
df.write.format("delta").mode("overwrite").save("/delta-data/sales")
```
Creates Parquet files + `_delta_log/` folder. This is the moment a plain folder becomes a Delta table.

### Part 2 — INSERT (append)
```python
new_df.write.format("delta").mode("append").save("/delta-data/sales")
```
Writes new Parquet files. Log version 1 records `add` actions for the new files.

### Part 3 — UPDATE
```python
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "/delta-data/sales")
delta_table.update(condition="region = 'EU'", set={"amount": col("amount") * 1.1})
```
Reads affected Parquet files, writes **new** files with modified values, logs `remove` (old) + `add` (new). **Copy-on-write** — the old file is never modified.

### Part 4 — DELETE
```python
delta_table.delete("amount < 500")
```
Same pattern: reads, writes new files with only surviving rows, logs the swap.

### Part 5 — MERGE (upsert)
```python
delta_table.alias("sales").merge(
    updates.alias("updates"),
    "sales.order_id = updates.order_id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()
```
The most powerful operation: atomically UPDATE existing rows + INSERT new rows in one pass. Equivalent to SQL `MERGE INTO`.

### Part 6 — TIME TRAVEL
```python
# Read the table as it was at version 0
v0 = spark.read.format("delta").option("versionAsOf", 0).load("/delta-data/sales")

# See full history
delta_table.history().select("version", "timestamp", "operation").show()
```
Delta reads the log only up to the requested version. Old Parquet files are still on disk, so old data is accessible.

### Part 7 — SCHEMA ENFORCEMENT
```python
bad_df = spark.createDataFrame(bad_data, bad_schema)  # has extra 'discount' column
bad_df.write.format("delta").mode("append").save(...)  # FAILS
```
Delta rejects writes whose schema doesn't match the table. Protects data integrity — no garbage gets in.

### Part 8 — Inspect `_delta_log/` on disk
The script reads the actual JSON log files and prints them. You see `commitInfo`, `metaData`, `add`, `remove` actions — exactly what Appendix 5 described.

After all operations: **9 Parquet files** on disk (some "dead" — superseded by UPDATE/DELETE but kept for time travel), **5 log files** (versions 0–4).

### Part 9 — VACUUM
```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
delta_table.vacuum(retentionHours=0)
```
Removes unreferenced Parquet files. 9 files → 3 files. Time travel to pre-vacuum versions is now broken (those files are gone).

**Production rule:** keep `retentionHours=168` (7 days). Only set to 0 for demos.

## What's on disk after the lab

```
delta-data/sales/
├── part-00000-xxxx.snappy.parquet    ← current data (3 surviving files)
├── part-00001-xxxx.snappy.parquet
├── part-00001-xxxx.snappy.parquet
└── _delta_log/
    ├── 00000000000000000000.json     ← version 0: initial WRITE
    ├── 00000000000000000001.json     ← version 1: INSERT (append)
    ├── 00000000000000000002.json     ← version 2: UPDATE
    ├── 00000000000000000003.json     ← version 3: DELETE
    ├── 00000000000000000004.json     ← version 4: MERGE
    ├── 00000000000000000005.json     ← version 5: VACUUM start
    └── 00000000000000000006.json     ← version 6: VACUUM end
```

## Gotchas encountered

1. **Delta version must match Spark.** Delta 3.1.0 works with Spark 3.5.1. Delta 3.2.1 throws `NoSuchMethodError`.
2. **Ivy cache permissions.** The `apache/spark` image's default `~/.ivy2` isn't writable. Fix: `--conf spark.jars.ivy=/tmp/.ivy2`.
3. **Shared storage required.** Workers must see the same `/delta-data` path as the driver. In Docker: mount the same host directory. In production: use S3/HDFS.
4. **VACUUM safety check.** By default Delta refuses `retentionHours < 168` (7 days). Override with `retentionDurationCheck.enabled=false` for demos only.

## Key takeaways

- Parquet files are **immutable**. Every UPDATE/DELETE writes **new** files.
- `_delta_log/` is the **source of truth** for what's current.
- Time travel = reading an older prefix of the log.
- VACUUM = garbage collection for dead Parquet files. Breaks old time travel.
- Schema enforcement = Delta rejects writes with wrong columns at the door.
- MERGE = the most common production pattern (upsert from a staging table into the main table).
