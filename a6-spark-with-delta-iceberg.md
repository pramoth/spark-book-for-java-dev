# Appendix 6 — How Spark Works with Delta and Iceberg

Spark itself **doesn't know** what Delta or Iceberg are out of the box. They are **external libraries** that plug into Spark's **DataSource API**. Once loaded, they feel completely native.

## 1. Setup — adding the library

**Delta Lake:**
```bash
spark-shell --packages io.delta:delta-spark_2.13:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**Iceberg:**
```bash
spark-shell --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.5.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3://bucket/warehouse
```

Two things get registered:
- **SQL extensions** — new keywords (`MERGE`, `VACUUM`, `OPTIMIZE`, `VERSION AS OF`).
- **Catalog** — how Spark resolves table names.

## 2. Reading and writing — feels native

### Delta — DataFrame API

```python
df.write.format("delta").mode("append").save("s3://bucket/sales")
df = spark.read.format("delta").load("s3://bucket/sales")

# Time travel
df_old = spark.read.format("delta").option("versionAsOf", 5).load("s3://bucket/sales")
df_old = spark.read.format("delta").option("timestampAsOf", "2026-04-10").load("s3://bucket/sales")
```

### Delta — SQL

```sql
CREATE TABLE sales (order_id BIGINT, region STRING, amount DOUBLE) USING delta;

INSERT INTO sales VALUES (1, 'EU', 99.50);

UPDATE sales SET amount = amount * 1.1 WHERE region = 'EU';
DELETE FROM sales WHERE amount < 10;

MERGE INTO sales t
USING updates s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM sales VERSION AS OF 5;
```

`UPDATE`, `DELETE`, `MERGE` are new in Spark SQL when Delta is loaded — vanilla Spark SQL on Parquet doesn't support these.

### Iceberg — same pattern

```python
df.writeTo("local.db.sales").append()
df = spark.read.format("iceberg").load("local.db.sales")
```

```sql
CREATE TABLE local.db.sales (order_id BIGINT, region STRING, amount DOUBLE)
  USING iceberg PARTITIONED BY (region);

SELECT * FROM local.db.sales.snapshots;       -- metadata tables
SELECT * FROM local.db.sales.history;
SELECT * FROM local.db.sales FOR VERSION AS OF 1234567;
```

## 3. What happens under the hood (SELECT)

```
 ┌─────────────────────────────────────────────┐
 │ 1. Spark SQL parser sees "sales"            │
 │    → asks DeltaCatalog: what is this?       │
 └──────────────┬──────────────────────────────┘
                ▼
 ┌─────────────────────────────────────────────┐
 │ 2. DeltaCatalog returns a DeltaTable object │
 │    pointing at s3://bucket/sales/           │
 └──────────────┬──────────────────────────────┘
                ▼
 ┌─────────────────────────────────────────────┐
 │ 3. Delta reads _delta_log/*.json            │
 │    → builds list of ACTIVE parquet files    │
 │    → extracts per-file stats (min/max)      │
 └──────────────┬──────────────────────────────┘
                ▼
 ┌─────────────────────────────────────────────┐
 │ 4. Catalyst optimizer applies WHERE filter  │
 │    → skips files where stats exclude match  │
 │      (file-level pruning)                   │
 └──────────────┬──────────────────────────────┘
                ▼
 ┌─────────────────────────────────────────────┐
 │ 5. Spark builds physical plan:              │
 │    read ONLY surviving Parquets             │
 │    → normal Parquet scan tasks              │
 │    → distributed across executors           │
 └─────────────────────────────────────────────┘
```

After step 4, Delta/Iceberg get out of the way. Actual data read = normal Parquet on executors. Table format tells Spark *which* files to read, not *how* to read them.

## 4. Writing — where the magic is

`INSERT INTO sales VALUES (...)`:

1. Executors write new Parquet files to `s3://bucket/sales/`.
2. They report *"wrote file X with Y rows, stats Z"* to the driver.
3. Driver writes one new JSON to `_delta_log/` (e.g., `000...0047.json`) listing `add` actions.
4. Log write is **atomic** (S3 PUT-if-absent or coordinator). Another writer won? Driver **retries** with new base version.

That atomic log write is how **concurrent writes** work safely on S3.

## 5. Catalyst integration

Both Delta and Iceberg plug deeply into Catalyst:
- **Custom rules** — rewrite `MERGE` into physical operations.
- **Statistics** — feed per-file stats to the optimizer.
- **Partition pruning** — extract partition filters before reading.
- **Column pruning** — tell the Parquet reader only to load needed columns.

Not bolted on — **optimizer-integrated**.

## Mental model

> Spark is the **engine**. Delta/Iceberg are **table format plugins** that teach Spark how to read a folder of Parquet files as a *versioned, ACID table*. From your code, it's `format("delta")` or `format("iceberg")` — underneath, those libraries drive the transaction log, stats, pruning, and commit protocol.

## Client Q&A

- **"Can we use both Delta and Iceberg in one cluster?"** — Yes, load both libraries, use different catalogs.
- **"Do we need Databricks for Delta?"** — No, Delta is open source (Linux Foundation).
- **"Can non-Spark engines read Delta/Iceberg?"** — Increasingly yes: Trino, Flink, DuckDB, Snowflake, BigQuery all have readers.
