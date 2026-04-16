# Lab 4 — Apache Iceberg Side-by-Side

Same operations as Lab 2 (Delta Lake), but with Iceberg. Compare the developer experience and what ends up on disk.

## Run command

```bash
cd /Users/pramoth/work/tmp/spark-book/spark-lab

rm -rf delta-data/iceberg-warehouse

docker run --rm -it \
  --network spark-lab_spark-net \
  -v $(pwd)/jobs:/jobs \
  -v $(pwd)/delta-data:/delta-data \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=/delta-data/iceberg-warehouse \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /jobs/lab4_iceberg.py
```

## What the lab covers (9 parts)

### Part 1 — CREATE table (SQL-first)

```sql
CREATE DATABASE IF NOT EXISTS local.db;
CREATE TABLE local.db.sales (
    order_id BIGINT, region STRING, product STRING, amount DOUBLE
) USING iceberg;
INSERT INTO local.db.sales VALUES (1, 'EU', 'laptop', 999.99), ...;
```

Iceberg uses a **catalog.database.table** naming convention — you never type file paths. The catalog maps `local.db.sales` to a physical directory automatically.

### Part 2 — INSERT

```sql
INSERT INTO local.db.sales VALUES (7, 'US', 'laptop', 1299.00), ...;
```

### Part 3 — UPDATE (standard SQL)

```sql
UPDATE local.db.sales SET amount = amount * 1.1 WHERE region = 'EU';
```

Unlike Delta (which requires the Python `DeltaTable` API), Iceberg UPDATE/DELETE/MERGE are **native SQL**. This is one of Iceberg's biggest ergonomic advantages.

### Part 4 — DELETE

```sql
DELETE FROM local.db.sales WHERE amount < 500;
```

### Part 5 — MERGE (standard SQL)

```sql
MERGE INTO local.db.sales AS sales
USING updates
ON sales.order_id = updates.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### Part 6 — TIME TRAVEL (snapshot-based)

Iceberg uses **snapshot IDs** (long numbers) instead of Delta's simple version numbers:

```sql
SELECT * FROM local.db.sales VERSION AS OF 2199076190430318106;
```

Iceberg has **built-in metadata tables** — no API needed:

```sql
SELECT snapshot_id, committed_at, operation FROM local.db.sales.snapshots;
SELECT * FROM local.db.sales.history;
SELECT file_path, record_count FROM local.db.sales.files;
SELECT path, length FROM local.db.sales.manifests;
```

These metadata tables are one of Iceberg's standout features. Delta requires the Python `DeltaTable.history()` API; Iceberg lets you query metadata as regular SQL tables.

### Part 7 — SCHEMA EVOLUTION (native ALTER TABLE)

```sql
ALTER TABLE local.db.sales ADD COLUMNS (discount DOUBLE);
```

Existing rows get NULL for the new column — **no data rewrite** needed. Iceberg handles schema evolution natively; Delta requires a `mergeSchema` option during writes.

### Part 8 — Files on disk

Iceberg's physical structure:

```
iceberg-warehouse/db/sales/
├── data/                    ← Parquet files
│   ├── 00000-xxx.parquet
│   └── 00001-xxx.parquet
└── metadata/                ← Avro manifests + JSON metadata
    ├── v1.metadata.json     ← table-level pointer
    ├── snap-xxxx.avro       ← manifest list
    └── xxxx-m0.avro         ← manifest (file list + stats)
```

Compared to Delta's flat `_delta_log/` of JSON files, Iceberg has a **two-level metadata tree** (metadata.json → manifest list → manifests). This scales better to millions of files.

### Part 9 — Delta vs Iceberg comparison table

| | Delta Lake (Lab 2) | Iceberg (Lab 4) |
|---|---|---|
| **Write API** | `df.write.format("delta").save("/path")` | `spark.sql("INSERT INTO catalog.t")` |
| **UPDATE/DELETE** | Python API (`delta_table.update(...)`) | **Standard SQL** |
| **MERGE** | Python fluent API | **Standard SQL** |
| **Time travel** | `versionAsOf` (0, 1, 2...) | `VERSION AS OF <snapshot_id>` |
| **History** | `delta_table.history()` | `SELECT * FROM t.snapshots` |
| **Schema evolution** | mergeSchema option | `ALTER TABLE ADD COLUMNS` |
| **Table naming** | File path (`"/path/to/table"`) | Catalog (`"local.db.sales"`) |
| **Metadata** | JSON in `_delta_log/` (flat) | Avro manifests + JSON (two-level tree) |
| **Cleanup** | `VACUUM` | `expire_snapshots` + `remove_orphan_files` |
| **Ecosystem** | Databricks-centric | Vendor-neutral (Netflix, Apple, AWS) |

## Catalog-based naming (Iceberg) vs path-based (Delta)

Delta: you remember the directory path.
```python
spark.read.format("delta").load("/delta-data/bronze/orders")
```

Iceberg: a catalog registry maps names to paths.
```python
spark.sql("SELECT * FROM local.db.sales")
```

The catalog is an extra layer of indirection — a lookup table that converts logical names to physical paths. This makes tables discoverable (`SHOW TABLES`), renameable, and shareable across engines without path coordination.

In production, catalog backends include: Hive Metastore, AWS Glue, Nessie, Unity Catalog, Polaris. All are just registries mapping "table X lives at path Y".

## Key takeaway

> Both Delta and Iceberg do the same thing: ACID transactions on Parquet files. The developer experience is very similar. Pick based on your ecosystem (Databricks → Delta, vendor-neutral/multi-engine → Iceberg), not on features — they've converged.
