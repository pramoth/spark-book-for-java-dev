"""
Lab 4 — Apache Iceberg Side-by-Side
====================================
Same operations as Lab 2 (Delta), but with Iceberg.
Compare the developer experience and what ends up on disk.

Run with:
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
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = (SparkSession.builder
         .appName("lab4-iceberg")
         .getOrCreate())

# Iceberg uses a CATALOG.DATABASE.TABLE naming convention
# We configured "local" as the catalog name in spark-submit
TABLE = "local.db.sales"


# ════════════════════════════════════════════════════════════
# PART 1 — CREATE an Iceberg table
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 1: Creating an Iceberg table")
print("="*60)

# Create the database first (Iceberg requires it)
spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

# Create table using SQL (Iceberg's primary interface)
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
spark.sql(f"""
    CREATE TABLE {TABLE} (
        order_id BIGINT,
        region STRING,
        product STRING,
        amount DOUBLE
    ) USING iceberg
""")

# Insert data using SQL
spark.sql(f"""
    INSERT INTO {TABLE} VALUES
        (1, 'EU', 'laptop', 999.99),
        (2, 'US', 'phone', 699.50),
        (3, 'EU', 'tablet', 449.00),
        (4, 'APAC', 'laptop', 1099.00),
        (5, 'US', 'phone', 599.00),
        (6, 'EU', 'phone', 749.99)
""")

print("Wrote 6 rows as Iceberg table.")
print(f"Table name: {TABLE}")

spark.sql(f"SELECT * FROM {TABLE} ORDER BY order_id").show()
print(f"Row count: {spark.sql(f'SELECT COUNT(*) FROM {TABLE}').collect()[0][0]}")


# ════════════════════════════════════════════════════════════
# PART 2 — INSERT MORE ROWS
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 2: INSERT — appending new rows")
print("="*60)

spark.sql(f"""
    INSERT INTO {TABLE} VALUES
        (7, 'US', 'laptop', 1299.00),
        (8, 'APAC', 'tablet', 399.00)
""")

spark.sql(f"SELECT * FROM {TABLE} ORDER BY order_id").show()
print(f"Row count after insert: {spark.sql(f'SELECT COUNT(*) FROM {TABLE}').collect()[0][0]}")


# ════════════════════════════════════════════════════════════
# PART 3 — UPDATE
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 3: UPDATE — increase EU prices by 10%")
print("="*60)

# Iceberg supports UPDATE directly in SQL
spark.sql(f"""
    UPDATE {TABLE}
    SET amount = amount * 1.1
    WHERE region = 'EU'
""")

print("After UPDATE (EU prices +10%):")
spark.sql(f"SELECT * FROM {TABLE} ORDER BY order_id").show()


# ════════════════════════════════════════════════════════════
# PART 4 — DELETE
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 4: DELETE — remove orders under $500")
print("="*60)

spark.sql(f"""
    DELETE FROM {TABLE}
    WHERE amount < 500
""")

print("After DELETE (amount < 500 removed):")
spark.sql(f"SELECT * FROM {TABLE} ORDER BY order_id").show()
print(f"Row count after delete: {spark.sql(f'SELECT COUNT(*) FROM {TABLE}').collect()[0][0]}")


# ════════════════════════════════════════════════════════════
# PART 5 — MERGE (upsert)
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 5: MERGE — upsert new and updated orders")
print("="*60)

# Create a temporary view for the updates
schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("region", StringType(), False),
    StructField("product", StringType(), False),
    StructField("amount", DoubleType(), False),
])
updates = spark.createDataFrame([
    (1, "EU", "laptop", 1200.00),   # exists → UPDATE
    (9, "APAC", "phone", 549.00),   # new → INSERT
], schema)

updates.createOrReplaceTempView("updates")
print("Incoming updates:")
updates.show()

spark.sql(f"""
    MERGE INTO {TABLE} AS sales
    USING updates
    ON sales.order_id = updates.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("After MERGE:")
spark.sql(f"SELECT * FROM {TABLE} ORDER BY order_id").show()


# ════════════════════════════════════════════════════════════
# PART 6 — TIME TRAVEL (Iceberg-style)
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 6: TIME TRAVEL — Iceberg snapshots")
print("="*60)

# Iceberg has built-in metadata tables you can query
print("Snapshot history:")
spark.sql(f"SELECT snapshot_id, committed_at, operation, summary FROM {TABLE}.snapshots").show(truncate=False)

print("Full history (detailed):")
spark.sql(f"SELECT * FROM {TABLE}.history").show(truncate=False)

# Get the first snapshot ID for time travel
snapshots = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at").collect()
first_snapshot = snapshots[0][0]
second_snapshot = snapshots[1][0]

print(f"\nVersion at snapshot {first_snapshot} (original 6 rows):")
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {first_snapshot} ORDER BY order_id").show()

print(f"Version at snapshot {second_snapshot} (after INSERT):")
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {second_snapshot} ORDER BY order_id").show()

print("Current version (latest):")
spark.sql(f"SELECT * FROM {TABLE} ORDER BY order_id").show()


# ════════════════════════════════════════════════════════════
# PART 7 — SCHEMA EVOLUTION (Iceberg is more flexible)
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 7: SCHEMA EVOLUTION — adding a column")
print("="*60)

# Iceberg supports schema evolution natively via ALTER TABLE
spark.sql(f"ALTER TABLE {TABLE} ADD COLUMNS (discount DOUBLE)")

print("After adding 'discount' column:")
spark.sql(f"SELECT * FROM {TABLE} ORDER BY order_id").show()
print("(Existing rows have NULL for the new column — no rewrite needed)")

# Update the new column for some rows
spark.sql(f"UPDATE {TABLE} SET discount = 0.10 WHERE region = 'EU'")
print("After setting discount for EU orders:")
spark.sql(f"SELECT * FROM {TABLE} ORDER BY order_id").show()


# ════════════════════════════════════════════════════════════
# PART 8 — LOOK AT THE FILES ON DISK
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 8: What's actually on disk?")
print("="*60)

# Iceberg's metadata tables show you the files
print("Data files currently in the table:")
spark.sql(f"SELECT file_path, file_format, record_count, file_size_in_bytes FROM {TABLE}.files").show(truncate=False)

print("All manifests:")
spark.sql(f"SELECT path, length, partition_spec_id, added_snapshot_id FROM {TABLE}.manifests").show(truncate=False)

# Also look at the physical filesystem
import os

warehouse = "/delta-data/iceberg-warehouse/db/sales"
if os.path.exists(warehouse):
    print(f"\nPhysical directory structure:")
    for root, dirs, files in os.walk(warehouse):
        level = root.replace(warehouse, "").count(os.sep)
        indent = "  " * level
        dirname = os.path.basename(root)
        print(f"{indent}{dirname}/")
        if level < 3:  # don't go too deep
            sub_indent = "  " * (level + 1)
            for f in sorted(files)[:5]:
                size = os.path.getsize(os.path.join(root, f))
                print(f"{sub_indent}{f}  ({size} bytes)")
            if len(files) > 5:
                print(f"{sub_indent}... and {len(files) - 5} more files")


# ════════════════════════════════════════════════════════════
# PART 9 — Delta vs Iceberg comparison
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 9: Delta vs Iceberg — what you just experienced")
print("="*60)

print("""
┌─────────────────────┬──────────────────────────────┬──────────────────────────────┐
│                     │ Delta Lake (Lab 2)           │ Iceberg (Lab 4)              │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ Write API           │ df.write.format("delta")     │ spark.sql("INSERT INTO")     │
│                     │ .save("/path")               │ or df.writeTo("catalog.t")   │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ UPDATE/DELETE       │ DeltaTable Python API        │ Standard SQL (native)        │
│                     │ delta_table.update(...)       │ UPDATE t SET ... WHERE ...   │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ MERGE               │ delta_table.alias().merge()   │ Standard SQL MERGE INTO      │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ Time travel         │ option("versionAsOf", N)     │ VERSION AS OF <snapshot_id>  │
│                     │ versions: 0, 1, 2, ...       │ snapshots: long IDs          │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ History             │ delta_table.history()         │ SELECT * FROM t.snapshots    │
│                     │                              │ SELECT * FROM t.history      │
│                     │                              │ SELECT * FROM t.files        │
│                     │                              │ (built-in metadata tables!)  │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ Schema evolution    │ mergeSchema option            │ ALTER TABLE ADD COLUMNS      │
│                     │ (append-only by default)     │ (native, no rewrite)         │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ Table naming        │ File path based              │ Catalog.database.table       │
│                     │ "/path/to/table"             │ "local.db.sales"             │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ Metadata format     │ JSON files in _delta_log/    │ Avro manifests + JSON        │
│                     │ (flat, one file per version) │ metadata (two-level tree)    │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ Cleanup             │ VACUUM                       │ expire_snapshots +           │
│                     │                              │ remove_orphan_files          │
├─────────────────────┼──────────────────────────────┼──────────────────────────────┤
│ Ecosystem           │ Databricks-centric           │ Vendor-neutral               │
│                     │ (but open source)            │ (Netflix, Apple, AWS backing)│
└─────────────────────┴──────────────────────────────┴──────────────────────────────┘

Key insight: both do the same thing (ACID on Parquet in S3).
The developer experience is very similar. The metadata internals differ.
Pick based on your ecosystem, not on features — they've converged.
""")


# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("LAB 4 COMPLETE")
print("="*60)

input("Press Enter to stop Spark and exit...")
spark.stop()
