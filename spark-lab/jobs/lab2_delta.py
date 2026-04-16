"""
Lab 2 — Delta Lake Basics
=========================
Run with:
  docker run --rm -it \
    --network spark-lab_spark-net \
    -v /Users/pramoth/work/tmp/spark-book/spark-lab/jobs:/jobs \
    -v /Users/pramoth/work/tmp/spark-book/spark-lab/delta-data:/delta-data \
    apache/spark:3.5.1 \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --packages io.delta:delta-spark_2.12:3.2.1 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      /jobs/lab2_delta.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = (SparkSession.builder
         .appName("lab2-delta-lake")
         .getOrCreate())

DELTA_PATH = "/delta-data/sales"


# ════════════════════════════════════════════════════════════
# PART 1 — CREATE A DELTA TABLE
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 1: Creating a Delta table")
print("="*60)

# Define schema explicitly (best practice — no schema inference jobs)
schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("region", StringType(), False),
    StructField("product", StringType(), False),
    StructField("amount", DoubleType(), False),
])

data = [
    (1, "EU", "laptop", 999.99),
    (2, "US", "phone", 699.50),
    (3, "EU", "tablet", 449.00),
    (4, "APAC", "laptop", 1099.00),
    (5, "US", "phone", 599.00),
    (6, "EU", "phone", 749.99),
]

df = spark.createDataFrame(data, schema)

# Write as Delta (this creates the _delta_log/ folder)
df.write.format("delta").mode("overwrite").save(DELTA_PATH)

print("Wrote 6 rows as Delta table.")
print(f"Location: {DELTA_PATH}")

# Read it back
sales = spark.read.format("delta").load(DELTA_PATH)
sales.show()
print(f"Row count: {sales.count()}")


# ════════════════════════════════════════════════════════════
# PART 2 — INSERT MORE ROWS
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 2: INSERT — appending new rows")
print("="*60)

new_data = [
    (7, "US", "laptop", 1299.00),
    (8, "APAC", "tablet", 399.00),
]
new_df = spark.createDataFrame(new_data, schema)

# Append mode = INSERT
new_df.write.format("delta").mode("append").save(DELTA_PATH)

sales = spark.read.format("delta").load(DELTA_PATH)
sales.orderBy("order_id").show()
print(f"Row count after insert: {sales.count()}")


# ════════════════════════════════════════════════════════════
# PART 3 — UPDATE
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 3: UPDATE — increase EU prices by 10%")
print("="*60)

# To use UPDATE/DELETE/MERGE we need the DeltaTable API
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, DELTA_PATH)

# UPDATE sales SET amount = amount * 1.1 WHERE region = 'EU'
delta_table.update(
    condition="region = 'EU'",
    set={"amount": col("amount") * 1.1}
)

print("After UPDATE (EU prices +10%):")
spark.read.format("delta").load(DELTA_PATH).orderBy("order_id").show()


# ════════════════════════════════════════════════════════════
# PART 4 — DELETE
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 4: DELETE — remove orders under $500")
print("="*60)

# DELETE FROM sales WHERE amount < 500
delta_table.delete("amount < 500")

print("After DELETE (amount < 500 removed):")
remaining = spark.read.format("delta").load(DELTA_PATH)
remaining.orderBy("order_id").show()
print(f"Row count after delete: {remaining.count()}")


# ════════════════════════════════════════════════════════════
# PART 5 — MERGE (upsert)
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 5: MERGE — upsert new and updated orders")
print("="*60)

# Incoming data: order 1 has a new price, order 9 is brand new
updates_data = [
    (1, "EU", "laptop", 1200.00),    # exists → UPDATE
    (9, "APAC", "phone", 549.00),    # new → INSERT
]
updates = spark.createDataFrame(updates_data, schema)

print("Incoming updates:")
updates.show()

# MERGE INTO sales USING updates ON sales.order_id = updates.order_id
# WHEN MATCHED THEN UPDATE SET *
# WHEN NOT MATCHED THEN INSERT *
delta_table.alias("sales").merge(
    updates.alias("updates"),
    "sales.order_id = updates.order_id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

print("After MERGE:")
spark.read.format("delta").load(DELTA_PATH).orderBy("order_id").show()


# ════════════════════════════════════════════════════════════
# PART 6 — TIME TRAVEL
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 6: TIME TRAVEL — reading old versions")
print("="*60)

# Show the history
history = delta_table.history()
print("Delta table history:")
history.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

# Read version 0 (the original 6 rows before any changes)
print("Version 0 (original data):")
v0 = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_PATH)
v0.orderBy("order_id").show()
print(f"Version 0 row count: {v0.count()}")

# Read version 1 (after INSERT)
print("Version 1 (after INSERT):")
v1 = spark.read.format("delta").option("versionAsOf", 1).load(DELTA_PATH)
v1.orderBy("order_id").show()
print(f"Version 1 row count: {v1.count()}")

# Current version
print("Current version (latest):")
current = spark.read.format("delta").load(DELTA_PATH)
current.orderBy("order_id").show()
print(f"Current row count: {current.count()}")


# ════════════════════════════════════════════════════════════
# PART 7 — SCHEMA ENFORCEMENT
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 7: SCHEMA ENFORCEMENT — Delta rejects bad writes")
print("="*60)

bad_schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("region", StringType(), False),
    StructField("product", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("discount", DoubleType(), False),  # extra column!
])

bad_data = [(10, "US", "laptop", 999.00, 0.15)]
bad_df = spark.createDataFrame(bad_data, bad_schema)

print("Trying to append data with an extra 'discount' column...")
try:
    bad_df.write.format("delta").mode("append").save(DELTA_PATH)
    print("ERROR: Write succeeded — this should not happen!")
except Exception as e:
    error_msg = str(e)
    # Print just the relevant part
    if "schema" in error_msg.lower() or "mismatch" in error_msg.lower():
        print(f"BLOCKED by Delta! Schema mismatch detected.")
    else:
        print(f"BLOCKED by Delta! Error: {error_msg[:200]}")
    print("\nThis is schema enforcement in action:")
    print("  - The table has 4 columns: order_id, region, product, amount")
    print("  - The write tried to add a 5th column: discount")
    print("  - Delta rejected it to protect data integrity")


# ════════════════════════════════════════════════════════════
# PART 8 — LOOK AT THE _delta_log/ ON DISK
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 8: What's actually on disk?")
print("="*60)

import os, json

delta_log_path = DELTA_PATH + "/_delta_log"
if os.path.exists(delta_log_path):
    files = sorted(os.listdir(delta_log_path))
    print(f"\nFiles in _delta_log/ ({len(files)} files):")
    for f in files:
        size = os.path.getsize(os.path.join(delta_log_path, f))
        print(f"  {f}  ({size} bytes)")

    # Read version 0's log to show what's inside
    v0_log = os.path.join(delta_log_path, "00000000000000000000.json")
    if os.path.exists(v0_log):
        print(f"\nContents of version 0 log (first 3 entries):")
        with open(v0_log) as f:
            for i, line in enumerate(f):
                if i >= 3:
                    print("  ...")
                    break
                entry = json.loads(line)
                # Pretty print with indentation
                print(f"  {json.dumps(entry, indent=4)[:300]}")

# Count parquet files
parquet_files = [f for f in os.listdir(DELTA_PATH) if f.endswith(".parquet")]
print(f"\nParquet data files: {len(parquet_files)}")
print("(Some are 'dead' — removed by DELETE/UPDATE but still on disk for time travel)")

total_size = sum(os.path.getsize(os.path.join(DELTA_PATH, f)) for f in parquet_files)
print(f"Total Parquet size: {total_size:,} bytes")


# ════════════════════════════════════════════════════════════
# PART 9 — VACUUM (cleanup old files)
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("PART 9: VACUUM — cleaning up old Parquet files")
print("="*60)

print(f"Parquet files BEFORE vacuum: {len(parquet_files)}")

# By default VACUUM refuses to delete files < 7 days old (safety).
# For this lab, we override that check.
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# VACUUM removes files no longer referenced by any version
# retentionHours=0 means "delete everything not in the current version"
# WARNING: after vacuum, time travel to old versions will FAIL
delta_table.vacuum(retentionHours=0)

parquet_after = [f for f in os.listdir(DELTA_PATH) if f.endswith(".parquet")]
print(f"Parquet files AFTER vacuum: {len(parquet_after)}")
print("\nOld files removed! Time travel to pre-vacuum versions is now broken.")
print("(In production, keep retentionHours=168 (7 days) for safety.)")


# ════════════════════════════════════════════════════════════
# DONE
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("LAB 2 COMPLETE")
print("="*60)
print("""
What you just did:
  1. Created a Delta table (Parquet + _delta_log/)
  2. INSERT — appended new rows
  3. UPDATE — modified existing rows (copy-on-write)
  4. DELETE — removed rows (copy-on-write)
  5. MERGE — upsert (update existing + insert new in one atomic op)
  6. TIME TRAVEL — read any past version by number
  7. SCHEMA ENFORCEMENT — Delta rejected a write with wrong columns
  8. Inspected _delta_log/ on disk — saw the actual JSON log entries
  9. VACUUM — cleaned up dead Parquet files

Key takeaways:
  - Parquet files are IMMUTABLE. Every UPDATE/DELETE writes NEW files.
  - The _delta_log/ is the source of truth for "what's current".
  - Time travel works by reading older log versions.
  - VACUUM deletes unreferenced files — breaks time travel for old versions.
  - Schema enforcement prevents bad writes at the door.
""")

input("Press Enter to stop Spark and exit...")
spark.stop()
