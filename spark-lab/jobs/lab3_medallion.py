"""
Lab 3 — Medallion Architecture (Bronze → Silver → Gold)
=======================================================
The most common real-world lakehouse pattern.

Run with:
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
      /jobs/lab3_medallion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, trim, lower,
    upper, when, sum as spark_sum, count, avg, round as spark_round,
    year, month, dayofmonth, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
)
from delta.tables import DeltaTable

spark = (SparkSession.builder
         .appName("lab3-medallion")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

BRONZE_PATH = "/delta-data/bronze/orders"
SILVER_PATH = "/delta-data/silver/orders"
GOLD_DAILY_PATH = "/delta-data/gold/daily_revenue"
GOLD_PRODUCT_PATH = "/delta-data/gold/product_summary"
RAW_PATH = "/delta-data/raw"


# ════════════════════════════════════════════════════════════
# STEP 0 — Generate realistic messy raw data
# ════════════════════════════════════════════════════════════
# In production this would be CSV/JSON files landing in S3
# from an external system. We simulate the mess.

print("\n" + "="*60)
print("STEP 0: Generating messy raw data")
print("="*60)

raw_data = [
    # Normal orders
    (1001, "2026-04-01 10:15:00", "laptop",    "EU",   999.99, 1),
    (1002, "2026-04-01 11:30:00", "phone",     "US",   699.50, 2),
    (1003, "2026-04-01 14:45:00", "tablet",    "APAC", 449.00, 1),
    (1004, "2026-04-02 09:00:00", "laptop",    "US",   1099.00, 1),
    (1005, "2026-04-02 13:20:00", "headphones","EU",   199.99, 3),
    (1006, "2026-04-02 16:00:00", "phone",     "EU",   749.99, 1),
    (1007, "2026-04-03 08:30:00", "tablet",    "US",   529.00, 2),
    (1008, "2026-04-03 12:00:00", "laptop",    "APAC", 1299.00, 1),
    (1009, "2026-04-03 15:45:00", "phone",     "US",   649.00, 1),
    (1010, "2026-04-03 17:30:00", "headphones","APAC", 249.99, 2),

    # ---- MESSY DATA (real-world problems) ----

    # DUPLICATE: order 1001 appears again (upstream sent it twice)
    (1001, "2026-04-01 10:15:00", "laptop",    "EU",   999.99, 1),

    # DUPLICATE: order 1005 with slightly different timestamp
    (1005, "2026-04-02 13:20:01", "headphones","EU",   199.99, 3),

    # BAD REGION: lowercase, needs normalization
    (1011, "2026-04-04 09:00:00", "phone",     "eu",   599.00, 1),
    (1012, "2026-04-04 10:30:00", "tablet",    "us",   479.00, 1),

    # BAD REGION: extra whitespace
    (1013, "2026-04-04 11:00:00", "laptop",    " APAC ", 1199.00, 1),

    # NULL PRODUCT: missing product name
    (1014, "2026-04-04 12:00:00", None,        "EU",   399.00, 1),

    # NEGATIVE AMOUNT: refund or data error?
    (1015, "2026-04-04 13:00:00", "phone",     "US",   -50.00, 1),

    # ZERO QUANTITY: shouldn't exist
    (1016, "2026-04-04 14:00:00", "laptop",    "EU",   999.00, 0),
]

# Use StringType for all columns initially (simulating raw CSV/JSON)
raw_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_ts", StringType(), True),   # string, not timestamp!
    StructField("product", StringType(), True),
    StructField("region", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
])

raw_df = spark.createDataFrame(raw_data, raw_schema)

# Write as CSV (simulating a raw landing zone)
raw_df.write.mode("overwrite").option("header", "true").csv(RAW_PATH)

print(f"Generated {raw_df.count()} raw records (with duplicates + bad data)")
print("Messy data includes:")
print("  - 2 duplicate orders (1001 and 1005)")
print("  - 2 lowercase regions ('eu', 'us')")
print("  - 1 region with whitespace (' APAC ')")
print("  - 1 null product")
print("  - 1 negative amount")
print("  - 1 zero quantity")


# ════════════════════════════════════════════════════════════
# STEP 1 — BRONZE: Ingest raw data as-is
# ════════════════════════════════════════════════════════════
# Bronze = raw data + ingestion metadata. No cleaning.
# "Store what you received, exactly as you received it."

print("\n" + "="*60)
print("STEP 1: BRONZE — Raw ingestion (no cleaning)")
print("="*60)

bronze = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(RAW_PATH))

# Add ingestion metadata (when did we ingest this?)
bronze = (bronze
          .withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", lit("raw/orders_batch_1.csv")))

# Write to Bronze Delta table
bronze.write.format("delta").mode("overwrite").save(BRONZE_PATH)

bronze_table = spark.read.format("delta").load(BRONZE_PATH)
print(f"Bronze table: {bronze_table.count()} rows (ALL raw records, including bad ones)")
bronze_table.show(truncate=False)


# ════════════════════════════════════════════════════════════
# STEP 2 — SILVER: Clean, deduplicate, validate
# ════════════════════════════════════════════════════════════
# Silver = trusted, clean, queryable data.
# This is where all the data quality work happens.

print("\n" + "="*60)
print("STEP 2: SILVER — Clean, deduplicate, validate")
print("="*60)

silver = spark.read.format("delta").load(BRONZE_PATH)

# --- Cleaning step 1: Normalize region ---
# Trim whitespace, uppercase
silver = silver.withColumn("region", upper(trim(col("region"))))
print("After region normalization:")
silver.select("order_id", "region").filter("order_id IN (1011, 1012, 1013)").show()

# --- Cleaning step 2: Parse timestamp ---
# Convert string → proper timestamp type
silver = silver.withColumn("order_ts", to_timestamp(col("order_ts")))

# --- Cleaning step 3: Remove nulls and invalid data ---
before_count = silver.count()

# Drop rows with null product
silver = silver.filter(col("product").isNotNull())

# Drop rows with non-positive amount
silver = silver.filter(col("amount") > 0)

# Drop rows with zero or null quantity
silver = silver.filter((col("quantity").isNotNull()) & (col("quantity") > 0))

after_null_filter = silver.count()
print(f"Removed {before_count - after_null_filter} invalid rows (null product, negative amount, zero qty)")

# --- Cleaning step 4: Deduplicate ---
# Use row_number() to keep only the first occurrence of each order_id
window = Window.partitionBy("order_id").orderBy("order_ts")
silver = (silver
          .withColumn("_row_num", row_number().over(window))
          .filter(col("_row_num") == 1)
          .drop("_row_num"))

after_dedup = silver.count()
print(f"Removed {after_null_filter - after_dedup} duplicate rows")
print(f"Silver table: {after_dedup} clean rows")

# --- Add computed columns ---
silver = (silver
          .withColumn("total_amount", spark_round(col("amount") * col("quantity"), 2))
          .withColumn("order_date", col("order_ts").cast("date"))
          .withColumn("order_year", year(col("order_ts")))
          .withColumn("order_month", month(col("order_ts"))))

# Drop ingestion metadata columns (Bronze-only)
silver = silver.drop("_ingested_at", "_source_file")

# Write to Silver Delta table
silver.write.format("delta").mode("overwrite").save(SILVER_PATH)

print("\nSilver table (clean, deduplicated, enriched):")
silver_table = spark.read.format("delta").load(SILVER_PATH)
silver_table.orderBy("order_id").show(truncate=False)


# ════════════════════════════════════════════════════════════
# STEP 3 — GOLD: Business-ready aggregates
# ════════════════════════════════════════════════════════════
# Gold = pre-computed tables ready for BI dashboards and analysts.
# Each Gold table answers a specific business question.

print("\n" + "="*60)
print("STEP 3: GOLD — Business aggregates")
print("="*60)

silver_df = spark.read.format("delta").load(SILVER_PATH)

# --- Gold table 1: Daily revenue by region ---
print("\nGold: Daily Revenue by Region")
daily_revenue = (silver_df
    .groupBy("order_date", "region")
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        count("order_id").alias("order_count"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value"),
    )
    .orderBy("order_date", "region"))

daily_revenue.write.format("delta").mode("overwrite").save(GOLD_DAILY_PATH)
daily_revenue.show(truncate=False)

# --- Gold table 2: Product summary ---
print("Gold: Product Summary")
product_summary = (silver_df
    .groupBy("product")
    .agg(
        spark_sum("quantity").alias("total_units_sold"),
        spark_sum("total_amount").alias("total_revenue"),
        spark_round(avg("amount"), 2).alias("avg_unit_price"),
        count("order_id").alias("order_count"),
    )
    .orderBy(col("total_revenue").desc()))

product_summary.write.format("delta").mode("overwrite").save(GOLD_PRODUCT_PATH)
product_summary.show(truncate=False)


# ════════════════════════════════════════════════════════════
# STEP 4 — Verify the full pipeline
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("STEP 4: Pipeline summary")
print("="*60)

bronze_count = spark.read.format("delta").load(BRONZE_PATH).count()
silver_count = spark.read.format("delta").load(SILVER_PATH).count()
gold_daily_count = spark.read.format("delta").load(GOLD_DAILY_PATH).count()
gold_product_count = spark.read.format("delta").load(GOLD_PRODUCT_PATH).count()

print(f"""
Pipeline results:
  Raw CSV records:     {raw_df.count()}  (messy, with all problems)
  Bronze (raw Delta):  {bronze_count}  (same count — Bronze preserves everything)
  Silver (cleaned):    {silver_count}  (duplicates removed, nulls dropped, types fixed)
  Gold daily_revenue:  {gold_daily_count} rows  (aggregated by date + region)
  Gold product_summary:{gold_product_count} rows  (aggregated by product)

Data quality transformations applied in Silver:
  - Region normalized (trimmed, uppercased)
  - Timestamps parsed from string to proper type
  - Null products removed
  - Negative amounts removed
  - Zero quantities removed
  - Duplicates removed (kept earliest by order_id)
  - Computed columns added (total_amount, order_date, year, month)
""")


# ════════════════════════════════════════════════════════════
# STEP 5 — Time travel across layers
# ════════════════════════════════════════════════════════════
print("="*60)
print("STEP 5: Delta history across all layers")
print("="*60)

for name, path in [("Bronze", BRONZE_PATH), ("Silver", SILVER_PATH),
                    ("Gold daily", GOLD_DAILY_PATH), ("Gold product", GOLD_PRODUCT_PATH)]:
    dt = DeltaTable.forPath(spark, path)
    hist = dt.history().select("version", "timestamp", "operation")
    print(f"\n{name} history:")
    hist.show(truncate=False)


# ════════════════════════════════════════════════════════════
# STEP 6 — What's on disk?
# ════════════════════════════════════════════════════════════
print("="*60)
print("STEP 6: Lakehouse structure on disk")
print("="*60)

import os

def count_files(path, ext):
    if not os.path.exists(path):
        return 0
    return len([f for f in os.listdir(path) if f.endswith(ext)])

print(f"""
/delta-data/
├── raw/                          (CSV landing zone)
├── bronze/orders/                ({count_files(BRONZE_PATH, '.parquet')} parquet + _delta_log/)
├── silver/orders/                ({count_files(SILVER_PATH, '.parquet')} parquet + _delta_log/)
├── gold/
│   ├── daily_revenue/            ({count_files(GOLD_DAILY_PATH, '.parquet')} parquet + _delta_log/)
│   └── product_summary/          ({count_files(GOLD_PRODUCT_PATH, '.parquet')} parquet + _delta_log/)
""")

print("""
This is a complete lakehouse on your laptop:
  - Raw CSV lands in a landing zone
  - Bronze preserves it as-is in Delta (auditable, replayable)
  - Silver cleans, deduplicates, and enriches
  - Gold serves pre-computed answers to business questions

In production:
  - Raw = S3 bucket where upstream systems drop files
  - Bronze/Silver/Gold = Delta or Iceberg tables in the lakehouse
  - Airflow/Dagster triggers each step on a schedule
  - Trino or Spark SQL serves analyst queries on Gold tables
  - Each layer has its own Delta history for auditing and rollback
""")


# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("LAB 3 COMPLETE")
print("="*60)

input("Press Enter to stop Spark and exit...")
spark.stop()
