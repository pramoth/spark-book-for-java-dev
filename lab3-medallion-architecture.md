# Lab 3 — Medallion Architecture (Bronze → Silver → Gold)

The most common real-world lakehouse pattern. Build a complete ETL pipeline that ingests messy data, cleans it, and produces business-ready aggregates — all stored as Delta tables.

## The pattern

```
Raw data (CSV/JSON)  →  Bronze (raw, as-is)  →  Silver (cleaned)  →  Gold (business-ready)
     ↓                      ↓                       ↓                     ↓
  "landing zone"      "append-only log"       "deduplicated,       "aggregated tables
                                                typed, validated"     analysts query"
```

## Run command

```bash
cd /Users/pramoth/work/tmp/spark-book/spark-lab

# Clean previous data
rm -rf delta-data/bronze delta-data/silver delta-data/gold delta-data/raw

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
```

## The messy input data (18 records)

The lab generates realistic e-commerce orders with real-world data quality problems:

| Problem | Example | Count |
|---|---|---|
| Duplicate orders | order_id 1001 appears twice | 2 |
| Lowercase regions | `"eu"`, `"us"` instead of `"EU"`, `"US"` | 2 |
| Whitespace in region | `" APAC "` | 1 |
| Null product | product = NULL | 1 |
| Negative amount | amount = -50.00 | 1 |
| Zero quantity | quantity = 0 | 1 |

Plus 10 clean records. Total: 18 rows.

## Step-by-step walkthrough

### Step 0 — Generate raw data

Creates a CSV file in `/delta-data/raw/` simulating files landing from an external system.

### Step 1 — Bronze: Ingest raw data as-is

```python
bronze = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)
bronze = (bronze
          .withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_file", lit("raw/orders_batch_1.csv")))
bronze.write.format("delta").mode("overwrite").save(BRONZE_PATH)
```

**Bronze rule: never clean, never drop, never transform.** Store exactly what you received, plus metadata about when/where you got it.

- **Input:** 18 CSV records (including all problems)
- **Output:** 18 Delta rows (same count — Bronze preserves everything)
- **Added:** `_ingested_at` timestamp, `_source_file` provenance

### Step 2 — Silver: Clean, deduplicate, validate

This is where all data quality work happens:

1. **Normalize regions:** `trim()` + `upper()` → `"eu"` becomes `"EU"`, `" APAC "` becomes `"APAC"`
2. **Parse timestamps:** string → proper timestamp type
3. **Remove invalid rows:** null product, negative amount, zero quantity → 3 rows dropped
4. **Deduplicate:** `row_number()` over `PARTITION BY order_id ORDER BY order_ts`, keep first → 2 duplicates dropped
5. **Add computed columns:** `total_amount` (amount × quantity), `order_date`, `order_year`, `order_month`

- **Input:** 18 Bronze rows
- **Output:** 13 Silver rows (clean, trusted, enriched)
- **Dropped:** 5 rows (3 invalid + 2 duplicates)

### Step 3 — Gold: Business aggregates

Two Gold tables, each answering a specific business question:

**Gold 1: Daily Revenue by Region**
```
+----------+------+-------------+-----------+---------------+
|order_date|region|total_revenue|order_count|avg_order_value|
+----------+------+-------------+-----------+---------------+
|2026-04-01|APAC  |449.0        |1          |449.0          |
|2026-04-01|EU    |999.99       |1          |999.99         |
|2026-04-01|US    |1399.0       |1          |1399.0         |
|...       |...   |...          |...        |...            |
+----------+------+-------------+-----------+---------------+
```

**Gold 2: Product Summary**
```
+----------+----------------+-------------+--------------+-----------+
|product   |total_units_sold|total_revenue|avg_unit_price|order_count|
+----------+----------------+-------------+--------------+-----------+
|laptop    |4               |4596.99      |1149.25       |4          |
|phone     |5               |3396.99      |674.37        |4          |
|tablet    |4               |1986.0       |485.67        |3          |
|headphones|5               |1099.95      |224.99        |2          |
+----------+----------------+-------------+--------------+-----------+
```

## The data journey

```
18 raw records (messy)
    │
    ▼  BRONZE: store as-is + metadata
18 Bronze rows
    │
    ▼  SILVER: clean + deduplicate + validate
    │   - 3 removed: null product, negative amount, zero qty
    │   - 2 removed: duplicate order IDs
    │   - Regions normalized, timestamps parsed
    │   - Computed columns added
13 Silver rows (clean, trusted)
    │
    ▼  GOLD: aggregate for business
10 rows: daily revenue by region
 4 rows: product summary
```

## What's on disk after the lab

```
delta-data/
├── raw/                    CSV landing zone
├── bronze/orders/          18 rows, all mess preserved + metadata
│   └── _delta_log/
├── silver/orders/          13 rows, clean + enriched
│   └── _delta_log/
└── gold/
    ├── daily_revenue/      10 rows, by date + region
    │   └── _delta_log/
    └── product_summary/    4 rows, by product
        └── _delta_log/
```

Every layer is a Delta table with its own version history. Time travel works independently per layer.

## Why each layer exists

| Layer | Question it answers | Who uses it |
|---|---|---|
| **Bronze** | "What did we receive?" | Auditors, compliance, debugging |
| **Silver** | "What's the clean, trusted data?" | Data engineers, data scientists |
| **Gold** | "What's the revenue by region?" | Analysts, dashboards, BI tools |

If a Silver cleaning rule is wrong, fix the code and rerun from Bronze — no data loss, because Bronze keeps the raw truth.

## In production

| Lab version | Production version |
|---|---|
| Python list → CSV | External systems drop files in S3 |
| `spark-submit` by hand | Airflow/Dagster schedules each step |
| Local Docker volume | S3 + Iceberg/Delta |
| `show()` to terminal | Trino/Spark SQL serves analyst queries on Gold |
| One-shot overwrite | Incremental MERGE (only process new data) |

## Key takeaways

- **Bronze stores everything.** No cleaning, no dropping. Add ingestion metadata only.
- **Silver is the workhorse.** All data quality logic lives here: dedup, null handling, type casting, normalization, enrichment.
- **Gold is pre-computed.** Each Gold table answers one business question. Analysts never touch Silver directly.
- **Each layer is an independent Delta table** with its own history, schema, and retention.
- **The pipeline is idempotent.** Run it again → same result. This is critical for production reliability.
