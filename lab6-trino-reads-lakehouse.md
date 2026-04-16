# Lab 6 — Trino Reads the Lakehouse

The classic "Spark writes, Trino reads" pattern. Spark handles heavy ETL; Trino serves fast interactive SQL for analysts and dashboards. Both read the same Delta tables on disk.

## Architecture

```
  ┌─────────┐                        ┌─────────┐
  │  Spark  │── writes Delta ──────→│  Delta  │←── reads Delta ──│  Trino  │
  │  (ETL)  │     tables            │  files  │     tables       │  (SQL)  │
  └─────────┘                        │ on disk │                  └─────────┘
                                     └─────────┘
                                    (same Parquet +
                                     _delta_log/)
```

Two engines, one data layer. No data copying. No ETL from Spark to Trino. They share the filesystem.

## Setup

### Add Trino to docker-compose.yml

```yaml
trino:
  image: trinodb/trino:439
  container_name: trino
  hostname: trino
  ports:
    - "18081:8080"
  volumes:
    - ./delta-data:/delta-data
    - ./trino-config/catalog:/etc/trino/catalog
  networks:
    - spark-net
```

### Create the Delta catalog config

```
trino-config/catalog/delta.properties
```

```properties
connector.name=delta_lake
hive.metastore=file
hive.metastore.catalog.dir=file:///delta-data
delta.register-table-procedure.enabled=true
```

This tells Trino: "there's a Delta Lake catalog, the files are under `/delta-data`, and you can register tables manually."

### Start Trino

```bash
cd /Users/pramoth/work/tmp/spark-book/spark-lab
docker compose up -d trino
```

### Register Spark's Delta tables in Trino

Trino's file-based metastore doesn't auto-discover Delta tables. You register them once:

```bash
docker exec trino trino --execute "
CREATE SCHEMA IF NOT EXISTS delta.lakehouse;

CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'bronze_orders', table_location => 'file:///delta-data/bronze/orders');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'silver_orders', table_location => 'file:///delta-data/silver/orders');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_daily_revenue', table_location => 'file:///delta-data/gold/daily_revenue');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'gold_product_summary', table_location => 'file:///delta-data/gold/product_summary');
CALL delta.system.register_table(schema_name => 'lakehouse', table_name => 'sales', table_location => 'file:///delta-data/sales');
"
```

In production, you'd use a **Hive Metastore** or **AWS Glue Catalog** instead — tables are auto-discovered when Spark writes them. The manual registration is a lab simplification.

## Querying from Trino

### List tables

```bash
docker exec trino trino --execute "SHOW TABLES IN delta.lakehouse"
```

### Query Silver orders

```bash
docker exec trino trino --execute "
SELECT * FROM delta.lakehouse.silver_orders ORDER BY order_id
"
```

Returns the exact same 13 rows Spark wrote in Lab 3 — because Trino reads the same Parquet files + `_delta_log/`.

### Analyst queries

```sql
-- Revenue by region
SELECT region, COUNT(*) AS orders, ROUND(SUM(total_amount), 2) AS revenue
FROM delta.lakehouse.silver_orders
GROUP BY region
ORDER BY revenue DESC;

-- Read Gold tables directly
SELECT * FROM delta.lakehouse.gold_product_summary ORDER BY total_revenue DESC;
```

### Cross-table join (Bronze vs Silver — see what got filtered)

```sql
SELECT b.order_id, b.product, b.region, b.amount, b.quantity,
       CASE WHEN s.order_id IS NULL THEN 'FILTERED' ELSE 'KEPT' END AS status
FROM delta.lakehouse.bronze_orders b
LEFT JOIN delta.lakehouse.silver_orders s ON b.order_id = s.order_id
ORDER BY b.order_id;
```

Shows which rows survived the Silver cleaning and which were dropped — all from a fast Trino query.

## Trino web UI

Open **http://localhost:18081** in your browser. Shows:
- Running and completed queries
- Query performance (rows read, time, stages)
- Worker status

## Spark vs Trino — when to use which

| Use case | Use Spark | Use Trino |
|---|---|---|
| **ETL (read → transform → write)** | ✅ | ❌ (Trino is read-mostly) |
| **Interactive analyst SQL** | Slow (30s startup) | ✅ (sub-second) |
| **BI dashboard queries** | ❌ (not designed for concurrency) | ✅ (many concurrent queries) |
| **Ad-hoc exploration** | OK (Jupyter) | ✅ (faster for SQL) |
| **Streaming ingestion** | ✅ | ❌ |
| **ML / data science** | ✅ | ❌ |

**The pattern:** Spark writes the lakehouse (Bronze → Silver → Gold). Trino reads it for analyst queries. Both use Delta as the shared format.

## Limitations in this lab

- **Time travel not supported** in Trino's Delta connector (Spark can do it, Trino can't for Delta).
- **File-based metastore** requires manual table registration. In production, a Hive Metastore or Glue Catalog auto-discovers tables.
- **Single-node Trino** — real deployments have coordinator + multiple worker nodes for parallel query processing.

## Trino CLI tips

```bash
# Interactive SQL prompt
docker exec -it trino trino

# Run a single query
docker exec trino trino --execute "SELECT COUNT(*) FROM delta.lakehouse.silver_orders"

# Output as CSV
docker exec trino trino --output-format CSV --execute "SELECT * FROM delta.lakehouse.silver_orders"
```

## What you now have — the complete mini data platform

```
Your laptop (docker-compose)
├── Kafka        (event streaming)     — Lab 5
├── Spark        (ETL + streaming)     — Labs 1-5
├── Delta Lake   (table format)        — Labs 2-5
├── Iceberg      (table format)        — Lab 4
├── Trino        (interactive SQL)     — Lab 6
├── Jupyter      (notebook playground) — Playground
└── All sharing the same /delta-data/ filesystem
```

This is **Chapter 8, Pattern 2 + Pattern 3 combined** — running on your laptop. In production, replace the local filesystem with S3 and scale each component independently.

## Key takeaway

> Trino and Spark are not competitors — they're complements. Spark is the **write engine** (ETL, streaming, ML). Trino is the **read engine** (fast SQL, dashboards, ad-hoc). Delta Lake (or Iceberg) is the **shared format** that lets both engines see the same data without any copying or sync.
