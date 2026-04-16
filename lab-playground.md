# Lab Playground — Interactive PySpark REPL

Launch an interactive PySpark shell connected to your Docker cluster with Delta Lake loaded. All tables from Lab 2 and Lab 3 are accessible.

## Launch the playground

```bash
cd /Users/pramoth/work/tmp/spark-book/spark-lab

docker run --rm -it \
  --network spark-lab_spark-net \
  -v $(pwd)/delta-data:/delta-data \
  apache/spark:3.5.1 \
  /opt/spark/bin/pyspark \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.1.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.jars.ivy=/tmp/.ivy2
```

You get a `>>>` prompt. Type Python/SQL interactively. Results appear instantly.

## Available Delta tables

| Path | What's inside |
|---|---|
| `/delta-data/sales` | Lab 2 — small sales table (CRUD demo) |
| `/delta-data/bronze/orders` | Lab 3 — 18 raw rows (messy, unmodified) |
| `/delta-data/silver/orders` | Lab 3 — 13 clean rows (deduplicated, validated) |
| `/delta-data/gold/daily_revenue` | Lab 3 — revenue aggregated by date + region |
| `/delta-data/gold/product_summary` | Lab 3 — revenue aggregated by product |

## Things to try

### Read any table

```python
spark.read.format("delta").load("/delta-data/silver/orders").show()
spark.read.format("delta").load("/delta-data/gold/daily_revenue").show()
spark.read.format("delta").load("/delta-data/gold/product_summary").show()
```

### Register as SQL tables and query with SQL

```python
spark.read.format("delta").load("/delta-data/silver/orders").createOrReplaceTempView("orders")

spark.sql("SELECT * FROM orders WHERE region = 'EU'").show()

spark.sql("""
  SELECT region, SUM(total_amount) AS revenue
  FROM orders
  GROUP BY region
  ORDER BY revenue DESC
""").show()

spark.sql("""
  SELECT product, COUNT(*) AS cnt, SUM(total_amount) AS total
  FROM orders
  GROUP BY product
  ORDER BY total DESC
""").show()
```

### Time travel — read past versions

```python
# Bronze as it was at version 0
spark.read.format("delta").option("versionAsOf", 0).load("/delta-data/bronze/orders").show()

# Lab 2 sales table — original 6 rows before any changes
spark.read.format("delta").option("versionAsOf", 0).load("/delta-data/sales").show()
```

### View Delta history

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/delta-data/silver/orders")
dt.history().show(truncate=False)

dt2 = DeltaTable.forPath(spark, "/delta-data/sales")
dt2.history().select("version", "operation", "operationParameters").show(truncate=False)
```

### Try a live UPDATE

```python
from pyspark.sql.functions import lit

dt = DeltaTable.forPath(spark, "/delta-data/silver/orders")

# Rename EU to EUROPE
dt.update("region = 'EU'", {"region": lit("EUROPE")})
spark.read.format("delta").load("/delta-data/silver/orders").show()

# Check history — you'll see a new version
dt.history().select("version", "operation").show()
```

### Try a live DELETE

```python
dt = DeltaTable.forPath(spark, "/delta-data/silver/orders")
dt.delete("product = 'headphones'")
spark.read.format("delta").load("/delta-data/silver/orders").show()
```

### Inspect schema

```python
spark.read.format("delta").load("/delta-data/silver/orders").printSchema()
```

Output:
```
root
 |-- order_id: long
 |-- order_ts: timestamp
 |-- product: string
 |-- region: string
 |-- amount: double
 |-- quantity: integer
 |-- total_amount: double
 |-- order_date: date
 |-- order_year: integer
 |-- order_month: integer
```

### Write your own Gold table

```python
orders = spark.read.format("delta").load("/delta-data/silver/orders")

# Top customers by total spend (pretend order_id is customer_id)
my_gold = (orders
    .groupBy("region")
    .agg({"total_amount": "sum", "order_id": "count"})
    .withColumnRenamed("sum(total_amount)", "total_spend")
    .withColumnRenamed("count(order_id)", "order_count")
    .orderBy("total_spend", ascending=False))

my_gold.show()

# Save it as a new Gold table
my_gold.write.format("delta").mode("overwrite").save("/delta-data/gold/region_spend")
```

### Compare Bronze vs Silver (see what cleaning did)

```python
bronze = spark.read.format("delta").load("/delta-data/bronze/orders")
silver = spark.read.format("delta").load("/delta-data/silver/orders")

print(f"Bronze: {bronze.count()} rows")
print(f"Silver: {silver.count()} rows")
print(f"Dropped: {bronze.count() - silver.count()} rows")

# See the rows that exist in Bronze but not Silver (the ones we cleaned out)
bronze.createOrReplaceTempView("bronze")
silver.createOrReplaceTempView("silver")

spark.sql("""
  SELECT b.order_id, b.product, b.region, b.amount, b.quantity
  FROM bronze b
  LEFT ANTI JOIN silver s ON b.order_id = s.order_id
""").show()
```

### Exit

```python
exit()
```

## Tips

- **`show()`** prints the first 20 rows. Use `show(50)` for more, or `show(truncate=False)` to see full column values.
- **`printSchema()`** is the fastest way to see column names and types.
- **`count()`** triggers a full scan — use it to verify row counts.
- **`explain()`** shows the physical plan without executing — useful for understanding what Spark will do.
- **Tab completion** works in the PySpark REPL for column names and methods.
- **Ctrl+D** or `exit()` to quit.

## This is how Spark developers work day-to-day

The interactive REPL is the Spark equivalent of a SQL client connected to a database. Data engineers use it to:

- Explore new datasets before writing ETL code.
- Debug pipeline issues by reading intermediate tables.
- Test transformations interactively before putting them in a script.
- Ad-hoc analysis when a quick answer is needed.

In production, most teams use **Databricks notebooks** or **Jupyter + PySpark** instead of the raw REPL — same interactive feel, but with rich output (charts, formatted tables, markdown cells).
