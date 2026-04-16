"""
Lab 5 — Spark Structured Streaming: Kafka → Delta Lake
=======================================================
Reads events from Kafka in real-time, transforms them,
and writes to a Delta table. The table grows as events arrive.

Run with:
  docker run --rm -it \
    --network spark-lab_spark-net \
    -v $(pwd)/jobs:/jobs \
    -v $(pwd)/delta-data:/delta-data \
    apache/spark:3.5.1 \
    /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      /jobs/lab5_streaming.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    round as spark_round, window, sum as spark_sum, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
)

spark = (SparkSession.builder
         .appName("lab5-streaming")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

DELTA_PATH = "/delta-data/streaming/orders"
CHECKPOINT_PATH = "/delta-data/streaming/_checkpoints/orders"

# ════════════════════════════════════════════════════════════
# STEP 1 — Read from Kafka
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("STEP 1: Reading from Kafka topic 'orders'")
print("="*60)

# This is the Structured Streaming "readStream" API
# It looks almost identical to a batch read — that's the point!
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load())

print("Kafka stream schema (raw):")
raw_stream.printSchema()
# Kafka gives you: key (binary), value (binary), topic, partition, offset, timestamp
# We need to parse the value (JSON string) into structured columns.


# ════════════════════════════════════════════════════════════
# STEP 2 — Parse and transform
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("STEP 2: Parsing JSON and transforming")
print("="*60)

# Define the schema of our JSON events
event_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("order_ts", StringType(), True),
    StructField("product", StringType(), True),
    StructField("region", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
])

# Parse: binary value → string → JSON → structured columns
# This is the SAME DataFrame API as batch — filter, withColumn, etc.
parsed = (raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), event_schema).alias("data"))
    .select("data.*")
    .withColumn("order_ts", to_timestamp(col("order_ts")))
    .withColumn("total_amount", spark_round(col("amount") * col("quantity"), 2))
    .withColumn("_ingested_at", current_timestamp())
)

print("Parsed stream schema:")
parsed.printSchema()


# ════════════════════════════════════════════════════════════
# STEP 3 — Write to Delta Lake (streaming sink)
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("STEP 3: Writing stream to Delta Lake")
print("="*60)
print(f"Output path: {DELTA_PATH}")
print(f"Checkpoint:  {CHECKPOINT_PATH}")
print()
print("The stream is now running. As the producer sends events to Kafka,")
print("Spark will read them in micro-batches and append to the Delta table.")
print()
print("While this runs, open another terminal and check the Delta table:")
print("  ./repl.sh")
print('  >>> spark.read.format("delta").load("/delta-data/streaming/orders").show()')
print('  >>> spark.read.format("delta").load("/delta-data/streaming/orders").count()')
print()
print("Press Ctrl+C to stop the streaming job.")
print("="*60)

# writeStream — the streaming equivalent of df.write
# It looks almost identical to a batch write!
query = (parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")    # micro-batch every 5 seconds
    .start(DELTA_PATH))

# The query runs in the background.
# awaitTermination() blocks until you Ctrl+C.
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n\nStopping streaming query...")
    query.stop()

# ════════════════════════════════════════════════════════════
# STEP 4 — Show what was written
# ════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("STEP 4: What the stream wrote to Delta")
print("="*60)

final = spark.read.format("delta").load(DELTA_PATH)
total = final.count()
print(f"\nTotal rows in Delta table: {total}")
print("\nSample rows:")
final.orderBy(col("order_id").desc()).show(10)

print("By region:")
final.groupBy("region").agg(
    count("*").alias("orders"),
    spark_round(spark_sum("total_amount"), 2).alias("revenue")
).orderBy("region").show()

from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, DELTA_PATH)
print("Delta history (one version per micro-batch):")
dt.history().select("version", "timestamp", "operation").show(20, truncate=False)

print("""
What just happened:
  1. The producer sent JSON events to Kafka (1/sec)
  2. Spark Structured Streaming read them in micro-batches (every 5 sec)
  3. Each micro-batch: parse JSON → add columns → append to Delta
  4. Each micro-batch = one new Delta version (one new Parquet file)

This is the "streaming into the lakehouse" pattern from Chapter 8.
In production:
  - The producer is your application / IoT device / payment system
  - Kafka is your event highway
  - Spark Structured Streaming is the bridge into the lakehouse
  - Downstream: batch Gold tables, Trino dashboards, ML features
""")

spark.stop()
