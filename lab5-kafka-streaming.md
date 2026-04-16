# Lab 5 — Kafka → Spark Structured Streaming → Delta Lake

Real-time data ingestion: a producer sends events to Kafka, Spark Structured Streaming reads them in micro-batches, and appends to a Delta table. The lakehouse grows as events arrive.

## Architecture

```
Producer (Python)        Kafka            Spark Structured       Delta Lake
─────────────────       ──────            Streaming              ──────────
JSON events (1/sec) ──→ "orders" topic ──→ readStream ──→ parse ──→ /streaming/orders/
                        (event buffer)    (micro-batch 5s)         Parquet + _delta_log
```

## Prerequisites

Ensure docker-compose includes Kafka:

```yaml
kafka:
  image: apache/kafka:3.7.0
  container_name: kafka
  hostname: kafka
  ports:
    - "9092:9092"
  environment:
    KAFKA_NODE_ID: 1
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
    CLUSTER_ID: lab5-kafka-cluster-id-001
  networks:
    - spark-net
```

Uses **KRaft mode** (Kafka without ZooKeeper) — the modern Kafka default since 3.3+.

## Step 1 — Start the cluster

```bash
cd /Users/pramoth/work/tmp/spark-book/spark-lab
docker compose up -d
```

Verify: `docker compose ps` should show 4 containers (master, 2 workers, kafka).

## Step 2 — Start the producer

```bash
docker run --rm -d --name kafka-producer \
  --network spark-lab_spark-net \
  -e PYTHONUNBUFFERED=1 \
  -v $(pwd)/jobs:/jobs \
  python:3.11-slim \
  bash -c "pip install kafka-python-ng -q 2>/dev/null && python /jobs/lab5_producer.py"
```

Watch it: `docker logs -f kafka-producer`

You'll see one event per second:
```
→ order 2000: keyboard     US   $  440.43 x1
→ order 2001: phone        EU   $ 1385.18 x5
→ order 2002: tablet       EU   $  971.59 x4
```

**Important:** `-e PYTHONUNBUFFERED=1` — without this, Python buffers stdout in detached mode and `docker logs` shows nothing.

## Step 3 — Start the streaming job

```bash
rm -rf delta-data/streaming

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
```

Note the extra package: `spark-sql-kafka-0-10_2.12:3.5.1` — this is Spark's Kafka connector.

The job runs until you press **Ctrl+C**.

## Step 4 — Watch it work

While the streaming job runs, open another terminal:

```bash
cd /Users/pramoth/work/tmp/spark-book/spark-lab
./repl.sh
```

```python
# Watch the table grow in real-time
spark.read.format("delta").load("/delta-data/streaming/orders").count()
# Run this again in 10 seconds — the count increases!

spark.read.format("delta").load("/delta-data/streaming/orders").show()

# Check Delta versions (one per micro-batch)
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/delta-data/streaming/orders")
dt.history().select("version", "timestamp", "operation").show()
```

## What the code does

### Reading from Kafka

```python
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "earliest")
    .load())
```

This is `readStream` instead of `read` — same API shape, but creates a **continuous query** instead of a one-shot read. Kafka gives you binary key + value; the value is your JSON.

### Parsing JSON

```python
parsed = (raw_stream
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), event_schema).alias("data"))
    .select("data.*")
    .withColumn("order_ts", to_timestamp(col("order_ts")))
    .withColumn("total_amount", spark_round(col("amount") * col("quantity"), 2))
    .withColumn("_ingested_at", current_timestamp()))
```

**This is the exact same DataFrame API as batch.** `filter`, `withColumn`, `select` — all identical. That's Spark's "unified batch and streaming" promise from Chapter 4.

### Writing to Delta

```python
query = (parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")
    .start(DELTA_PATH))
```

- **`writeStream`** instead of `write` — continuous output.
- **`outputMode("append")`** — each micro-batch adds new rows (no updates/deletes).
- **`checkpointLocation`** — Spark stores progress here. If the job crashes and restarts, it resumes from the last checkpoint, not from the beginning. **Exactly-once delivery.**
- **`trigger(processingTime="5 seconds")`** — a new micro-batch every 5 seconds.

### Checkpoints — the key to reliability

The checkpoint directory stores:
- Which Kafka offsets have been processed.
- Which micro-batch is in progress.
- Which Delta commits succeeded.

If the streaming job crashes at any point, restart it with the same checkpoint path and it **picks up exactly where it left off**. No duplicate data, no lost events. This is the "exactly-once" guarantee.

## What ends up on disk

```
delta-data/streaming/
├── orders/                     ← the Delta table
│   ├── part-00000-xxx.parquet  ← one file per micro-batch
│   ├── part-00000-yyy.parquet
│   ├── ...
│   └── _delta_log/
│       ├── 000...000.json      ← version 0 (batch 0)
│       ├── 000...001.json      ← version 1 (batch 1)
│       └── ...                 ← one version per micro-batch
└── _checkpoints/orders/        ← Spark's progress tracker
    ├── offsets/
    ├── commits/
    └── metadata
```

Each micro-batch = one new Parquet file + one new Delta log entry. The table version count equals the number of micro-batches that ran.

## Cleanup

```bash
docker stop kafka-producer 2>/dev/null
# The streaming job stops on Ctrl+C
```

## Key takeaways

- **`readStream` / `writeStream`** — the streaming API is almost identical to batch. Same DataFrame transformations in between. That's the unified model.
- **Micro-batching** — Spark doesn't process events one by one. It buffers 5 seconds of events, processes them as a batch, writes one Parquet file. This is why Spark streaming latency is ~seconds, not milliseconds.
- **Checkpoints = exactly-once** — crash and restart without duplicates or data loss.
- **Each micro-batch = one Delta version** — you can time-travel to any batch.
- **The Delta table looks identical to a batch-written table** — downstream consumers (Trino, Gold aggregation jobs, ML pipelines) can't tell the difference. They just see a Delta table that keeps getting new rows.

## In production

| Lab version | Production version |
|---|---|
| `python:3.11-slim` producer | Your app / IoT devices / payment system → Kafka |
| Single Kafka container | Kafka cluster (3+ brokers) on Confluent / MSK / Redpanda |
| `trigger(processingTime="5 seconds")` | Tuned per workload: 1s for near-real-time, 1min for cost savings |
| Local Delta table | S3 + Iceberg/Delta |
| Manual `docker run` | Airflow triggers, K8s CronJobs, Databricks scheduled jobs |
| Ctrl+C to stop | Graceful shutdown with monitoring + alerting |
