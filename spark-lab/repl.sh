#!/bin/bash
# Universal PySpark REPL — loads both Delta Lake and Iceberg
# Access all lab tables from one interactive session.
#
# Usage: ./repl.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

docker run --rm -it \
  --network spark-lab_spark-net \
  -v "$SCRIPT_DIR/delta-data:/delta-data" \
  -v "$SCRIPT_DIR/jobs:/jobs" \
  apache/spark:3.5.1 \
  /opt/spark/bin/pyspark \
    --master spark://spark-master:7077 \
    --packages io.delta:delta-spark_2.12:3.1.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=/delta-data/iceberg-warehouse \
    --conf spark.jars.ivy=/tmp/.ivy2
