# Chapter 6 — Docker Hands-On

Time to stop talking and **run Spark**. By the end you'll have a real mini-cluster running in Docker, submit a job to it, and read the Web UI fluently.

## What you need

1. **Docker Desktop** (or OrbStack) installed and running.
2. ~4 GB of free RAM.
3. A working directory. We use `/Users/pramoth/work/tmp/spark-book/spark-lab/`.

Nothing else. No Java, no Spark install, no Python — Docker pulls everything.

## Stage 1 — Sanity check (local mode)

```bash
docker run -it --rm apache/spark:3.5.1 /opt/spark/bin/pyspark
```

Inside the PySpark prompt:
```python
spark.range(1000000).filter("id % 2 == 0").count()
# 500000
```

This is **local mode** — one JVM pretending to be driver + executor. Good for dev, not a real cluster.

## Stage 2 — Standalone cluster via docker compose

Create `docker-compose.yml`:

```yaml
services:
  spark-master:
    image: apache/spark:3.5.1
    container_name: spark-master
    hostname: spark-master
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master", "--host", "spark-master", "--port", "7077", "--webui-port", "8080"]
    ports:
      - "18080:8080"    # master web UI (host:container) — 18080 if 8080 is in use
      - "7077:7077"     # master RPC
    networks:
      - spark-net

  spark-worker-1:
    image: apache/spark:3.5.1
    container_name: spark-worker-1
    hostname: spark-worker-1
    depends_on: [spark-master]
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "--cores", "2", "--memory", "1g", "--webui-port", "8081", "spark://spark-master:7077"]
    ports:
      - "8081:8081"
    networks:
      - spark-net

  spark-worker-2:
    image: apache/spark:3.5.1
    container_name: spark-worker-2
    hostname: spark-worker-2
    depends_on: [spark-master]
    command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "--cores", "2", "--memory", "1g", "--webui-port", "8081", "spark://spark-master:7077"]
    ports:
      - "8082:8081"
    networks:
      - spark-net

networks:
  spark-net:
    driver: bridge
```

### Important notes on this file

- **One image, three services.** The Master and Workers are the *same binary*; only the launched Java class differs. That's true for real Spark deployments too.
- **`command:` in list form** — YAML's folded `>` string form can trip docker compose's argument parsing. List form is reliable.
- **Bitnami's image is gone.** Broadcom (which owns VMware → Bitnami) removed free Bitnami images from Docker Hub in 2025. Use `apache/spark`.
- **Port 8080 collision.** If you already have something on port 8080 (many Java apps default there), map to `18080:8080` instead. Check with `lsof -iTCP:8080 -sTCP:LISTEN`.

### Bring it up

```bash
cd /Users/pramoth/work/tmp/spark-book/spark-lab
docker compose up -d
docker compose ps
```

Then open **http://localhost:18080** — you should see the Spark Master UI with **Alive Workers: 2**, **Cores in use: 4 Total**.

**This is the architecture from Chapter 2 made real.** The "Driver → Cluster Manager → Executors" picture is literally what you're looking at — just without the driver, because you haven't submitted a job yet.

### Troubleshooting

- Master UI shows 404 → something else is listening on that host port. Pick a different host port.
- Workers not registering → `docker compose logs spark-worker-1`.
- Apple Silicon issues → add `platform: linux/amd64` to each service.

## Stage 3 — Submit a job

Put a PySpark script at `jobs/wordcount.py`:

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col

spark = (SparkSession.builder
         .appName("wordcount-demo")
         .getOrCreate())

data = [
    ("spark is fast",),
    ("spark is distributed",),
    ("spark is fun",),
    ("docker makes spark easy",),
    ("learning spark with docker",),
] * 10000  # 50,000 rows

df = spark.createDataFrame(data, ["line"])

words = df.select(explode(split(lower(col("line")), " ")).alias("word"))
counts = words.groupBy("word").count().orderBy(col("count").desc())

counts.show()
print("Total unique words:", counts.count())

# Keep the driver alive so the UI stays reachable
input("Press Enter to stop Spark and exit...")
spark.stop()
```

Submit it from a one-shot client container:

```bash
docker run --rm -it \
  --network spark-lab_spark-net \
  -p 14040:4040 \
  -v /Users/pramoth/work/tmp/spark-book/spark-lab/jobs:/jobs \
  apache/spark:3.5.1 \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --executor-memory 512m \
    --total-executor-cores 2 \
    --conf spark.driver.host=spark-client \
    /jobs/wordcount.py
```

### What each flag does

- `--network spark-lab_spark-net` — join the compose network (project name prefix is the folder name).
- `-p 14040:4040` — expose the **driver UI** (each Spark driver starts a Jetty server on port 4040).
- `-v /host/jobs:/jobs` — mount your code into the container.
- `--master spark://spark-master:7077` — connect to the Standalone cluster manager.
- `--deploy-mode client` — the driver runs in **this** container. Your terminal sees logs and output.
- `--executor-memory 512m --total-executor-cores 2` — request resources from the master.

### Watch it work

During execution, open two browser tabs:

- **http://localhost:18080** — Master UI. You'll see the running application with its cores usage.
- **http://localhost:14040** — Driver UI. Jobs tab, Stages tab, SQL tab, Executors tab. This is the one worth exploring (see Chapter 2 and Appendices 12–14 for how to read it).

## Stage 4 — Cleanup

```bash
cd /Users/pramoth/work/tmp/spark-book/spark-lab
docker compose down
```

Containers and network removed. Files (`docker-compose.yml`, `jobs/`) are untouched.

## The "keep the UI alive" trick

The driver UI is served by a Jetty web server **inside the driver JVM**. When `spark.stop()` runs (or the script exits), the UI dies with it.

Three options:
1. **`input("Press Enter ...")`** — simplest, blocks until you hit Enter.
2. **`time.sleep(60)`** — auto-exits after a fixed time.
3. **Spark History Server** — a separate long-running process that reads persistent event logs and rebuilds the UI *after* the job ends. This is the production pattern; in big clusters you log to S3 and one History Server shows all past jobs.

For learning, Option 1 is what you want.

## Homework ideas

- **Kill a worker mid-job** → `docker stop spark-worker-2` while a job is running. Watch Spark recompute lost partitions (Chapter 2's fault tolerance, live).
- **Make the job bigger** → bump `* 10000` to `* 1000000` (5 M rows). Watch the number of tasks and stage durations change.
- **Add Delta Lake** → pass `--packages io.delta:delta-spark_2.12:3.2.0` to `spark-submit` and try `CREATE TABLE ... USING delta` (Appendix 6).
- **Read a real file** → mount `jobs/data/sales.parquet`, use `spark.read.parquet("/jobs/data/sales.parquet")`.

## Client conversation

When a client says *"we want to try Spark before committing to cloud"*, this docker compose setup is exactly what you recommend:

- A genuine distributed cluster.
- Reproducible across machines.
- Zero cloud cost.
- A stepping stone to K8s later (same images can go to production).

It is **not** production-grade (no HA, no auth, no persistence, no monitoring), but it is perfect for learning, demos, and dev loops.

## Mental model

> **Spark in Docker ≠ Spark on Kubernetes.** We're running Standalone-mode Spark inside containers that happen to be orchestrated by Docker Compose. From Spark's perspective, each container is a normal Linux host. It's the simplest way to experience a real distributed Spark cluster on your laptop.
