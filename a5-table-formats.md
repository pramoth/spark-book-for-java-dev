# Appendix 5 — Table Formats on Disk (Delta, Iceberg)

## Starting point: plain Parquet

A sales table without any table format is just:

```
s3://bucket/sales/
├── part-00000.parquet
├── part-00001.parquet
├── part-00002.parquet
```

Three files, no metadata. Want to know columns? Read a Parquet header. Want to `DELETE WHERE region='EU'`? Manually rewrite files. Two concurrent writers? Chaos.

## Adding Delta Lake

Delta adds **one folder**: `_delta_log/`.

```
s3://bucket/sales/
├── part-00000-abc.parquet          ← actual data (unchanged)
├── part-00001-def.parquet
├── part-00002-ghi.parquet
└── _delta_log/                     ← the table format
    ├── 00000000000000000000.json   ← version 0
    ├── 00000000000000000001.json   ← version 1
    ├── 00000000000000000002.json   ← version 2
    └── 00000000000000000002.checkpoint.parquet
```

Parquet files are still normal Parquet. Delta is just the extra log folder.

## What's inside a log file

Each `.json` is an **append-only list of actions**:

```json
{"commitInfo":{
   "timestamp":1744588800000,
   "operation":"WRITE",
   "operationParameters":{"mode":"Append"},
   "isolationLevel":"Serializable"
}}
{"metaData":{
   "id":"3f2c...",
   "format":{"provider":"parquet"},
   "schemaString":"{\"type\":\"struct\",\"fields\":[
       {\"name\":\"order_id\",\"type\":\"long\"},
       {\"name\":\"region\",\"type\":\"string\"},
       {\"name\":\"amount\",\"type\":\"double\"}]}",
   "partitionColumns":["region"]
}}
{"add":{
   "path":"region=EU/part-00000-abc.parquet",
   "size":134217728,
   "dataChange":true,
   "stats":"{\"numRecords\":1000000,
             \"minValues\":{\"order_id\":1,\"amount\":0.5},
             \"maxValues\":{\"order_id\":1000000,\"amount\":9999.9},
             \"nullCount\":{\"order_id\":0}}"
}}
```

Three kinds of actions:
- **`commitInfo`** — who did what, when. For audits.
- **`metaData`** — schema + partition columns.
- **`add`** — *"this Parquet file is now part of the table"*, with **per-column stats**.

When you `DELETE`, you get a `remove`:

```json
{"remove":{
   "path":"region=EU/part-00000-abc.parquet",
   "deletionTimestamp":1744600000000,
   "dataChange":true
}}
```

**Delta doesn't delete the Parquet file from S3.** It just writes a log entry saying *"pretend this file isn't part of the table anymore"*. The file stays until `VACUUM`. That enables **time travel**.

## How a query uses the log

`SELECT * FROM sales WHERE region='EU' AND amount > 1000`:

1. Read `_delta_log/*.json`, replay to build the **current file list**.
2. For each active file, look at the **stats**: *"amount max = 50, skip. amount max = 9999, read."* → **file-level skipping**.
3. Open only the surviving Parquets. Parquet's own row-group stats further skip.

This is why lakehouses are fast despite being "just files" — **the log lets you skip 90%+ of data before opening a file**.

## Time travel

```sql
SELECT * FROM sales VERSION AS OF 5;
SELECT * FROM sales TIMESTAMP AS OF '2026-04-10';
```

Spark reads the log **only up to version 5**. No backup, no snapshot — just replaying a different prefix of the log.

## How Iceberg differs

Two-level metadata structure:

```
s3://bucket/sales/
├── data/
│   ├── 00000-abc.parquet
│   └── 00001-def.parquet
└── metadata/
    ├── v1.metadata.json           ← table-level pointer
    ├── v2.metadata.json
    ├── snap-1234.avro             ← manifest list
    └── 5678-m0.avro               ← manifest (data files + stats)
```

- **metadata.json** — current schema + pointer to active snapshot.
- **manifest list** (Avro) — list of manifests in this snapshot.
- **manifest** (Avro) — list of data files with stats.

More indirection, scales to millions of files better than Delta's flat JSON log. Iceberg wins in very large deployments; Delta dominates the Databricks ecosystem.

**Hudi** is the third option — similar, focused on heavy upsert workloads.

## Mental model

> A **table format** is a **transaction log** + **file manifest with stats**, layered on top of plain Parquet files in object storage. It turns "a folder of files" into "a versioned, ACID-compliant table" — without moving the data.

## Why this matters for clients

- Data (Parquet) is still open and portable. Switching formats doesn't require rewriting data.
- Multiple engines can read the same table — Spark writes, Trino reads, Flink streams.
- Cheap S3 + warehouse semantics = the lakehouse value prop in one sentence.
