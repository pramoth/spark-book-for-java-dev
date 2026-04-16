# Appendix 15 — Data Lake vs Delta Lake (the naming confusion)

## The problem

"Delta Lake" and "Data Lake" are **not the same thing** and are **not variations of each other**. They just both contain the word "Lake". This confuses everyone.

## Three separate things

### 1. Data Lake (a concept)

A **place** where you dump files. Just a folder on S3/HDFS.

```
s3://my-bucket/
├── logs_2026_01.json
├── sales.csv
├── users.parquet
└── who_put_this_here.xlsx
```

No rules, no transactions, no schema. Cheap storage. The concept was coined ~2010 to contrast with expensive data warehouses.

**"Data Lake" is an idea, not a technology.** S3 is a data lake. HDFS is a data lake. A folder on your desktop could be one. There's no software called "Data Lake" you install.

### 2. Delta Lake (a specific open-source project)

A **software library** created by Databricks in 2019. A JAR you add to Spark. Adds a transaction log (`_delta_log/`) on top of Parquet files.

```
s3://my-bucket/sales/
├── part-00000.parquet
├── part-00001.parquet
└── _delta_log/          ← THIS is what Delta Lake adds
    ├── 000...000.json
    └── 000...001.json
```

**"Delta Lake" is a product you install.** GitHub repo, version numbers, Maven artifact (`io.delta:delta-spark`). You add it to `spark-submit --packages` and it gives your Parquet files ACID, UPDATE/DELETE, time travel, schema enforcement.

### 3. Data Lakehouse (a concept)

An architecture pattern: take a Data Lake + add a table format (Delta Lake or Iceberg or Hudi) = warehouse-like reliability on cheap storage.

**"Data Lakehouse" is an idea, not a technology.** You build one by combining a data lake + a table format library.

## The relationship

```
Data Lake    +   Delta Lake (software)   =   Data Lakehouse
(concept:         (library: adds               (concept: 
 files on S3)      transactions)                lake + reliability)
```

Or with Iceberg:
```
Data Lake    +   Apache Iceberg          =   Data Lakehouse
(files on S3)    (same purpose,               (same concept,
                  different library)            different software)
```

## Analogy

- **Data Lake** = a dirt road (cheap, no rules, muddy).
- **Delta Lake** = the asphalt and lane markings (specific product, makes it safe).
- **Data Lakehouse** = the concept of "a paved road".

You don't choose between a dirt road and asphalt — you *add* asphalt *to* the dirt road. Same: you *add* Delta Lake *to* your data lake to make it a lakehouse.

## Why the naming is confusing

Databricks was clever with branding:
- Product: "**Delta** Lake" — one word away from "Data Lake".
- Architecture: "**Data Lakehouse**" — combines both.
- Result: *"We're building a **data lakehouse** on our **data lake** using **Delta Lake**."*

Three "lakes", all different.

## Cheat sheet

| Term | What it is | Install it? |
|---|---|---|
| **Data Lake** | Files on S3. A concept. | No. Just a folder. |
| **Delta Lake** | JAR by Databricks. Adds transactions to Parquet. | **Yes.** `--packages io.delta:delta-spark` |
| **Apache Iceberg** | JAR (Netflix origin). Same purpose as Delta. | **Yes.** `--packages org.apache.iceberg:...` |
| **Apache Hudi** | JAR (Uber origin). Same purpose. | **Yes.** |
| **Data Lakehouse** | Architecture: lake + table format. A concept. | No. You build it by combining the above. |

## Mental model

> **Data Lake = where your files live** (S3). **Delta Lake = software you add** to give those files ACID transactions. **Data Lakehouse = what you call it** after you've added that software. The three "lakes" are: a place, a product, and a pattern.
