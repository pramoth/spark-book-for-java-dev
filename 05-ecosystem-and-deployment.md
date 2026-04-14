# Chapter 5 — Ecosystem & Deployment

You understand the cluster managers themselves (Appendix 2). This chapter is about *how teams actually deploy Spark in the real world* — what choices they face, what they cost, and what to ask clients.

## The deployment spectrum

From most DIY to most managed:

```
  More control                                              Less effort
       │                                                         │
       ▼                                                         ▼
 ┌──────────┐  ┌──────┐  ┌─────┐  ┌──────────┐  ┌──────────────┐
 │Standalone│  │ YARN │  │ K8s │  │EMR/Dataproc│  │  Databricks  │
 │ on bare  │  │  on  │  │  on │  │  (managed) │  │  (premium)   │
 │  metal   │  │Hadoop│  │cloud│  │            │  │              │
 └──────────┘  └──────┘  └─────┘  └──────────┘  └──────────────┘
```

## 1. Standalone on bare metal / VMs

You install Spark on a few Linux machines and run Spark's built-in Master + Workers.

**Pros:** simplest setup, no external dependencies, great for learning and small production.
**Cons:** no multi-tenancy, no fine-grained resource isolation, you manage everything.
**Who uses it:** small teams, internal tools, labs.

## 2. YARN on Hadoop

You already have a Hadoop cluster. Spark plugs in.

**Pros:** battle-tested at huge scale, mature security (Kerberos), multi-tenancy, co-location with HDFS.
**Cons:** heavy, slow upgrades, on-prem-centric.
**Who uses it:** big enterprises with on-prem data centers — banks, telcos, government.
**Trend:** declining. Most clients migrating off YARN to K8s or managed services.

## 3. Kubernetes (the modern cloud-native choice)

Spark talks to the K8s API to spawn executor pods on demand.

**Pros:**
- Same K8s cluster runs Spark + microservices + Airflow + everything. One platform team, not two.
- Pay only for what you use — pods come up for the job, go down after.
- Perfect for **transient/ephemeral clusters**.
- Cloud-portable — identical on EKS / GKE / AKS / on-prem.

**Cons:** K8s expertise required, Spark-on-K8s is younger, network/storage tuning matters.
**Who uses it:** companies building a modern data platform from scratch.
**Trend:** growing fast. Default for greenfield.

## 4. Cloud-managed Spark services

### AWS EMR
- Click "create cluster" → AWS provisions EC2 nodes with Spark.
- **EMR on EC2** (classic) and **EMR on EKS** (Spark on K8s, AWS manages K8s).
- **EMR Serverless** — AWS scales for you.

### Google Dataproc
- Same idea on GCP. Tight integration with BigQuery, GCS.
- **Dataproc Serverless** available.

### Azure Synapse / HDInsight
- Microsoft's equivalents.

**Pros:** zero cluster ops, native cloud integration, auto-scaling.
**Cons:** vendor lock-in, markup over raw compute, less customization.
**Who uses them:** teams wanting Spark without becoming infra experts.

## 5. Databricks (premium, opinionated)

Founded by the **original Spark creators** (Berkeley AMPLab). Runs on AWS, Azure, GCP.

**What you get beyond vanilla Spark:**
- **Photon** — proprietary C++ rewrite of Spark's execution engine, 2–3× faster.
- **Delta Lake** — invented by Databricks, first-class integration.
- **Notebooks + collaboration**.
- **Unity Catalog** — data governance / lineage / access control.
- **Workflows** — built-in scheduler.
- **MLflow** — experiment tracking, model registry.
- **SQL Warehouse** — Spark SQL behind a serverless endpoint.

**Pros:** most polished Spark, best performance (Photon), strongest ecosystem, best support.
**Cons:** **expensive** — DBU markup 30–80%. Lock-in risk. Overkill for small workloads.
**Who uses it:** mid-to-large enterprises wanting a turnkey data platform.

## Quick comparison

| | Standalone | YARN | K8s | EMR/Dataproc | Databricks |
|---|---|---|---|---|---|
| **Ops effort** | Low (small) | High | Medium-High | Low | Lowest |
| **Cost** | $ | $$ | $$ | $$ | $$$$ |
| **Performance** | Baseline | Baseline | Baseline | Baseline | **2–3× (Photon)** |
| **Multi-tenant** | ❌ | ✅ | ✅ | ✅ | ✅ |
| **Cloud-native** | ❌ | ❌ | ✅ | ✅ | ✅ |
| **Vendor lock-in** | None | None | None | Medium | Medium-High |
| **Best for** | Learning, small | On-prem legacy | Modern cloud | Easy cloud | Premium platform |

## Long-running vs transient clusters

### Long-running (always-on)
- Cluster runs 24/7, accepts many jobs.
- Driver and executors persist.
- Lower latency to start jobs.
- More wasteful if utilization is uneven.
- **Pick for:** interactive notebooks, BI tools, streaming jobs (they run continuously).

### Transient (ephemeral / per-job)
- Spin up for one job, tear down at the end.
- Pay only for actual job runtime.
- ~1–3 minute startup cost.
- **Pick for:** nightly ETL, scheduled batch jobs.

> **Modern best practice:** transient for batch, long-running for interactive and streaming.

## The cost conversation

Three cost components:
1. **Compute** (CPU + RAM hours) — 60–80% of total.
2. **Storage** (S3 / managed disk) — 5–15%.
3. **Platform markup** (EMR/Databricks fees) — 10–40%.

**Common traps:**
- Pick Databricks for "easy" → shocked by bill.
- Pick raw open-source Spark to "save money" → spend more on a 5-engineer platform team.

> **Right question:** "How much engineering time do you want to spend running infrastructure vs. building data products?"

## Storage: the other half

Spark is **stateless compute**. Options:

- **HDFS** — classic, on-prem, declining.
- **S3 / GCS / Azure Blob** — cheap, durable, scalable. Modern default.
- **Local SSD scratch** — shuffle and spill.
- **Lakehouse formats (Delta/Iceberg/Hudi)** — on top of the above.

### Storage-compute decoupling

**Classic Hadoop (2006):** *"Move compute to data."* Put HDFS on the same machines as YARN. Data locality was the key optimization.

**Modern (2020+):** *"Decouple storage from compute."* Two things changed:
1. Network got fast — S3 reads ≈ local disk speed.
2. Object storage is cheap, durable, infinite.

So: data in S3, spin up compute (Spark / DuckDB / Trino) when needed. Cluster dies → data fine.

**Why it matters:**
- Scale storage and compute independently.
- Multiple engines share the same data.
- Transient clusters become possible.

## Client heuristics

- **"Hadoop / HDFS"** → YARN. Don't migrate prematurely.
- **"EKS / GKE / cloud-native"** → K8s or EKS-managed.
- **"No infra team, need ASAP"** → Managed (EMR / Dataproc).
- **"Best performance, money no object"** → Databricks (Photon).
- **"Cost-sensitive but can run infra"** → Spark on K8s + S3.
- **"2 engineers and 100 GB"** → Don't use Spark. Use DuckDB.

## Mental model

> Spark deployment = choose a **cluster manager** (Standalone / YARN / K8s) and a **runner** (DIY / managed / Databricks). Every option uses the same Spark engine. **More effort = less money; less effort = more money.**

## Quiz (with answers)

**Q1. 5-person startup, 200 GB data, no platform team, nightly ETL — what do you suggest?**
A: **Skip Spark entirely.** 200 GB fits in a single beefy cloud VM running DuckDB or Polars — faster, cheaper, trivial ops. If they insist on Spark, use EMR Serverless or Dataproc Serverless, not Databricks (too expensive for this size). Warn against DIY Spark-on-K8s — they don't have the people.

**Q2. Long-running vs transient, and when to pick each?**
A: Long-running: cluster stays 24/7, for interactive notebooks, BI, and streaming. Transient: spun up per job, torn down after, for scheduled batch ETL. Modern practice: transient for batch, long-running for interactive/streaming.

**Q3. Why are modern Spark deployments storage-compute decoupled vs Hadoop?**
A: Hadoop put HDFS and YARN on the same nodes ("move compute to data") because networks were slow. Modern networks are fast and S3 is cheap+durable, so data lives in S3 and clusters come and go. Enables independent scaling, multi-engine access, and transient clusters. Delta/Iceberg layer on top makes it reliable.

**Q4. Client says "Databricks is the official Spark" — correct?**
A: No. Spark is an **Apache open-source project**, runs anywhere. Databricks is a **commercial company** founded by Spark's creators selling a premium managed platform. Databricks employees contribute heavily to open-source Spark, but Spark on EMR / Dataproc / K8s / your laptop is all "real Spark". Databricks adds proprietary extras: **Photon** (faster engine), Unity Catalog, Workflows, etc. You pay Databricks for performance + zero ops, not because vanilla Spark is incomplete.
