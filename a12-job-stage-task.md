# Appendix 12 — Job, Stage, Task: The Three-Level Hierarchy

Spark has three nested levels of work. Once you see the relationship, the UI becomes readable.

```
   APPLICATION  =  your whole spark-submit run
        │
        ├── JOB  0  ← triggered by first show()
        │     └── STAGE 0 (2 tasks)
        │
        ├── JOB  1  ← also triggered by show()
        │     ├── STAGE 1 (skipped)
        │     └── STAGE 2 (1 task)
        │
        ├── JOB  2  ← triggered by counts.count()
        │     └── STAGE 3 (2 tasks)
        │
        ├── JOB  3  ← part of count()
        │     ├── STAGE 4 (skipped)
        │     └── STAGE 5 (1 task)
        │
        └── JOB  4  ← final piece of count()
              ├── STAGE 6 (skipped)
              ├── STAGE 7 (skipped)
              └── STAGE 8 (1 task)
```

## Level 1 — Job: "one action"

> **A job = everything Spark must run to answer one action() call.**

`show()`, `count()`, `collect()`, `write()`, `take()`, `first()` — each call creates at least one job. Transformations (`filter`, `map`, `groupBy`, `join`) do **not** create jobs; they just build plan nodes.

**But: action ≠ exactly one job.** A single action often produces 2–3 jobs because:
- `show()` calls `take(20)`, which first probes 1 partition and then reads more if needed.
- **AQE (Adaptive Query Execution)** splits execution at every shuffle to re-inspect runtime stats. `count()` with two shuffles in the pipeline → 3 jobs, one per "stretch" between shuffles.

That's why a 2-action script can show 5 jobs in the UI.

## Level 2 — Stage: "everything between shuffles"

> **A stage = a chunk of work that can run without moving data between executors.**

Spark takes each job's DAG and cuts it at every `Exchange` (shuffle). Each chunk between shuffles is one stage.

```
Stage N ──shuffle──▶ Stage N+1 ──shuffle──▶ Stage N+2
(scan +              (partial                (sort +
 partial agg)         merge)                  final action)
```

Within a stage, Spark pipelines every operation into one fused JVM method (**WholeStageCodegen**). Across stages, it must wait for the shuffle to finish.

### Skipped stages

When you see "skipped" in the UI, it means Spark **reused shuffle files from an earlier job** instead of recomputing them.

**Skipped = cached win, not an error.**

But: shuffle reuse works only **within a single physical plan** — it doesn't cross action boundaries. To share work across actions, use `cache()` / `persist()`. (See Appendix 14.)

## Level 3 — Task: "one partition of one stage"

> **A task = one partition being processed by one stage, running on one executor core.**

Task count = partition count. If a stage has 200 partitions, it has 200 tasks. If the cluster has 4 cores, those 200 tasks run 4 at a time, in 50 waves.

Partition count comes from:
- How the data was read (file size ÷ 128 MB block).
- `repartition(N)` / `coalesce(N)`.
- `spark.sql.shuffle.partitions` (default **200** — a notorious over-default).
- **AQE** can coalesce oversized defaults at runtime.

## The relationship in one table

| Level | Definition | Triggered by | Contains |
|---|---|---|---|
| **Job** | Work for one action | `show()`, `count()`, `write()`, `take()` | 1+ stages |
| **Stage** | Work between shuffles | DAG cut at `Exchange` nodes | 1+ tasks |
| **Task** | One partition processed by one stage | Partitioning of the data | runs on 1 core |

## Plain English

> **An action creates a job.** Spark builds the DAG and **cuts it at every shuffle boundary — each chunk is a stage.** Each stage has **one task per partition**, and tasks run on executor cores in parallel. When a later job needs shuffle data that an earlier job already produced, those stages are **skipped**.

## Reading the UI

1. **Jobs tab** → list of jobs, one row per action (possibly more).
2. Click a job → see its DAG visualization — skipped stages are grayed out.
3. **Stages tab** → flat list of all stages across all jobs, each showing task count and status.
4. Click a stage → task-level metrics: runtime, input size, shuffle read/write, executor hostname.
5. **SQL / DataFrame tab** → per-action query with logical plan → optimized plan → physical plan.
6. **Executors tab** → the running executors and which tasks each handled.

## Mental model

> **Action → Job. Shuffle → stage boundary. Partition → task.** One action triggers one or more jobs. Each job's DAG is cut into stages wherever there's a shuffle. Each stage has one task per partition. Skipped stages mean Spark reused shuffle data from earlier jobs within the same physical plan.
