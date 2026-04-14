# Chapter 2 — Spark Architecture

This is the most important chapter. Once you see the picture, everything else (performance, cost, failures, client questions) falls out of it.

## The cast of characters

```
           ┌─────────────────┐
           │     DRIVER      │  ← your program runs here
           │  (SparkContext) │     builds the DAG, schedules work
           └────────┬────────┘
                    │
           ┌────────▼────────┐
           │ CLUSTER MANAGER │  ← gives Spark machines to use
           │ (YARN/K8s/...)  │     "I need 10 executors, 4 cores each"
           └────────┬────────┘
                    │
      ┌─────────────┼─────────────┐
      ▼             ▼             ▼
 ┌─────────┐   ┌─────────┐   ┌─────────┐
 │EXECUTOR │   │EXECUTOR │   │EXECUTOR │  ← JVMs that actually
 │ (JVM)   │   │ (JVM)   │   │ (JVM)   │     crunch the data
 │ tasks   │   │ tasks   │   │ tasks   │
 │ cache   │   │ cache   │   │ cache   │
 └─────────┘   └─────────┘   └─────────┘
```

### 1. Driver
The process running **your** code (`main` method). It:
- Builds the computation plan (DAG).
- Breaks it into **stages** and **tasks**.
- Sends tasks to executors and collects results.
- If the driver dies, the whole job dies.

### 2. Cluster Manager
A separate system whose only job is **"give me N machines with X cores and Y GB RAM"**. Spark supports several:
- **Standalone** (Spark's own, simple)
- **YARN** (Hadoop ecosystem)
- **Kubernetes** (modern default)
- **Mesos** (legacy)

Spark is agnostic — same code runs on any of them.

### 3. Executors
JVM processes running on worker machines. Each executor:
- Runs multiple **tasks** in parallel (one per core).
- Holds cached data in memory (the "in-memory" magic).
- Reports results back to the driver.

## The key concepts that fall out of this picture

### Partitions
Your 2 TB file gets split into chunks called **partitions** (typically 128 MB each). Each partition = one task = one core's work. 2 TB ÷ 128 MB ≈ **16,000 partitions** → Spark can use 16,000 cores in parallel if you have them.

> **Rule of thumb:** parallelism in Spark = number of partitions.

### DAG and lazy evaluation
When you write:
```
df.filter(...).map(...).groupBy(...).count()
```
Spark **does nothing yet**. It just records the recipe. Only when you call an **action** (like `count()`, `show()`, `write()`) does it:
1. Build a DAG (directed acyclic graph) of all operations.
2. Optimize it (reorder filters, combine steps, push down predicates).
3. Split into stages at **shuffle boundaries**.
4. Send tasks to executors.

This is why Spark is fast: it sees the *whole* job before running, like a SQL query planner.

### Transformations vs Actions
- **Transformation** = lazy, returns a new DataFrame (`filter`, `map`, `join`). Nothing runs.
- **Action** = triggers execution (`count`, `collect`, `write`, `show`).

Java Streams has the same idea (`.filter()` is lazy, `.collect()` is terminal).

### Shuffle — the expensive thing
Some operations need data from *other* partitions: `groupBy`, `join`, `distinct`, `orderBy`. Spark must **shuffle** — send data across the network between executors. Shuffles are:
- Slow (network + disk).
- The #1 cause of slow Spark jobs.
- The boundary between **stages**.

> **Client red flag:** "our Spark job is slow" → 90% of the time the answer is "too many shuffles" or "skewed shuffle" (one partition has 10× the data).

### Fault tolerance
If an executor dies mid-job, Spark doesn't restart the whole thing. Because the DAG is a recipe, Spark just **recomputes the lost partitions** from the last checkpoint. Fault tolerance comes for free from the lazy DAG model.

## The mental model

> You write single-machine-looking code on the **driver**. Spark turns it into a DAG, the **cluster manager** hands over machines, and **executors** run parallel tasks on **partitions** of your data. Shuffles are expensive. Actions trigger execution. Failures recompute, not restart.

## Quiz (with answers)

**Q1. Difference between transformation and action?**
A: Transformation is lazy (returns a new DataFrame, nothing runs). Action triggers execution — like Java Stream's terminal operation. `filter` is a transformation, `count` is an action.

**Q2. Why are shuffles expensive, and which operations cause them?**
A: They need data from multiple nodes, sending it over the network. Caused by `groupBy`, `orderBy`, `join`, `distinct`.

**Q3. 100 GB of data, 128 MB partitions — how many tasks for a filter?**
A: ~800 tasks (100 GB / 128 MB = ~781–800). One task per partition.

**Q4. If an executor crashes, does Spark restart the whole job?**
A: No. Spark asks the cluster manager for a replacement executor and **recomputes only the lost partitions** from the DAG recipe — that's what "resilient" means.
