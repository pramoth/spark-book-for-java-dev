# Chapter 3 — RDD, DataFrame, Dataset

Spark gives you **three APIs** for the same underlying engine. They appeared in this order historically, and each one added something the previous lacked.

## 1. RDD — Resilient Distributed Dataset (2010, the original)

An **RDD** is a distributed collection of Java/Scala objects. Think: `List<T>` spread across many machines.

```java
JavaRDD<String> lines = sc.textFile("logs.txt");
JavaRDD<String> errors = lines.filter(line -> line.contains("ERROR"));
long count = errors.count();
```

**Characteristics:**
- You work with **raw objects** (`String`, `Person`, whatever).
- API is **functional** — `map`, `filter`, `reduce`, like Java Streams.
- Spark treats your lambda as a **black box** — it cannot see inside to optimize.
- **Type-safe** at compile time.

**Problem:** because Spark can't see inside your lambda, it can't optimize. If you write a dumb query, you get a dumb execution.

## 2. DataFrame (2015) — the game changer

A **DataFrame** is a distributed table with **named, typed columns** — like a SQL table or a pandas DataFrame.

```python
df = spark.read.parquet("logs.parquet")
errors = df.filter(df.level == "ERROR")
errors.groupBy("service").count().show()
```

**Characteristics:**
- You work with **rows and columns**, not objects.
- API is **declarative** — you describe *what* you want, Spark figures out *how*.
- Spark sees the full structure → can apply the **Catalyst optimizer**.
- **10–100× faster** than equivalent RDD code, typically.
- **NOT type-safe** on column names — typos crash at runtime.

## 3. Dataset (2016) — best of both worlds (Scala/Java only)

A **Dataset** is a DataFrame **plus compile-time types**. Define a Java class and Spark gives you an API that's both **optimized** and **type-safe**.

```java
public class LogEntry {
    public String service;
    public String level;
    public long timestamp;
}

Dataset<LogEntry> logs = spark.read()
    .parquet("logs.parquet")
    .as(Encoders.bean(LogEntry.class));

Dataset<LogEntry> errors = logs.filter(log -> log.level.equals("ERROR"));
```

**Characteristics:**
- **Type-safe** like RDDs.
- **Optimized** like DataFrames (when you use the column API).
- **Java/Scala only** — Python and R lack compile-time types.

**Nuance:** when you write a lambda (`.filter(log -> log.level...)`), you fall back to black-box mode and lose *some* optimization. Column API (`.filter(col("level").equalTo("ERROR"))`) is fully optimized — also gets **predicate pushdown** into Parquet, skipping row groups on disk.

## Summary table

| | RDD | DataFrame | Dataset |
|---|---|---|---|
| **Year** | 2010 | 2015 | 2016 |
| **Works with** | Raw objects | Rows + columns | Typed objects |
| **Catalyst optimized** | ❌ | ✅ | ✅ (partial with lambdas) |
| **Compile-time type safety** | ✅ | ❌ | ✅ |
| **Python** | ✅ (rare) | ✅ (main API) | ❌ |
| **Java/Scala** | ✅ | ✅ | ✅ |
| **Today's default** | Legacy | Python/SQL users | Java/Scala users |

## Storage difference

- **RDD** stores raw **JVM objects** — heavy, GC pressure.
- **DataFrame / Dataset** stores data in **Tungsten** — compact binary columnar format, off-heap, cache-friendly. Physically smaller in memory *and* optimizer-friendly.

All three are still **distributed across executors** — partitioned the same way. The three APIs are just different programming interfaces over the same distributed substrate.

## Which should you use?

- **Default today:** DataFrame (if Python) or Dataset (if Java/Scala).
- **RDD:** only when you need fine-grained control — rare.
- **SQL:** write SQL strings against registered tables — same engine, same optimizer.

All three — DataFrame, Dataset, SQL — compile down to the **same Catalyst logical plan**. Pick based on ergonomics, not performance.

## Mental model

> **RDD = distributed `List<T>`** (flexible, slow, compile-safe)
> **DataFrame = distributed SQL table** (optimized, loose types)
> **Dataset = DataFrame with a Java class attached** (optimized + compile-safe, JVM only)

## Quiz (with answers)

**Q1. Why can Spark optimize DataFrame queries but not RDD operations?**
A: DataFrame operations have a structured, declarative form Spark understands. RDD lambdas are black boxes Spark can't see inside.

**Q2. Building a production Java ETL pipeline — which API?**
A: Dataset, because it combines Catalyst optimization with compile-time type safety in Java.

**Q3. True/false: a Python developer can use Datasets.**
A: False. Datasets require compile-time types, which Python doesn't have.

**Q4. Tradeoff between lambda filter and column-API filter on a Dataset?**
A: Lambdas are black boxes — Spark can't optimize them and must deserialize every row into a JVM object. The column API gets full Catalyst optimization *and* predicate pushdown (e.g., skipping Parquet row groups on disk).
