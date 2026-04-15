# Appendix 13 — Mapping Code to Stages

The missing piece that makes Spark UI reading click: **which line of your code produced which stage?**

## The wordcount script, annotated

```python
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col

# ──────────────────────────────────────────────────────────
# LAZY SECTION — builds a plan. No jobs/stages/tasks yet.
# ──────────────────────────────────────────────────────────

spark = (SparkSession.builder.appName("wordcount-demo").getOrCreate())
# ↑ creates SparkContext. No data work.

df = spark.createDataFrame(data, ["line"])
# ↑ logical plan: "Python list → rows". Still lazy.

words = df.select(explode(split(lower(col("line")), " ")).alias("word"))
# ↑ still lazy. Adds Project → Generate(explode).

counts = words.groupBy("word").count().orderBy(col("count").desc())
# ↑ still lazy. Adds Aggregate → Sort.
#   Spark now has a full logical plan but has NOT executed anything.

# ──────────────────────────────────────────────────────────
# ACTION #1 — show()
# ──────────────────────────────────────────────────────────
counts.show()
# Triggers Job 0 and Job 1 (show() internally calls take(20)).
#
#   Job 0 "showString"
#     └─ Stage 0 (2 tasks, COMPLETE)
#           scan Python data → lower/split/explode
#           → partial count per partition
#           → write shuffle files for groupBy
#           (ends at Exchange for groupBy)
#
#   Job 1 "showString"
#     ├─ Stage 1 (SKIPPED)    reuses Stage 0's shuffle files
#     └─ Stage 2 (1 task)     read shuffle → final count
#                             → sort by count desc → take 20

# ──────────────────────────────────────────────────────────
# ACTION #2 — count()
# ──────────────────────────────────────────────────────────
print("Total unique words:", counts.count())
# Triggers Job 2, Job 3, Job 4.
# AQE splits execution at every shuffle. Two shuffles in the
# pipeline → 3 jobs with pauses in between for re-planning.
#
#   Job 2 "count"
#     └─ Stage 3 (2 tasks)    re-scan data, partial count,
#                             shuffle write (groupBy)
#
#   Job 3 "count"
#     ├─ Stage 4 (SKIPPED)    reuses Stage 3's shuffle
#     └─ Stage 5 (1 task)     read → final count → shuffle
#                             write (orderBy)
#
#   Job 4 "count"
#     ├─ Stage 6 (SKIPPED)    reused
#     ├─ Stage 7 (SKIPPED)    reused
#     └─ Stage 8 (1 task)     read sorted data → count rows → 10
```

## Why `count()` takes 3 jobs

You expect `count()` to be "one action = one job". It isn't, because the pipeline has **two shuffles** (`groupBy` and `orderBy`), and **Adaptive Query Execution (AQE)** splits execution at each one to inspect runtime statistics:

```
Job 2: run up to the first shuffle (groupBy)   →  pause,
                                                  inspect stats,
                                                  decide next plan
Job 3: run up to the second shuffle (orderBy)  →  pause, inspect
Job 4: run the final stage                     →  return count
```

Between jobs, AQE looks at things like *"how much data did the shuffle actually produce?"* and decides how many partitions the next stage should have. That's why Stages 5 and 8 have only 1 task — AQE saw the shuffle output was tiny and coalesced 200 default partitions into 1.

Without AQE, you'd see **1 job with 3 stages**. With AQE, you see **3 jobs**, each re-planned based on real runtime data. Usually worth it.

## Why `show()` only takes 2 jobs

`show()` calls `take(20)`, which short-circuits as soon as 20 rows are available. It doesn't need to touch every row, so it only needs enough work to fill 20 rows — 2 jobs vs `count()`'s 3.

## Stage-to-SQL mapping

| Stage | SQL equivalent |
|---|---|
| **0, 3, 6** (same logic, different runs) | `SELECT explode(split(lower(line), ' ')) AS word FROM data` + partial `COUNT(*) GROUP BY word` |
| **1, 4, 7** | Merge partial counts across the network (`GROUP BY` finishing) |
| **2, 5** | Sort by count desc (`ORDER BY`) |
| **8** | `SELECT COUNT(*) FROM ...` |

Stages 0, 3, and 6 do the **same logical work** — which is why the second and third occurrences are skipped. Spark reuses the shuffle files from the first run of that stage within the same physical plan.

## Visual timeline

```
Time →

Action:       show()                       count()
               │                             │
               ▼                             ▼
Jobs:      [Job 0][Job 1]               [Job 2][Job 3][Job 4]

Run:         S0      S2                   S3     S5     S8
Skipped:             (S1)                        (S4)   (S6, S7)
                     reused                      reused  reused
                     S0                          S3      S3+S5
```

"Skipped" means: *"the shuffle files already exist on disk from an earlier job in this action's plan — don't run anything, just read them"*.

## The rule

> **One line of Spark code ≠ one stage.** A single action creates one or more **jobs**. Each job's plan gets cut at shuffles into **stages**. Each stage has **one task per partition**. When multiple jobs in the same action share upstream work, later stages are **skipped**.

## Mental model

> Your script's execution is **wider than your script's control flow**. What looks like "two Python lines" in your code turns into 5 jobs, 9 stages, and dozens of internal operations — because Spark optimizes across the whole lazy plan, cuts it at shuffles, and reuses work between jobs within the same physical plan.
