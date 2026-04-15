import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col

# ──────────────────────────────────────────────────────────
# LAZY SECTION — builds a plan, no jobs/stages/tasks yet
# ──────────────────────────────────────────────────────────

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

# ──────────────────────────────────────────────────────────
# ACTION #1 — show()
#
# Triggers Job 0 and Job 1 (two jobs because show() calls
# take(20), which first probes 1 partition then reads more).
#
#   Job 0 "showString"
#     └─ Stage 0 (2 tasks)     scan → lower/split/explode
#                              → partial count → shuffle write
#
#   Job 1 "showString"
#     ├─ Stage 1 (SKIPPED)     reuses Stage 0's shuffle files
#     └─ Stage 2 (1 task)      read shuffle → final count
#                              → orderBy → take 20
# ──────────────────────────────────────────────────────────
counts.show()

# ──────────────────────────────────────────────────────────
# ACTION #2 — count()
#
# Triggers Job 2, Job 3, Job 4. Spark does NOT reuse Stage 0's
# shuffle files here because this action builds a NEW physical
# plan whose node IDs don't match. Cross-action reuse needs
# explicit cache(). (See Appendix 14.)
#
# AQE splits execution at every shuffle to inspect runtime
# stats, which is why count() produces 3 jobs.
#
#   Job 2 "count"
#     └─ Stage 3 (2 tasks)     re-scan → explode → partial
#                              count → shuffle write (groupBy)
#
#   Job 3 "count"
#     ├─ Stage 4 (SKIPPED)     reuses Stage 3's shuffle
#     └─ Stage 5 (1 task)      read shuffle → final count
#                              → shuffle write (orderBy)
#
#   Job 4 "count"
#     ├─ Stage 6 (SKIPPED)     reused
#     ├─ Stage 7 (SKIPPED)     reused
#     └─ Stage 8 (1 task)      read sorted → count rows → 10
# ──────────────────────────────────────────────────────────
print("Total unique words:", counts.count())

print("Sleeping 60 seconds — open http://localhost:4040 to see the driver UI")
time.sleep(60)

input("Press Enter to stop Spark and exit...")
spark.stop()
