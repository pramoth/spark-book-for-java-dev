# Appendix 14 — Why Stages Get Skipped (and When They Don't)

## The question

You run:
```python
counts.show()     # Stage 0 scans, explodes, shuffles
counts.count()    # Stage 3 scans, explodes, shuffles — AGAIN
```

Both stages do the same logical work on the same data. Why isn't Stage 3 skipped the way Stages 1, 4, 6, 7 are?

## The answer

> **Spark's shuffle reuse only matches on exact physical plan node identity, not on logical semantics.** Stage 0 and Stage 3 compute the same thing to your eye, but Spark's planner built them as different physical nodes (in different runs of Catalyst), so the lookup for "do I already have shuffle files for this node?" fails.

## How shuffle reuse actually works

Shuffle files aren't indexed by *"what they represent"*. They're indexed by **the identity of the physical plan node that produced them**, plus the optimizer's node IDs from that plan compilation.

When a later job compiles its physical plan, Spark asks at each `Exchange`:
> *"Have I already executed this **exact same plan node** and still have its shuffle files on disk?"*

- If yes → mark the stage `SKIPPED`, read the old files.
- If no → run from scratch.

## Why two actions produce two plans

When you call `counts.show()`:
1. Spark takes the logical plan of `counts`.
2. Modifies it (adds `LIMIT 21` for `take(20)`).
3. Runs Catalyst → optimized plan → physical plan with fresh node IDs.
4. Executes — Stage 0's shuffle files are tagged with **these** node IDs.

When you then call `counts.count()`:
1. Spark takes the logical plan of `counts` again.
2. Modifies it differently (for `count()`, not `show()`).
3. Runs Catalyst **from scratch** → produces a **new physical plan with new node IDs**.
4. Looks up "do I have shuffle files for these new node IDs?" → **No** (the existing ones are tagged with the show()-path IDs).
5. Runs Stage 3 from scratch.

Same logical work, different plan identity, no reuse.

## Why stages 4, 6, 7 *are* skipped

Because they're requested **within the same action**. Once `count()` started, Spark built one physical plan containing multiple shuffle points. Jobs 3 and 4 (of the same `count()` action) reused shuffle files from Job 2 (of the same action), because the node IDs were stable across those jobs — they're part of the same physical plan.

So:
- **Within a single action** → shuffle files reuse automatically (free).
- **Across actions** → no reuse unless you `cache()`.

## How to reuse across actions — `cache()`

`cache()` uses a different mechanism. It's indexed by the **logical plan identity** of the cached DataFrame, which is stable across actions.

```python
words = df.select(explode(split(lower(col("line")), " ")).alias("word"))
counts = words.groupBy("word").count().orderBy(col("count").desc())

counts.cache()   # persist the materialized result

counts.show()    # populates the cache as a side effect
counts.count()   # reads from memory — zero shuffles, zero scans
```

After `cache()`:
- The first action runs the full pipeline and writes results to memory.
- Every subsequent action that references the cached DataFrame **skips the entire upstream computation** and reads from memory directly.
- The "Storage" tab in the Spark UI shows the cached data: size, partition count, hit rate.

## `cache()` vs `persist()`

- **`cache()`** — alias for `persist(MEMORY_AND_DISK)`. Uses memory first, spills to disk if memory is full.
- **`persist(storageLevel)`** — explicit control:
  - `MEMORY_ONLY` — fastest, but evicts on memory pressure.
  - `MEMORY_AND_DISK` — spill to disk as fallback.
  - `DISK_ONLY` — for really huge intermediate results.
  - `MEMORY_AND_DISK_SER` — serialize before storing (smaller, slower access).

## Side-by-side

| Mechanism | Indexed by | Cross-action? | How you get it |
|---|---|---|---|
| **Shuffle file reuse** | Physical plan node ID | ❌ (within one action) | Automatic |
| **`cache()` / `persist()`** | Logical plan | ✅ | Explicit call |

Both live alongside each other. Shuffle reuse is a free optimization within one action. `cache()` is the explicit way to share work across actions.

## The classic mistake

```python
df = heavy_pipeline(raw)
rows = df.count()         # runs whole pipeline
df.write.parquet("out/")  # runs whole pipeline AGAIN
```

**Fix:**
```python
df = heavy_pipeline(raw)
df.cache()
rows = df.count()          # runs once, materializes cache
df.write.parquet("out/")   # reads from cache
```

One `cache()` line can cut production Spark job runtime in half. This is one of the most common tuning wins in real Spark code.

## When NOT to cache

- **Single-action pipelines** — one `write()` or one `count()` with no reuse. Caching just wastes memory.
- **Cached data is larger than cluster memory** — you'll spill to disk, which might be slower than recomputing.
- **Cached intermediate is cheap to recompute** — simple filters on small data.

`cache()` is a tool for deliberate reuse, not a default.

## Mental model

> **Spark skips stages only within a single physical plan.** Across actions, it re-runs everything from the beginning unless you `cache()` or `persist()`. The "skipped" stages you see in the UI are within-action reuse. For cross-action reuse, tell Spark explicitly.
