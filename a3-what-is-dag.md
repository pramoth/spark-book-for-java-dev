# Appendix 3 — What is a DAG?

**DAG = Directed Acyclic Graph.**

## Breaking down the name

- **Graph** — nodes connected by edges (like a flowchart).
- **Directed** — edges have arrows; they go one way only.
- **Acyclic** — no loops; you can't follow arrows and end up back where you started.

A DAG is **a flowchart with arrows that never loops back**.

## What it looks like in Spark

```
df.filter(...).map(...).join(other).groupBy(...).count()
```

Spark builds:
```
  [read file]           [read other]
       │                      │
       ▼                      │
   [filter]                   │
       │                      │
       ▼                      │
    [map]                     │
       │                      │
       └────────┬─────────────┘
                ▼
             [join]           ← shuffle here
                │
                ▼
            [groupBy]         ← shuffle here
                │
                ▼
             [count]          ← action, triggers execution
```

Each node = an operation. Each arrow = data flowing. No arrow points backwards → **acyclic**.

## Why Spark uses a DAG (not just a list of steps)

1. **Optimization** — seeing the whole graph lets Spark reorder, combine, and skip steps. Example: `filter` after `join` → push the filter *before* the join (**predicate pushdown**).
2. **Parallelism** — independent branches run simultaneously on different executors.
3. **Fault tolerance** — the DAG *is* the recipe. Lost partition → walk back up the arrows and recompute only what's needed.
4. **Stage boundaries** — Spark cuts the DAG at shuffle points to form **stages**. Everything inside a stage pipelines in memory.

## Mental model

> The DAG is Spark's **query plan** — like a SQL execution plan, but for any code you write. Lazy evaluation exists *so that* Spark can see the full DAG before running anything.

## Where you've seen this before

- **Maven/Gradle build graphs** — module A depends on B and C.
- **Git commit history** — commits point to parents, never loop.
- **Java Stream pipelines** — conceptually a tiny linear DAG.
