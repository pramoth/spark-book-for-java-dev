# Chapter 1 — Why Spark Exists

## The core problem: data outgrew single machines

Imagine you have a **2 TB** log file and need to count errors per service. Your laptop has 16 GB RAM and 1 TB disk. You literally **cannot load it**, and even streaming it through one CPU would take hours or days.

Two ways out:
- **Scale up** (bigger machine) — expensive, hits a physical ceiling.
- **Scale out** (many machines working together) — cheap, nearly unlimited, but *hard to program correctly*.

Spark is a framework that makes "scale out" feel almost as easy as writing a loop on one machine.

## What came before Spark

**Hadoop MapReduce (2006)** was the first mainstream answer. It split data across many machines (HDFS) and ran computations in two rigid phases: **Map** (transform each record) then **Reduce** (aggregate). It worked, but had two big pains:

1. **Every step wrote to disk.** A 5-step job = 5 round-trips to disk. Slow.
2. **Rigid API.** Complex jobs (ML, iterative algorithms, interactive queries) were painful to express in Map+Reduce.

## What Spark changed (2010, Berkeley)

Spark kept the "distribute work across a cluster" idea but added:

1. **In-memory computation** — intermediate results stay in RAM across steps. 10–100× faster than MapReduce for multi-step jobs.
2. **Rich API** — `map`, `filter`, `join`, `groupBy`, `window`, SQL… not just Map+Reduce. Feels like Java Streams but distributed.
3. **Unified engine** — same engine handles batch, SQL, streaming, ML, graph. Before Spark you needed a different tool for each.
4. **Lazy evaluation + DAG optimizer** — Spark builds a plan of your whole job, optimizes it, *then* runs it.

## The mental model to carry forward

> Spark = a distributed version of Java Streams, with a query optimizer, that can survive machine failures.

That one sentence covers 80% of client conversations.

## Quiz (with answers)

**Q1. Why can't you just buy a bigger server instead of using Spark?**
A: It hits a physical ceiling — you can only scale up so far before no bigger machine exists (or the price becomes absurd).

**Q2. What was the main performance problem with Hadoop MapReduce that Spark fixed?**
A: MapReduce wrote intermediate results to disk between every step. Spark keeps them in memory.

**Q3. In one sentence: what does Spark give you that plain Java Streams on one machine doesn't?**
A: A distributed version of Java Streams — the same style of API, but running across many machines.
