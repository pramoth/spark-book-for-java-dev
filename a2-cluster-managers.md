# Appendix 2 — Cluster Managers: Standalone, YARN, Kubernetes

## The universal pattern

Every cluster manager has **two logical responsibilities**:

1. **Resource manager** — knows which machines exist and who's using them.
2. **Per-machine agent** — actually launches processes on each machine.

Three modes, same pattern, different vocabulary:

| Mode | Resource Manager | Per-Node Agent | Who launches executors? |
|---|---|---|---|
| **Standalone** | Spark Master | Spark Worker | Worker forks executor JVM |
| **YARN** | ResourceManager (RM) | NodeManager (NM) | NM launches executor in a "YARN container" |
| **Kubernetes** | K8s API Server | kubelet (on each node) | kubelet starts executor pod |

## Kubernetes mode

In K8s mode there is **no "Spark Master"**. The roles are:

- **Driver** (your app) — talks to K8s.
- **K8s API server** — acts as the cluster manager.
- **Executor pods** — spawned by K8s on demand.

```
   ┌──────────────┐
   │    DRIVER    │  (runs in its own pod)
   │   your app   │
   └──────┬───────┘
          │ 1. "I need 5 executors, 4 cores, 8GB RAM each"
          ▼
   ┌──────────────┐
   │  K8s API     │
   │   Server     │
   └──────┬───────┘
          │ 2. schedules pods on available nodes
          ▼
   ┌────────┐ ┌────────┐ ┌────────┐
   │Executor│ │Executor│ │Executor│  (each is a K8s pod)
   │  pod   │ │  pod   │ │  pod   │
   └────┬───┘ └────┬───┘ └────┬───┘
        │          │          │
        └──────────┼──────────┘
                   │ 3. executors connect BACK to the driver
                   ▼
             (driver's pod IP:port)
```

### Step by step

1. `spark-submit --master k8s://...` creates a **driver pod** in K8s.
2. Driver starts up, builds the DAG, needs executors.
3. Driver calls the K8s API: *"create N pods with this image, these resources, this env"*.
4. K8s schedules pods on worker nodes.
5. Each executor pod starts its JVM, reads the driver address, and **opens a TCP connection back to the driver**.

### How the driver monitors executors

Not through K8s — through Spark's own protocol:

**Heartbeats:** every executor sends a heartbeat to the driver every ~10s including "I'm alive", task metrics, memory usage.

**If heartbeats stop (~120s timeout):**
1. Driver asks K8s to kill that pod.
2. Driver asks K8s for a replacement.
3. Driver recomputes lost partitions on other executors.
4. New pod comes up, registers, joins the pool.

### Two layers of monitoring

| Layer | Watches | Purpose |
|---|---|---|
| **K8s layer** | Pods (liveness/readiness, restarts) | Infrastructure health |
| **Spark layer** | Executors via heartbeats | Task-level progress |

K8s cares: "is the container process running?"
Spark cares: "is the executor responsive and making progress?"

## YARN mode

**YARN** (Yet Another Resource Negotiator) comes from Hadoop. Dominant 2014–2020, still huge in on-prem enterprises.

```
          ┌──────────────────┐
          │  ResourceManager │   ← cluster-wide brain
          │      (RM)        │
          └────────┬─────────┘
                   │
      ┌────────────┼────────────┐
      ▼            ▼            ▼
 ┌─────────┐  ┌─────────┐  ┌─────────┐
 │NodeMgr  │  │NodeMgr  │  │NodeMgr  │  ← one per machine
 │  (NM)   │  │  (NM)   │  │  (NM)   │
 └─────────┘  └─────────┘  └─────────┘
```

### Important: "YARN container" ≠ Docker container

YARN uses the word **container** to mean "a reserved slice of CPU + RAM isolated by Linux cgroups". Predates Docker. Terminology collision only.

### The flow

1. Client sends app to **ResourceManager**.
2. RM picks a node, asks its NodeManager to launch the **ApplicationMaster (AM)**.
3. AM (Spark's driver-side negotiator) asks RM for more containers.
4. RM grants containers; NMs launch them.
5. Executor JVMs start, connect back to the driver.

### Two sub-modes (also apply to K8s)

- **`--deploy-mode client`** — driver runs on your laptop / edge node. Good for interactive (spark-shell, notebooks). Laptop dies → job dies.
- **`--deploy-mode cluster`** — driver runs inside the cluster. Good for production batch. Laptop can disconnect.

## The key insight

> **Driver, Executors, and the heartbeat protocol are always the same.** Only the "who gives me machines?" layer changes. Understanding the architecture from Chapter 2 means switching between Standalone / YARN / K8s is just learning new vocabulary.

## Client heuristics

- Client says **"Hadoop / Cloudera / HDFS"** → **YARN**.
- Client says **"EKS / GKE / AKS / cloud-native"** → **Kubernetes**.
- Client says **"Databricks / EMR / Dataproc"** → managed, they abstract the cluster manager.
- Small setup, no Hadoop, no K8s → **Standalone**.
