# Appendix 1 — Does Spark Spawn Docker Containers?

**Short answer: no.** Spark does not spawn Docker containers by itself.

## What Spark actually spawns

Spark spawns **JVM processes** (executors), not containers. In default/standalone mode:

1. Start a **Spark Master** process (a JVM).
2. Start **Spark Worker** processes (JVMs) on each machine.
3. When you submit a job, the Master tells Workers: *"launch an executor JVM for this application"*.
4. Workers `fork` a new JVM process — plain OS process, no container.

Spark's unit of isolation is a **JVM process**, not a container.

## Where Docker/containers come in

Containers are an **external choice** about how you package and deploy Spark processes.

| Mode | Who runs the JVMs? |
|---|---|
| **Bare metal / VM** | Spark installed on Linux, JVMs run directly on the host. No Docker. |
| **Docker Compose** | *You* write a compose file that runs Master + Workers each in their own container. Spark doesn't know — from its view it's just Linux hosts. |
| **Kubernetes mode** | Spark *does* talk to the K8s API and asks it to spawn pods for each executor. This is the only mode where Spark itself drives container creation — and it's K8s doing the actual spawning. |

## The key insight

> Spark's architecture (Driver, Executors, Cluster Manager) is **independent** of containerization. Containers are just a packaging/deployment detail layered *underneath*.
