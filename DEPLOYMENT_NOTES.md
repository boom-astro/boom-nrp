# Deployment Notes & Troubleshooting (NRP Nautilus)

Deploying the BOOM stack on NRP Nautilus presents specific challenges related to network storage latency. This document outlines the critical "gotchas" and our verified solutions.

## 1. Kafka Metadata Latency (The 5s Timeout Bug)

### The Issue
The default storage on NRP is CephFS (`rook-cephfs-east`). While reliable, CephFS metadata operations (creating directories and files) can have high latency. 
- The BOOM Kafka setup uses **15 partitions** per topic.
- Creating 15 partitions requires the broker to create 15 directories and ~45 index files simultaneously.
- On CephFS, this process takes **8-12 seconds**.
- The BOOM Rust producer has a hardcoded **5-second message timeout** (`message.timeout.ms: "5000"`).

**Result:** The producer triggers auto-creation of a topic and immediately tries to send data. Because CephFS hasn't finished creating the partitions within 5 seconds, the producer errors out and the ingestion fails.

### The Solution: Fast Ephemeral Storage
We moved the Kafka data volume in `k8s/kafka.yaml` from a PersistentVolumeClaim (CephFS) to an **`emptyDir`** (local node NVMe).
- **Benefit:** Partition creation now takes **<100ms**, well within the 5s producer timeout.
- **Trade-off:** Kafka data is ephemeral. If the broker pod restarts, you must re-ingest the alerts. This is acceptable for validation/testing phases.

## 2. Ingestion Orchestration (Race Condition)

### The Issue
Even with fast storage, there is a race condition where the producer might start sending before the broker has fully finalized the partition leader election.

### The Solution: InitContainer Guard
The ingestion jobs (`k8s/ztf-ingest-job.yaml`) now use an `initContainer` to handle topic setup:
1. It explicitly deletes any stale topic.
2. It creates the new topic with the correct partition count.
3. It runs a `describe` loop that **waits until all 15 partitions are ready** (showing an active leader).
4. Only after this verification does the main Rust producer container start.

## 3. MongoDB Resources
Ensure MongoDB has sufficient memory limits. On NRP, pods may be evicted if they exceed their memory quota during heavy ingestion. We recommend a limit of at least **4Gi** for the `mongo-0` pod during full-night loads.
