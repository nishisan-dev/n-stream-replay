# n-stream-replay vs. Apache Kafka MirrorMaker 2

**Looking for a Kafka MirrorMaker alternative?** n-stream-replay is a **durable, topic-aware Kafka store-and-forward relay** with an independent disk-backed queue per destination. Apache Kafka MirrorMaker 2 (MM2) is a **Kafka Connect-based cross-cluster replication platform**. Their use cases overlap, but they are not interchangeable.

## The short answer

- **Choose n-stream-replay** for explicit per-topic routing, disk-backed buffering per sink, selective key extraction, and forwarding events to destinations that may become disconnected.
- **Choose MirrorMaker 2** for broader Kafka cluster migration/replication workflows involving topic configurations, ACLs, and consumer-group offset checkpoints.

## Feature comparison

| Capability | n-stream-replay (v2.4.0) | Apache Kafka MirrorMaker 2 |
| --- | --- | --- |
| Kafka-to-Kafka record forwarding | Yes | Yes |
| One source topic to several destinations | Yes, multiple `to` targets | Yes, using replication flows |
| Many-to-one topic distribution | Yes, routes can share `toTopic` | Aggregation across flows is supported, with replication policies |
| Topic renaming | Explicit `toTopic` mapping per route | Configurable replication policy |
| Independent local disk queue per sink | Yes, via NQueue | Not the primary MM2 architecture; delivery uses Kafka Connect tasks and Kafka |
| Destination downtime handling | Sink-specific persistent backlog and retry, subject to queue bounds | Connector/task recovery and Kafka-based replication |
| JSON/YAML field-to-partition-key extraction | Yes, per target | Not a built-in MM2 routing feature; custom transformations would be required |
| Source ingestion rate cap | Yes, per-source `maxConsumeRatePerSec` | Different consumer/connector tuning controls |
| Consumer-group offset checkpoints | No | Yes |
| Topic configuration/ACL replication | No | Yes, subject to connector configuration and permissions |
| Topic/partition discovery and metadata replication | No automated MM2-equivalent metadata sync | Yes, through MM2 connectors |
| Kafka Connect cluster required | No | Built on Kafka Connect |
| Message delivery guarantee | At-least-once; duplicates possible | Depends on MM2/Kafka Connect deployment and failure conditions |
| Partition-preserving mirroring | Yes, per target (`partitioning: preserve`; destination topic must pre-exist with enough partitions) | Not evaluated here |
| Unbounded buffering / guaranteed zero loss | Not by default (`drop-oldest`); `overflowPolicy: block` with `onPoison: halt` and `onWriteError: halt` stops queue, poison and write-error drops by pausing the source or halting the sink, but disk loss and repeated header keys are outside the guarantee | No universal zero-loss guarantee |
| Published throughput benchmark vs. MM2 | None asserted | Not evaluated here |

## Why disk-backed queues per destination matter

Imagine an order-event stream with two destinations: a disaster-recovery Kafka cluster and an analytics cluster. The DR link fails for several hours.

In n-stream-replay, each destination has its own durable local queue. The analytics path can continue delivering, while the DR path retains a backlog and retries when the link returns.

```text
Kafka source
    |
    +-- orders.events --> [NQueue: DR] ------> Kafka DR (temporarily offline)
    |
    +-- orders.events --> [NQueue: analytics] -> Kafka analytics (online)
```

This **decoupling is operationally useful** when destinations have different availability and latency characteristics. However, it is bounded by disk capacity, `maxDepth`, `retentionTime`, and failure policies. Records may be deliberately dropped if configured limits are exceeded.

## The important differences

### 1. Store-and-forward is not full Kafka cluster replication

MM2 offers metadata-oriented replication capabilities such as topic configuration and ACL synchronization, heartbeat topics, and consumer offset checkpoints. n-stream-replay forwards selected records according to explicit routes; it does not transfer consumer group offsets to target clusters.

If an application must fail over to a destination cluster *at a translated consumer-group position*, MM2 has facilities specifically intended for that workflow. n-stream-replay currently does not.

### 2. Routing is explicit, not a discovery-based cluster mirror

An n-stream-replay source consumes the set of `fromTopic` names referenced by its routes. Its `toTopic` mapping can preserve or rename destination topics; separate routes can implement merge, fan-out, and one-to-one forwarding.

MM2 also supports multiple replication flows and customizable topic naming. It should **not** be described as lacking fan-out, topic renaming, or multi-cluster aggregation.

### 3. Durable delivery does not equal exactly-once

n-stream-replay commits source offsets only after the destination queues affected by the batch are synchronized under its default `sync-on-commit` durability mode. On crashes and retries, the same event can still be delivered more than once.

`maxDepth`, `retentionTime`, invalid partition-key extraction, oversized messages, and local write-error handling can introduce intentional loss. Design downstream consumers to be idempotent, monitor drop counters, and size buffers against worst-case outages.

### 4. Deployments have different operational shapes

n-stream-replay is a standalone Java application using Kafka clients, Spring Boot Actuator, and NQueue. It does not need a Kafka Connect deployment.

MM2 operates through the Kafka Connect ecosystem, with connectors and tasks designed for replicating records and associated cluster state.

## Choosing an approach

**n-stream-replay fits when:**

- You distribute selected Kafka topics to independent business systems or datacenters.
- One target's outage must not immediately stall deliveries to other targets.
- You need local store-and-forward persistence with configurable retention and durability policies.
- You want a small standalone Java deployment with per-source rate limiting.
- You need payload-preserving routing while deriving a Kafka key from a JSON/YAML field for specific targets.

**MirrorMaker 2 fits when:**

- You need Kafka cluster replication/migration with consumer offset checkpoints.
- You need topic configurations or ACLs mirrored alongside records.
- You prefer Kafka Connect management and monitoring.
- You need MM2's automatic discovery mechanisms and replication policies.

Neither solution is "universally better." Choose according to the failure modes and metadata requirements of your application, then benchmark your own message sizes, throughput, partitions, disks, and outage scenarios.

## Learn more

- [n-stream-replay README](../README.md)
- [Architecture and guarantees](architecture.md)
- [End-to-end cross-datacenter example](usage-example.md)
- [Default routing YAML](../config/n-stream-replay.yaml)
- [Apache Kafka: Geo-replication and MirrorMaker 2](https://kafka.apache.org/28/operations/geo-replication-cross-cluster-data-mirroring/)
- [Apache Kafka: MirrorMaker configurations](https://kafka.apache.org/41/configuration/mirrormaker-configs/)
- [KIP-382: MirrorMaker 2 architecture](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/95650722/KIP-382%2BMirrorMaker%2B2.0)
