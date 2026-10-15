# n-stream-replay — Durable Apache Kafka Store-and-Forward Relay

[![Latest release](https://img.shields.io/github/v/release/nishisan-dev/n-stream-replay)](https://github.com/nishisan-dev/n-stream-replay/releases)
[![License: GPL-3.0](https://img.shields.io/github/license/nishisan-dev/n-stream-replay)](LICENSE)
![Java 25](https://img.shields.io/badge/Java-25-orange)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9.1-black)

**A lightweight Kafka MirrorMaker 2 alternative for durable event forwarding, cross-cluster topic routing, and disconnected destinations.**

n-stream-replay is a **Java-based Kafka relay with disk-backed store-and-forward queues**. It consumes records from configured source topics, routes them to one or more Kafka destinations, and persists messages in an **independent durable queue per sink**. If a destination Kafka cluster goes offline, its backlog can be retried after recovery without blocking other destinations.

Choose it for **Kafka topic mirroring, fan-out, many-to-one routing, topic renaming, resilient cross-datacenter event distribution, and intermittent network connectivity**. It is **not a drop-in replacement** for every Apache Kafka MirrorMaker 2 feature: it does not synchronize consumer-group checkpoints, ACLs, or topic configurations.

**[Português (Brasil)](README.pt-BR.md)** · **[Architecture](docs/architecture.md)** · **[Complete configuration example](docs/usage-example.md)** · **[MirrorMaker 2 comparison](docs/mirrormaker-vs-n-stream-replay.md)** · **[Releases](https://github.com/nishisan-dev/n-stream-replay/releases)**

## Why n-stream-replay?

When a Kafka destination is unavailable, directly forwarding from a consumer to a producer can couple the source's progress to that destination's availability. n-stream-replay separates these responsibilities: **consume and persist locally, then forward independently per sink**.

![n-stream-replay Kafka cross-cluster store-and-forward topology](docs/diagrams/topology.png)

### Features

- **Simple declarative YAML:** define `sources`, `sinks`, and `routes` directly, without creating Kafka Connect connector resources or writing custom routing plugins for this use case.
- **Disk-backed store-and-forward:** one NQueue-based durable backlog per destination, surviving process restarts.
- **Destination isolation:** an offline sink does not hold up the other sinks; pending records are retried with backoff when connectivity returns.
- **Flexible topic routing:** mirror the original topic name, rename it, fan out a topic to multiple destinations, or merge several source topics into a destination topic.
- **Payload preservation:** forward the original Kafka message payload without reserializing it; retain keys, headers, and timestamps unless a target explicitly overrides its key.
- **Optional JSON/YAML partition keys:** extract a scalar from a configured payload field for a specific target; route-level decoder overrides a source-level default.
- **Source-side rate limiting:** cap consumption using `maxConsumeRatePerSec`, or set it to `0` for no limit.
- **Configurable durability and retention:** choose `sync-on-commit`, `per-record-fsync`, or `os-managed`; configure queue bounds and local write-error behavior.
- **Prometheus observability:** source, sink, queue-depth, retry, throughput, and intentional-drop counters via Spring Boot Actuator.
- **Direct Kafka clients:** built with `kafka-clients` rather than a Kafka Connect cluster or Spring Kafka.

## How it works

```text
                         ┌──> [Durable NQueue: DR] ──> Kafka cluster DR
Kafka source ──> Router ─┤
                         └──> [Durable NQueue: analytics] ──> Kafka analytics
                                |
                                └── independent retry/backoff per destination
```

1. A source consumer polls Kafka topics selected by the route definitions.
2. Routing resolves the destination sinks, destination topic names, and any optional partition key.
3. Each destination receives a copy in its own persistent queue.
4. Under the default `sync-on-commit` policy, affected queues are synchronized to disk **before** the corresponding source offsets are committed.
5. A separate forwarder per sink delivers queued records. Unavailable sinks retain their backlog subject to configured queue limits and retention.

The delivery contract is **at-least-once**, not exactly-once. Retries or crashes can produce duplicates, so downstream consumers should be idempotent or deduplicate when required.

## Quick start

**Requirements:** JDK 25, Maven, a reachable Kafka broker, and resolvable `dev.nishisan` dependencies. The project uses `nishi-utils-core:8.1.0` and `nishi-utils-spring:2.1.1`. If the Spring companion artifact is not installed in your Maven environment, install it from its source checkout first:

```bash
mvn -f /path/to/nishi-utils-spring/pom.xml clean install
```

Clone, build, and start:

```bash
git clone https://github.com/nishisan-dev/n-stream-replay.git
cd n-stream-replay
mvn clean package
./scripts/start.sh
```

The included [configuration](config/n-stream-replay.yaml) targets `localhost:9092` for both source and sink; adapt the broker addresses, source topics, and destination topics before using it outside a local development setup. Destination topics and Kafka permissions must be prepared as required by your broker configuration.

Health and Prometheus endpoints (when the default web port is available):

```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8080/actuator/prometheus
```

Run the unit and Kafka/Testcontainers integration test suite with Docker available:

```bash
mvn verify
```

## Example: Kafka replication with fan-out and topic remapping

The YAML below routes `orders.events` to two different Kafka clusters. One keeps the topic name; the other publishes to `orders.analytics` and derives the partition key from a JSON field.

```yaml
nstreamreplay:
  sources:
    - id: orders-source
      bootstrapServers: "kafka-primary:9092"
      groupId: "n-stream-replay.orders"
      autoOffsetReset: earliest

  sinks:
    - id: disaster-recovery
      bootstrapServers: "kafka-dr:9092"
      queue:
        basePath: "var/n-stream-replay/queues"
        maxDepth: 1000000
        retentionTime: 0s
        durability: sync-on-commit
        onWriteError: halt

    - id: analytics
      bootstrapServers: "kafka-analytics:9092"
      queue:
        basePath: "var/n-stream-replay/queues"
        maxDepth: 200000
        retentionTime: 6h
        durability: sync-on-commit
        onWriteError: drop

  routes:
    - id: orders-to-both
      source: orders-source
      fromTopic: orders.events
      decoder: json
      to:
        - { sink: disaster-recovery, toTopic: orders.events }
        - { sink: analytics, toTopic: orders.analytics, partitionKey: "payload.customerId" }
```

This is an **illustrative configuration**, not a zero-data-loss guarantee. A queue can intentionally discard old messages when `maxDepth` or `retentionTime` limits are reached, even if records were originally persisted. `onWriteError: halt` concerns **local queue write errors**; it does not turn bounded retention into unlimited storage. See the [production-style AcmePay scenario](docs/usage-example.md) for more settings, examples, and operational tradeoffs.

### Routing modes

| Mode | Configuration idea | Result |
| --- | --- | --- |
| Topic mirror | Omit `toTopic` | Source and target topic names match |
| Topic rename | Set `toTopic: orders.mirror` | Forward to another topic name |
| Fan-out | Multiple `to` targets | One source topic to multiple destinations |
| Merge | Multiple routes share a sink and `toTopic` | Multiple source topics to one destination topic |
| Key extraction | `decoder: json` or `yaml` plus `partitionKey` on a target | Set the Kafka key from a scalar payload field |
| Rate limiting | `maxConsumeRatePerSec` on a source | Bound ingestion rate per source |

## Reliability, boundaries, and operations

**Durability:** `sync-on-commit` (default) syncs affected destination queues before source offset commits. `per-record-fsync` makes each queue insertion more conservative at a throughput cost; `os-managed` is faster but accepts a larger crash-loss window.

**Delivery:** at-least-once. There is no exactly-once delivery or built-in duplicate suppression.

**Bounded queues:** `maxDepth` and `retentionTime` can drop old, undelivered records. Track `dropped`, `expired`, and routing-drop metrics and alert on nonzero increases. Provision disks and choose queue limits consistent with your outage tolerance.

**Errors:** transient destination failures are retried. Non-retryable/oversized records can be discarded as poisoned records. Invalid extracted partition keys can cause target-specific drops without corrupting the payload for other routes.

**Metadata scope:** this is an event relay, **not** a Kafka-cluster metadata replication system. Topic creation/configuration, ACL mirroring, and consumer-group offset synchronization are outside its current scope.

**Monitoring:** view `/actuator/prometheus` for `nstreamreplay_*` metrics, including per-sink queue depth and online status. Detailed names and correctness guarantees are documented in [architecture](docs/architecture.md).

## n-stream-replay vs. Apache Kafka MirrorMaker 2

Use **n-stream-replay** when you specifically need **persistent local queues per destination, fine-grained per-topic routing, selective key extraction, or independent handling of offline Kafka sinks**.

Use **MirrorMaker 2** when you need the established Kafka Connect-based replication toolchain, including **consumer-group checkpoints, topic configuration replication, and ACL synchronization**.

Both can support inter-cluster topic replication and fan-out. This project does **not** claim to outperform MirrorMaker 2 in throughput, latency, or reliability without controlled benchmarks.

Read the [feature-by-feature comparison and migration considerations](docs/mirrormaker-vs-n-stream-replay.md).

## Technical stack

- **Java:** 25
- **Spring Boot:** 3.5.6 (web servlet for health and metrics)
- **Apache Kafka client:** 3.9.1 (raw KafkaConsumer/KafkaProducer APIs)
- **Durable queue:** `dev.nishisan:nishi-utils-core:8.1.0` / NQueue
- **Metrics bridge:** `dev.nishisan:nishi-utils-spring:2.1.1` / Micrometer + Prometheus

For Java 25 with SASL/SCRAM, this version uses Kafka client 3.9.1 and includes a compatibility test for the `Subject.getSubject()` issue encountered with client 3.9.0.

## Documentation

- [Architecture, correctness guarantees, and metrics](docs/architecture.md)
- [End-to-end multi-datacenter Kafka relay example](docs/usage-example.md)
- [Full example YAML](docs/examples/acmepay-relay.yaml)
- [Default sample configuration](config/n-stream-replay.yaml)
- [MirrorMaker 2 comparison](docs/mirrormaker-vs-n-stream-replay.md)
- [Portuguese README](README.pt-BR.md)
- [Release history](https://github.com/nishisan-dev/n-stream-replay/releases)

## License

Distributed under the [GNU General Public License v3.0](LICENSE). See the license file for terms.

If this project solves an event-delivery problem for you, consider **starring the repository** to help other Kafka developers discover it.
