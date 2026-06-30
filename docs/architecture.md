# Arquitetura — n-stream-replay

## 1. Visão geral

`n-stream-replay` é um **relay Kafka store-and-forward** com **roteamento por tópico**. Declaram-se **origens** (sources) e **destinos** (sinks) como conexões puras, e **rotas** que mapeiam um **tópico de origem → N alvos `{sink, toTopic?}`** (de-para, fan-out, merge, espelho). Não há transformação de mensagem nem rate-limit: os bytes de `key`/`value` e os headers são repassados intactos.

Cada destino possui uma **fila durável em disco** (`NQueue` do `nishi-utils-core`). Se um destino fica offline, suas mensagens ficam na fila e são **re-entregues quando ele volta**, sem travar a origem nem os demais destinos.

```
origem ──► SourceConsumer ──► fila NQueue (por destino) ──► SinkForwarder ──► destino
                          └─► fila NQueue (por destino) ──► SinkForwarder ──► destino
```

A aplicação é um **Spring Boot web servlet** apenas para expor métricas em `/actuator/prometheus` (ponte `StatsUtils → Micrometer` do `nishi-utils-spring`). Todo o I/O Kafka usa `kafka-clients` cru (sem Spring Kafka). Entrega: **fat jar executável**.

## 2. Topologia

![Topologia](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/n-stream-replay/main/docs/diagrams/topology.puml)

## 3. Componentes (C4)

![C4 Container](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/n-stream-replay/main/docs/diagrams/c4_container.puml)

| Componente | Papel |
|---|---|
| `ReplayEngine` (`SmartLifecycle`) | Constrói e possui as instâncias dinâmicas; start/stop ordenado (forwarders **antes** das origens); tick de métricas. |
| `SourceConsumer` (1 thread/origem) | `KafkaConsumer` cru, `enable.auto.commit=false`; assina a união dos `fromTopic`; **roteia cada record por tópico** (resolve destino + `destinationTopic`); rate limit opcional via `ConsumeRateLimiter` (`maxConsumeRatePerSec`); group-commit (sync) **antes** do `commitSync`. |
| `RouteTable` | Resolve, no boot, **origem → (tópico → alvos)**; alvo = `{sink, toTopic?}` (toTopic ausente = preserva). |
| `SinkChannel` (1 por destino) | Unidade de **isolamento**: agrega fila + producer + forwarder + thread. |
| `DurableSinkQueue` | Dupla-fila `backlog`+`retry`; no move, mantém o lock do backlog apenas no `poll` e persiste na retry depois de liberá-lo; drop-oldest por contagem/tempo; at-least-once. |
| `KafkaSink` | `KafkaProducer` cru; publica no `record.destinationTopic()` (resolvido pela rota); preserva key/value/headers/timestamp; guarda de oversize; poison vs transitório. |
| `SinkForwarder` (1 thread/destino) | Drena → publica → confirma o prefixo entregue; backoff em broker-down; sweep de expirados. |
| `ReplayMetrics` + `StatsUtils` | Exporta métricas em `/actuator/prometheus`. |

## 4. Fluxo de dados e store-and-forward

![Sequência store-and-forward](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/n-stream-replay/main/docs/diagrams/sequence_store_and_forward.puml)

## 5. Garantias de corretude

- **At-least-once.** O offset da origem só é commitado **depois** de o lote ser enfileirado e sincronizado em **todas** as filas-destino tocadas. Após crash/restart pode haver **duplicatas** no destino (sem dedup) — sinks devem tolerar redelivery. A proveniência (`sourceTopic/partition/offset`) e o `destinationTopic` resolvido são gravados em cada registro.
- **Durabilidade (`sync-on-commit`, default).** As filas abrem com `fsync=false` (rápido por registro) e a origem força `sync()` (fsync) das filas tocadas no boundary do batch, antes do commit. Um fsync por destino por poll-batch. Alternativas por destino: `per-record-fsync` (mais seguro/lento) e `os-managed` (mais rápido; aceita perda na cauda).
- **Bounded drop-oldest.** A fila é limitada por `maxDepth` (contagem) e/ou `retentionTime` (tempo, via `expireAfterWrite` no backlog). Ao estourar, os registros **mais antigos não entregues são descartados** (perda intencional, contabilizada em `dropped`/`expired`). Itens reservados na `retry` nunca são descartados.
- **Isolamento.** Cada destino tem fila/producer/forwarder próprios; um destino offline só faz crescer/dropar a sua fila, sem afetar a origem nem os demais destinos.
- **Ordem.** 1 thread de poll por origem + 1 forwarder por destino ⇒ FIFO por destino e ordem por chave preservada. Alvos `{sink, destino}` idênticos no mesmo record são deduplicados. Invariante: nunca adicionar um 2º forwarder por fila sem sharding por `hash(key)`.
- **Poison vs transitório.** Erro não-retryable (`ApiException` não-`RetriableException`, ex.: `RecordTooLargeException`) e oversize pré-send são descartados (`poisoned`), avançando o FIFO; erro transitório (broker fora) mantém o lote na fila + backoff.

## 6. Configuração

Configuração de app (sources/sinks/routes) em YAML externo: [`config/n-stream-replay.yaml`](../config/n-stream-replay.yaml). Ligada a `@ConfigurationProperties(prefix="nstreamreplay")` (constructor-binding a records imutáveis, `ignoreUnknownFields=false` = fail-loud). Validação cruzada (ids únicos, referências de rota → source/sink) no boot pelo `ConfigValidator`. Importada via `spring.config.import` do `application.yml` interno; override por `--spring.config.additional-location`.

## 7. Métricas (`/actuator/prometheus`)

Convenção `nstreamreplay.<dim>.<id>.<metric>`:

- Por origem (hit counter): `source.<id>.consumed`, `source.<id>.commit_errors`.
- Por destino (gauges, tick periódico): `sink.<id>.depth`, `sink.<id>.online` (1/0), `sink.<id>.published_total`, `sink.<id>.dropped_total`, `sink.<id>.expired_total`, `sink.<id>.poisoned_total`, `sink.<id>.offer_errors_total`, `sink.<id>.retry_backoffs_total`.
- Diagnóstico da origem (totais cumulativos): `poll_nanos_total`, `limiter_wait_nanos_total`,
  `offer_nanos_total`, `sync_nanos_total`, `commit_nanos_total`, `polled_batches_total` e
  `configured_rate_per_sec`.
- Diagnóstico do destino/fila: `queue_peek_nanos_total`, `publish_nanos_total`,
  `ack_nanos_total`, `offer_lock_wait_nanos_total` e `sync_lock_wait_nanos_total`.

O arquivo de stats também apresenta esses tempos como milissegundos por janela, permitindo
separar espera no limiter, I/O local, contenção dos locks e publicação Kafka.

> **Perda silenciosa:** `dropped_total` e `expired_total` são os dois sinais de descarte intencional — devem ser exportados e **alertados** (`increase(...) > 0`).

Requer `nishi.utils.stats.enabled=true`, `management.endpoints.web.exposure.include=...,prometheus` e `io.micrometer:micrometer-registry-prometheus` (já no `pom`).

## 8. Build e execução

> Pré-requisito (uma vez): `mvn -f /caminho/nishi-utils-spring/pom.xml clean install` (publica `nishi-utils-spring:2.1.1` no `~/.m2`).

```bash
mvn clean package      # fat jar executável em target/
mvn verify             # unit + ITs (Testcontainers Kafka)
./scripts/start.sh     # executa o jar com ./config/
```

Java 25, Spring Boot 3.5.6, `kafka-clients` 3.9.0, `nishi-utils-core` 8.1.0, `nishi-utils-spring` 2.1.1.
