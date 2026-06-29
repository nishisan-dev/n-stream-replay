# Plano — Bootstrap do projeto `n-stream-replay`

## Context

Projeto **greenfield** (pasta vazia) em `/home/lucas/Projects/nishisan/n-stream-replay`. Objetivo do MVP: um **relay Kafka store-and-forward** onde se declaram **origens** (sources Kafka) e **destinos** (sinks Kafka), e **pipelines** que amarram **1 origem → N destinos**. Sem transformação de mensagem e sem rate-limit nesta fase. Cada **destino** tem uma **fila durável (NQueue)** própria: se o destino estiver offline, as mensagens dele são persistidas em disco e re-entregues quando ele voltar — isolando destinos entre si (um destino offline não trava a origem nem os demais).

Fluxo: `origem → pipeline (resolve destinos) → fila-NQueue-por-destino → destino`.

Configuração em **YAML**, entrega como **fat jar**, métricas via **`nishi-utils` stats + `nishi-utils-spring`** (`StatsUtils → Micrometer → /actuator/prometheus`).

### Decisões já confirmadas pelo usuário
1. **App Spring Boot 3.5.6 web (servlet), Java 25**; métricas via `nishi-utils-spring:2.1.1`. Consumers/producers em **kafka-clients cru** (sem Spring Kafka). Fat jar via **spring-boot-maven-plugin**.
2. **Bounded drop-oldest** por destino (limite por contagem e/ou tempo; ao encher descarta os mais antigos e conta métrica). Destinos isolados.
3. **Store-and-forward** puro (`DELETE_ON_CONSUME`): a mensagem sai da fila após entrega confirmada. Sem replay-por-offset no MVP.

### Bibliotecas (fatos verificados)
- `dev.nishisan:nishi-utils-core:8.1.0` — **já no `~/.m2`** (release 21). Contém `dev.nishisan.utils.queue.NQueue` e `dev.nishisan.utils.stats.StatsUtils`. Traz `jackson-dataformat-yaml` + `lz4-java` em compile (não precisamos declarar).
- `dev.nishisan:nishi-utils-spring:2.1.1` — **NÃO está no `~/.m2`** (só 1.0.2/1.0.3); existe como fonte em `/home/lucas/Projects/nishisan/nishi-utils-spring` (release **25**). **Pré-requisito: `mvn clean install` nesse projeto** antes de buildar o n-stream-replay.
- Ambiente: **JDK 25.0.3** e Maven 3.6.3 sobre JDK 25 → compatível com `release=25`. `kafka-clients:3.9.0` (padrão da casa).

---

## Arquitetura

Cardinalidade variável (listas YAML) ⇒ sources/sinks/pipelines **não** são beans estáticos. Um único bean orquestrador **`ReplayEngine` (`SmartLifecycle`)** lê os records de `@ConfigurationProperties`, **constrói e possui** as instâncias de runtime e controla start/stop em ordem explícita.

**Unidade de isolamento por destino — `SinkChannel`** (1 por `sinkId`): agrega a fila durável + o producer Kafka + o forwarder + a thread daemon. Espelha o padrão provado `ape-probe/.../output/metric/{MetricStreamQueue,MetricStreamForwarder,KafkaMetricStreamSink}.java`.

**Por origem — `SourceConsumer`**: 1 thread de poll, dona do `KafkaConsumer` (não thread-safe). Espelha `ngrrd-consumer/.../NgrrdKafkaConsumerService.java`.

### Caminho de dados (1 origem → N destinos)
`SourceConsumer.run()`:
1. resolve `targets = pipelineRegistry.sinksFor(sourceId)` (união dos destinos de todos os pipelines daquela origem, dedup por `sinkId`).
2. `records = consumer.poll(timeout)`.
3. para cada record → monta **um** `ReplayRecord` (com proveniência: `sourceTopic, partition, offset, seq, ts, key, value, headers`); para **cada** `SinkChannel` em `targets`: `channel.offer(record)` (não-bloqueante, drop-oldest na fila daquele destino).
4. `sink.sync()` em **todos** os destinos tocados (**group-commit**) e **só então** `consumer.commitSync()`.

### Invariantes de corretude (load-bearing — vindas da revisão adversarial)
- **Entrega = at-least-once.** Commit do offset **só após** `offer`+`sync` em todas as filas-destino. Duplicatas são possíveis após crash/restart (sem dedup no MVP) — **documentar como contrato** ("sinks devem tolerar redelivery"). Mitigações baratas: gravar proveniência no `ReplayRecord`; ligar `enable.idempotence=true` (dedup só intra-sessão do producer, **não** cross-restart).
- **Durabilidade = "sync-on-commit" (default).** Abrir as NQueues com `withFsync(false)` e chamar `sink.sync()` (que força fsync independente do flag) no boundary do batch, antes do `commitSync()`. Um fsync por destino por poll-batch (amortizado). Invariante: crash antes do `sync` → sem perda (offset não commitado); crash entre `sync` e `commit` → duplicata, sem perda. Knob no YAML `queue.durability`: `sync-on-commit` (default) | `per-record-fsync` (`withFsync(true)`) | `os-managed` (warning + métrica).
- **Drop-oldest por contagem = dual-queue + lock externo.** NQueue **não** tem bound nativo por contagem. Cada `SinkChannel` usa **duas** `NQueue<ReplayRecord>` (`backlog` + `retry/inflight`) sob **um único `ReentrantLock` externo** e um contador `pending` (`AtomicLong`) — **não** usar `NQueue.size()` no hot-path. `offer`: sob lock, se `pending >= maxDepth` → `backlog.poll()` (descarta a cabeça, `dropped++`) e então `backlog.offer(rec)`. Forwarder: sob lock move backlog→retry (`peekBatch`), **solta o lock** para publicar na rede, e sob lock dá `ack` (poll destrutivo na retry) só do **prefixo contíguo entregue**. Eviction toca **só o backlog** → nunca dropa item em voo, nunca reordena. **Opções obrigatórias** em ambas as filas: `withMemoryBuffer(false)`, `withShortCircuit(false)`.
- **Tempo vs contagem.** `withRetentionPolicy(DELETE_ON_CONSUME)`; bound de profundidade = drop-oldest manual; bound de tempo = **`withExpireAfterWrite(maxAge)` só no backlog** (TTL que dropa não-entregue antigo — exatamente o desejado); retry com `expireAfterWrite(ZERO)`. **Não** usar `TIME_BASED`+`clampToConsumer` (recusa dropar não-entregue). `flushExpired()` periódico, alimentando a métrica `expired`.
- **Poison vs transitório** (forwarder): publica em ordem; `ApiException && !RetriableException` (ex. `RecordTooLargeException`) e oversize pré-send → **poison** (descarta 1, conta, avança — evita head-of-line block); broker down/timeout/erro desconhecido → **transitório** (mantém na fila + backoff, não dá ack). Producer com timeouts curtos (`MAX_BLOCK_MS=5s`, `REQUEST_TIMEOUT_MS=10s`, `DELIVERY_TIMEOUT_MS=15s`), `acks` config, `enable.idempotence=true`.
- **Disco cheio / `offer` IOException**: `queue.onWriteError: drop` (default — isolamento vence: loga + métrica `offerErrors`, origem segue e commita) | `halt` (opt-in).
- **Boot/recovery**: abrir filas → subir forwarders → **depois** subscrever sources (forwarders drenam backlog do disco no boot, independde a origem estar viva). Cursor de consumo da NQueue é persistido (não re-entrega o já-ackado). Sem marker `.clean-shutdown` (modelo tolera duplicatas).
- **Ordem**: 1 poll thread/origem + 1 forwarder/destino ⇒ FIFO por destino e ordem por chave preservada. Invariante a documentar: nunca adicionar 2º forwarder por fila sem sharding por `hash(key)`.

### Ciclo de vida (`ReplayEngine implements SmartLifecycle`)
- `getPhase()` alto (`Integer.MAX_VALUE - 1024`) → sobe depois do web/actuator e para antes dele. `isAutoStartup()=true`.
- `start()`: abre `SinkChannel`s e sobe forwarders → depois sobe `SourceConsumer`s.
- `stop()`: `running=false` + `consumer.wakeup()` (join) → para forwarders (interrupt+join) → `flush/sync/close` das filas e `close` dos producers. Idempotente.

---

## Estrutura de pastas e arquivos

```
n-stream-replay/
├── pom.xml
├── README.md
├── config/n-stream-replay.yaml          # externo, versionado (sources/sinks/pipelines)
├── scripts/start.sh                      # java -jar + JAVA_OPTS (ZGC) + --spring.config.additional-location
├── docs/architecture.md
├── docs/diagrams/{topology.puml,c4_container.puml,sequence_store_and_forward.puml}
├── planning/                             # cópia deste plano
└── src/
    ├── main/java/dev/nishisan/nstreamreplay/
    │   ├── NStreamReplayApplication.java          # @SpringBootApplication + @ConfigurationPropertiesScan + main
    │   ├── config/  NStreamReplayProperties, SourceProperties, SinkProperties,
    │   │            QueueProperties, PipelineProperties, ConfigValidator
    │   ├── model/   ReplayRecord (Serializable: key/value/headers/seq/sourceTopic/partition/offset/ts)
    │   ├── source/  SourceConsumer (poll loop, fan-out, sync-on-commit, wakeup, RebalanceListener)
    │   ├── sink/    SinkChannel, DurableSinkQueue (backlog+retry+lock+drop-oldest+expire),
    │   │            KafkaSink (producer + poison/oversize), SinkForwarder (Runnable daemon)
    │   ├── pipeline/ PipelineRegistry (sourceId → List<SinkChannel>), SinkRegistry (sinkId → SinkChannel)
    │   ├── runtime/ ReplayEngine (SmartLifecycle: build/own/start/stop ordenado)
    │   └── stats/   ReplayMetrics (@Component; wrapper de StatsUtils + nomes + sanitização de id)
    │   └── resources/ application.yml [, logback-spring.xml]
    └── test/java/...                              # unit + IT (Testcontainers kafka)
```

### Schema YAML (externo — `config/n-stream-replay.yaml`)
```yaml
nstreamreplay:
  sources:
    - id: orders-src
      bootstrapServers: "kafka-a:9092"
      topics: [ "orders.in" ]
      groupId: "n-stream-replay.orders"
      autoOffsetReset: latest        # earliest|latest|none
      maxPollRecords: 500
      pollTimeoutMs: 1000
      extraProps: {}                 # passthrough cru (SASL/TLS etc.)
  sinks:
    - id: dc2-mirror
      bootstrapServers: "kafka-b:9092"
      topic: "orders.mirror"
      acks: all
      lingerMs: 50
      compressionType: lz4
      maxRequestSize: 1048576
      extraProps: {}
      queue:
        basePath: "var/n-stream-replay/queues"   # fila em basePath/<sinkId>/
        maxDepth: 200000             # bound por CONTAGEM (drop-oldest)
        retentionTime: 6h            # bound por TEMPO (expireAfterWrite no backlog); 0 = off
        durability: sync-on-commit   # sync-on-commit|per-record-fsync|os-managed
        onWriteError: drop           # drop|halt
        batchSize: 256
        forwarderRetryBackoffMs: 1000
  pipelines:
    - id: orders-fanout
      source: orders-src
      sinks: [ dc2-mirror ]          # fan-out 1→N por id
```

**Binding**: `@ConfigurationProperties(prefix="nstreamreplay", ignoreUnknownFields=false)` + `@Validated`, constructor-binding a **records imutáveis** com `@DefaultValue`; `Duration` nativo (`6h`/`1000ms`). Validação cruzada (ids únicos, pipeline→source/sink existentes) num **`ConfigValidator`** que falha o boot (fail-loud). Config externa via `spring.config.import: optional:file:./config/n-stream-replay.yaml` no `application.yml` interno; override por `--spring.config.additional-location` no `start.sh`.

### `application.yml` interno (Spring/actuator)
```yaml
spring: { application: { name: n-stream-replay }, config: { import: "optional:file:./config/n-stream-replay.yaml" } }
management:
  endpoints: { web: { exposure: { include: health,info,prometheus } } }
nishi: { utils: { stats: { enabled: true } } }
```

### Métricas (`StatsUtils` → `/actuator/prometheus`)
Nomes `[a-z0-9_]` (sanitizar `id`; ciente de que hit-counter vira Gauge-rate + Counter): por origem `…source.<id>.consumed`, `…source.<id>.commitErrors`; por destino `…sink.<id>.forwarded`, `…sink.<id>.depth` (gauge; **alerta perto de maxDepth**), `…sink.<id>.dropped` (**perda silenciosa**), `…sink.<id>.expired` (**perda silenciosa** — vem do retorno de `flushExpired()`), `…sink.<id>.poisoned`, `…sink.<id>.online` (1/0), `…sink.<id>.offerErrors`, `…sink.<id>.retryBackoffs`.

---

## Plano de execução (commits atômicos, TDD)

Branch: `feature/bootstrap-n-stream-replay`. Cada passo = 1 commit lógico.

0. **Pré-requisito**: `mvn -f /home/lucas/Projects/nishisan/nishi-utils-spring/pom.xml clean install` (publica `2.1.1` no `~/.m2`; `core 8.1.0` já presente → resolve sem rede).
1. **Scaffolding**: `git init`, `pom.xml` (parent `spring-boot-starter-parent:3.5.6`, Java 25, kafka 3.9.0 pinado, deps web/actuator/validation/micrometer-prometheus/nishi-utils-core/nishi-utils-spring/kafka-clients + test), `.gitignore`, `application.yml`, `NStreamReplayApplication`, estrutura de pastas, `scripts/start.sh`, `README.md`, cópia deste plano em `planning/`.
2. **Config + validação** (TDD): records `*Properties` + `ConfigValidator`; testes de binding e de falha (ids duplicados, ref inexistente, unknown field).
3. **`ReplayRecord`** + serialização (teste round-trip Serializable).
4. **`DurableSinkQueue`** (TDD): backlog+retry+lock, `offer` com drop-oldest, `peekBatch`/`ack` (prefixo contíguo), `expireAfterWrite` + `flushExpired`, contadores. Testes: drop-oldest por contagem, FIFO/ordem, ack parcial, sem corrida offer×forward, expiração por tempo.
5. **`KafkaSink`** (TDD): producer config + classificação poison/transient/oversize (testes unitários com mocks/erros simulados).
6. **`SinkForwarder`** + **`SinkChannel`** (TDD): loop drena→publica→ack; backoff em transitório; métricas. Testes do algoritmo do forwarder.
7. **`SourceConsumer`** + **registries**: poll loop, fan-out, sync-on-commit, wakeup, RebalanceListener.
8. **`ReplayEngine` (SmartLifecycle)** + **`ReplayMetrics`**: wiring dinâmico, ordem start/stop.
9. **Testes de integração (Testcontainers Kafka)**: (a) source→2 sinks end-to-end; (b) **destino offline → backlog cresce → volta → drena** (o caso central); (c) drop-oldest sob backlog estourado; (d) restart re-drena backlog do disco.
10. **Documentação**: `docs/architecture.md` + diagramas PlantUML (topology, c4_container, sequence store-and-forward) incorporados via `uml.nishisan.dev`; `README` com build/run.
11. **Build final + smoke**: `mvn clean package` → fat jar executável; rodar contra Kafka local de teste; conferir `/actuator/prometheus` com as métricas.

---

## Verificação (fim-a-fim)
- `mvn clean install` (lib) + `mvn clean package` (app) verdes; suíte de testes (unit + IT) passando — **não commitar produção sem testes verdes**.
- Subir Kafka de teste (Testcontainers nos ITs; ou `docker` local no smoke). Produzir N mensagens na origem; verificar que chegam a todos os destinos do pipeline.
- **Cenário store-and-forward**: derrubar um destino, produzir mensagens, verificar `sink.<id>.depth` subindo e `online=0`; religar o destino e verificar drenagem completa (sem perda dentro de `maxDepth`/`retentionTime`).
- **Isolamento**: com um destino offline, confirmar que a origem continua consumindo e os outros destinos seguem recebendo.
- `java -jar target/n-stream-replay-*.jar --spring.config.additional-location=file:./config/` sobe e expõe `GET /actuator/prometheus` com `nstreamreplay_*`.
- DoD docs: `docs/` atualizado, diagramas PlantUML renderizando, plano copiado em `planning/`.

## Riscos / notas
- `nishi-utils-spring:2.1.1` exige JDK 25 (ok no ambiente); sem ele instalado no m2 o build falha — passo 0 é obrigatório.
- MVP é **at-least-once**: duplicatas após crash são esperadas (contrato documentado). Exactly-once/dedup fica para fase futura.
- `dropped` e `expired` são os dois sinais de **perda silenciosa** — devem ser exportados e alertados.
