# Design — Roteamento por tópico (n-stream-replay v2.0.0)

## Contexto

Na v1, o roteamento é grosso: um `source` consome uma lista de `topics` com um consumer e faz
**fan-out de todos os tópicos** para **todos** os sinks do seu `pipeline`; cada `sink` grava em um
`topic` **fixo**. Não há como, dentro de um único source, mandar o tópico A para um destino e o
tópico B para outro, nem preservar/renomear o nome do tópico no destino.

A v2.0.0 introduz um **modelo de rotas completo** `(source, fromTopic) → [{sink, toTopic?}]`,
permitindo de-para por tópico, fan-out 1→N, merge N→1, um sink escrevendo em vários tópicos e
espelho (preservar o nome de origem). É uma **quebra limpa** do modelo de configuração (sem
compatibilidade com a v1), coerente com a diretriz de "ir sempre à frente".

## Objetivos

- Rotear cada tópico de origem para sinks/tópicos de destino distintos, dentro de um único source.
- Suportar: de-para 1:1, fan-out (1 tópico → N destinos), merge (N tópicos → 1 destino), 1 sink
  escrevendo em vários tópicos, e preservação do nome do tópico de origem.
- Manter as garantias da v1: at-least-once, durabilidade configurável, bounded drop-oldest,
  isolamento por destino, ordem por chave.

## Não-objetivos (v2.0.0)

- Curingas/padrões no casamento de tópicos (`orders.*`). Apenas **nome exato**.
- Transformação de payload, rate-limit, dedup/exactly-once. (Seguem fora do escopo.)
- Compatibilidade com configs v1.

## Decisões (confirmadas)

1. **Modelo de rotas completo** `(source, fromTopic) → [{sink, toTopic?}]`.
2. **Quebra limpa** — redefinição do modelo; nenhum caminho de legado v1.
3. **Casamento por nome exato**; `toTopic` omitido ⇒ **preserva** o nome de origem (espelho).

## Modelo de configuração

### Entidades

- **`source`** — conexão de consumo. Campos: `id`, `bootstrapServers`, `groupId`, `clientId?`,
  `autoOffsetReset` (default `latest`), `maxPollRecords` (default 500), `pollTimeoutMs`
  (default 1000), `extraProps`. **Remove `topics`**: os tópicos consumidos são a **união dos
  `fromTopic`** das rotas que referenciam este source.
- **`sink`** — conexão de destino + fila durável. Campos: `id`, `bootstrapServers`, `acks`
  (default `1`), `lingerMs` (default 50), `compressionType` (default `lz4`), `maxRequestSize`
  (default 1048576), `extraProps`, `queue{}` (inalterado: `basePath`, `maxDepth`, `retentionTime`,
  `durability`, `onWriteError`, `batchSize`, `forwarderRetryBackoffMs`). **Remove `topic`**: o
  tópico de destino vem da rota.
- **`route`** (substitui `pipeline`) — `id`, `source` (id), `fromTopic` (nome exato), `to`
  (lista de `{ sink, toTopic? }`). `toTopic` omitido ⇒ usa `fromTopic` (preserva).

### Exemplo

```yaml
nstreamreplay:
  sources:
    - id: orders-sp
      bootstrapServers: "kafka-sp:9093"
      groupId: "nsr.orders-sp"
  sinks:
    - id: dr-rio            # sem topic: aceita o que as rotas mandarem
      bootstrapServers: "kafka-rio:9093"
      queue: { maxDepth: 5000000, durability: per-record-fsync, onWriteError: halt }
    - id: analytics
      bootstrapServers: "kafka-analytics:9092"
      queue: { maxDepth: 200000, retentionTime: 12h, durability: os-managed }
  routes:
    - id: orders-mirror
      source: orders-sp
      fromTopic: orders.events
      to:
        - { sink: dr-rio }                       # preserva => orders.events no Rio
        - { sink: analytics, toTopic: orders.raw }
    - id: returns-route
      source: orders-sp
      fromTopic: returns.events                  # outro tópico da MESMA origem...
      to:
        - { sink: analytics, toTopic: returns.raw }   # ...para outro destino/tópico
```

Casos cobertos: **de-para** (rotas distintas por `fromTopic`), **fan-out** (`to` com vários
alvos), **merge** (duas rotas com o mesmo `{sink, toTopic}`), **1 sink → N tópicos** (o mesmo
sink em rotas com `toTopic` diferentes), **espelho** (`toTopic` omitido).

## Roteamento e semântica

### Caminho de dados

1. Cada `SourceConsumer` assina a **união dos `fromTopic`** das rotas do seu source.
2. Para cada `ConsumerRecord`, resolve a lista de alvos de `(sourceId, record.topic())`. Para cada
   alvo `{sink, toTopic?}`, monta um `ReplayRecord` com `destinationTopic` resolvido
   (`toTopic` ou `record.topic()`) e faz `offer` na fila daquele sink.
3. Alvos idênticos `(sink, destinationTopic)` para o mesmo record são **deduplicados**.
4. **Group-commit:** após enfileirar o batch em todos os sinks tocados, faz `sync()` nos que
   exigem e então `commitSync()` (at-least-once — inalterado).

### Fila e entrega

- **Uma fila durável por sink** (isolamento por cluster de destino, inalterado). Como um sink pode
  receber records destinados a tópicos diferentes, **o `ReplayRecord` carrega `destinationTopic`**.
- `KafkaSink` publica cada record em `record.destinationTopic()` (em vez de um `topic` fixo). A
  guarda de oversize, classificação poison/transitório e timeouts seguem por sink (inalterados).
- Ordem por chave preservada (1 forwarder por sink; 1 thread de poll por source).

### Modelo de objetos (impacto)

| Componente | Mudança |
|---|---|
| `config.SourceProperties` | remove `topics` |
| `config.SinkProperties` | remove `topic` |
| `config.RouteProperties` (novo; substitui `PipelineProperties`) | `id`, `source`, `fromTopic`, `to: List<RouteTarget>` |
| `config.RouteTarget` (novo) | `sink`, `toTopic?` |
| `config.ConfigValidator` | valida ids únicos + refs de rota (source/sink existentes) + source com ≥1 rota |
| `model.ReplayRecord` | adiciona `destinationTopic` |
| `pipeline.PipelineRegistry` → `route.RouteTable` | `Map<sourceId, Map<fromTopic, List<Target>>>`; `Target = {SinkChannel, toTopicOrNull}`; expõe `topicsFor(sourceId)` (união) e `targetsFor(sourceId, topic)` |
| `source.SourceConsumer` | assina união de `fromTopic`; roteia por `record.topic()`; resolve `destinationTopic`; dedup de alvos |
| `sink.KafkaSink` | publica em `record.destinationTopic()`; sem `topic` fixo |
| `sink.SinkChannel` / `SinkTarget` | `offer(ReplayRecord)` inalterado (record já traz o destino) |
| `runtime.ReplayEngine` | abre sinks referenciados por rotas; monta `RouteTable`; sobe um source por source-id ativo |

## Validação (boot, fail-fast)

- ids únicos por categoria (`source`, `sink`, `route`).
- `route.source` existe; cada `route.to[].sink` existe; `fromTopic` não vazio; `to` não vazio.
- Aviso (não erro) se um `source` declarado não é referenciado por nenhuma rota (não consome nada)
  ou um `sink` declarado não recebe nenhuma rota (ocioso) — ambos ignorados, como na v1.
- Merge (duas rotas → mesmo `sink`+`toTopic`) é **permitido** (intencional).

## Compatibilidade e migração

- **Breaking:** configs v1 (`source.topics`, `sink.topic`, `pipelines`) não são mais válidas;
  `ignoreUnknownFields=false` fará o boot falhar em chaves antigas (fail-loud) — desejável.
- **Filas em disco:** o formato do `ReplayRecord` muda (novo campo) → filas gravadas pela v1 ficam
  incompatíveis. Projeto novo, sem dados de produção: aceitável. Documentar que se deve **drenar/
  limpar** o `basePath` antes de subir a v2 (ou usar um `basePath` novo).
- Atualizar README, `docs/architecture.md` + diagramas e o exemplo AcmePay para o modelo de rotas;
  nota de migração v1→v2 na release notes.

## Testes

- **Unit:**
  - `ConfigValidator`: ids duplicados, rota com source/sink inexistente, source sem rota (aviso).
  - `RouteTable`: união de tópicos por source; resolução de alvos por `(source, topic)`;
    `destinationTopic` (explícito vs preserva); dedup de alvos idênticos.
  - `ReplayRecord`: round-trip de serialização com `destinationTopic`.
  - `SourceConsumer.enqueueBatch`: record do tópico A vai só aos alvos de A; B só aos de B;
    fan-out; group-commit/sync apenas nos sinks tocados.
  - `KafkaSink`: publica em `record.destinationTopic()` (por-record).
  - binding `@ConfigurationProperties` do novo schema (defaults, `toTopic` ausente = preserva).
- **IT (Testcontainers Kafka):**
  - de-para: source com 2 tópicos → tópico A→sink X, tópico B→sink Y.
  - fan-out 1→N (um tópico para 2 sinks, com `toTopic` distinto).
  - merge N→1 (dois tópicos para o mesmo `sink`+`toTopic`).
  - espelho (preserva nome no destino).

## Release

Versão **2.0.0** (major, breaking). Atualizar `pom.xml`, documentação, diagramas e exemplo;
publicar release notes com a nota de migração v1→v2.
