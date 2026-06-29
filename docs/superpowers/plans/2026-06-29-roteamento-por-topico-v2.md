# Roteamento por tópico (n-stream-replay v2.0.0) — Plano de Implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Substituir o roteamento grosso (source.topics → todos os sinks; sink.topic fixo) por um modelo de rotas `(source, fromTopic) → [{sink, toTopic?}]`, em quebra limpa.

**Architecture:** `source` e `sink` viram conexões puras (source sem `topics`, sink sem `topic`). Um bloco `routes` mapeia tópico de origem → alvos `(sink, toTopic?)`. O `ReplayRecord` passa a carregar `destinationTopic`; mantém-se **uma fila durável por sink** e o `KafkaSink` publica no tópico do próprio record. `toTopic` omitido = preserva o nome de origem.

**Tech Stack:** Java 25, Spring Boot 3.5.6, kafka-clients 3.9.0, nishi-utils-core 8.1.0, nishi-utils-spring 2.1.1, JUnit 5 + AssertJ + Testcontainers.

## Global Constraints

- Java 25; `mvn verify` deve ficar verde (unit + IT) ao fim de cada task que toca código.
- Quebra limpa: NENHUM caminho de legado v1 (`source.topics`, `sink.topic`, `pipelines`) ao final.
- `@ConfigurationProperties(prefix="nstreamreplay", ignoreUnknownFields=false)` (fail-loud).
- At-least-once, durabilidade `sync-on-commit` (default), bounded drop-oldest e isolamento por sink: inalterados.
- Casamento de tópico por **nome exato**; sem curingas.
- Sem transformação de payload, rate-limit ou dedup.
- Mensagens de commit na voz do time (sem referência a agente).
- Rodar Maven com `-o` (offline) quando possível; ITs exigem `-Dapi.version=1.43` (já no failsafe do pom) e usam `apache/kafka:3.8.0`.

---

### Task 1: `ReplayRecord` carrega `destinationTopic` (aditivo)

Adiciona o campo mantendo o comportamento v1 (a origem o preenche com o tópico de origem; o `KafkaSink` ainda ignora). Mantém o build verde.

**Files:**
- Modify: `src/main/java/dev/nishisan/nstreamreplay/model/ReplayRecord.java`
- Modify (construtores): `src/main/java/dev/nishisan/nstreamreplay/source/SourceConsumer.java` (método `toReplayRecord`)
- Test: `src/test/java/dev/nishisan/nstreamreplay/model/ReplayRecordTest.java`
- Test (ajustar construtores): `KafkaSinkTest.java`, `SinkForwarderTest.java`, `SinkChannelTest`(via helpers em test), `DurableSinkQueueTest.java`, `SourceConsumerTest.java`, `it/StreamReplayIT.java`

**Interfaces:**
- Produces: `ReplayRecord(String sourceTopic, int partition, long offset, long timestamp, byte[] key, byte[] value, Map<String,byte[]> headers, String destinationTopic)` e accessor `String destinationTopic()`.

- [ ] **Step 1: Escrever o teste falho** (round-trip preserva `destinationTopic`)

Em `ReplayRecordTest.java`, adicione:

```java
@Test
void preservaDestinationTopicNoRoundTrip() throws Exception {
    ReplayRecord original = new ReplayRecord(
            "orders.in", 1, 7L, 123L,
            "k".getBytes(java.nio.charset.StandardCharsets.UTF_8),
            "v".getBytes(java.nio.charset.StandardCharsets.UTF_8),
            java.util.Map.of(), "orders.mirror");
    ReplayRecord restored = roundTrip(original);
    assertThat(restored.destinationTopic()).isEqualTo("orders.mirror");
    assertThat(restored).isEqualTo(original);
}
```

- [ ] **Step 2: Rodar e ver falhar (compilação)**

Run: `mvn -o -q test-compile`
Expected: FAIL — `ReplayRecord` não aceita o 8º argumento.

- [ ] **Step 3: Adicionar o campo** em `ReplayRecord.java`

Acrescente `destinationTopic` como último parâmetro do record e inclua no `equals`/`hashCode`:

```java
public record ReplayRecord(
        String sourceTopic, int partition, long offset, long timestamp,
        byte[] key, byte[] value, Map<String, byte[]> headers,
        String destinationTopic) implements Serializable {

    @Serial private static final long serialVersionUID = 2L;

    public ReplayRecord {
        headers = (headers == null) ? Map.of() : Map.copyOf(headers);
    }
    // valueSize() inalterado
    // equals: incluir Objects.equals(destinationTopic, ...) na comparação;
    // hashCode: incluir destinationTopic via Objects.hash(...). Ajustar o pattern do equals
    // para o novo número de componentes do record.
}
```

(No `equals`, ajuste o record-pattern para 8 componentes e compare `destinationTopic` com `Objects.equals`; no `hashCode`, inclua `destinationTopic` no `Objects.hash(sourceTopic, partition, offset, timestamp, destinationTopic)`.)

- [ ] **Step 4: Propagar o construtor nos callers de produção**

Em `SourceConsumer.toReplayRecord`, passe o tópico de origem como destino (interino — comportamento v1 preservado):

```java
private static ReplayRecord toReplayRecord(ConsumerRecord<byte[], byte[]> r) {
    Map<String, byte[]> headers = new LinkedHashMap<>();
    for (Header h : r.headers()) headers.put(h.key(), h.value());
    return new ReplayRecord(r.topic(), r.partition(), r.offset(), r.timestamp(),
            r.key(), r.value(), headers, r.topic());
}
```

- [ ] **Step 5: Propagar nos testes**

Em cada construtor `new ReplayRecord(...)` dos testes (`KafkaSinkTest`, `SinkForwarderTest`, `SinkChannelTest`, `DurableSinkQueueTest`, `SourceConsumerTest`, `StreamReplayIT`), adicione um 8º argumento `destinationTopic`. Use o mesmo valor do tópico de destino esperado naquele teste (ex.: em `KafkaSinkTest.rec(...)` use `"dest"`; nos demais, use o `sourceTopic` do próprio record). Exemplo em `KafkaSinkTest`:

```java
private static ReplayRecord rec(int valueBytes) {
    byte[] value = new byte[valueBytes];
    return new ReplayRecord("src", 0, valueBytes, 1L,
            "k".getBytes(StandardCharsets.UTF_8), value, Map.of(), "dest");
}
```

- [ ] **Step 6: Rodar a suíte unitária**

Run: `mvn -o -q test`
Expected: PASS (todos verdes; comportamento inalterado).

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "ReplayRecord carrega destinationTopic (prep v2)"
```

---

### Task 2: Tipos de rota na config — `RouteTarget`, `RouteProperties`, campo `routes`

Introduz os novos tipos de configuração ao lado de `pipelines` (transitório). Apenas binding; sem uso no runtime ainda. Build verde.

**Files:**
- Create: `src/main/java/dev/nishisan/nstreamreplay/config/RouteTarget.java`
- Create: `src/main/java/dev/nishisan/nstreamreplay/config/RouteProperties.java`
- Modify: `src/main/java/dev/nishisan/nstreamreplay/config/NStreamReplayProperties.java` (adicionar `routes`, manter `pipelines` por ora opcional)
- Test: `src/test/java/dev/nishisan/nstreamreplay/config/NStreamReplayPropertiesBindingTest.java`

**Interfaces:**
- Produces: `RouteTarget(String sink, String toTopic)` com `toTopic` anulável; `RouteProperties(String id, String source, String fromTopic, List<RouteTarget> to)`.

- [ ] **Step 1: Escrever o teste de binding falho**

Em `NStreamReplayPropertiesBindingTest`, adicione um teste que liga um YAML com `routes` e `toTopic` omitido:

```java
@Test
void ligaRotasComToTopicOpcional() {
    String yaml = """
            nstreamreplay:
              sources:
                - { id: s1, bootstrapServers: "h:9092", topics: [t], groupId: g }
              sinks:
                - { id: d1, bootstrapServers: "h:9092", topic: out }
              pipelines:
                - { id: p1, source: s1, sinks: [ d1 ] }
              routes:
                - id: r1
                  source: s1
                  fromTopic: orders.in
                  to:
                    - { sink: d1 }
                    - { sink: d1, toTopic: orders.raw }
            """;
    NStreamReplayProperties props = bind(yaml);
    RouteProperties r = props.routes().get(0);
    assertThat(r.fromTopic()).isEqualTo("orders.in");
    assertThat(r.to()).hasSize(2);
    assertThat(r.to().get(0).toTopic()).isNull();          // preserva
    assertThat(r.to().get(1).toTopic()).isEqualTo("orders.raw");
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -o -q test-compile`
Expected: FAIL — `routes()`/`RouteProperties` inexistentes.

- [ ] **Step 3: Criar `RouteTarget.java`**

```java
package dev.nishisan.nstreamreplay.config;

import jakarta.validation.constraints.NotBlank;

/** Alvo de uma rota: o sink de destino e, opcionalmente, o tópico de destino
 *  (se omitido, preserva o tópico de origem). */
public record RouteTarget(
        @NotBlank String sink,
        String toTopic) {
}
```

- [ ] **Step 4: Criar `RouteProperties.java`**

```java
package dev.nishisan.nstreamreplay.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/** Rota: amarra um tópico de origem a N alvos (sink + tópico de destino). */
public record RouteProperties(
        @NotBlank String id,
        @NotBlank String source,
        @NotBlank String fromTopic,
        @NotEmpty @Valid List<RouteTarget> to) {
}
```

- [ ] **Step 5: Adicionar `routes` em `NStreamReplayProperties`** (mantendo `pipelines` transitoriamente opcional)

```java
@ConfigurationProperties(prefix = "nstreamreplay", ignoreUnknownFields = false)
@Validated
public record NStreamReplayProperties(
        @NotEmpty @Valid List<SourceProperties> sources,
        @NotEmpty @Valid List<SinkProperties> sinks,
        @Valid List<PipelineProperties> pipelines,   // transitório (removido na Task 6)
        @Valid List<RouteProperties> routes) {
}
```

- [ ] **Step 6: Rodar o teste de binding**

Run: `mvn -o -q test -Dtest=NStreamReplayPropertiesBindingTest`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "Adiciona tipos de configuração de rota (RouteProperties/RouteTarget)"
```

---

### Task 3: `RouteTable` (tabela de roteamento resolvida)

Substitui conceitualmente o `PipelineRegistry`. Resolve, no boot, origem→(tópico→alvos), com alvos apontando para `SinkTarget` já resolvidos. Classe nova, ainda não usada pelo runtime. Build verde.

**Files:**
- Create: `src/main/java/dev/nishisan/nstreamreplay/route/RouteTable.java`
- Test: `src/test/java/dev/nishisan/nstreamreplay/route/RouteTableTest.java`

**Interfaces:**
- Consumes: `RouteProperties`, `RouteTarget`, `dev.nishisan.nstreamreplay.sink.SinkTarget`.
- Produces:
  - `RouteTable.Target(SinkTarget sink, String toTopic)` (`toTopic` anulável = preserva)
  - `static RouteTable build(List<RouteProperties> routes, Map<String, ? extends SinkTarget> sinksById)`
  - `List<String> topicsFor(String sourceId)` (união dos `fromTopic`)
  - `List<Target> targetsFor(String sourceId, String topic)` (vazio se não houver rota)
  - `Set<String> activeSourceIds()`
  - `Set<String> referencedSinkIds()`

- [ ] **Step 1: Escrever os testes falhos**

`RouteTableTest.java` (use um `FakeSinkTarget` de `dev.nishisan.nstreamreplay.testutil`):

```java
package dev.nishisan.nstreamreplay.route;

import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.sink.SinkTarget;
import dev.nishisan.nstreamreplay.testutil.FakeSinkTarget;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RouteTableTest {
    private static Map<String, SinkTarget> sinks(String... ids) {
        Map<String, SinkTarget> m = new LinkedHashMap<>();
        for (String id : ids) m.put(id, new FakeSinkTarget(id, false));
        return m;
    }

    @Test
    void uniaoDeTopicosPorOrigemEAlvosPorTopico() {
        List<RouteProperties> routes = List.of(
                new RouteProperties("r1", "s1", "orders.in",
                        List.of(new RouteTarget("dr", null), new RouteTarget("an", "orders.raw"))),
                new RouteProperties("r2", "s1", "returns.in",
                        List.of(new RouteTarget("an", "returns.raw"))));
        RouteTable t = RouteTable.build(routes, sinks("dr", "an"));

        assertThat(t.topicsFor("s1")).containsExactlyInAnyOrder("orders.in", "returns.in");
        assertThat(t.targetsFor("s1", "orders.in"))
                .extracting(x -> x.sink().id(), RouteTable.Target::toTopic)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("dr", null),
                                 org.assertj.core.groups.Tuple.tuple("an", "orders.raw"));
        assertThat(t.targetsFor("s1", "desconhecido")).isEmpty();
        assertThat(t.activeSourceIds()).containsExactly("s1");
        assertThat(t.referencedSinkIds()).containsExactlyInAnyOrder("dr", "an");
    }

    @Test
    void sinkInexistenteLancaErro() {
        List<RouteProperties> routes = List.of(
                new RouteProperties("r1", "s1", "t", List.of(new RouteTarget("fantasma", null))));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> RouteTable.build(routes, sinks("dr")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("fantasma");
    }
}
```

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -o -q test-compile`
Expected: FAIL — `RouteTable` inexistente.

- [ ] **Step 3: Implementar `RouteTable.java`**

```java
package dev.nishisan.nstreamreplay.route;

import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.sink.SinkTarget;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tabela de roteamento resolvida no boot: origem -> (tópico de origem -> alvos). Cada alvo aponta
 * para um {@link SinkTarget} já resolvido e o tópico de destino (nulo = preserva a origem).
 * Assume config já validada pelo ConfigValidator.
 */
public final class RouteTable {

    /** Alvo resolvido: sink + tópico de destino (nulo = preserva o tópico de origem). */
    public record Target(SinkTarget sink, String toTopic) {
        public String resolveTopic(String sourceTopic) {
            return (toTopic == null || toTopic.isBlank()) ? sourceTopic : toTopic;
        }
    }

    private final Map<String, Map<String, List<Target>>> bySourceThenTopic;
    private final Set<String> referencedSinkIds;

    private RouteTable(Map<String, Map<String, List<Target>>> bySourceThenTopic,
                       Set<String> referencedSinkIds) {
        this.bySourceThenTopic = bySourceThenTopic;
        this.referencedSinkIds = referencedSinkIds;
    }

    public static RouteTable build(List<RouteProperties> routes,
                                   Map<String, ? extends SinkTarget> sinksById) {
        Map<String, Map<String, List<Target>>> map = new LinkedHashMap<>();
        Set<String> referenced = new LinkedHashSet<>();
        for (RouteProperties route : routes) {
            Map<String, List<Target>> byTopic =
                    map.computeIfAbsent(route.source(), k -> new LinkedHashMap<>());
            List<Target> targets = byTopic.computeIfAbsent(route.fromTopic(), k -> new ArrayList<>());
            for (RouteTarget rt : route.to()) {
                SinkTarget sink = sinksById.get(rt.sink());
                if (sink == null) {
                    throw new IllegalStateException(
                            "rota '" + route.id() + "' referencia sink inexistente: " + rt.sink());
                }
                targets.add(new Target(sink, rt.toTopic()));
                referenced.add(rt.sink());
            }
        }
        return new RouteTable(map, referenced);
    }

    public List<String> topicsFor(String sourceId) {
        Map<String, List<Target>> byTopic = bySourceThenTopic.get(sourceId);
        return byTopic == null ? List.of() : List.copyOf(byTopic.keySet());
    }

    public List<Target> targetsFor(String sourceId, String topic) {
        Map<String, List<Target>> byTopic = bySourceThenTopic.get(sourceId);
        if (byTopic == null) return List.of();
        return byTopic.getOrDefault(topic, List.of());
    }

    public Set<String> activeSourceIds() {
        return bySourceThenTopic.keySet();
    }

    public Set<String> referencedSinkIds() {
        return referencedSinkIds;
    }
}
```

- [ ] **Step 4: Rodar os testes**

Run: `mvn -o -q test -Dtest=RouteTableTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "RouteTable: resolve origem->tópico->alvos no boot"
```

---

### Task 4: Validação de rotas no `ConfigValidator`

Adiciona validação cruzada das rotas (ids únicos, source/sink existentes) ao lado da validação de pipelines (transitória). Build verde.

**Files:**
- Modify: `src/main/java/dev/nishisan/nstreamreplay/config/ConfigValidator.java`
- Test: `src/test/java/dev/nishisan/nstreamreplay/config/ConfigValidatorTest.java`

**Interfaces:**
- Consumes: `NStreamReplayProperties.routes()`, `RouteProperties`, `RouteTarget`.

- [ ] **Step 1: Escrever testes falhos** (rota com source/sink inexistente; id de rota duplicado)

Em `ConfigValidatorTest`, adicione helpers e testes (reaproveite os helpers `src`/`sink` existentes; adicione `route`):

```java
private static RouteProperties route(String id, String source, String fromTopic, String... sinkIds) {
    java.util.List<RouteTarget> to = new java.util.ArrayList<>();
    for (String s : sinkIds) to.add(new RouteTarget(s, null));
    return new RouteProperties(id, source, fromTopic, to);
}

@Test
void rejeitaRotaComSourceInexistente() {
    NStreamReplayProperties props = new NStreamReplayProperties(
            List.of(src("s1")), List.of(sink("d1")), null,
            List.of(route("r1", "ausente", "t", "d1")));
    assertThatThrownBy(() -> ConfigValidator.validate(props))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("ausente");
}

@Test
void rejeitaRotaComSinkInexistente() {
    NStreamReplayProperties props = new NStreamReplayProperties(
            List.of(src("s1")), List.of(sink("d1")), null,
            List.of(route("r1", "s1", "t", "fantasma")));
    assertThatThrownBy(() -> ConfigValidator.validate(props))
            .isInstanceOf(IllegalStateException.class).hasMessageContaining("fantasma");
}

@Test
void aceitaRotasValidas() {
    NStreamReplayProperties props = new NStreamReplayProperties(
            List.of(src("s1")), List.of(sink("d1"), sink("d2")), null,
            List.of(route("r1", "s1", "orders.in", "d1", "d2")));
    assertThatCode(() -> ConfigValidator.validate(props)).doesNotThrowAnyException();
}
```

(Os testes existentes que chamam `cfg(sources, sinks, pipelines)` precisam passar o 4º argumento `routes`; atualize o helper `cfg` para `cfg(sources, sinks, pipelines, routes)` ou adicione um overload. Os testes de pipeline existentes passam `routes=null`.)

- [ ] **Step 2: Rodar e ver falhar**

Run: `mvn -o -q test-compile`
Expected: FAIL (assinatura de `NStreamReplayProperties`/ausência de validação).

- [ ] **Step 3: Implementar validação de rotas** em `ConfigValidator.validate`

Após a validação existente, trate `routes` (null-safe enquanto coexiste com pipelines):

```java
List<RouteProperties> routes = props.routes() == null ? List.of() : props.routes();
uniqueOrThrow(routes, RouteProperties::id, "route");
for (RouteProperties route : routes) {
    if (!sourceIds.contains(route.source())) {
        throw new IllegalStateException(
                "rota '" + route.id() + "' referencia source inexistente: " + route.source());
    }
    for (RouteTarget t : route.to()) {
        if (!sinkIds.contains(t.sink())) {
            throw new IllegalStateException(
                    "rota '" + route.id() + "' referencia sink inexistente: " + t.sink());
        }
    }
}
```

(`sourceIds`/`sinkIds` já são calculados no método. Importe `RouteProperties`/`RouteTarget`.)

- [ ] **Step 4: Rodar os testes**

Run: `mvn -o -q test -Dtest=ConfigValidatorTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "ConfigValidator valida rotas (ids únicos, refs de source/sink)"
```

---

### Task 5: Migrar o runtime para rotas (quebra do modelo v1)

Vira a chave: o `KafkaSink` publica no tópico do record, o `SourceConsumer` roteia por tópico e resolve `destinationTopic`, e o `ReplayEngine` monta a `RouteTable`. Remove o uso de `pipelines`/`sink.topic`/`source.topics` no runtime e adapta os testes (unit) e os ITs para o modelo de rotas. Build verde ao fim (`mvn verify`).

**Files:**
- Modify: `src/main/java/dev/nishisan/nstreamreplay/sink/KafkaSink.java`
- Modify: `src/main/java/dev/nishisan/nstreamreplay/sink/SinkChannel.java` (chamada ao construtor de `KafkaSink`)
- Modify: `src/main/java/dev/nishisan/nstreamreplay/source/SourceConsumer.java`
- Modify: `src/main/java/dev/nishisan/nstreamreplay/runtime/ReplayEngine.java`
- Test: `KafkaSinkTest.java`, `SourceConsumerTest.java`, `SinkChannelTest`, `it/StreamReplayIT.java`

**Interfaces:**
- Consumes: `RouteTable`, `RouteTable.Target`, `ReplayRecord.destinationTopic()`.
- Produces:
  - `KafkaSink(SinkProperties settings)` e construtor de teste `KafkaSink(Producer<byte[],byte[]> producer, int maxPayloadBytes)` (sem `topic`).
  - `SourceConsumer(SourceProperties props, RouteTable routes, ReplayMetrics metrics)` e construtor de teste com `Supplier<Consumer<byte[],byte[]>>`.

- [ ] **Step 1: `KafkaSink` publica no tópico do record**

Em `KafkaSink.java`: remova o campo `topic` e o parâmetro `topic` dos construtores; `toProducerRecord` usa o tópico do record:

```java
public KafkaSink(SinkProperties settings) {
    this(buildProducer(settings), settings.maxRequestSize() - RECORD_OVERHEAD_BYTES);
}
KafkaSink(Producer<byte[], byte[]> producer, int maxPayloadBytes) {
    this.producer = producer;
    this.maxPayloadBytes = maxPayloadBytes;
}
private ProducerRecord<byte[], byte[]> toProducerRecord(ReplayRecord rec) {
    Long ts = rec.timestamp() >= 0 ? rec.timestamp() : null;
    List<Header> headers = new ArrayList<>(rec.headers().size());
    for (Map.Entry<String, byte[]> h : rec.headers().entrySet())
        headers.add(new RecordHeader(h.getKey(), h.getValue()));
    return new ProducerRecord<>(rec.destinationTopic(), null, ts, rec.key(), rec.value(), headers);
}
```

Nos logs que usavam `topic`, troque por `rec.destinationTopic()`.

- [ ] **Step 2: Ajustar `SinkChannel.open`** (construtor do `KafkaSink` sem topic)

```java
Sink sink = new KafkaSink(props);   // já sem topic — nada a mudar além de compilar
```

(O `KafkaSink(props)` continua válido; confirme que não há outra referência a `props.topic()`.)

- [ ] **Step 3: `SourceConsumer` roteia por tópico**

Troque a dependência `List<SinkTarget> targets` por `RouteTable routes`. Assine a união de tópicos e roteie por record:

```java
public SourceConsumer(SourceProperties props, RouteTable routes, ReplayMetrics metrics) {
    this(props, routes, metrics, () -> new KafkaConsumer<>(consumerProperties(props)));
}
SourceConsumer(SourceProperties props, RouteTable routes, ReplayMetrics metrics,
               Supplier<Consumer<byte[], byte[]>> consumerFactory) {
    this.props = props; this.routes = routes; this.metrics = metrics;
    this.consumerFactory = consumerFactory;
    this.pollTimeout = Duration.ofMillis(props.pollTimeoutMs());
}
// em run(): c.subscribe(routes.topicsFor(props.id()), new LoggingRebalanceListener());

void enqueueBatch(ConsumerRecords<byte[], byte[]> records) throws IOException {
    java.util.Set<SinkTarget> touched = new java.util.LinkedHashSet<>();
    for (ConsumerRecord<byte[], byte[]> record : records) {
        metrics.onConsumed(props.id());
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (RouteTable.Target t : routes.targetsFor(props.id(), record.topic())) {
            String destTopic = t.resolveTopic(record.topic());
            if (!seen.add(t.sink().id() + " " + destTopic)) continue;   // dedup por (sink,destTopic)
            t.sink().offer(toReplayRecord(record, destTopic));
            touched.add(t.sink());
        }
    }
    for (SinkTarget s : touched) if (s.requiresSyncBeforeCommit()) s.sync();
}

private static ReplayRecord toReplayRecord(ConsumerRecord<byte[], byte[]> r, String destinationTopic) {
    Map<String, byte[]> headers = new LinkedHashMap<>();
    for (Header h : r.headers()) headers.put(h.key(), h.value());
    return new ReplayRecord(r.topic(), r.partition(), r.offset(), r.timestamp(),
            r.key(), r.value(), headers, destinationTopic);
}
```

Remova o campo `syncTargets` (agora calculado por batch) e o `toReplayRecord` antigo de 1 argumento.

- [ ] **Step 4: `ReplayEngine` monta a `RouteTable`**

Em `start()`:

```java
openReferencedChannels(routeReferencedSinks());          // abre só os sinks referenciados pelas rotas
RouteTable routeTable = RouteTable.build(properties.routes(), channels);
channels.values().forEach(SinkChannel::startForwarder);
startSources(routeTable);
```

- `routeReferencedSinks()`: `Set<String>` = `properties.routes()` ⋃ `to[].sink`. (ou use `RouteTable.build(...).referencedSinkIds()` antes de abrir — mas `build` exige os channels; então calcule o set direto das rotas para abrir os channels, depois `build`.)
- `openReferencedChannels(Set<String> referenced)`: abre `SinkChannel.open(sink)` para cada `SinkProperties` cujo id ∈ referenced; avisa e ignora os demais.
- `startSources(RouteTable rt)`: para cada `SourceProperties` cujo id ∈ `rt.activeSourceIds()`, `new SourceConsumer(src, rt, metrics)` + thread `nsr-source-<id>`; avisa e ignora origens sem rota.

(Remova `PipelineRegistry` do import/uso.)

- [ ] **Step 5: Atualizar testes unitários afetados**

- `KafkaSinkTest`: construa `new KafkaSink(producer, 0)` (sem topic) e `new KafkaSink(producer, 10)`; os records já têm `destinationTopic` (Task 1). Adicione asserção de que `producer.sent.get(i).topic()` == `destinationTopic` do record.
- `SourceConsumerTest`: construa via `RouteTable`. Ex.: `RouteTable rt = RouteTable.build(List.of(new RouteProperties("r","s1","t", List.of(new RouteTarget("a", null), new RouteTarget("b","b.out")))), Map.of("a", a, "b", b));` com `FakeSinkTarget a,b`; `new SourceConsumer(source("s1"), rt, NOOP, () -> mock)`. Verifique que `a` recebe com `destinationTopic="t"` (preserva) e `b` com `"b.out"`. Adicione um teste de roteamento por tópico: rota de `t1`→`a` e `t2`→`b`; records de `t1` só vão a `a`, de `t2` só a `b`.
- `SinkChannelTest`: nenhuma mudança de assinatura além do `KafkaSink` (usa `FakeSink`, não `KafkaSink`); confirme compilação.
- `StreamReplayIT`: migre o config programático para rotas (veja Task 7) — no mínimo ajuste para compilar/passar com o novo `SourceConsumer(props, routeTable, metrics)` e sinks sem topic. (O detalhamento dos cenários novos fica na Task 7; aqui basta deixar o IT existente verde no modelo de rotas.)

- [ ] **Step 6: Rodar a suíte completa**

Run: `mvn -q verify`
Expected: PASS (unit + IT). Se algum teste v1 ainda referenciar `sink.topic`/`pipelines`, ajuste para rotas.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "Runtime roteia por tópico (KafkaSink/SourceConsumer/ReplayEngine via RouteTable)"
```

---

### Task 6: Remover o modelo v1 morto

Remove `pipelines`, `PipelineProperties`, `PipelineRegistry`, `source.topics`, `sink.topic` e limpa testes. Build verde.

**Files:**
- Modify: `NStreamReplayProperties.java` (remover `pipelines`), `SourceProperties.java` (remover `topics`), `SinkProperties.java` (remover `topic`), `ConfigValidator.java` (remover ramo de pipelines)
- Delete: `config/PipelineProperties.java`, `pipeline/PipelineRegistry.java`, `src/test/java/dev/nishisan/nstreamreplay/pipeline/PipelineRegistryTest.java`
- Modify (testes): `NStreamReplayPropertiesBindingTest`, `ConfigValidatorTest`, e quaisquer testes/configs que ainda citem `topics:`/`topic:`/`pipelines:`

- [ ] **Step 1: Remover campos e tipos v1**

- `NStreamReplayProperties`: remover o parâmetro `pipelines`; assinatura final `(@NotEmpty sources, @NotEmpty sinks, @NotEmpty @Valid List<RouteProperties> routes)`.
- `SourceProperties`: remover `topics` (e o `@NotEmpty`); manter o resto.
- `SinkProperties`: remover `topic` (e o `@NotBlank`); manter o resto.
- `ConfigValidator`: remover o bloco que valida `props.pipelines()`; `routes` passa a ser obrigatório (`@NotEmpty` no record).
- Deletar `PipelineProperties.java`, `PipelineRegistry.java`, `PipelineRegistryTest.java`.

- [ ] **Step 2: Limpar os testes/binding**

- `NStreamReplayPropertiesBindingTest`: remover `topics:`/`topic:`/`pipelines:` dos YAMLs; manter só `routes`. Garanta que o source não declara mais `topics` e o sink não declara `topic`.
- `ConfigValidatorTest`: atualizar o helper `cfg`/`src`/`sink`/`route` para a assinatura final (sem pipelines); remover testes específicos de pipeline.

- [ ] **Step 3: Rodar a suíte completa**

Run: `mvn -q verify`
Expected: PASS. (Se o compilador apontar referências remanescentes a `topics()`/`topic()`/`pipelines()`, corrija.)

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "Remove modelo v1 (pipelines, source.topics, sink.topic)"
```

---

### Task 7: ITs do modelo de rotas (de-para, fan-out, merge, espelho)

Reescreve `StreamReplayIT` para validar os cenários do modelo de rotas com Kafka real.

**Files:**
- Modify: `src/test/java/dev/nishisan/nstreamreplay/it/StreamReplayIT.java`

**Interfaces:**
- Consumes: `RouteTable`, `RouteProperties`, `RouteTarget`, `SinkChannel`, `SourceConsumer(props, routeTable, metrics)`.

- [ ] **Step 1: Helpers de rota no IT**

Adicione helper para montar `RouteTable` a partir de `SinkChannel`s e iniciar um `SourceConsumer`:

```java
private SourceConsumer startSource(String sourceId, RouteTable rt) {
    SourceConsumer src = new SourceConsumer(source(sourceId), rt, NOOP);
    Thread t = new Thread(src, sourceId + "-thread"); t.setDaemon(true); t.start();
    return src;
}
```

Onde `source(id)` cria `SourceProperties` sem `topics` (já removido na Task 6) e `NOOP = new ReplayMetrics((StatsUtils) null)`.

- [ ] **Step 2: Teste de-para (tópico A→sink X, tópico B→sink Y)**

```java
@Test
void deParaRoteiaCadaTopicoParaSeuSink(@TempDir Path tmp) throws Exception {
    SinkChannel x = SinkChannel.open(sinkConn("x", bootstrap(), tmp.resolve("x")));
    SinkChannel y = SinkChannel.open(sinkConn("y", bootstrap(), tmp.resolve("y")));
    x.startForwarder(); y.startForwarder();
    RouteTable rt = RouteTable.build(List.of(
            new RouteProperties("rA", "s", "dp.a", List.of(new RouteTarget("x", "dp.x"))),
            new RouteProperties("rB", "s", "dp.b", List.of(new RouteTarget("y", "dp.y")))),
            Map.of("x", x, "y", y));
    SourceConsumer src = startSource("s", rt);
    try {
        produceTo("dp.a", 5);   // 5 msgs no tópico A
        produceTo("dp.b", 7);   // 7 msgs no tópico B
        assertThat(consumeValues("dp.x", 5, Duration.ofSeconds(30))).hasSize(5);
        assertThat(consumeValues("dp.y", 7, Duration.ofSeconds(30))).hasSize(7);
    } finally { src.stop(); x.close(); y.close(); }
}
```

(Adapte `sinkConn(id, bootstrap, basePath)` = `SinkProperties` sem topic; `produceTo(topic, n)` = produz n mensagens no tópico dado; `source("s")` consome a união `dp.a`,`dp.b` via `rt.topicsFor`.)

- [ ] **Step 3: Testes fan-out, merge e espelho**

- **fan-out:** uma rota `from "fo.in"` com `to: [{x,"fo.x"},{y,"fo.y"}]`; produzir N em `fo.in`; conferir N em `fo.x` e N em `fo.y`.
- **merge:** duas rotas `from "m.a"`→`{x,"m.out"}` e `from "m.b"`→`{x,"m.out"}`; produzir em ambos; conferir soma em `m.out`.
- **espelho:** rota `from "mir.in"` com `to: [{x}]` (toTopic omitido); conferir as mensagens em `mir.in` no cluster do sink x (preserva o nome).

- [ ] **Step 4: Rodar os ITs**

Run: `mvn -q verify`
Expected: PASS (todos os cenários de rota verdes).

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "ITs de roteamento por tópico (de-para, fan-out, merge, espelho)"
```

---

### Task 8: Documentação, exemplo e bump 2.0.0

Atualiza a doc/diagramas/exemplo para o modelo de rotas e sobe a versão.

**Files:**
- Modify: `pom.xml` (`<version>2.0.0</version>`), `README.md`, `docs/architecture.md`, `docs/usage-example.md`, `docs/examples/acmepay-relay.yaml`, `docs/diagrams/c4_container.puml`, `docs/diagrams/sequence_store_and_forward.puml`, `config/n-stream-replay.yaml`

- [ ] **Step 1: Bump da versão**

Em `pom.xml`, `<version>1.0.0</version>` → `<version>2.0.0</version>`.

- [ ] **Step 2: Migrar `config/n-stream-replay.yaml` e o exemplo AcmePay para rotas**

Reescreva `config/n-stream-replay.yaml` e `docs/examples/acmepay-relay.yaml` no modelo de rotas: `sources` sem `topics`, `sinks` sem `topic`, e bloco `routes:` (use os exemplos do spec). No AcmePay, expresse `orders.events` → `{dr-rio}` (preserva) + `{analytics, toTopic: orders.raw}`, e `payments.events` → `{fraud}` + `{cold-archive, toTopic: payments.archive}`.

- [ ] **Step 3: Atualizar README, arquitetura, exemplo de uso e diagramas**

- README: tabela de modelo (source/sink/route), remover menção a `sink.topic`/`source.topics`.
- `docs/architecture.md`: descrever o modelo de rotas, `destinationTopic`, e a tabela de componentes (RouteTable no lugar de PipelineRegistry).
- `docs/usage-example.md`: atualizar a config e a narrativa para rotas.
- `docs/diagrams/*.puml`: trocar “pipeline” por “route”; no sequence, mostrar resolução de `destinationTopic` por rota. Validar a sintaxe: `java -jar <plantuml.jar> -checkonly -failfast2 docs/diagrams/*.puml`.

- [ ] **Step 4: Verificação final + smoke**

Run: `mvn -q verify`
Expected: PASS. Em seguida, smoke do fat jar com o `config/n-stream-replay.yaml` de rotas (basePath em diretório gravável, broker `localhost:9092`): confirmar `ReplayEngine iniciado` e métricas `nstreamreplay_sink_*` em `/actuator/prometheus`.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "Documentação e versão 2.0.0 (modelo de rotas)"
```

---

## Notas de execução

- Release: após o merge, publicar a tag/release `2.0.0` com nota de migração v1→v2 (configs antigas não são mais válidas; limpar/usar novo `basePath` para as filas, pois o formato do `ReplayRecord` mudou).
- Sequência segura: Tasks 1→4 mantêm o build verde de forma aditiva; a Task 5 é a virada de comportamento (e ponto de maior risco); a Task 6 remove o legado; 7–8 fecham ITs e docs.
