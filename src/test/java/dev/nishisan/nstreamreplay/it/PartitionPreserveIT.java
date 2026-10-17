package dev.nishisan.nstreamreplay.it;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.NStreamReplayProperties;
import dev.nishisan.nstreamreplay.config.OnWriteError;
import dev.nishisan.nstreamreplay.config.OverflowPolicy;
import dev.nishisan.nstreamreplay.config.Partitioning;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.config.SinkProperties;
import dev.nishisan.nstreamreplay.config.SourceProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import dev.nishisan.nstreamreplay.route.RouteTable;
import dev.nishisan.nstreamreplay.runtime.AdminTopicPartitionCounter;
import dev.nishisan.nstreamreplay.runtime.PartitionPreserveCheck;
import dev.nishisan.nstreamreplay.sink.SinkChannel;
import dev.nishisan.nstreamreplay.sink.SinkTarget;
import dev.nishisan.nstreamreplay.source.SourceConsumer;
import dev.nishisan.nstreamreplay.stats.ReplayMetrics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Integração (Kafka real) do espelhamento com {@code partitioning: preserve} e da fila sem descarte
 * ({@code overflowPolicy: block}). O cenário de referência é um tópico de CDC cujos marcadores de
 * controle (mesma key fixa) são publicados uma vez em CADA partição: reparticionar pela key jogaria
 * as cópias numa partição só e quebraria quem reconstrói o estado por partição.
 */
@Testcontainers
class PartitionPreserveIT {

    private static final byte[] CONTROL_KEY = "__nishidb_control__".getBytes(StandardCharsets.UTF_8);
    private static final byte[] BINARY_KEY = {0x00, (byte) 0xFF, 0x7F, (byte) 0x80};
    private static final int PARTITIONS = 4;
    private static final int PER_PARTITION = 30;
    private static final ReplayMetrics NOOP = new ReplayMetrics((dev.nishisan.utils.stats.StatsUtils) null);

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    /** Segundo cluster, usado como destino que "cai" (container pausado) no teste de block. */
    @Container
    static final KafkaContainer KAFKA_DEST = new KafkaContainer("apache/kafka:3.8.0");

    /** Registro normalizado para comparação: bytes em hex (a key do CDC não é texto). */
    record Seen(String key, String value, Map<String, String> headers) { }

    // ---------- helpers ----------

    private static String hex(byte[] bytes) {
        return bytes == null ? "null" : java.util.HexFormat.of().formatHex(bytes);
    }

    private static void createTopic(String bootstrap, String topic, int partitions) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private static Properties producerProps(String bootstrap) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }

    /**
     * Produz o dataset de CDC na origem, com partição explícita: por partição, um marcador
     * {@code reset} (key de controle), registros de dados (keys distintas, uma binária e uma igual
     * em todas as partições) e um marcador {@code snapshot_end}. Devolve o esperado por partição.
     */
    private static Map<Integer, List<Seen>> produceCdcDataset(String bootstrap, String topic) {
        Map<Integer, List<Seen>> expected = new TreeMap<>();
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps(bootstrap))) {
            for (int p = 0; p < PARTITIONS; p++) {
                List<Seen> list = new ArrayList<>();
                for (int i = 0; i < PER_PARTITION; i++) {
                    byte[] key;
                    String reason = null;
                    if (i == 0) {
                        key = CONTROL_KEY;
                        reason = "reset";
                    } else if (i == PER_PARTITION - 1) {
                        key = CONTROL_KEY;
                        reason = "snapshot_end";
                    } else if (i == 5) {
                        key = BINARY_KEY;
                    } else if (i == 6) {
                        key = "shared-entity".getBytes(StandardCharsets.UTF_8);   // mesma key nas 4 partições
                    } else {
                        key = ("entity-" + p + "-" + i).getBytes(StandardCharsets.UTF_8);
                    }
                    byte[] value = ("p" + p + "-i" + i).getBytes(StandardCharsets.UTF_8);
                    List<Header> headers = new ArrayList<>();
                    Map<String, String> seenHeaders = new LinkedHashMap<>();
                    if (reason != null) {
                        headers.add(new RecordHeader("nishidb-reason", reason.getBytes(StandardCharsets.UTF_8)));
                        seenHeaders.put("nishidb-reason", hex(reason.getBytes(StandardCharsets.UTF_8)));
                    }
                    producer.send(new ProducerRecord<>(topic, p, null, key, value, headers));
                    list.add(new Seen(hex(key), hex(value), seenHeaders));
                }
                expected.put(p, list);
            }
            producer.flush();
        }
        return expected;
    }

    /** Lê o tópico inteiro por partição (assign + seekToBeginning) até {@code expectedTotal} registros. */
    private static Map<Integer, List<Seen>> readByPartition(String bootstrap, String topic, int expectedTotal,
                                                            Duration timeout) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        Map<Integer, List<Seen>> out = new TreeMap<>();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream()
                    .map(i -> new TopicPartition(topic, i.partition())).toList();
            c.assign(tps);
            c.seekToBeginning(tps);
            int total = 0;
            long deadline = System.nanoTime() + timeout.toNanos();
            while (total < expectedTotal && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(300))) {
                    Map<String, String> headers = new LinkedHashMap<>();
                    for (Header h : r.headers()) {
                        headers.put(h.key(), hex(h.value()));
                    }
                    out.computeIfAbsent(r.partition(), k -> new ArrayList<>())
                            .add(new Seen(hex(r.key()), hex(r.value()), headers));
                    total++;
                }
            }
        }
        return out;
    }

    private SinkChannel openSink(String id, String bootstrap, Path base, int maxDepth, int batchSize,
                                 OverflowPolicy policy) throws Exception {
        QueueProperties queue = new QueueProperties(base.toString(), maxDepth, Duration.ZERO,
                Durability.SYNC_ON_COMMIT, OnWriteError.HALT, batchSize, 200L, policy);
        SinkProperties props = new SinkProperties(id, bootstrap, "all", 0, "lz4", 1_048_576, Map.of(), queue);
        SinkChannel ch = SinkChannel.open(props);
        ch.startForwarder();
        return ch;
    }

    private SourceConsumer startSource(String id, String bootstrap, String group, int maxPoll,
                                       Map<String, String> extra, RouteTable routes) {
        SourceProperties props = new SourceProperties(id, bootstrap, group, null, "earliest", maxPoll, 300L, 0L, extra);
        SourceConsumer src = new SourceConsumer(props, routes, NOOP);
        Thread t = new Thread(src, id + "-thread");
        t.setDaemon(true);
        t.start();
        return src;
    }

    private static RouteTable cdcRoute(String sourceId, String from, String to, Partitioning mode, SinkChannel sink) {
        return RouteTable.build(List.of(new RouteProperties("cdc", sourceId, from,
                List.of(new RouteTarget("mirror", to, null, mode)))), Map.<String, SinkTarget>of("mirror", sink));
    }

    private static void assertMirroredPerPartition(Map<Integer, List<Seen>> expected, Map<Integer, List<Seen>> actual) {
        assertThat(actual.keySet()).isEqualTo(expected.keySet());
        for (var entry : expected.entrySet()) {
            // mesma partição, mesma ordem, key crua e headers idênticos
            assertThat(actual.get(entry.getKey()))
                    .as("partição %d", entry.getKey())
                    .containsExactlyElementsOf(entry.getValue());
        }
    }

    // ---------- preserve ----------

    @Test
    void preserveMantemParticaoOrdemKeyCrusEHeaders(@TempDir Path tmp) throws Exception {
        String src = "cdc.src." + UUID.randomUUID();
        String dst = "cdc.dst." + UUID.randomUUID();
        createTopic(KAFKA.getBootstrapServers(), src, PARTITIONS);
        createTopic(KAFKA.getBootstrapServers(), dst, PARTITIONS);
        Map<Integer, List<Seen>> expected = produceCdcDataset(KAFKA.getBootstrapServers(), src);

        SinkChannel sink = openSink("mirror", KAFKA.getBootstrapServers(), tmp, 100_000, 256, OverflowPolicy.BLOCK);
        SourceConsumer source = startSource("cdc-src", KAFKA.getBootstrapServers(), "grp-" + UUID.randomUUID(), 100,
                Map.of(), cdcRoute("cdc-src", src, dst, Partitioning.PRESERVE, sink));
        try {
            Map<Integer, List<Seen>> actual = readByPartition(KAFKA.getBootstrapServers(), dst,
                    PARTITIONS * PER_PARTITION, Duration.ofSeconds(60));

            assertMirroredPerPartition(expected, actual);
            // os marcadores de controle (mesma key) chegaram uma vez em CADA partição
            assertThat(actual.values()).allSatisfy(list -> assertThat(
                    list.stream().filter(s -> s.key().equals(hex(CONTROL_KEY))).count()).isEqualTo(2));
            assertThat(sink.dropped()).isZero();
        } finally {
            source.stop();
            sink.close();
        }
    }

    @Test
    void modoKeyJuntaOsMarcadoresDeControleEQuebraAsMesmasAsserções(@TempDir Path tmp) throws Exception {
        String src = "cdc.src." + UUID.randomUUID();
        String dst = "cdc.dst." + UUID.randomUUID();
        createTopic(KAFKA.getBootstrapServers(), src, PARTITIONS);
        createTopic(KAFKA.getBootstrapServers(), dst, PARTITIONS);
        Map<Integer, List<Seen>> expected = produceCdcDataset(KAFKA.getBootstrapServers(), src);

        SinkChannel sink = openSink("mirror", KAFKA.getBootstrapServers(), tmp, 100_000, 256, OverflowPolicy.BLOCK);
        SourceConsumer source = startSource("cdc-src", KAFKA.getBootstrapServers(), "grp-" + UUID.randomUUID(), 100,
                Map.of(), cdcRoute("cdc-src", src, dst, Partitioning.KEY, sink));
        try {
            Map<Integer, List<Seen>> actual = readByPartition(KAFKA.getBootstrapServers(), dst,
                    PARTITIONS * PER_PARTITION, Duration.ofSeconds(60));

            // Controle negativo: com partitioning=key as 8 cópias do marcador caem numa partição só...
            Set<Integer> controlPartitions = new HashSet<>();
            actual.forEach((partition, list) -> {
                if (list.stream().anyMatch(s -> s.key().equals(hex(CONTROL_KEY)))) {
                    controlPartitions.add(partition);
                }
            });
            assertThat(controlPartitions).hasSize(1);
            // ...e a asserção que o modo preserve satisfaz FALHA.
            assertThatThrownBy(() -> assertMirroredPerPartition(expected, actual))
                    .isInstanceOf(AssertionError.class);
        } finally {
            source.stop();
            sink.close();
        }
    }

    // ---------- checagem de subida (Admin real) ----------

    @Test
    void checagemDeSubidaUsaAdminRealParaContarParticoes() throws Exception {
        String src = "chk.src." + UUID.randomUUID();
        String okDst = "chk.ok." + UUID.randomUUID();
        String smallDst = "chk.small." + UUID.randomUUID();
        String missing = "chk.missing." + UUID.randomUUID();
        createTopic(KAFKA.getBootstrapServers(), src, PARTITIONS);
        createTopic(KAFKA.getBootstrapServers(), okDst, PARTITIONS + 2);
        createTopic(KAFKA.getBootstrapServers(), smallDst, 2);

        var counts = new AdminTopicPartitionCounter().partitionCounts(
                Map.of("bootstrap.servers", KAFKA.getBootstrapServers()), Set.of(src, okDst, missing));
        assertThat(counts).containsEntry(src, PARTITIONS).containsEntry(okDst, PARTITIONS + 2)
                .doesNotContainKey(missing);

        assertThat(violation(src, okDst)).isNull();
        assertThat(violation(src, smallDst)).contains("2 partição").contains("4 partição").contains(smallDst);
        assertThat(violation(src, missing)).contains("não existe").contains(missing);
    }

    private static String violation(String from, String to) {
        SourceProperties source = new SourceProperties("s", KAFKA.getBootstrapServers(), "g", null, "earliest",
                500, 1000L, 0L, Map.of());
        SinkProperties sink = new SinkProperties("d", KAFKA.getBootstrapServers(), "all", 0, "lz4", 1_048_576,
                Map.of(), new QueueProperties("var/q", 10, Duration.ZERO, Durability.OS_MANAGED,
                OnWriteError.HALT, 10, 10L));
        NStreamReplayProperties props = new NStreamReplayProperties(List.of(source), List.of(sink), List.of(
                new RouteProperties("r", "s", from, List.of(new RouteTarget("d", to, null, Partitioning.PRESERVE)))));
        try {
            PartitionPreserveCheck.verify(props, new AdminTopicPartitionCounter());
            return null;
        } catch (IllegalStateException e) {
            return e.getMessage();
        }
    }

    // ---------- partição inválida não descarta ----------

    @Test
    void particaoDeDestinoInexistenteNaoDescartaORegistroERecuperaQuandoAParticaoExiste(@TempDir Path tmp)
            throws Exception {
        String dst = "bad.dst." + UUID.randomUUID();
        createTopic(KAFKA.getBootstrapServers(), dst, 2);
        SinkChannel sink = openSink("mirror", KAFKA.getBootstrapServers(), tmp, 1_000, 10, OverflowPolicy.BLOCK);
        try {
            // p0 e p1 válidas; p3 não existe no destino (2 partições): a cabeça válida sai, o resto fica.
            for (int partition : new int[]{0, 1, 3, 0}) {
                sink.offer(new ReplayRecord("src", partition, 0L, 1L, CONTROL_KEY,
                        ("v" + partition).getBytes(StandardCharsets.UTF_8), Map.of(), dst, partition));
            }
            sink.sync();
            await().atMost(Duration.ofSeconds(30)).until(() -> sink.publishedTotal() == 2);
            Thread.sleep(8_000);   // várias tentativas (max.block.ms=5s) com a partição ausente
            assertThat(sink.publishedTotal()).isEqualTo(2);
            assertThat(sink.poisonedTotal()).isZero();     // não foi tratado como veneno
            assertThat(sink.dropped()).isZero();
            assertThat(sink.depth()).isEqualTo(2);         // p3 e o que veio depois continuam na fila

            try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                admin.createPartitions(Map.of(dst, NewPartitions.increaseTo(4))).all().get();
            }
            await().atMost(Duration.ofSeconds(60)).until(() -> sink.publishedTotal() == 4);
            assertThat(sink.depth()).isZero();
            Map<Integer, List<Seen>> actual = readByPartition(KAFKA.getBootstrapServers(), dst, 4, Duration.ofSeconds(30));
            assertThat(actual.get(3)).hasSize(1);
            assertThat(actual.get(0)).hasSize(2);
        } finally {
            sink.close();
        }
    }

    // ---------- block: destino cai, nada se perde, o grupo não sofre rebalance ----------

    private static Set<String> memberIds(String bootstrap, String group) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap))) {
            var description = admin.describeConsumerGroups(List.of(group)).describedGroups().get(group).get();
            Set<String> ids = new HashSet<>();
            description.members().forEach(m -> ids.add(m.consumerId()));
            return ids;
        }
    }

    @Test
    void blockSemPerdaComDestinoForaEGrupoSemRebalance(@TempDir Path tmp) throws Exception {
        String src = "blk.src." + UUID.randomUUID();
        String dst = "blk.dst." + UUID.randomUUID();
        String group = "blk-grp-" + UUID.randomUUID();
        int total = 200;
        createTopic(KAFKA.getBootstrapServers(), src, 2);
        createTopic(KAFKA_DEST.getBootstrapServers(), dst, 2);

        // fila pequena (maxDepth=20) e max.poll.interval curto: bloquear o poll estouraria o intervalo
        SinkChannel sink = openSink("mirror", KAFKA_DEST.getBootstrapServers(), tmp, 20, 5, OverflowPolicy.BLOCK);
        SourceConsumer source = startSource("blk-src", KAFKA.getBootstrapServers(), group, 5,
                Map.of("max.poll.interval.ms", "5000", "session.timeout.ms", "6000", "heartbeat.interval.ms", "1000"),
                cdcRoute("blk-src", src, dst, Partitioning.PRESERVE, sink));
        var dockerClient = KAFKA_DEST.getDockerClient();
        boolean paused = false;
        try {
            // garante o grupo estável com um membro antes de derrubar o destino
            produceNumbered(src, 0, 10);
            await().atMost(Duration.ofSeconds(30)).until(() -> sink.publishedTotal() >= 10);
            Set<String> membersBefore = memberIds(KAFKA.getBootstrapServers(), group);
            assertThat(membersBefore).hasSize(1);

            dockerClient.pauseContainerCmd(KAFKA_DEST.getContainerId()).exec();
            paused = true;
            produceNumbered(src, 10, total);

            await().atMost(Duration.ofSeconds(60)).until(() -> sink.saturated() && source.pausedPartitions() == 2);
            long maxDepthSeen = 0;
            // segura o destino fora bem mais que max.poll.interval.ms (5 s): o loop de poll tem de seguir vivo
            long until = System.nanoTime() + Duration.ofSeconds(14).toNanos();
            while (System.nanoTime() < until) {
                maxDepthSeen = Math.max(maxDepthSeen, sink.depth());
                assertThat(sink.dropped()).isZero();
                Thread.sleep(250);
            }
            assertThat(maxDepthSeen).isBetween(20L, 20L + 5L);   // marca d'água + no máximo um lote
            assertThat(memberIds(KAFKA.getBootstrapServers(), group)).isEqualTo(membersBefore);   // sem rebalance
            assertThat(source.consumedTotal()).isLessThan(total);                                // origem freada

            dockerClient.unpauseContainerCmd(KAFKA_DEST.getContainerId()).exec();
            paused = false;

            Map<Integer, List<Seen>> actual = readByPartition(KAFKA_DEST.getBootstrapServers(), dst, total,
                    Duration.ofSeconds(90));
            Set<String> values = new HashSet<>();
            actual.values().forEach(list -> list.forEach(s -> values.add(s.value())));
            assertThat(values).hasSize(total);              // nenhum registro perdido (duplicata seria aceitável)
            // ordem por partição: a 1ª ocorrência de cada registro respeita a ordem da origem
            actual.forEach((partition, list) -> {
                List<Integer> firstOccurrences = list.stream().map(Seen::value).distinct()
                        .map(v -> Integer.parseInt(new String(java.util.HexFormat.of().parseHex(v),
                                StandardCharsets.UTF_8).substring(1))).toList();
                assertThat(firstOccurrences).as("partição %d", partition).isSorted();
                assertThat(firstOccurrences).allMatch(n -> n % 2 == partition);
            });
            assertThat(sink.dropped()).isZero();
            assertThat(memberIds(KAFKA.getBootstrapServers(), group)).isEqualTo(membersBefore);
            await().atMost(Duration.ofSeconds(30)).until(() -> !sink.saturated() && source.pausedPartitions() == 0);
        } finally {
            if (paused) {
                dockerClient.unpauseContainerCmd(KAFKA_DEST.getContainerId()).exec();
            }
            source.stop();
            sink.close();
        }
    }

    private static void produceNumbered(String topic, int fromInclusive, int toExclusive) {
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps(KAFKA.getBootstrapServers()))) {
            for (int i = fromInclusive; i < toExclusive; i++) {
                producer.send(new ProducerRecord<>(topic, i % 2, null,
                        ("k" + i).getBytes(StandardCharsets.UTF_8), ("n" + i).getBytes(StandardCharsets.UTF_8)));
            }
            producer.flush();
        }
    }
}
