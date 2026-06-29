package dev.nishisan.nstreamreplay.it;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.OnWriteError;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.config.SinkProperties;
import dev.nishisan.nstreamreplay.config.SourceProperties;
import dev.nishisan.nstreamreplay.route.RouteTable;
import dev.nishisan.nstreamreplay.sink.SinkChannel;
import dev.nishisan.nstreamreplay.sink.SinkTarget;
import dev.nishisan.nstreamreplay.source.SourceConsumer;
import dev.nishisan.nstreamreplay.stats.ReplayMetrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integração end-to-end (Kafka real via Testcontainers) do modelo de rotas v2: de-para por tópico,
 * fan-out, merge e isolamento de destino offline. (Modo espelho — toTopic omitido — é coberto por
 * SourceConsumerTest; num único cluster ele criaria loop origem=destino.)
 */
@Testcontainers
class StreamReplayIT {

    private static final String DEAD_BROKER = "localhost:1";

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    private static final ReplayMetrics NOOP = new ReplayMetrics((dev.nishisan.utils.stats.StatsUtils) null);

    private String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    // ---------- helpers ----------

    private void produce(String topic, int n) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> prod = new KafkaProducer<>(p)) {
            for (int i = 0; i < n; i++) {
                prod.send(new ProducerRecord<>(topic,
                        ("k" + i).getBytes(StandardCharsets.UTF_8),
                        ("v" + i).getBytes(StandardCharsets.UTF_8)));
            }
            prod.flush();
        }
    }

    private List<String> consumeValues(String topic, int expected, Duration timeout) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        List<String> values = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            c.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (values.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(300));
                for (ConsumerRecord<byte[], byte[]> r : recs) {
                    values.add(new String(r.value(), StandardCharsets.UTF_8));
                }
            }
        }
        return values;
    }

    private SinkChannel openSink(String id, String bootstrap, Path base) throws IOException {
        QueueProperties queue = new QueueProperties(base.toString(), 1_000_000, Duration.ZERO,
                Durability.OS_MANAGED, OnWriteError.DROP, 256, 200L);
        // topic é ignorado no v2 (vem da rota); mantido não-vazio só por validação do record.
        SinkProperties props = new SinkProperties(id, bootstrap, "unused", "all", 0, "lz4",
                1_048_576, Map.of(), queue);
        SinkChannel ch = SinkChannel.open(props);
        ch.startForwarder();
        return ch;
    }

    private SourceConsumer startSource(String id, RouteTable routes) {
        SourceProperties props = new SourceProperties(id, bootstrap(), List.of("ignored"),
                "grp-" + UUID.randomUUID(), null, "earliest", 500, 400L, Map.of());
        SourceConsumer src = new SourceConsumer(props, routes, NOOP);
        Thread t = new Thread(src, id + "-thread");
        t.setDaemon(true);
        t.start();
        return src;
    }

    private static RouteTarget to(String sink, String toTopic) {
        return new RouteTarget(sink, toTopic);
    }

    // ---------- testes ----------

    @Test
    void deParaRoteiaCadaTopicoParaSeuDestino(@TempDir Path tmp) throws Exception {
        SinkChannel x = openSink("x", bootstrap(), tmp.resolve("x"));
        SinkChannel y = openSink("y", bootstrap(), tmp.resolve("y"));
        RouteTable rt = RouteTable.build(List.of(
                new RouteProperties("rA", "s", "dp.a", List.of(to("x", "dp.x"))),
                new RouteProperties("rB", "s", "dp.b", List.of(to("y", "dp.y")))),
                Map.<String, SinkTarget>of("x", x, "y", y));
        SourceConsumer src = startSource("s", rt);
        try {
            produce("dp.a", 5);
            produce("dp.b", 7);
            assertThat(consumeValues("dp.x", 5, Duration.ofSeconds(30))).hasSize(5);
            assertThat(consumeValues("dp.y", 7, Duration.ofSeconds(30))).hasSize(7);
        } finally {
            src.stop();
            x.close();
            y.close();
        }
    }

    @Test
    void fanoutUmTopicoParaVariosDestinos(@TempDir Path tmp) throws Exception {
        SinkChannel x = openSink("x", bootstrap(), tmp.resolve("x"));
        SinkChannel y = openSink("y", bootstrap(), tmp.resolve("y"));
        RouteTable rt = RouteTable.build(List.of(
                new RouteProperties("r", "s", "fo.in", List.of(to("x", "fo.x"), to("y", "fo.y")))),
                Map.<String, SinkTarget>of("x", x, "y", y));
        SourceConsumer src = startSource("s", rt);
        try {
            produce("fo.in", 6);
            assertThat(consumeValues("fo.x", 6, Duration.ofSeconds(30))).hasSize(6);
            assertThat(consumeValues("fo.y", 6, Duration.ofSeconds(30))).hasSize(6);
        } finally {
            src.stop();
            x.close();
            y.close();
        }
    }

    @Test
    void mergeVariosTopicosNoMesmoDestino(@TempDir Path tmp) throws Exception {
        SinkChannel x = openSink("x", bootstrap(), tmp.resolve("x"));
        RouteTable rt = RouteTable.build(List.of(
                new RouteProperties("rA", "s", "m.a", List.of(to("x", "m.out"))),
                new RouteProperties("rB", "s", "m.b", List.of(to("x", "m.out")))),
                Map.<String, SinkTarget>of("x", x));
        SourceConsumer src = startSource("s", rt);
        try {
            produce("m.a", 4);
            produce("m.b", 3);
            assertThat(consumeValues("m.out", 7, Duration.ofSeconds(30))).hasSize(7);
        } finally {
            src.stop();
            x.close();
        }
    }

    @Test
    void destinoOfflineNaoBloqueiaOrigemNemDestinoSaudavel(@TempDir Path tmp) throws Exception {
        SinkChannel healthy = openSink("ok", bootstrap(), tmp.resolve("ok"));
        SinkChannel dead = openSink("dead", DEAD_BROKER, tmp.resolve("dead"));
        RouteTable rt = RouteTable.build(List.of(
                new RouteProperties("r", "s", "iso.in", List.of(to("ok", "iso.out"), to("dead", "iso.dead")))),
                Map.<String, SinkTarget>of("ok", healthy, "dead", dead));
        SourceConsumer src = startSource("s", rt);
        try {
            produce("iso.in", 8);
            assertThat(consumeValues("iso.out", 8, Duration.ofSeconds(30))).hasSize(8);
            // destino offline retém tudo sem travar a origem/o destino saudável.
            await().atMost(Duration.ofSeconds(20)).until(() -> dead.depth() == 8);
            assertThat(dead.publishedTotal()).isZero();
        } finally {
            src.stop();
            healthy.close();
            dead.close();
        }
    }
}
