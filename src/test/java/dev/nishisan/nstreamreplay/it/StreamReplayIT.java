package dev.nishisan.nstreamreplay.it;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.OnWriteError;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.config.SinkProperties;
import dev.nishisan.nstreamreplay.config.SourceProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import dev.nishisan.nstreamreplay.sink.SinkChannel;
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
 * Integração end-to-end contra um Kafka real (Testcontainers): fan-out 1->N, isolamento de um
 * destino offline e recuperação store-and-forward após "restart".
 */
@Testcontainers
class StreamReplayIT {

    private static final String DEAD_BROKER = "localhost:1";   // nada escutando: destino offline

    // apache/kafka:3.9.0 rejeita advertised.listeners=0.0.0.0 no format (incompat. com o
    // KafkaContainer do Testcontainers 1.21.x); 3.8.0 funciona e é compatível com o cliente 3.9.0.
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    private String bootstrap() {
        return KAFKA.getBootstrapServers();
    }

    // ---------- helpers ----------

    private static String s(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

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
                    values.add(s(r.value()));
                }
            }
        }
        return values;
    }

    private static QueueProperties queue(Path base) {
        return new QueueProperties(base.toString(), 1_000_000, Duration.ZERO,
                Durability.SYNC_ON_COMMIT, OnWriteError.DROP, 256, 200L);
    }

    private static SinkProperties sink(String id, String bootstrap, String topic, Path base) {
        return new SinkProperties(id, bootstrap, topic, "all", 0, "lz4", 1_048_576, Map.of(), queue(base));
    }

    private SourceProperties source(String id, String topic) {
        return new SourceProperties(id, bootstrap(), List.of(topic), "grp-" + UUID.randomUUID(),
                null, "earliest", 500, 400L, Map.of());
    }

    private static List<String> expectedValues(int n) {
        List<String> v = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            v.add("v" + i);
        }
        return v;
    }

    // ---------- testes ----------

    @Test
    void fanoutEntregaTodosOsRegistrosAosDoisDestinos(@TempDir Path tmp) throws Exception {
        int n = 20;
        SinkChannel d1 = SinkChannel.open(sink("d1", bootstrap(), "it1.dst1", tmp.resolve("d1")));
        SinkChannel d2 = SinkChannel.open(sink("d2", bootstrap(), "it1.dst2", tmp.resolve("d2")));
        d1.startForwarder();
        d2.startForwarder();
        SourceConsumer src = new SourceConsumer(source("s1", "it1.src"), List.of(d1, d2),
                new ReplayMetrics((dev.nishisan.utils.stats.StatsUtils) null));
        Thread srcThread = new Thread(src, "it1-source");
        srcThread.start();
        try {
            produce("it1.src", n);

            assertThat(consumeValues("it1.dst1", n, Duration.ofSeconds(30)))
                    .containsExactlyElementsOf(expectedValues(n));
            assertThat(consumeValues("it1.dst2", n, Duration.ofSeconds(30)))
                    .containsExactlyElementsOf(expectedValues(n));
            await().atMost(Duration.ofSeconds(10)).until(() -> d1.depth() == 0 && d2.depth() == 0);
        } finally {
            src.stop();
            srcThread.join(5_000);
            d1.close();
            d2.close();
        }
    }

    @Test
    void destinoOfflineNaoBloqueiaOrigemNemDestinoSaudavel(@TempDir Path tmp) throws Exception {
        int n = 10;
        SinkChannel healthy = SinkChannel.open(sink("ok", bootstrap(), "it2.dst", tmp.resolve("ok")));
        SinkChannel dead = SinkChannel.open(sink("dead", DEAD_BROKER, "it2.dead", tmp.resolve("dead")));
        healthy.startForwarder();
        dead.startForwarder();
        SourceConsumer src = new SourceConsumer(source("s2", "it2.src"), List.of(healthy, dead),
                new ReplayMetrics((dev.nishisan.utils.stats.StatsUtils) null));
        Thread srcThread = new Thread(src, "it2-source");
        srcThread.start();
        try {
            produce("it2.src", n);

            // Destino saudável recebe tudo, apesar do outro estar offline (isolamento).
            assertThat(consumeValues("it2.dst", n, Duration.ofSeconds(30)))
                    .containsExactlyElementsOf(expectedValues(n));
            // Destino offline: a origem ainda enfileirou tudo nele (store) sem bloquear, e nada
            // foi entregue (broker morto). Isso prova o isolamento: um destino offline não trava
            // a origem nem o destino saudável.
            await().atMost(Duration.ofSeconds(20)).until(() -> dead.depth() == n);
            assertThat(dead.publishedTotal()).isZero();
        } finally {
            src.stop();
            srcThread.join(5_000);
            healthy.close();
            dead.close();
        }
    }

    @Test
    void recuperaBacklogDoDiscoEEntregaQuandoOBrokerVolta(@TempDir Path tmp) throws Exception {
        int n = 15;
        Path qbase = tmp.resolve("it3");
        String topic = "it3.dst";

        // Fase 1 (offline): persiste o backlog no disco apontando para um broker morto.
        SinkChannel offline = SinkChannel.open(sink("it3", DEAD_BROKER, topic, qbase));
        for (int i = 0; i < n; i++) {
            offline.offer(new ReplayRecord(topic, 0, i, i,
                    ("k" + i).getBytes(StandardCharsets.UTF_8),
                    ("v" + i).getBytes(StandardCharsets.UTF_8), Map.of(), topic));
        }
        offline.sync();
        offline.close();   // nada entregue; 15 registros persistidos em disco

        // Fase 2 (online/"restart"): reabre a MESMA fila apontando para o broker real e drena.
        SinkChannel online = SinkChannel.open(sink("it3", bootstrap(), topic, qbase));
        assertThat(online.depth()).isEqualTo(n);   // backlog recuperado do disco
        online.startForwarder();
        try {
            assertThat(consumeValues(topic, n, Duration.ofSeconds(30)))
                    .containsExactlyElementsOf(expectedValues(n));
            await().atMost(Duration.ofSeconds(10)).until(() -> online.depth() == 0);
            assertThat(online.online()).isTrue();
        } finally {
            online.close();
        }
    }
}
