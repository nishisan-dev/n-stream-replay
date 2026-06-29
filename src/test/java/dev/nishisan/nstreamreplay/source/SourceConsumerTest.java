package dev.nishisan.nstreamreplay.source;

import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.config.SourceProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import dev.nishisan.nstreamreplay.route.RouteTable;
import dev.nishisan.nstreamreplay.sink.SinkTarget;
import dev.nishisan.nstreamreplay.stats.ReplayMetrics;
import dev.nishisan.nstreamreplay.testutil.FakeSinkTarget;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SourceConsumerTest {

    private static final ReplayMetrics NOOP = new ReplayMetrics((dev.nishisan.utils.stats.StatsUtils) null);

    private static SourceProperties source(String id) {
        return new SourceProperties(id, "ignored:9092", "g", null, "earliest", 500, 200L, Map.of());
    }

    /** RouteTable com uma rota: sourceId/fromTopic -> targets (toTopic null = preserva). */
    private static RouteTable singleRoute(String sourceId, String fromTopic, SinkTarget... targets) {
        Map<String, SinkTarget> sinks = new LinkedHashMap<>();
        List<RouteTarget> to = new ArrayList<>();
        for (SinkTarget t : targets) {
            sinks.put(t.id(), t);
            to.add(new RouteTarget(t.id(), null));
        }
        return RouteTable.build(List.of(new RouteProperties("r", sourceId, fromTopic, to)), sinks);
    }

    private static ConsumerRecord<byte[], byte[]> kafkaRec(String topic, int partition, long offset, String value) {
        return new ConsumerRecord<>(topic, partition, offset, null, value.getBytes(StandardCharsets.UTF_8));
    }

    private static int val(ReplayRecord r) {
        return Integer.parseInt(new String(r.value(), StandardCharsets.UTF_8));
    }

    @Test
    void fanoutParaTodosOsAlvosComDestinoPreservadoESyncSoNosQueExigem() throws Exception {
        FakeSinkTarget a = new FakeSinkTarget("a", true);   // exige sync
        FakeSinkTarget b = new FakeSinkTarget("b", false);  // não exige
        SourceConsumer consumer = new SourceConsumer(
                source("s1"), singleRoute("s1", "t", a, b), NOOP, () -> {
            throw new IllegalStateException("não usa consumer neste teste");
        });

        TopicPartition tp = new TopicPartition("t", 0);
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(tp, List.of(
                kafkaRec("t", 0, 0L, "1"), kafkaRec("t", 0, 1L, "2"))));

        consumer.enqueueBatch(records);

        assertThat(a.received.stream().map(SourceConsumerTest::val)).containsExactly(1, 2);
        assertThat(b.received.stream().map(SourceConsumerTest::val)).containsExactly(1, 2);
        // toTopic omitido => destino preserva o tópico de origem.
        assertThat(a.received.get(0).destinationTopic()).isEqualTo("t");
        assertThat(a.received.get(0).sourceTopic()).isEqualTo("t");
        assertThat(a.received.get(1).offset()).isEqualTo(1L);
        // Group-commit: sync só no destino que exige, uma vez por lote.
        assertThat(a.syncs.get()).isEqualTo(1);
        assertThat(b.syncs.get()).isZero();
    }

    @Test
    void roteiaCadaTopicoParaSeusAlvos() throws Exception {
        FakeSinkTarget a = new FakeSinkTarget("a", false);
        FakeSinkTarget b = new FakeSinkTarget("b", false);
        // duas rotas na mesma origem: t1 -> a (preserva), t2 -> b (renomeia para t2.out)
        Map<String, SinkTarget> sinks = new LinkedHashMap<>();
        sinks.put("a", a);
        sinks.put("b", b);
        RouteTable routes = RouteTable.build(List.of(
                new RouteProperties("r1", "s1", "t1", List.of(new RouteTarget("a", null))),
                new RouteProperties("r2", "s1", "t2", List.of(new RouteTarget("b", "t2.out")))), sinks);
        SourceConsumer consumer = new SourceConsumer(source("s1"), routes, NOOP, () -> {
            throw new IllegalStateException("não usa consumer neste teste");
        });

        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(
                new TopicPartition("t1", 0), List.of(kafkaRec("t1", 0, 0L, "1")),
                new TopicPartition("t2", 0), List.of(kafkaRec("t2", 0, 0L, "2"))));

        consumer.enqueueBatch(records);

        // de-para: t1 só em a (preserva), t2 só em b (renomeado).
        assertThat(a.received.stream().map(SourceConsumerTest::val)).containsExactly(1);
        assertThat(a.received.get(0).destinationTopic()).isEqualTo("t1");
        assertThat(b.received.stream().map(SourceConsumerTest::val)).containsExactly(2);
        assertThat(b.received.get(0).destinationTopic()).isEqualTo("t2.out");
    }

    @Test
    void calculaOffsetLagEConsumedTotal() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        MockConsumer<byte[], byte[]> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(tp));
            mock.updateBeginningOffsets(Map.of(tp, 0L));
            mock.updateEndOffsets(Map.of(tp, 10L));            // 10 disponíveis no tópico
            mock.addRecord(kafkaRec("t", 0, 0L, "1"));
            mock.addRecord(kafkaRec("t", 0, 1L, "2"));         // consome 2 -> position=2
        });

        FakeSinkTarget target = new FakeSinkTarget("d", false);
        SourceConsumer consumer = new SourceConsumer(source("s1"), singleRoute("s1", "t", target), NOOP, () -> mock);

        Thread t = new Thread(consumer, "lag-source");
        t.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.consumedTotal() == 2);
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.offsetLag() == 8L);
        } finally {
            consumer.stop();
            t.join(5_000);
        }
    }

    @Test
    void pollLoopRealConsomeEnfileiraECommita() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        java.util.concurrent.atomic.AtomicLong committedOffset = new java.util.concurrent.atomic.AtomicLong(-1L);
        MockConsumer<byte[], byte[]> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public void commitSync() {
                super.commitSync();
                committedOffset.set(position(tp));
            }
        };
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(tp));
            mock.updateBeginningOffsets(Map.of(tp, 0L));
            mock.addRecord(kafkaRec("t", 0, 0L, "10"));
            mock.addRecord(kafkaRec("t", 0, 1L, "20"));
        });

        FakeSinkTarget target = new FakeSinkTarget("d", false);
        SourceConsumer consumer = new SourceConsumer(source("s1"), singleRoute("s1", "t", target), NOOP, () -> mock);

        Thread t = new Thread(consumer, "test-source");
        t.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> target.received.size() == 2);
            assertThat(target.received.stream().map(SourceConsumerTest::val)).containsExactly(10, 20);
            await().atMost(Duration.ofSeconds(2)).until(() -> committedOffset.get() == 2L);
        } finally {
            consumer.stop();
            t.join(5_000);
        }
    }
}
