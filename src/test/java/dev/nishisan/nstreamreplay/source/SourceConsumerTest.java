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
        return new SourceProperties(id, "ignored:9092", "g", null, "earliest", 500, 200L, 0L, Map.of());
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

    private static SourceProperties jsonSource() {
        return new SourceProperties("s1", "ignored:9092", "g", null, "earliest", 500, 200L, 0L, Map.of(), "json");
    }

    private static RouteTable keyedRoutes(FakeSinkTarget target) {
        return RouteTable.build(List.of(new RouteProperties("r", "s1", "t", List.of(
                new RouteTarget(target.id(), "keyed", "payload.Id"),
                new RouteTarget(target.id(), "keyed", "payload.Id"),
                new RouteTarget(target.id(), "root", "id"),
                new RouteTarget(target.id(), "raw")))), Map.of(target.id(), target));
    }

    @Test
    void fanoutExtractsOnceDeduplicatesAndCountsDropsPerTarget() throws Exception {
        FakeSinkTarget target = new FakeSinkTarget("d", true);
        var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        var stats = new dev.nishisan.utils.stats.StatsUtils();
        stats.registerListener(new dev.nishisan.utils.spring.stats.StatsUtilsMetricBind(registry));
        var metrics = new ReplayMetrics(stats);
        SourceConsumer consumer = new SourceConsumer(jsonSource(), keyedRoutes(target), metrics);
        byte[] originalKey = "original".getBytes(StandardCharsets.UTF_8);
        String[] values = {"{", "{}", "{\"payload\":{\"Id\":[]},\"id\":{}}",
                "{\"payload\":{\"Id\":42},\"id\":true}", null};
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            records.add(new ConsumerRecord<>("t", 0, i, originalKey,
                    values[i] == null ? null : values[i].getBytes(StandardCharsets.UTF_8)));
            records.get(i).headers().add("header", new byte[]{1, 2});
        }
        consumer.enqueueBatch(new ConsumerRecords<>(Map.of(new TopicPartition("t", 0), records)));
        assertThat(target.received).hasSize(7); // five raw + two keyed; duplicate target removed
        var keyed = target.received.stream().filter(r -> r.destinationTopic().equals("keyed")).toList();
        assertThat(keyed).hasSize(1);
        assertThat(keyed.get(0).key()).isEqualTo("42".getBytes(StandardCharsets.UTF_8));
        assertThat(keyed.get(0).value()).isEqualTo(records.get(3).value());
        assertThat(keyed.get(0).timestamp()).isEqualTo(records.get(3).timestamp());
        assertThat(keyed.get(0).headers().get("header")).containsExactly((byte) 1, (byte) 2);
        assertThat(target.received.stream().filter(r -> r.destinationTopic().equals("root")).findFirst().orElseThrow().key())
                .isEqualTo("true".getBytes(StandardCharsets.UTF_8));
        assertThat(target.received.stream().filter(r -> r.destinationTopic().equals("raw")))
                .allSatisfy(r -> assertThat(r.key()).isEqualTo(originalKey));
        assertThat(consumer.routingStats()).isEqualTo(new SourceConsumer.RoutingSnapshot(1, 2, 4, 2));
        assertThat(target.syncs.get()).isEqualTo(1);
        metrics.updateSource(consumer);
        assertThat(registry.get("nstreamreplay.source.s1.decode_errors_total").gauge().value()).isEqualTo(1);
        assertThat(registry.get("nstreamreplay.source.s1.decode_dropped_total").gauge().value()).isEqualTo(2);
        assertThat(registry.get("nstreamreplay.source.s1.partition_key_missing_dropped_total").gauge().value()).isEqualTo(4);
        assertThat(registry.get("nstreamreplay.source.s1.partition_key_type_dropped_total").gauge().value()).isEqualTo(2);
    }

    @Test
    void commitsDiscardedRecordsAndContinuesWithNextPoll() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        var committed = new java.util.concurrent.atomic.AtomicLong(-1);
        MockConsumer<byte[], byte[]> mock = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override public void commitSync() {
                super.commitSync();
                committed.set(position(tp));
            }
        };
        mock.schedulePollTask(() -> {
            mock.rebalance(List.of(tp));
            mock.updateBeginningOffsets(Map.of(tp, 0L));
            mock.updateEndOffsets(Map.of(tp, 3L));
            mock.addRecord(kafkaRec("t", 0, 0, "{"));
            mock.addRecord(kafkaRec("t", 0, 1, "{}"));
        });
        mock.schedulePollTask(() -> {
            assertThat(committed.get()).isEqualTo(2);
            mock.addRecord(kafkaRec("t", 0, 2, "{\"id\":\"next\"}"));
        });
        FakeSinkTarget target = new FakeSinkTarget("d", true);
        var routes = RouteTable.build(List.of(new RouteProperties("r", "s1", "t",
                List.of(new RouteTarget("d", null, "id")))), Map.of("d", target));
        SourceConsumer consumer = new SourceConsumer(jsonSource(), routes, NOOP, () -> mock);
        Thread thread = new Thread(consumer, "keyed-source-test");
        thread.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> committed.get() == 3);
            assertThat(target.received).hasSize(1);
            assertThat(target.received.get(0).key()).isEqualTo("next".getBytes(StandardCharsets.UTF_8));
            assertThat(consumer.routingStats()).isEqualTo(new SourceConsumer.RoutingSnapshot(1, 1, 1, 0));
            assertThat(target.syncs.get()).isEqualTo(1);
        } finally {
            consumer.stop();
            thread.join(5000);
        }
    }

    @Test
    void withoutKeyDoesNotDecodeEvenWithJsonConfigured() throws Exception {
        FakeSinkTarget target = new FakeSinkTarget("d", false);
        SourceConsumer consumer = new SourceConsumer(jsonSource(), singleRoute("s1", "t", target), NOOP);
        consumer.enqueueBatch(new ConsumerRecords<>(Map.of(new TopicPartition("t", 0),
                List.of(kafkaRec("t", 0, 0, "invalid json")))));
        assertThat(target.received).hasSize(1);
        assertThat(consumer.routingStats()).isEqualTo(new SourceConsumer.RoutingSnapshot(0, 0, 0, 0));
    }

    @Test
    void extractedKeyAndOriginalRecordsSurviveQueueReopen(@org.junit.jupiter.api.io.TempDir java.nio.file.Path tmp) throws Exception {
        FakeSinkTarget target = new FakeSinkTarget("d", false);
        SourceConsumer consumer = new SourceConsumer(jsonSource(), keyedRoutes(target), NOOP);
        consumer.enqueueBatch(new ConsumerRecords<>(Map.of(new TopicPartition("t", 0),
                List.of(kafkaRec("t", 0, 0, "{\"payload\":{\"Id\":\"durable\"},\"id\":1}")))));
        var queueProps = new dev.nishisan.nstreamreplay.config.QueueProperties(tmp.toString(), 100, Duration.ZERO,
                dev.nishisan.nstreamreplay.config.Durability.SYNC_ON_COMMIT,
                dev.nishisan.nstreamreplay.config.OnWriteError.DROP, 10, 100);
        try (var queue = dev.nishisan.nstreamreplay.sink.DurableSinkQueue.open("d", queueProps)) {
            for (ReplayRecord record : target.received) queue.offer(record);
            queue.sync();
        }
        try (var queue = dev.nishisan.nstreamreplay.sink.DurableSinkQueue.open("d", queueProps)) {
            assertThat(queue.peekBatch()).containsExactlyElementsOf(target.received);
            assertThat(queue.peekBatch().get(0).key()).isEqualTo("durable".getBytes(StandardCharsets.UTF_8));
        }
    }
}
