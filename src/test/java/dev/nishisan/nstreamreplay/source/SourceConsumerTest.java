package dev.nishisan.nstreamreplay.source;

import dev.nishisan.nstreamreplay.config.SourceProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class SourceConsumerTest {

    private static final ReplayMetrics NOOP = new ReplayMetrics((dev.nishisan.utils.stats.StatsUtils) null);

    private static SourceProperties source(String id, String topic) {
        return new SourceProperties(id, "ignored:9092", List.of(topic), "g", null,
                "earliest", 500, 200L, Map.of());
    }

    private static ConsumerRecord<byte[], byte[]> kafkaRec(String topic, int partition, long offset, String value) {
        return new ConsumerRecord<>(topic, partition, offset, null, value.getBytes(StandardCharsets.UTF_8));
    }

    private static int val(ReplayRecord r) {
        return Integer.parseInt(new String(r.value(), StandardCharsets.UTF_8));
    }

    @Test
    void enqueueBatchFazFanoutParaTodosOsDestinosESyncSoNosQueExigem() throws Exception {
        FakeSinkTarget a = new FakeSinkTarget("a", true);   // exige sync
        FakeSinkTarget b = new FakeSinkTarget("b", false);  // não exige
        SourceConsumer consumer = new SourceConsumer(
                source("s1", "t"), List.of(a, b), NOOP, () -> {
            throw new IllegalStateException("não usa consumer neste teste");
        });

        TopicPartition tp = new TopicPartition("t", 0);
        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(Map.of(tp, List.of(
                kafkaRec("t", 0, 0L, "1"), kafkaRec("t", 0, 1L, "2"))));

        consumer.enqueueBatch(records);

        // Fan-out: ambos os destinos recebem ambos os registros, em ordem.
        assertThat(a.received.stream().map(SourceConsumerTest::val)).containsExactly(1, 2);
        assertThat(b.received.stream().map(SourceConsumerTest::val)).containsExactly(1, 2);
        // Proveniência preservada.
        assertThat(a.received.get(0).sourceTopic()).isEqualTo("t");
        assertThat(a.received.get(1).offset()).isEqualTo(1L);
        // Group-commit: sync só no destino que exige, uma vez por lote.
        assertThat(a.syncs.get()).isEqualTo(1);
        assertThat(b.syncs.get()).isZero();
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
        SourceConsumer consumer = new SourceConsumer(source("s1", "t"), List.of(target), NOOP, () -> mock);

        Thread t = new Thread(consumer, "lag-source");
        t.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.consumedTotal() == 2);
            // endOffsets=10, position=2 => offset lag = 8
            await().atMost(Duration.ofSeconds(5)).until(() -> consumer.offsetLag() == 8L);
        } finally {
            consumer.stop();
            t.join(5_000);
        }
    }

    @Test
    void pollLoopRealConsomeEnfileiraECommita() throws Exception {
        TopicPartition tp = new TopicPartition("t", 0);
        // Captura o offset commitado de dentro da thread do consumer (o run() fecha o
        // MockConsumer ao parar, então não dá para lê-lo depois do join).
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
        SourceConsumer consumer = new SourceConsumer(source("s1", "t"), List.of(target), NOOP, () -> mock);

        Thread t = new Thread(consumer, "test-source");
        t.start();
        try {
            await().atMost(Duration.ofSeconds(5)).until(() -> target.received.size() == 2);
            assertThat(target.received.stream().map(SourceConsumerTest::val)).containsExactly(10, 20);
            // Offset commitado após o lote (at-least-once): próximo offset = 2.
            await().atMost(Duration.ofSeconds(2)).until(() -> committedOffset.get() == 2L);
        } finally {
            consumer.stop();
            t.join(5_000);
        }
    }
}
