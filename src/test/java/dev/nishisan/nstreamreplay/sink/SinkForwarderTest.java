package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.OnWriteError;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class SinkForwarderTest {

    private static QueueProperties cfg(Path tmp, int batchSize) {
        return new QueueProperties(tmp.toString(), 1_000_000, Duration.ZERO,
                Durability.OS_MANAGED, OnWriteError.DROP, batchSize, 0L);
    }

    private static ReplayRecord rec(int n) {
        return new ReplayRecord("t", 0, n, n, null,
                String.valueOf(n).getBytes(StandardCharsets.UTF_8), Map.of(), "t");
    }

    private static int val(ReplayRecord r) {
        return Integer.parseInt(new String(r.value(), StandardCharsets.UTF_8));
    }

    @Test
    void drenaTudoQuandoOnline(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100))) {
            for (int i = 1; i <= 5; i++) {
                q.offer(rec(i));
            }
            FakeSink sink = new FakeSink();
            SinkForwarder fwd = new SinkForwarder("s", q, sink, 0L);

            assertThat(fwd.forwardOnce()).isTrue();

            assertThat(q.depth()).isZero();
            assertThat(fwd.publishedTotal()).isEqualTo(5);
            assertThat(fwd.online()).isTrue();
            assertThat(sink.delivered.stream().map(SinkForwarderTest::val)).containsExactly(1, 2, 3, 4, 5);
        }
    }

    @Test
    void storeAndForwardRetemEnquantoOfflineEDrenaQuandoVolta(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100))) {
            for (int i = 1; i <= 5; i++) {
                q.offer(rec(i));
            }
            FakeSink sink = new FakeSink();
            SinkForwarder fwd = new SinkForwarder("s", q, sink, 0L);

            // Broker fora: nada entregue, tudo permanece na fila, marca offline.
            sink.behavior = FakeSink::brokerDown;
            assertThat(fwd.forwardOnce()).isTrue();
            assertThat(q.depth()).isEqualTo(5);
            assertThat(sink.delivered).isEmpty();
            assertThat(fwd.online()).isFalse();

            // Broker volta: drena tudo, em ordem.
            sink.behavior = FakeSink::allOk;
            assertThat(fwd.forwardOnce()).isTrue();
            assertThat(q.depth()).isZero();
            assertThat(fwd.online()).isTrue();
            assertThat(sink.delivered.stream().map(SinkForwarderTest::val)).containsExactly(1, 2, 3, 4, 5);
        }
    }

    @Test
    void poisonNaoTravaOFifo(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100))) {
            for (int i = 1; i <= 3; i++) {
                q.offer(rec(i));
            }
            FakeSink sink = new FakeSink();
            // 2 entregues + 1 poison (no fim do prefixo) -> acked=3, FIFO avança.
            sink.behavior = batch -> new PublishOutcome(2, 1);
            SinkForwarder fwd = new SinkForwarder("s", q, sink, 0L);

            assertThat(fwd.forwardOnce()).isTrue();
            assertThat(q.depth()).isZero();
            assertThat(fwd.publishedTotal()).isEqualTo(2);
            assertThat(fwd.poisonedTotal()).isEqualTo(1);
        }
    }

    @Test
    void entregaParcialTransitoriaRetemRestanteEContaBackoff(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100))) {
            for (int i = 1; i <= 4; i++) {
                q.offer(rec(i));
            }
            FakeSink sink = new FakeSink();
            // Entrega 2, depois um transitório encerra o prefixo (acked=2).
            sink.behavior = batch -> new PublishOutcome(2, 0);
            SinkForwarder fwd = new SinkForwarder("s", q, sink, 0L);

            assertThat(fwd.forwardOnce()).isTrue();
            assertThat(q.depth()).isEqualTo(2);
            assertThat(fwd.retryBackoffs()).isEqualTo(1);

            // Segundo ciclo entrega o restante reservado, em ordem.
            assertThat(fwd.forwardOnce()).isTrue();
            assertThat(q.depth()).isZero();
            assertThat(sink.delivered.stream().map(SinkForwarderTest::val))
                    .isEqualTo(IntStream.rangeClosed(1, 4).boxed().toList());
        }
    }

    @Test
    void filaVaziaNaoMudaOnline(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100))) {
            FakeSink sink = new FakeSink();
            SinkForwarder fwd = new SinkForwarder("s", q, sink, 0L);
            assertThat(fwd.forwardOnce()).isFalse();
            assertThat(fwd.online()).isTrue();   // sem itens => não altera o estado
            assertThat(sink.delivered).isEmpty();
        }
    }
}
