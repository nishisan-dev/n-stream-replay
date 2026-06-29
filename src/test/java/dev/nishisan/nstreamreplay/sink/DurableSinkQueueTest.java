package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.OnWriteError;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class DurableSinkQueueTest {

    private static QueueProperties cfg(Path tmp, int maxDepth, Duration retention, int batchSize) {
        return new QueueProperties(tmp.toString(), maxDepth, retention,
                Durability.OS_MANAGED, OnWriteError.DROP, batchSize, 1000L);
    }

    private static ReplayRecord rec(int n) {
        return new ReplayRecord("t", 0, n, n, null,
                String.valueOf(n).getBytes(StandardCharsets.UTF_8), Map.of(), "t");
    }

    private static int val(ReplayRecord r) {
        return Integer.parseInt(new String(r.value(), StandardCharsets.UTF_8));
    }

    /** Drena tudo (peek -> ack) preservando ordem, simulando entrega bem-sucedida. */
    private static List<Integer> drainAll(DurableSinkQueue q) throws IOException {
        List<Integer> out = new ArrayList<>();
        List<ReplayRecord> batch;
        while (!(batch = q.peekBatch()).isEmpty()) {
            for (ReplayRecord r : batch) {
                out.add(val(r));
                q.ack();
            }
        }
        return out;
    }

    @Test
    void preservaOrdemFifo(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100, Duration.ZERO, 10))) {
            for (int i = 1; i <= 5; i++) {
                q.offer(rec(i));
            }
            assertThat(q.depth()).isEqualTo(5);
            assertThat(drainAll(q)).containsExactly(1, 2, 3, 4, 5);
            assertThat(q.depth()).isZero();
            assertThat(q.dropped()).isZero();
        }
    }

    @Test
    void dropOldestDescartaOsMaisAntigosAoEstourarMaxDepth(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 3, Duration.ZERO, 10))) {
            for (int i = 1; i <= 5; i++) {
                q.offer(rec(i));
            }
            assertThat(q.depth()).isEqualTo(3);
            assertThat(q.dropped()).isEqualTo(2);
            // Sobrevivem os 3 mais recentes; os 2 mais antigos (1, 2) foram descartados.
            assertThat(drainAll(q)).containsExactly(3, 4, 5);
        }
    }

    @Test
    void itensReservadosNaRetryNuncaSaoDropados(@TempDir Path tmp) throws Exception {
        // maxDepth=2: prova que o drop-oldest atinge SÓ o backlog, nunca itens em voo na retry.
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 2, Duration.ZERO, 10))) {
            q.offer(rec(1));
            q.offer(rec(2));

            // Reserva r1,r2 na retry (backlog esvazia).
            List<ReplayRecord> reserved = q.peekBatch();
            assertThat(reserved.stream().map(DurableSinkQueueTest::val)).containsExactly(1, 2);

            // Fila cheia (pending=2) com backlog vazio -> o novo é dropado, NÃO os reservados.
            boolean accepted = q.offer(rec(3));
            assertThat(accepted).isFalse();
            assertThat(q.dropped()).isEqualTo(1);

            // peekBatch ainda devolve os reservados (retry-first), em ordem.
            assertThat(q.peekBatch().stream().map(DurableSinkQueueTest::val)).containsExactly(1, 2);
            q.ack();
            q.ack();
            assertThat(q.depth()).isZero();
        }
    }

    @Test
    void ackParcialMantemRestanteReservadoEmOrdem(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100, Duration.ZERO, 10))) {
            for (int i = 1; i <= 4; i++) {
                q.offer(rec(i));
            }
            List<ReplayRecord> batch = q.peekBatch();
            assertThat(batch).hasSize(4);

            // Simula entrega parcial: confirma só os 2 primeiros (prefixo contíguo).
            q.ack();
            q.ack();
            assertThat(q.depth()).isEqualTo(2);

            // Próximo peek devolve o restante reservado (retry-first), em ordem.
            assertThat(q.peekBatch().stream().map(DurableSinkQueueTest::val)).containsExactly(3, 4);
        }
    }

    @Test
    void recuperaBacklogDoDiscoAposReabrir(@TempDir Path tmp) throws Exception {
        QueueProperties cfg = cfg(tmp, 100, Duration.ZERO, 10);
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg)) {
            for (int i = 1; i <= 3; i++) {
                q.offer(rec(i));
            }
            q.sync();
        }
        // Reabre a MESMA fila (mesmo sinkId + basePath): backlog recuperado do disco.
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg)) {
            assertThat(q.depth()).isEqualTo(3);
            assertThat(drainAll(q)).containsExactly(1, 2, 3);
        }
    }

    @Test
    void expiraPorTempoEContabiliza(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100, Duration.ofMillis(150), 10))) {
            for (int i = 1; i <= 3; i++) {
                q.offer(rec(i));
            }
            assertThat(q.depth()).isEqualTo(3);

            Thread.sleep(350);
            long depth = q.sweepAndReconcile();

            assertThat(q.expired()).isEqualTo(3);
            assertThat(depth).isZero();
            assertThat(q.depth()).isZero();
            assertThat(q.peekBatch()).isEmpty();
        }
    }

    @Test
    void conservacaoSobConcorrenciaProdutorXForwarder(@TempDir Path tmp) throws Exception {
        int total = 2000;
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 1_000_000, Duration.ZERO, 64))) {
            AtomicBoolean producing = new AtomicBoolean(true);
            List<Integer> delivered = new ArrayList<>();

            Thread producer = new Thread(() -> {
                try {
                    for (int i = 0; i < total; i++) {
                        q.offer(rec(i));
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    producing.set(false);
                }
            }, "producer");

            producer.start();
            while (producing.get() || q.depth() > 0) {
                List<ReplayRecord> batch = q.peekBatch();
                for (ReplayRecord r : batch) {
                    delivered.add(val(r));
                    q.ack();
                }
            }
            producer.join();

            // maxDepth gigante -> sem drop; entrega exata e em ordem (1 produtor + 1 forwarder).
            assertThat(q.dropped()).isZero();
            assertThat(delivered).hasSize(total);
            assertThat(delivered).isEqualTo(IntStream.range(0, total).boxed().toList());
            assertThat(q.depth()).isZero();
        }
    }
}
