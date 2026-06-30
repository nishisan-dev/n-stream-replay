package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.OnWriteError;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import dev.nishisan.utils.queue.NQueue;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DurableSinkQueueTest {

    private static NQueue.Options rawQueueOptions() {
        return NQueue.Options.defaults()
                .withMemoryBuffer(false)
                .withShortCircuit(false)
                .withRetentionPolicy(NQueue.Options.RetentionPolicy.DELETE_ON_CONSUME)
                .withResetOnRestart(false)
                .withFsync(false);
    }

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
            }
            q.ack(batch.size());
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
    void montaLoteCompletoAtravesDeVariosChunks(@TempDir Path tmp) throws Exception {
        int batchSize = 150; // exercita 64 + 64 + 22 registros
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 1_000, Duration.ZERO, batchSize))) {
            for (int i = 0; i < batchSize; i++) {
                q.offer(rec(i));
            }

            List<ReplayRecord> batch = q.peekBatch();

            assertThat(batch).hasSize(batchSize);
            assertThat(batch.stream().map(DurableSinkQueueTest::val))
                    .containsExactlyElementsOf(IntStream.range(0, batchSize).boxed().toList());
            q.ack(batch.size());
            assertThat(q.depth()).isZero();
        }
    }

    @Test
    void offerAvancaDurantePersistenciaNaRetryMasSyncEsperaTransferencia() throws Exception {
        @SuppressWarnings("unchecked")
        NQueue<ReplayRecord> backlog = mock(NQueue.class);
        @SuppressWarnings("unchecked")
        NQueue<ReplayRecord> retry = mock(NQueue.class);
        when(retry.readRange(0, 1))
                .thenReturn(new NQueue.ReadRangeResult<>(List.of(), false, 0));
        when(backlog.poll(0L, TimeUnit.MILLISECONDS)).thenReturn(java.util.Optional.of(rec(1)));

        CountDownLatch retryOfferStarted = new CountDownLatch(1);
        CountDownLatch releaseRetryOffer = new CountDownLatch(1);
        doAnswer(invocation -> {
            retryOfferStarted.countDown();
            assertThat(releaseRetryOffer.await(5, TimeUnit.SECONDS)).isTrue();
            return 0L;
        }).when(retry).offer(any(ReplayRecord.class));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (DurableSinkQueue q = new DurableSinkQueue("s", backlog, retry, 100, 1, false)) {
            Future<List<ReplayRecord>> moving = executor.submit(q::peekBatch);
            assertThat(retryOfferStarted.await(2, TimeUnit.SECONDS)).isTrue();

            // O I/O da retry continua bloqueado, mas já não possui o backlogLock.
            Future<Boolean> offered = executor.submit(() -> q.offer(rec(2)));
            assertThat(offered.get(2, TimeUnit.SECONDS)).isTrue();

            CountDownLatch syncStarted = new CountDownLatch(1);
            Future<?> syncing = executor.submit(() -> {
                syncStarted.countDown();
                q.sync();
                return null;
            });
            assertThat(syncStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(syncing).isNotDone();

            releaseRetryOffer.countDown();
            assertThat(moving.get(2, TimeUnit.SECONDS).stream().map(DurableSinkQueueTest::val))
                    .containsExactly(1);
            syncing.get(2, TimeUnit.SECONDS);
        } finally {
            releaseRetryOffer.countDown();
            executor.shutdownNow();
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
    void ackEmLoteConfirmaPrefixoContiguoEmUmaChamada(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100, Duration.ZERO, 10))) {
            for (int i = 1; i <= 5; i++) {
                q.offer(rec(i));
            }
            List<ReplayRecord> batch = q.peekBatch();
            assertThat(batch).hasSize(5);

            q.ack(3);   // confirma 1,2,3 de uma vez (uma seção crítica)
            assertThat(q.depth()).isEqualTo(2);
            // restante reservado, em ordem.
            assertThat(q.peekBatch().stream().map(DurableSinkQueueTest::val)).containsExactly(4, 5);

            q.ack(0);   // no-op
            assertThat(q.depth()).isEqualTo(2);
            q.ack(2);
            assertThat(q.depth()).isZero();
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
    void recuperaRetryLegadaAntesDoBacklogSemReordenar(@TempDir Path tmp) throws Exception {
        QueueProperties cfg = cfg(tmp, 100, Duration.ZERO, 10);
        // Backlog novo contém 3,4.
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg)) {
            q.offer(rec(3));
            q.offer(rec(4));
            q.sync();
        }
        // Simula retry persistida por uma versão anterior: é o prefixo mais antigo 1,2.
        Path sinkDir = tmp.resolve("s");
        try (NQueue<ReplayRecord> legacyRetry = NQueue.open(sinkDir, "retry", rawQueueOptions())) {
            legacyRetry.offer(rec(1));
            legacyRetry.offer(rec(2));
            legacyRetry.sync();
        }

        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg)) {
            assertThat(q.peekBatch().stream().map(DurableSinkQueueTest::val)).containsExactly(1, 2);
            q.ack(2);
            assertThat(q.peekBatch().stream().map(DurableSinkQueueTest::val)).containsExactly(3, 4);
        }
    }

    @Test
    void parcialPromovidoParaRetrySobreviveRestart(@TempDir Path tmp) throws Exception {
        QueueProperties cfg = cfg(tmp, 100, Duration.ZERO, 10);
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg)) {
            for (int i = 1; i <= 4; i++) {
                q.offer(rec(i));
            }
            assertThat(q.peekBatch()).hasSize(4);
            q.ack(2); // 1,2 entregues; 3,4 copiados+sincronizados na retry.
            assertThat(q.depth()).isEqualTo(2);
        }

        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg)) {
            assertThat(q.peekBatch().stream().map(DurableSinkQueueTest::val)).containsExactly(3, 4);
            q.ack(2);
            assertThat(q.depth()).isZero();
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
    void ttlNaoExpiraLoteReservadoEmPublicacao(@TempDir Path tmp) throws Exception {
        try (DurableSinkQueue q = DurableSinkQueue.open("s", cfg(tmp, 100, Duration.ofMillis(100), 10))) {
            q.offer(rec(1));
            assertThat(q.peekBatch()).hasSize(1);

            Thread.sleep(250);
            assertThat(q.sweepAndReconcile()).isEqualTo(1);
            assertThat(q.expired()).isZero();

            q.ack();
            assertThat(q.depth()).isZero();
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
                }
                q.ack(batch.size());
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
