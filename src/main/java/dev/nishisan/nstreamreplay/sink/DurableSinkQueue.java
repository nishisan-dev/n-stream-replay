package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import dev.nishisan.utils.queue.NQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Fila durável store-and-forward de um destino, no padrão <b>dupla-fila</b> ({@code backlog}
 * + {@code retry}) — espelha o {@code MetricStreamQueue} do ape-probe, adaptado para drop-oldest
 * por contagem <i>e</i> por tempo, e at-least-once.
 *
 * <p><b>Locks separados por fila:</b> a {@code NQueue} já é thread-safe internamente; o
 * {@link #backlogLock} serializa as operações de {@code backlog} (offer da origem, poll do move)
 * e o {@link #retryLock} as de {@code retry} (move, ack do forwarder). Com isso a origem
 * ({@code offer}) e a confirmação ({@code ack}) <b>deixam de contender</b> — só
 * {@link #peekBatch()}, {@link #sync()} e {@link #sweepAndReconcile()} tocam as duas, sempre na
 * ordem {@code backlog -> retry} (evita deadlock).
 *
 * <p><b>Fluxo:</b> a origem {@link #offer(ReplayRecord)} no {@code backlog} (nunca bloqueia;
 * drop-oldest ao atingir {@code maxDepth}). O forwarder {@link #peekBatch()} reserva um lote
 * movendo {@code backlog -> retry}, publica na rede (fora do lock) e confirma o prefixo
 * entregue com {@link #ack()} (poll destrutivo na {@code retry}). Um crash entre o move e o
 * ack faz os itens reaparecerem na {@code retry} no boot (at-least-once).
 *
 * <p><b>TTL sem divergência:</b> o {@code backlog} é tocado apenas por {@code offer},
 * {@code poll} (não-bloqueante, honra {@code expireAfterWrite}) e {@code flushExpired} — nunca
 * por {@code readRange}. O peek de reservados usa {@code readRange} apenas na {@code retry},
 * que tem TTL desligado. Isso evita a divergência {@code readRange}×{@code poll} sob TTL
 * alertada no Javadoc da {@code NQueue}.
 *
 * <p>Não é thread-safe entre múltiplos forwarders: assume <b>um</b> produtor (thread de poll
 * da origem) e <b>um</b> forwarder por instância; o lock serializa a interação entre eles.
 */
public final class DurableSinkQueue implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSinkQueue.class);
    private static final String BACKLOG_NAME = "backlog";
    private static final String RETRY_NAME = "retry";
    /** poll não-bloqueante (timeout 0 retorna imediatamente). */
    private static final long NONBLOCKING = 0L;

    private final String sinkId;
    private final NQueue<ReplayRecord> backlog;
    private final NQueue<ReplayRecord> retry;
    private final int maxDepth;
    private final int batchSize;
    private final boolean retentionActive;

    private final ReentrantLock backlogLock = new ReentrantLock();
    private final ReentrantLock retryLock = new ReentrantLock();
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong expired = new AtomicLong();
    private volatile boolean closed = false;

    DurableSinkQueue(String sinkId, NQueue<ReplayRecord> backlog, NQueue<ReplayRecord> retry,
                     int maxDepth, int batchSize, boolean retentionActive) {
        this.sinkId = sinkId;
        this.backlog = backlog;
        this.retry = retry;
        this.maxDepth = maxDepth;
        this.batchSize = batchSize;
        this.retentionActive = retentionActive;
        this.pending.set(backlog.size() + retry.size());
    }

    /** Abre (ou recupera) as filas em disco em {@code basePath/<sinkId>/}. */
    public static DurableSinkQueue open(String sinkId, QueueProperties cfg) throws IOException {
        Path baseDir = Path.of(cfg.basePath(), sinkId);
        Files.createDirectories(baseDir);
        boolean perRecordFsync = cfg.durability() == Durability.PER_RECORD_FSYNC;

        NQueue.Options backlogOpts = baseOptions(perRecordFsync);
        if (cfg.hasRetentionTime()) {
            // Drop-oldest por TEMPO de dados NÃO entregues: expira o prefixo antigo do backlog.
            backlogOpts.withExpireAfterWrite(cfg.retentionTime());
        }
        // retry: TTL desligado — nunca expira item em voo (reservado para entrega).
        NQueue.Options retryOpts = baseOptions(perRecordFsync);

        NQueue<ReplayRecord> backlog = NQueue.open(baseDir, BACKLOG_NAME, backlogOpts);
        NQueue<ReplayRecord> retry = NQueue.open(baseDir, RETRY_NAME, retryOpts);
        DurableSinkQueue q = new DurableSinkQueue(sinkId, backlog, retry,
                cfg.maxDepth(), cfg.batchSize(), cfg.hasRetentionTime());
        long depth = q.pending.get();
        if (depth > 0) {
            LOG.info("[{}] fila recuperou {} registro(s) pendente(s) do disco em {}", sinkId, depth, baseDir);
        }
        return q;
    }

    private static NQueue.Options baseOptions(boolean perRecordFsync) {
        return NQueue.Options.defaults()
                .withMemoryBuffer(false)                                   // obrigatório: poll no backlog intercala com offer
                .withShortCircuit(false)                                   // obrigatório: sem hand-off em RAM (persiste sempre)
                .withRetentionPolicy(NQueue.Options.RetentionPolicy.DELETE_ON_CONSUME)
                .withResetOnRestart(false)
                .withLockTryTimeout(Duration.ofMillis(5))
                .withRevalidationInterval(Duration.ofSeconds(30))
                .withFsync(perRecordFsync);
    }

    /**
     * Enfileira o registro no backlog; nunca bloqueia. Ao atingir {@code maxDepth}, aplica
     * DROP-oldest (descarta a cabeça do backlog) e admite o novo. Retorna {@code false} apenas
     * no caso degenerado de backlog vazio com a fila cheia (tudo em voo na retry,
     * {@code maxDepth <= batchSize}), descartando o novo registro.
     */
    public boolean offer(ReplayRecord record) throws IOException {
        ensureOpen();
        backlogLock.lock();
        try {
            if (pending.get() >= maxDepth) {
                Optional<ReplayRecord> evicted = backlog.poll(NONBLOCKING, TimeUnit.MILLISECONDS);
                if (evicted.isPresent()) {
                    pending.decrementAndGet();
                    long n = dropped.incrementAndGet();
                    if (n == 1 || n % 1000 == 0) {
                        LOG.warn("[{}] fila cheia em {} — DROP-oldest (dropped={})", sinkId, maxDepth, n);
                    }
                } else {
                    dropped.incrementAndGet();
                    return false;
                }
            }
            backlog.offer(record);
            pending.incrementAndGet();
            return true;
        } finally {
            backlogLock.unlock();
        }
    }

    /**
     * Lote a publicar: primeiro os já reservados na {@code retry} (inclui recovery), lidos sem
     * consumir; se vazia, move um lote do {@code backlog} para a {@code retry} e o devolve. Não
     * remove da retry — isso é o {@link #ack()}.
     */
    public List<ReplayRecord> peekBatch() throws IOException {
        if (closed) {
            return List.of();
        }
        // Reservados na retry primeiro (inclui recovery). Só o forwarder toca a retry (peek/ack na
        // mesma thread), então ela é estável entre esta leitura e o move abaixo.
        retryLock.lock();
        try {
            List<ReplayRecord> reserved = new ArrayList<>(retry.readRange(0, batchSize).items());
            if (!reserved.isEmpty()) {
                return reserved;
            }
        } finally {
            retryLock.unlock();
        }
        // Retry vazia: move um lote do backlog para a retry (precisa das duas; ordem backlog -> retry).
        backlogLock.lock();
        try {
            retryLock.lock();
            try {
                return moveBacklogBatchToRetry();
            } finally {
                retryLock.unlock();
            }
        } finally {
            backlogLock.unlock();
        }
    }

    private List<ReplayRecord> moveBacklogBatchToRetry() throws IOException {
        List<ReplayRecord> moved = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            // poll honra expireAfterWrite (expirados são pulados, não movidos para a retry).
            Optional<ReplayRecord> head = backlog.poll(NONBLOCKING, TimeUnit.MILLISECONDS);
            if (head.isEmpty()) {
                break;
            }
            retry.offer(head.get());
            moved.add(head.get());
            // pending inalterado: saiu do backlog, entrou na retry.
        }
        return moved;
    }

    /** Confirma (remove) um registro entregue, da cabeça da {@code retry} (poll destrutivo, FIFO). */
    public void ack() throws IOException {
        ack(1);
    }

    /**
     * Confirma (remove) {@code n} registros entregues — o prefixo contíguo da {@code retry} — numa
     * <b>única</b> seção crítica (uma aquisição de lock em vez de {@code n}). FIFO, poll destrutivo.
     */
    public void ack(int n) throws IOException {
        if (closed || n <= 0) {
            return;
        }
        retryLock.lock();
        try {
            for (int i = 0; i < n; i++) {
                Optional<ReplayRecord> acked = retry.poll(NONBLOCKING, TimeUnit.MILLISECONDS);
                if (acked.isPresent()) {
                    pending.decrementAndGet();
                } else {
                    LOG.warn("[{}] ack com retry vazia", sinkId);
                    break;
                }
            }
        } finally {
            retryLock.unlock();
        }
    }

    /**
     * Expira (e contabiliza) o prefixo vencido do backlog quando há retenção por tempo, e
     * reconcilia o contador {@code pending} ao tamanho real ({@code backlog + retry}),
     * corrigindo qualquer drift. Deve ser chamado periodicamente pelo forwarder.
     *
     * @return profundidade reconciliada
     */
    public long sweepAndReconcile() throws IOException {
        if (closed) {
            return pending.get();
        }
        backlogLock.lock();
        try {
            retryLock.lock();
            try {
                if (retentionActive) {
                    long n = backlog.flushExpired();
                    if (n > 0) {
                        long total = expired.addAndGet(n);
                        LOG.warn("[{}] {} registro(s) expirado(s) por tempo (expired={})", sinkId, n, total);
                    }
                }
                long truth = backlog.size() + retry.size();
                pending.set(truth);
                return truth;
            } finally {
                retryLock.unlock();
            }
        } finally {
            backlogLock.unlock();
        }
    }

    /** Força fsync das duas filas (group-commit no boundary do batch da origem). */
    public void sync() throws IOException {
        if (closed) {
            return;
        }
        backlogLock.lock();
        try {
            retryLock.lock();
            try {
                backlog.sync();
                retry.sync();
            } finally {
                retryLock.unlock();
            }
        } finally {
            backlogLock.unlock();
        }
    }

    /** Profundidade pendente (backlog + retry) — observabilidade. */
    public long depth() {
        return pending.get();
    }

    /** Total descartado por DROP-oldest de contagem — perda silenciosa, deve ser alertado. */
    public long dropped() {
        return dropped.get();
    }

    /** Total expirado por tempo ({@code retentionTime}) — perda silenciosa, deve ser alertado. */
    public long expired() {
        return expired.get();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("DurableSinkQueue[" + sinkId + "] fechada");
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        IOException first = null;
        try {
            backlog.close();
        } catch (IOException e) {
            first = e;
        }
        try {
            retry.close();
        } catch (IOException e) {
            if (first == null) {
                first = e;
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
