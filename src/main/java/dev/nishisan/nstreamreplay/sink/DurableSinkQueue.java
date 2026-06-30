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
 * Fila durável store-and-forward no padrão dupla-fila ({@code backlog + retry}). O backlog recebe
 * a origem e a retry protege o lote em voo; o forwarder move em chunks curtos sem manter o
 * {@code backlogLock} enquanto persiste o item na retry.
 *
 * <p><b>Contenção:</b> a NQueue já serializa internamente cada operação. {@link #backlogLock}
 * coordena offer e remoção. {@link #peekBatch()} o mantém apenas durante cada {@code poll} do
 * backlog e o libera antes do {@code offer} correspondente na retry. Assim, a origem pode
 * intercalar offers durante o I/O da retry sem reduzir o lote final enviado ao Kafka.
 *
 * <p><b>Fluxo:</b> o forwarder reserva movendo backlog para retry, publica fora dos locks e confirma
 * o prefixo entregue com {@link #ack(int)}. Crash depois do move reencontra os itens na retry.
 * O TTL fica ativo apenas no backlog; itens já reservados nunca expiram nem sofrem drop-oldest.
 *
 * <p>Suporta producers concorrentes, mas assume <b>um</b> forwarder por instância; um segundo
 * forwarder quebraria a reserva FIFO entre {@link #peekBatch()} e {@link #ack(int)}.
 */
public final class DurableSinkQueue implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DurableSinkQueue.class);
    private static final String BACKLOG_NAME = "backlog";
    private static final String RETRY_NAME = "retry";
    /** poll não-bloqueante (timeout 0 retorna imediatamente). */
    private static final long NONBLOCKING = 0L;
    /** Limita quanto tempo contínuo o forwarder mantém a retry reservada para uma transferência. */
    private static final int MOVE_CHUNK_SIZE = 64;

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
    private final AtomicLong offerLockWaitNanos = new AtomicLong();
    private final AtomicLong syncLockWaitNanos = new AtomicLong();
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
     * Enfileira o registro no backlog sem esperar capacidade (pode aguardar brevemente o lock local).
     * Ao atingir {@code maxDepth}, aplica DROP-oldest (descarta a cabeça do backlog) e admite o novo.
     * Retorna {@code false} apenas
     * no caso degenerado de backlog vazio com a fila cheia (tudo em voo na retry,
     * {@code maxDepth <= batchSize}), descartando o novo registro.
     */
    public boolean offer(ReplayRecord record) throws IOException {
        ensureOpen();
        lockMeasuringContention(backlogLock, offerLockWaitNanos);
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
     * Lote a publicar: primeiro os já reservados na retry; se vazia, compõe até batchSize movendo
     * chunks curtos do backlog. Cada item é removido sob o backlogLock, que é liberado antes de
     * persistir o item na retry; a retryLock impede sync/reconcile de observar a transferência no meio.
     */
    public List<ReplayRecord> peekBatch() throws IOException {
        if (closed) {
            return List.of();
        }
        retryLock.lock();
        try {
            List<ReplayRecord> reserved = new ArrayList<>(retry.readRange(0, batchSize).items());
            if (!reserved.isEmpty()) {
                return reserved;
            }
        } finally {
            retryLock.unlock();
        }

        List<ReplayRecord> moved = new ArrayList<>(batchSize);
        while (moved.size() < batchSize) {
            boolean exhausted;
            retryLock.lock();
            try {
                exhausted = moveBacklogChunkToRetry(moved);
            } finally {
                retryLock.unlock();
            }
            if (exhausted) {
                break;
            }
        }
        return moved;
    }

    /** Move no máximo um chunk; retorna true quando o backlog acabou. retryLock já adquirido. */
    private boolean moveBacklogChunkToRetry(List<ReplayRecord> moved) throws IOException {
        int chunkEnd = Math.min(batchSize, moved.size() + MOVE_CHUNK_SIZE);
        while (moved.size() < chunkEnd) {
            Optional<ReplayRecord> head;
            backlogLock.lock();
            try {
                head = backlog.poll(NONBLOCKING, TimeUnit.MILLISECONDS);
            } finally {
                backlogLock.unlock();
            }
            if (head.isEmpty()) {
                return true;
            }
            // O I/O/serialização da retry não precisa bloquear o produtor do backlog.
            retry.offer(head.get());
            moved.add(head.get());
            // pending inalterado: saiu do backlog, entrou na retry.
        }
        return false;
    }

    /** Confirma (remove) um registro entregue da cabeça da retry. */
    public void ack() throws IOException {
        ack(1);
    }

    /** Confirma o prefixo contíguo entregue numa única aquisição do retryLock. */
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

    /** Expira o prefixo vencido do backlog e reconcilia a profundidade física. */
    public long sweepAndReconcile() throws IOException {
        if (closed) {
            return pending.get();
        }
        retryLock.lock();
        try {
            backlogLock.lock();
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
                backlogLock.unlock();
            }
        } finally {
            retryLock.unlock();
        }
    }

    /** Força fsync das duas filas (group-commit no boundary do batch da origem). */
    public void sync() throws IOException {
        if (closed) {
            return;
        }
        lockMeasuringContention(retryLock, syncLockWaitNanos);
        try {
            lockMeasuringContention(backlogLock, syncLockWaitNanos);
            try {
                backlog.sync();
                retry.sync();
            } finally {
                backlogLock.unlock();
            }
        } finally {
            retryLock.unlock();
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

    /** Tempo acumulado efetivamente aguardando locks nos caminhos executados pela origem. */
    public LockTimingSnapshot lockTimings() {
        return new LockTimingSnapshot(offerLockWaitNanos.get(), syncLockWaitNanos.get());
    }

    private static void lockMeasuringContention(ReentrantLock lock, AtomicLong waitNanos) {
        if (lock.tryLock()) {
            return;
        }
        long started = System.nanoTime();
        lock.lock();
        waitNanos.addAndGet(System.nanoTime() - started);
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

    public record LockTimingSnapshot(long offerLockWaitNanos, long syncLockWaitNanos) {
    }
}
