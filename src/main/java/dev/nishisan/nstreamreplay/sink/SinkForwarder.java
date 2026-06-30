package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.model.ReplayRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Drena o {@link DurableSinkQueue} e publica no {@link Sink}, confirmando (FIFO) apenas o
 * prefixo contíguo efetivamente entregue. Roda na própria thread daemon; para de forma
 * cooperativa via {@link #stop()}. Espelha o {@code MetricStreamForwarder} do ape-probe.
 *
 * <p><b>Store-and-forward:</b> com o broker fora, o sink entrega 0 (ou parcial); os itens não
 * confirmados permanecem na fila (disco) e o forwarder faz backoff antes de retentar o mesmo
 * lote — sem perda até o teto da fila. Um único forwarder FIFO por destino preserva a ordem.
 *
 * <p>Faz periodicamente {@code sweepAndReconcile} (expira por tempo o backlog e reconcilia a
 * profundidade), inclusive enquanto o destino está offline — quando o draining não ocorre.
 */
public final class SinkForwarder implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(SinkForwarder.class);
    private static final long IDLE_SLEEP_MS = 100L;
    private static final long SWEEP_INTERVAL_MS = 1_000L;

    private final String sinkId;
    private final DurableSinkQueue queue;
    private final Sink sink;
    private final long retryBackoffMs;

    private final AtomicLong publishedTotal = new AtomicLong();
    private final AtomicLong poisonedTotal = new AtomicLong();
    private final AtomicLong retryBackoffs = new AtomicLong();
    /** Otimista: começa online; só vira offline quando uma publicação com itens falha. */
    private volatile boolean online = true;
    private volatile boolean running = true;

    private long lastSweepNanos = System.nanoTime() - msToNanos(SWEEP_INTERVAL_MS);

    public SinkForwarder(String sinkId, DurableSinkQueue queue, Sink sink, long retryBackoffMs) {
        this.sinkId = sinkId;
        this.queue = queue;
        this.sink = sink;
        this.retryBackoffMs = Math.max(0L, retryBackoffMs);
    }

    @Override
    public void run() {
        LOG.info("[{}] forwarder iniciado", sinkId);
        while (running) {
            try {
                maybeSweep();
                boolean had = forwardOnce();
                if (!had && running) {
                    Thread.sleep(IDLE_SLEEP_MS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.warn("[{}] forwarder — ciclo falhou, retentando", sinkId, e);
                if (!sleep(retryBackoffMs)) {
                    break;
                }
            }
        }
        LOG.info("[{}] forwarder encerrado", sinkId);
    }

    /** Sinaliza o encerramento cooperativo do loop. */
    public void stop() {
        this.running = false;
    }

    /**
     * Processa um lote: publica e confirma o prefixo entregue. Visível ao pacote para teste sem
     * subir a thread. Retorna {@code true} se havia itens.
     */
    boolean forwardOnce() throws Exception {
        List<ReplayRecord> items = queue.peekBatch();
        if (items.isEmpty()) {
            return false;
        }
        PublishOutcome outcome;
        try {
            outcome = sink.publish(items);
        } catch (Exception e) {
            LOG.warn("[{}] broker indisponível — {} item(ns) permanecem na fila", sinkId, items.size(), e);
            outcome = PublishOutcome.none();
        }
        int acked = outcome.acked();
        queue.ack(acked);   // confirma o prefixo entregue numa única seção crítica (retryLock)
        if (outcome.published() > 0) {
            publishedTotal.addAndGet(outcome.published());
        }
        if (outcome.poisoned() > 0) {
            poisonedTotal.addAndGet(outcome.poisoned());
        }
        online = acked > 0;   // entregou/descartou algo => broker alcançável; none() => offline
        if (acked < items.size() && running) {
            retryBackoffs.incrementAndGet();
            sleep(retryBackoffMs);
        }
        return true;
    }

    private void maybeSweep() throws java.io.IOException {
        long now = System.nanoTime();
        if (now - lastSweepNanos >= msToNanos(SWEEP_INTERVAL_MS)) {
            queue.sweepAndReconcile();
            lastSweepNanos = now;
        }
    }

    public long publishedTotal() {
        return publishedTotal.get();
    }

    public long poisonedTotal() {
        return poisonedTotal.get();
    }

    public long retryBackoffs() {
        return retryBackoffs.get();
    }

    public boolean online() {
        return online;
    }

    private static long msToNanos(long ms) {
        return ms * 1_000_000L;
    }

    private boolean sleep(long millis) {
        if (millis <= 0) {
            return running;
        }
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
