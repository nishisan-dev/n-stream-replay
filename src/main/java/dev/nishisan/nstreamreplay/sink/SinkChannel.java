package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.SinkProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Unidade de isolamento de um destino: agrega a {@link DurableSinkQueue}, o {@link Sink} e o
 * {@link SinkForwarder} (em sua própria thread daemon). A origem apenas {@link #offer} no canal;
 * o forwarder drena e entrega de forma independente. Um destino offline só faz crescer/dropar a
 * sua fila — sem afetar a origem nem os demais destinos.
 */
public final class SinkChannel implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SinkChannel.class);

    private final String id;
    private final DurableSinkQueue queue;
    private final Sink sink;
    private final SinkForwarder forwarder;
    private final Durability durability;

    private volatile Thread thread;

    SinkChannel(String id, DurableSinkQueue queue, Sink sink, SinkForwarder forwarder, Durability durability) {
        this.id = id;
        this.queue = queue;
        this.sink = sink;
        this.forwarder = forwarder;
        this.durability = durability;
    }

    /** Constrói o canal completo (fila em disco + producer Kafka + forwarder) a partir da config. */
    public static SinkChannel open(SinkProperties props) throws IOException {
        DurableSinkQueue queue = DurableSinkQueue.open(props.id(), props.queue());
        Sink sink = new KafkaSink(props);
        SinkForwarder forwarder = new SinkForwarder(
                props.id(), queue, sink, props.queue().forwarderRetryBackoffMs());
        return new SinkChannel(props.id(), queue, sink, forwarder, props.queue().durability());
    }

    /** Sobe a thread do forwarder (chamado pelo engine ANTES de iniciar as origens). */
    public void startForwarder() {
        Thread t = new Thread(forwarder, "nsr-fwd-" + id);
        t.setDaemon(true);
        t.start();
        this.thread = t;
        LOG.info("[{}] canal iniciado (profundidade inicial={})", id, queue.depth());
    }

    /**
     * Enfileira o registro na fila durável deste destino (não bloqueia). Pode lançar
     * {@link IOException} (ex.: disco cheio) — a origem decide drop/halt conforme a política.
     */
    public boolean offer(ReplayRecord record) throws IOException {
        return queue.offer(record);
    }

    /** A durabilidade exige {@code sync()} no boundary do commit da origem? (apenas SYNC_ON_COMMIT). */
    public boolean requiresSyncBeforeCommit() {
        return durability == Durability.SYNC_ON_COMMIT;
    }

    /** Força fsync da fila (group-commit antes do commit da origem). */
    public void sync() throws IOException {
        queue.sync();
    }

    public String id() {
        return id;
    }

    public long depth() {
        return queue.depth();
    }

    public long dropped() {
        return queue.dropped();
    }

    public long expired() {
        return queue.expired();
    }

    public long publishedTotal() {
        return forwarder.publishedTotal();
    }

    public long poisonedTotal() {
        return forwarder.poisonedTotal();
    }

    public long retryBackoffs() {
        return forwarder.retryBackoffs();
    }

    public boolean online() {
        return forwarder.online();
    }

    /** Para o forwarder, faz join, fecha o producer e a fila (libera o lock do disco). */
    @Override
    public void close() throws IOException {
        forwarder.stop();
        Thread t = this.thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            sink.close();
        } catch (Exception e) {
            LOG.warn("[{}] erro ao fechar o sink", id, e);
        }
        queue.close();
        LOG.info("[{}] canal encerrado", id);
    }
}
