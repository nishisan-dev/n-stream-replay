package dev.nishisan.nstreamreplay.stats;

import dev.nishisan.nstreamreplay.sink.SinkChannel;
import dev.nishisan.nstreamreplay.source.SourceConsumer;
import dev.nishisan.utils.stats.StatsUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fachada de métricas sobre o {@code StatsUtils} do nishi-utils (exposto em
 * {@code /actuator/prometheus} pela ponte do nishi-utils-spring). Centraliza a convenção de
 * nomes {@code nstreamreplay.<dim>.<id>.<metric>} e a sanitização de ids.
 *
 * <p>É <b>null-safe</b>: quando não há bean {@code StatsUtils} no contexto (ex.: stats
 * desabilitado ou em teste fora do Spring), os métodos viram no-op.
 *
 * <p><b>Apenas gauges ({@code notifyCurrentValue}):</b> a ponte do nishi-utils-spring, ao receber
 * um {@code notifyHitCounter}, registra no Micrometer um Gauge <i>e</i> um Counter com o
 * <b>mesmo nome</b>, o que o registry rejeita (conflito de tipo). Por isso todos os contadores
 * são mantidos como totais cumulativos aqui e publicados como gauges monotônicos — alertáveis no
 * Prometheus via {@code increase()}.
 */
@Component
public class ReplayMetrics {

    private static final String PREFIX = "nstreamreplay";

    private final StatsUtils stats;
    private final ConcurrentHashMap<String, AtomicLong> consumed = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> commitErrors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> sanitizedIds = new ConcurrentHashMap<>();

    @Autowired
    public ReplayMetrics(ObjectProvider<StatsUtils> provider) {
        this(provider.getIfAvailable());
    }

    public ReplayMetrics(StatsUtils stats) {
        this.stats = stats;
    }

    /** Um record consumido da origem (incrementa o total e publica o gauge). */
    public void onConsumed(String sourceId) {
        long total = consumed.computeIfAbsent(sourceId, k -> new AtomicLong()).incrementAndGet();
        gauge("source", sourceId, "consumed_total", total);
    }

    /** Uma falha de commit da origem. */
    public void onCommitError(String sourceId) {
        long total = commitErrors.computeIfAbsent(sourceId, k -> new AtomicLong()).incrementAndGet();
        gauge("source", sourceId, "commit_errors_total", total);
    }

    /** Empurra os indicadores correntes de um destino (chamado periodicamente pelo engine). */
    public void updateSink(SinkChannel ch) {
        gauge("sink", ch.id(), "depth", ch.depth());
        gauge("sink", ch.id(), "online", ch.online() ? 1L : 0L);
        gauge("sink", ch.id(), "published_total", ch.publishedTotal());
        gauge("sink", ch.id(), "dropped_total", ch.dropped());
        gauge("sink", ch.id(), "expired_total", ch.expired());
        gauge("sink", ch.id(), "poisoned_total", ch.poisonedTotal());
        gauge("sink", ch.id(), "offer_errors_total", ch.offerErrors());
        gauge("sink", ch.id(), "retry_backoffs_total", ch.retryBackoffs());
        var timings = ch.timings();
        gauge("sink", ch.id(), "queue_peek_nanos_total", timings.queuePeekNanos());
        gauge("sink", ch.id(), "publish_nanos_total", timings.publishNanos());
        gauge("sink", ch.id(), "ack_nanos_total", timings.ackNanos());
        var locks = ch.lockTimings();
        gauge("sink", ch.id(), "offer_lock_wait_nanos_total", locks.offerLockWaitNanos());
        gauge("sink", ch.id(), "sync_lock_wait_nanos_total", locks.syncLockWaitNanos());
    }

    /** Empurra contadores e tempos correntes da origem em baixa frequência. */
    public void updateSource(SourceConsumer source) {
        gauge("source", source.id(), "consumed_total", source.consumedTotal());
        gauge("source", source.id(), "configured_rate_per_sec", source.configuredMaxConsumeRatePerSec());
        var timings = source.timings();
        gauge("source", source.id(), "poll_nanos_total", timings.pollNanos());
        gauge("source", source.id(), "limiter_wait_nanos_total", timings.limiterWaitNanos());
        gauge("source", source.id(), "offer_nanos_total", timings.offerNanos());
        gauge("source", source.id(), "sync_nanos_total", timings.syncNanos());
        gauge("source", source.id(), "commit_nanos_total", timings.commitNanos());
        gauge("source", source.id(), "polled_batches_total", timings.polledBatches());
    }

    private void gauge(String dim, String id, String metric, long value) {
        if (stats != null) {
            stats.notifyCurrentValue(name(dim, id, metric), value);
        }
    }

    private String name(String dim, String id, String metric) {
        return PREFIX + "." + dim + "." + sanitizedIds.computeIfAbsent(id, ReplayMetrics::sanitize)
                + "." + metric;
    }

    /** Mantém só {@code [a-zA-Z0-9_]} (nomes de meter do Micrometer não têm tags aqui). */
    static String sanitize(String id) {
        return id.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
