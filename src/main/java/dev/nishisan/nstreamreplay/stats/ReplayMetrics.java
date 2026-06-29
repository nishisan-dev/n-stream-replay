package dev.nishisan.nstreamreplay.stats;

import dev.nishisan.nstreamreplay.sink.SinkChannel;
import dev.nishisan.utils.stats.StatsUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Fachada de métricas sobre o {@code StatsUtils} do nishi-utils (exposto em
 * {@code /actuator/prometheus} pela ponte do nishi-utils-spring). Centraliza a convenção de
 * nomes {@code nstreamreplay.<dim>.<id>.<metric>} e a sanitização de ids.
 *
 * <p>É <b>null-safe</b>: quando não há bean {@code StatsUtils} no contexto (ex.: stats
 * desabilitado ou em teste fora do Spring), os métodos viram no-op.
 *
 * <p>Eventos de origem usam hit counters (contador + taxa). Os indicadores por destino são
 * empurrados periodicamente como gauges ({@code notifyCurrentValue}): profundidade, online e os
 * totais acumulados (publicados/descartados/expirados/poison/erros), permitindo alertar perda
 * silenciosa via {@code increase()} no Prometheus.
 */
@Component
public class ReplayMetrics {

    private static final String PREFIX = "nstreamreplay";

    private final StatsUtils stats;

    @Autowired
    public ReplayMetrics(ObjectProvider<StatsUtils> provider) {
        this(provider.getIfAvailable());
    }

    public ReplayMetrics(StatsUtils stats) {
        this.stats = stats;
    }

    /** Um record consumido da origem. */
    public void onConsumed(String sourceId) {
        hit("source", sourceId, "consumed");
    }

    /** Uma falha de commit da origem. */
    public void onCommitError(String sourceId) {
        hit("source", sourceId, "commit_errors");
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
    }

    private void hit(String dim, String id, String metric) {
        if (stats != null) {
            stats.notifyHitCounter(name(dim, id, metric));
        }
    }

    private void gauge(String dim, String id, String metric, long value) {
        if (stats != null) {
            stats.notifyCurrentValue(name(dim, id, metric), value);
        }
    }

    private static String name(String dim, String id, String metric) {
        return PREFIX + "." + dim + "." + sanitize(id) + "." + metric;
    }

    /** Mantém só {@code [a-zA-Z0-9_]} (nomes de meter do Micrometer não têm tags aqui). */
    static String sanitize(String id) {
        return id.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
