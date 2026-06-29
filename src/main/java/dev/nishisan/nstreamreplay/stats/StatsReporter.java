package dev.nishisan.nstreamreplay.stats;

import dev.nishisan.nstreamreplay.sink.SinkChannel;
import dev.nishisan.nstreamreplay.source.SourceConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Relatório periódico de stats em arquivo: a cada intervalo (default 30s), emite um bloco no logger
 * dedicado {@code nsr.stats} (roteado para {@code logs/n-stream-replay-stats.log} pelo logback) com,
 * por origem, eventos/s, total consumido, offset lag e time lag; e, por destino, profundidade da
 * fila, publicados (total + /s), descartes (dropped/expired/poison), estado online e backoffs.
 * As taxas são derivadas do delta dos contadores cumulativos entre relatórios.
 *
 * <p>Padrão espelhado do {@code StatsDashboardReporter} (ngrrd/ape-probe): {@link #format} é puro
 * (testável) e o {@code report()} apenas lê os componentes vivos, calcula deltas e loga.
 */
public final class StatsReporter {

    private static final Logger STATS = LoggerFactory.getLogger("nsr.stats");

    private final List<SourceConsumer> sources;
    private final List<SinkChannel> sinks;
    private final long intervalMs;

    private final Map<String, Long> prevConsumed = new HashMap<>();
    private final Map<String, Long> prevPublished = new HashMap<>();
    private long lastReportMs = -1L;

    public StatsReporter(List<SourceConsumer> sources, Collection<SinkChannel> sinks, long intervalMs) {
        this.sources = List.copyOf(sources);
        this.sinks = List.copyOf(sinks);
        this.intervalMs = Math.max(1L, intervalMs);
    }

    /** Lê os componentes vivos, calcula taxas pelo delta e emite o bloco no logger {@code nsr.stats}. */
    public void report() {
        long now = System.currentTimeMillis();
        long elapsedMs = lastReportMs < 0 ? intervalMs : Math.max(1L, now - lastReportMs);
        lastReportMs = now;

        List<SourceLine> sourceLines = new ArrayList<>(sources.size());
        for (SourceConsumer s : sources) {
            long total = s.consumedTotal();
            long prev = prevConsumed.getOrDefault(s.id(), 0L);
            prevConsumed.put(s.id(), total);
            double rate = (total - prev) * 1000.0 / elapsedMs;
            sourceLines.add(new SourceLine(s.id(), total, rate, s.offsetLag(), s.ingestTimeLagMillis()));
        }

        List<SinkLine> sinkLines = new ArrayList<>(sinks.size());
        for (SinkChannel ch : sinks) {
            long published = ch.publishedTotal();
            long prev = prevPublished.getOrDefault(ch.id(), 0L);
            prevPublished.put(ch.id(), published);
            double rate = (published - prev) * 1000.0 / elapsedMs;
            sinkLines.add(new SinkLine(ch.id(), ch.depth(), published, rate, ch.dropped(), ch.expired(),
                    ch.poisonedTotal(), ch.online(), ch.retryBackoffs(), ch.offerErrors()));
        }

        STATS.info("stats (janela {} ms)\n{}", elapsedMs, format(sourceLines, sinkLines));
    }

    /** Formata o bloco de stats (função pura, sem efeitos — testável). */
    static String format(List<SourceLine> sources, List<SinkLine> sinks) {
        StringBuilder sb = new StringBuilder();
        sb.append("SOURCES:\n");
        sb.append(String.format(Locale.ROOT, "  %-20s %12s %12s %12s %12s%n",
                "id", "events/s", "consumed", "offsetLag", "timeLag(ms)"));
        for (SourceLine s : sources) {
            sb.append(String.format(Locale.ROOT, "  %-20s %12.1f %12d %12s %12s%n",
                    s.id(), s.eventsPerSec(), s.consumed(),
                    s.offsetLag() < 0 ? "n/a" : Long.toString(s.offsetLag()),
                    s.timeLagMs() < 0 ? "n/a" : Long.toString(s.timeLagMs())));
        }
        sb.append("SINKS:\n");
        sb.append(String.format(Locale.ROOT, "  %-20s %8s %12s %10s %8s %8s %8s %8s %8s%n",
                "id", "depth", "pub/s", "published", "dropped", "expired", "poison", "online", "backoff"));
        for (SinkLine s : sinks) {
            sb.append(String.format(Locale.ROOT, "  %-20s %8d %12.1f %10d %8d %8d %8d %8s %8d%n",
                    s.id(), s.depth(), s.publishedPerSec(), s.published(), s.dropped(), s.expired(),
                    s.poisoned(), s.online() ? "up" : "DOWN", s.retryBackoffs()));
        }
        return sb.toString();
    }

    public record SourceLine(String id, long consumed, double eventsPerSec, long offsetLag, long timeLagMs) {
    }

    public record SinkLine(String id, long depth, long published, double publishedPerSec,
                           long dropped, long expired, long poisoned, boolean online,
                           long retryBackoffs, long offerErrors) {
    }
}
