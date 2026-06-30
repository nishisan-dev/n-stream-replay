package dev.nishisan.nstreamreplay.stats;

import dev.nishisan.nstreamreplay.sink.DurableSinkQueue;
import dev.nishisan.nstreamreplay.sink.SinkChannel;
import dev.nishisan.nstreamreplay.sink.SinkForwarder;
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
    private final Map<String, SourceConsumer.TimingSnapshot> prevSourceTimings = new HashMap<>();
    private final Map<String, SinkForwarder.TimingSnapshot> prevSinkTimings = new HashMap<>();
    private final Map<String, DurableSinkQueue.LockTimingSnapshot> prevLockTimings = new HashMap<>();
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
        List<SourceTimingLine> sourceTimingLines = new ArrayList<>(sources.size());
        for (SourceConsumer s : sources) {
            long total = s.consumedTotal();
            long prev = prevConsumed.getOrDefault(s.id(), 0L);
            prevConsumed.put(s.id(), total);
            double rate = (total - prev) * 1000.0 / elapsedMs;
            sourceLines.add(new SourceLine(s.id(), total, rate, s.offsetLag(), s.ingestTimeLagMillis()));

            SourceConsumer.TimingSnapshot currentTiming = s.timings();
            SourceConsumer.TimingSnapshot previousTiming = prevSourceTimings.put(s.id(), currentTiming);
            sourceTimingLines.add(sourceTimingLine(s.id(), currentTiming, previousTiming));
        }

        List<SinkLine> sinkLines = new ArrayList<>(sinks.size());
        List<SinkTimingLine> sinkTimingLines = new ArrayList<>(sinks.size());
        for (SinkChannel ch : sinks) {
            long published = ch.publishedTotal();
            long prev = prevPublished.getOrDefault(ch.id(), 0L);
            prevPublished.put(ch.id(), published);
            double rate = (published - prev) * 1000.0 / elapsedMs;
            sinkLines.add(new SinkLine(ch.id(), ch.depth(), published, rate, ch.dropped(), ch.expired(),
                    ch.poisonedTotal(), ch.online(), ch.retryBackoffs(), ch.offerErrors()));

            var currentTiming = ch.timings();
            var previousTiming = prevSinkTimings.put(ch.id(), currentTiming);
            var currentLocks = ch.lockTimings();
            var previousLocks = prevLockTimings.put(ch.id(), currentLocks);
            sinkTimingLines.add(sinkTimingLine(ch.id(), currentTiming, previousTiming,
                    currentLocks, previousLocks));
        }

        STATS.info("stats (janela {} ms)\n{}{}", elapsedMs, format(sourceLines, sinkLines),
                formatTimings(sourceTimingLines, sinkTimingLines));
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

    /** Formata tempos gastos em cada etapa na janela atual (milissegundos acumulados). */
    static String formatTimings(List<SourceTimingLine> sources, List<SinkTimingLine> sinks) {
        StringBuilder sb = new StringBuilder();
        sb.append("TIMINGS (ms/window):\n");
        sb.append(String.format(Locale.ROOT, "  SOURCE %-16s %9s %9s %9s %9s %9s %8s%n",
                "id", "poll", "limiter", "offer", "sync", "commit", "batches"));
        for (SourceTimingLine s : sources) {
            sb.append(String.format(Locale.ROOT, "  SOURCE %-16s %9d %9d %9d %9d %9d %8d%n",
                    s.id(), s.pollMs(), s.limiterMs(), s.offerMs(), s.syncMs(), s.commitMs(), s.batches()));
        }
        sb.append(String.format(Locale.ROOT, "  SINK   %-16s %9s %9s %9s %12s %12s%n",
                "id", "peek", "publish", "ack", "offerLock", "syncLock"));
        for (SinkTimingLine s : sinks) {
            sb.append(String.format(Locale.ROOT, "  SINK   %-16s %9d %9d %9d %12d %12d%n",
                    s.id(), s.peekMs(), s.publishMs(), s.ackMs(),
                    s.offerLockWaitMs(), s.syncLockWaitMs()));
        }
        return sb.toString();
    }

    private static SourceTimingLine sourceTimingLine(String id, SourceConsumer.TimingSnapshot current,
                                                      SourceConsumer.TimingSnapshot previous) {
        SourceConsumer.TimingSnapshot p = previous == null
                ? new SourceConsumer.TimingSnapshot(0, 0, 0, 0, 0, 0) : previous;
        return new SourceTimingLine(id,
                nanosToMillis(delta(current.pollNanos(), p.pollNanos())),
                nanosToMillis(delta(current.limiterWaitNanos(), p.limiterWaitNanos())),
                nanosToMillis(delta(current.offerNanos(), p.offerNanos())),
                nanosToMillis(delta(current.syncNanos(), p.syncNanos())),
                nanosToMillis(delta(current.commitNanos(), p.commitNanos())),
                delta(current.polledBatches(), p.polledBatches()));
    }

    private static SinkTimingLine sinkTimingLine(
            String id,
            SinkForwarder.TimingSnapshot current,
            SinkForwarder.TimingSnapshot previous,
            DurableSinkQueue.LockTimingSnapshot currentLocks,
            DurableSinkQueue.LockTimingSnapshot previousLocks) {
        var p = previous == null
                ? new SinkForwarder.TimingSnapshot(0, 0, 0) : previous;
        var lp = previousLocks == null
                ? new DurableSinkQueue.LockTimingSnapshot(0, 0) : previousLocks;
        return new SinkTimingLine(id,
                nanosToMillis(delta(current.queuePeekNanos(), p.queuePeekNanos())),
                nanosToMillis(delta(current.publishNanos(), p.publishNanos())),
                nanosToMillis(delta(current.ackNanos(), p.ackNanos())),
                nanosToMillis(delta(currentLocks.offerLockWaitNanos(), lp.offerLockWaitNanos())),
                nanosToMillis(delta(currentLocks.syncLockWaitNanos(), lp.syncLockWaitNanos())));
    }

    private static long delta(long current, long previous) {
        return Math.max(0L, current - previous);
    }

    private static long nanosToMillis(long nanos) {
        return nanos / 1_000_000L;
    }

    public record SourceLine(String id, long consumed, double eventsPerSec, long offsetLag, long timeLagMs) {
    }

    public record SinkLine(String id, long depth, long published, double publishedPerSec,
                           long dropped, long expired, long poisoned, boolean online,
                           long retryBackoffs, long offerErrors) {
    }

    record SourceTimingLine(String id, long pollMs, long limiterMs, long offerMs,
                            long syncMs, long commitMs, long batches) {
    }

    record SinkTimingLine(String id, long peekMs, long publishMs, long ackMs,
                          long offerLockWaitMs, long syncLockWaitMs) {
    }
}
