package dev.nishisan.nstreamreplay.runtime;

import dev.nishisan.nstreamreplay.config.NStreamReplayProperties;
import dev.nishisan.nstreamreplay.config.SinkProperties;
import dev.nishisan.nstreamreplay.config.SourceProperties;
import dev.nishisan.nstreamreplay.pipeline.PipelineRegistry;
import dev.nishisan.nstreamreplay.sink.SinkChannel;
import dev.nishisan.nstreamreplay.source.SourceConsumer;
import dev.nishisan.nstreamreplay.stats.ReplayMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Orquestrador de runtime: constrói e possui as instâncias dinâmicas (canais de destino e
 * consumidores de origem) a partir da configuração validada, controlando start/stop em ordem
 * explícita. {@link SmartLifecycle} de fase alta — sobe depois do servidor web/actuator e para
 * antes dele.
 *
 * <p><b>Ordem de start:</b> abre os canais (recupera backlog do disco) → sobe os forwarders
 * (drenam o backlog independentemente da origem) → sobe os consumidores de origem. <b>Stop</b>
 * inverte: para as origens (wakeup) → fecha os canais (para forwarders, flush/close das filas e
 * producers).
 */
@Component
public class ReplayEngine implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(ReplayEngine.class);
    private static final long METRICS_TICK_SECONDS = 10L;

    private final NStreamReplayProperties properties;
    private final ReplayMetrics metrics;

    private final Map<String, SinkChannel> channels = new LinkedHashMap<>();
    private final List<SourceConsumer> sources = new ArrayList<>();
    private final List<Thread> sourceThreads = new ArrayList<>();
    private ScheduledExecutorService metricsTicker;

    private volatile boolean running = false;

    public ReplayEngine(NStreamReplayProperties properties, ReplayMetrics metrics) {
        this.properties = properties;
        this.metrics = metrics;
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        LOG.info("ReplayEngine iniciando: {} origem(ns), {} destino(s), {} pipeline(s)",
                properties.sources().size(), properties.sinks().size(), properties.pipelines().size());
        try {
            openReferencedChannels();
            PipelineRegistry pipelineRegistry = PipelineRegistry.build(properties.pipelines(), channels);
            channels.values().forEach(SinkChannel::startForwarder);   // forwarders ANTES das origens
            startSources(pipelineRegistry);
            startMetricsTicker();
            running = true;
            LOG.info("ReplayEngine iniciado ({} canal(is), {} origem(ns) ativa(s))",
                    channels.size(), sources.size());
        } catch (RuntimeException e) {
            LOG.error("Falha ao iniciar o ReplayEngine — revertendo recursos abertos", e);
            stop();
            throw e;
        }
    }

    private void openReferencedChannels() {
        Set<String> referenced = new LinkedHashSet<>();
        properties.pipelines().forEach(p -> referenced.addAll(p.sinks()));
        for (SinkProperties sink : properties.sinks()) {
            if (!referenced.contains(sink.id())) {
                LOG.warn("destino '{}' declarado mas não referenciado por nenhum pipeline — ignorado", sink.id());
                continue;
            }
            try {
                channels.put(sink.id(), SinkChannel.open(sink));
            } catch (IOException e) {
                throw new UncheckedIOException("falha ao abrir a fila do destino '" + sink.id() + "'", e);
            }
        }
    }

    private void startSources(PipelineRegistry pipelineRegistry) {
        Set<String> active = pipelineRegistry.activeSourceIds();
        for (SourceProperties src : properties.sources()) {
            if (!active.contains(src.id())) {
                LOG.warn("origem '{}' declarada mas não referenciada por nenhum pipeline — ignorada", src.id());
                continue;
            }
            SourceConsumer consumer = new SourceConsumer(src, pipelineRegistry.targetsFor(src.id()), metrics);
            Thread thread = new Thread(consumer, "nsr-source-" + src.id());
            sources.add(consumer);
            sourceThreads.add(thread);
            thread.start();
        }
    }

    private void startMetricsTicker() {
        metricsTicker = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "nsr-metrics");
            t.setDaemon(true);
            return t;
        });
        metricsTicker.scheduleAtFixedRate(this::pushMetrics,
                METRICS_TICK_SECONDS, METRICS_TICK_SECONDS, TimeUnit.SECONDS);
    }

    private void pushMetrics() {
        try {
            channels.values().forEach(metrics::updateSink);
        } catch (RuntimeException e) {
            LOG.warn("tick de métricas falhou", e);
        }
    }

    @Override
    public synchronized void stop() {
        // Para as origens primeiro (cessa novos offers), depois fecha os canais.
        sources.forEach(SourceConsumer::stop);
        joinAll(sourceThreads);
        sources.clear();
        sourceThreads.clear();

        if (metricsTicker != null) {
            metricsTicker.shutdownNow();
            metricsTicker = null;
        }

        channels.values().forEach(this::closeQuietly);
        channels.clear();
        running = false;
        LOG.info("ReplayEngine parado");
    }

    private void joinAll(List<Thread> threads) {
        for (Thread t : threads) {
            try {
                t.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void closeQuietly(SinkChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            LOG.warn("erro ao fechar o canal '{}'", channel.id(), e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Fase máxima: inicia por último (web/actuator já no ar) e para primeiro.
        return Integer.MAX_VALUE;
    }
}
