package dev.nishisan.nstreamreplay.source;

import com.fasterxml.jackson.databind.JsonNode;
import dev.nishisan.nstreamreplay.config.SourceProperties;
import dev.nishisan.nstreamreplay.decoder.Decoder;
import dev.nishisan.nstreamreplay.decoder.Decoders;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import dev.nishisan.nstreamreplay.route.PartitionKey;
import dev.nishisan.nstreamreplay.route.RouteTable;
import dev.nishisan.nstreamreplay.sink.SinkTarget;
import dev.nishisan.nstreamreplay.stats.ReplayMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Consumidor de uma origem: roda na própria thread, dono exclusivo do {@code KafkaConsumer} (não
 * thread-safe). {@code enable.auto.commit=false}, poll loop, e <b>commit só depois</b> de enfileirar
 * o lote nos destinos — at-least-once.
 *
 * <p><b>Roteamento por tópico (v2):</b> assina a união dos {@code fromTopic} das rotas desta origem
 * e, por record, resolve os alvos de {@code (origem, tópico do record)} via {@link RouteTable},
 * gravando em cada destino um {@link ReplayRecord} com o {@code destinationTopic} resolvido
 * (explícito ou preservado). Alvos {@code (sink, destino)} idênticos no mesmo record são dedup.
 *
 * <p><b>Backpressure ({@code overflowPolicy=block}):</b> antes de cada {@code poll}, as partições
 * cujos tópicos alimentam algum destino {@link SinkTarget#saturated() saturado} recebem
 * {@code pause()}; elas voltam com {@code resume()} quando o destino drena. O loop <b>continua
 * chamando {@code poll}</b> (que não devolve registros das partições pausadas), então o consumer
 * mantém heartbeat e {@code max.poll.interval.ms} em dia e o grupo não sofre rebalance, e nada é
 * descartado: o que não foi lido continua no tópico de origem. Como o freio atua entre polls, a
 * fila pode exceder {@code maxDepth} em até um lote ({@code maxPollRecords}) por origem. Um destino
 * saturado freia todo tópico que roteia para ele, inclusive o fan-out para destinos saudáveis.
 *
 * <p><b>sync-on-commit:</b> antes do {@code commitSync()}, força {@code sync()} nos destinos
 * tocados pelo lote que o exigem (group-commit).
 */
public final class SourceConsumer implements Runnable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SourceConsumer.class);

    /** Intervalo de cálculo do offset lag (chamada de rede ao broker; throttled). */
    private static final long LAG_INTERVAL_NANOS = 10_000_000_000L;

    private final SourceProperties props;
    private final RouteTable routes;
    private final ReplayMetrics metrics;
    private final Supplier<Consumer<byte[], byte[]>> consumerFactory;
    private final Duration pollTimeout;
    private final ConsumeRateLimiter rateLimiter;
    private final Map<String, Decoder> decoders = new LinkedHashMap<>();
    private final AtomicLong decodeErrors = new AtomicLong();
    private final AtomicLong decodeDropped = new AtomicLong();
    private final AtomicLong keyMissingDropped = new AtomicLong();
    private final AtomicLong keyTypeDropped = new AtomicLong();

    private final AtomicLong consumedTotal = new AtomicLong();
    private final AtomicLong pollNanos = new AtomicLong();
    private final AtomicLong limiterWaitNanos = new AtomicLong();
    private final AtomicLong offerNanos = new AtomicLong();
    private final AtomicLong syncNanos = new AtomicLong();
    private final AtomicLong commitNanos = new AtomicLong();
    private final AtomicLong polledBatches = new AtomicLong();
    private final AtomicLong backpressurePauses = new AtomicLong();
    private final AtomicLong pausedPartitions = new AtomicLong();
    /** Sigma(endOffsets - position) das partições atribuídas; -1 = ainda não calculado/indisponível. */
    private final AtomicLong offsetLag = new AtomicLong(-1L);
    /** now - timestamp do record mais novo do último poll (ms); -1 = nenhum record ainda. */
    private final AtomicLong ingestTimeLagMillis = new AtomicLong(-1L);
    private long lastLagNanos = System.nanoTime() - LAG_INTERVAL_NANOS;

    private volatile boolean running = true;
    private volatile Consumer<byte[], byte[]> consumer;

    public SourceConsumer(SourceProperties props, RouteTable routes, ReplayMetrics metrics) {
        this(props, routes, metrics, () -> new KafkaConsumer<>(consumerProperties(props)));
    }

    SourceConsumer(SourceProperties props, RouteTable routes, ReplayMetrics metrics,
                   Supplier<Consumer<byte[], byte[]>> consumerFactory) {
        this.props = props;
        decoders.put(props.decoder(), Decoders.create(props.decoder()));
        for (String topic : routes.topicsFor(props.id())) {
            for (RouteTable.Target target : routes.targetsFor(props.id(), topic)) {
                if (target.decoder() != null) {
                    decoders.computeIfAbsent(target.decoder(), Decoders::create);
                }
            }
        }
        this.routes = routes;
        this.metrics = metrics;
        this.consumerFactory = consumerFactory;
        this.pollTimeout = Duration.ofMillis(props.pollTimeoutMs());
        this.rateLimiter = new ConsumeRateLimiter(props.maxConsumeRatePerSec());
    }

    static Properties consumerProperties(SourceProperties props) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.bootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, props.groupId());
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, props.resolvedClientId());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, props.autoOffsetReset());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, props.maxPollRecords());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.extraProps().forEach(p::put);
        return p;
    }

    @Override
    public void run() {
        java.util.List<String> topics = routes.topicsFor(props.id());
        try (Consumer<byte[], byte[]> c = consumerFactory.get()) {
            consumer = c;
            c.subscribe(topics, new LoggingRebalanceListener());
            LOG.info("[{}] origem inscrita nos tópicos {} (groupId={})", props.id(), topics, props.groupId());
            if (rateLimiter.enabled()) {
                LOG.info("[{}] rate limit de consumo: {} registros/s", props.id(), rateLimiter.permitsPerSec());
            } else {
                LOG.info("[{}] rate limit de consumo: ilimitado (maxConsumeRatePerSec=0)", props.id());
            }
            LOG.info("[{}] consumo configurado: maxPollRecords={}, pollTimeoutMs={}",
                    props.id(), props.maxPollRecords(), props.pollTimeoutMs());
            while (running) {
                applyBackpressure(c);
                long pollStarted = System.nanoTime();
                ConsumerRecords<byte[], byte[]> records;
                try {
                    records = c.poll(pollTimeout);
                } finally {
                    pollNanos.addAndGet(System.nanoTime() - pollStarted);
                }
                maybeComputeOffsetLag(c);
                if (records.isEmpty()) {
                    continue;
                }
                polledBatches.incrementAndGet();
                updateTimeLag(records);
                enqueueBatch(records);
                commit(c);
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        } catch (IOException e) {
            LOG.error("[{}] origem interrompida por falha de escrita local (HALT) — sem commit", props.id(), e);
        } finally {
            consumer = null;
            LOG.info("[{}] origem encerrada", props.id());
        }
    }

    /**
     * Roteia o lote por tópico: para cada record, enfileira nos alvos de
     * {@code (origem, record.topic())} com o destino resolvido; depois faz o group-commit (sync)
     * dos destinos tocados que o exigem. Visível ao pacote para teste sem subir a thread.
     */
    void enqueueBatch(ConsumerRecords<byte[], byte[]> records) throws IOException {
        Set<SinkTarget> touched = new LinkedHashSet<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            long limiterStarted = System.nanoTime();
            rateLimiter.acquire();   // pacing do consumo (no-op se ilimitado)
            limiterWaitNanos.addAndGet(System.nanoTime() - limiterStarted);
            consumedTotal.incrementAndGet();
            metrics.onConsumed(props.id());
            Map<String, JsonNode> decoded = new LinkedHashMap<>();
            Set<String> failedDecoders = new HashSet<>();
            Set<String> seen = new HashSet<>();
            for (RouteTable.Target target : routes.targetsFor(props.id(), record.topic())) {
                String destinationTopic = target.resolveTopic(record.topic());
                if (!seen.add(target.sink().id() + '\0' + destinationTopic)) {
                    continue;   // dedup de (sink, destino) idênticos no mesmo record
                }
                byte[] key = record.key();
                if (target.partitionKey() != null) {
                    String format = target.decoder() == null ? props.decoder() : target.decoder();
                    if (!decoded.containsKey(format) && !failedDecoders.contains(format)) {
                        try {
                            decoded.put(format, decoders.get(format).decode(record.value()));
                        } catch (IOException e) {
                            if (failedDecoders.isEmpty()) decodeErrors.incrementAndGet();
                            failedDecoders.add(format);
                        }
                    }
                    if (failedDecoders.contains(format)) {
                        decodeDropped.incrementAndGet();
                        continue;
                    }
                    PartitionKey.Result result = target.partitionKey().resolve(decoded.get(format));
                    if (result.failure() != null) {
                        if (result.failure() == PartitionKey.Failure.MISSING) {
                            keyMissingDropped.incrementAndGet();
                        } else {
                            keyTypeDropped.incrementAndGet();
                        }
                        continue;
                    }
                    key = result.key();
                }
                long offerStarted = System.nanoTime();
                try {
                    // preserve: a partição de origem vira a partição de destino; nula = pela key.
                    Integer destinationPartition = target.preservesPartition() ? record.partition() : null;
                    target.sink().offer(toReplayRecord(record, destinationTopic, key, destinationPartition));
                } finally {
                    offerNanos.addAndGet(System.nanoTime() - offerStarted);
                }
                touched.add(target.sink());
            }
        }
        // Group-commit: durabiliza as filas tocadas antes do commit do offset.
        for (SinkTarget target : touched) {
            if (target.requiresSyncBeforeCommit()) {
                long syncStarted = System.nanoTime();
                try {
                    target.sync();
                } finally {
                    syncNanos.addAndGet(System.nanoTime() - syncStarted);
                }
            }
        }
    }

    /**
     * Reconcilia {@code pause()}/{@code resume()} com a saturação dos destinos: pausa as partições
     * atribuídas cujo tópico roteia para um destino saturado e retoma as pausadas que deixaram de
     * ser. Roda na thread de poll (dona do consumer). Visível ao pacote para teste.
     */
    void applyBackpressure(Consumer<byte[], byte[]> c) {
        Set<TopicPartition> paused = c.paused();
        Set<String> blockedTopics = new HashSet<>();
        for (String topic : routes.topicsFor(props.id())) {
            for (RouteTable.Target target : routes.targetsFor(props.id(), topic)) {
                if (target.sink().saturated()) {
                    blockedTopics.add(topic);
                    break;
                }
            }
        }
        if (blockedTopics.isEmpty() && paused.isEmpty()) {
            return;   // caminho comum: ninguém saturado, nada pausado
        }
        java.util.List<TopicPartition> toPause = new java.util.ArrayList<>();
        java.util.List<TopicPartition> toResume = new java.util.ArrayList<>();
        for (TopicPartition tp : c.assignment()) {
            boolean shouldPause = blockedTopics.contains(tp.topic());
            boolean isPaused = paused.contains(tp);
            if (shouldPause && !isPaused) {
                toPause.add(tp);
            } else if (!shouldPause && isPaused) {
                toResume.add(tp);
            }
        }
        if (!toPause.isEmpty()) {
            c.pause(toPause);
            backpressurePauses.incrementAndGet();
            LOG.warn("[{}] destino saturado — origem freada (pause) em {}", props.id(), toPause);
        }
        if (!toResume.isEmpty()) {
            c.resume(toResume);
            LOG.info("[{}] destino drenou — origem retomada (resume) em {}", props.id(), toResume);
        }
        pausedPartitions.set(c.paused().size());
    }

    private void commit(Consumer<byte[], byte[]> c) {
        long commitStarted = System.nanoTime();
        try {
            c.commitSync();
        } catch (KafkaException e) {
            metrics.onCommitError(props.id());
            LOG.warn("[{}] commitSync falhou — lote será reprocessado", props.id(), e);
        } finally {
            commitNanos.addAndGet(System.nanoTime() - commitStarted);
        }
    }

    private static ReplayRecord toReplayRecord(ConsumerRecord<byte[], byte[]> r, String destinationTopic,
                                               byte[] key, Integer destinationPartition) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        for (Header h : r.headers()) {
            headers.put(h.key(), h.value());
        }
        return new ReplayRecord(r.topic(), r.partition(), r.offset(), r.timestamp(),
                key, r.value(), headers, destinationTopic, destinationPartition);
    }

    /**
     * Calcula, de forma throttled, o offset lag total (Sigma endOffsets - position) das partições
     * atribuídas. Roda na thread de poll. Em falha (broker indisponível), marca -1.
     */
    private void maybeComputeOffsetLag(Consumer<byte[], byte[]> c) {
        long now = System.nanoTime();
        if (now - lastLagNanos < LAG_INTERVAL_NANOS) {
            return;
        }
        lastLagNanos = now;
        try {
            Set<TopicPartition> assigned = c.assignment();
            if (assigned.isEmpty()) {
                return;
            }
            Map<TopicPartition, Long> end = c.endOffsets(assigned);
            long lag = 0L;
            for (TopicPartition tp : assigned) {
                long position = c.position(tp);
                long endOffset = end.getOrDefault(tp, position);
                lag += Math.max(0L, endOffset - position);
            }
            offsetLag.set(lag);
        } catch (Exception e) {
            offsetLag.set(-1L);
        }
    }

    private void updateTimeLag(ConsumerRecords<byte[], byte[]> records) {
        long newest = -1L;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            if (record.timestamp() > newest) {
                newest = record.timestamp();
            }
        }
        if (newest > 0) {
            ingestTimeLagMillis.set(Math.max(0L, System.currentTimeMillis() - newest));
        }
    }

    public long consumedTotal() {
        return consumedTotal.get();
    }

    /** Quantas vezes a origem foi freada por destino saturado (pause) — backpressure do {@code block}. */
    public long backpressurePauses() {
        return backpressurePauses.get();
    }

    /** Partições atualmente pausadas por backpressure. */
    public long pausedPartitions() {
        return pausedPartitions.get();
    }

    public long offsetLag() {
        return offsetLag.get();
    }

    public long ingestTimeLagMillis() {
        return ingestTimeLagMillis.get();
    }

    /** Totais monotônicos de tempo por etapa, para diagnóstico de gargalos. */
    public TimingSnapshot timings() {
        return new TimingSnapshot(pollNanos.get(), limiterWaitNanos.get(), offerNanos.get(),
                syncNanos.get(), commitNanos.get(), polledBatches.get());
    }

    public RoutingSnapshot routingStats() {
        return new RoutingSnapshot(decodeErrors.get(), decodeDropped.get(), keyMissingDropped.get(), keyTypeDropped.get());
    }

    public record RoutingSnapshot(long decodeErrors, long decodeDropped, long keyMissingDropped, long keyTypeDropped) { }

    public long configuredMaxConsumeRatePerSec() {
        return props.maxConsumeRatePerSec();
    }

    /** Encerramento cooperativo: cessa o loop e acorda o {@code poll} via {@code wakeup}. */
    public void stop() {
        running = false;
        Consumer<byte[], byte[]> c = consumer;
        if (c != null) {
            c.wakeup();
        }
    }

    @Override
    public void close() {
        stop();
    }

    public String id() {
        return props.id();
    }

    public record TimingSnapshot(long pollNanos, long limiterWaitNanos, long offerNanos,
                                 long syncNanos, long commitNanos, long polledBatches) {
    }

    private final class LoggingRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.info("[{}] partições revogadas: {}", props.id(), partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.info("[{}] partições atribuídas: {}", props.id(), partitions);
            // Partições novas nascem despausadas: freia já as de destino saturado, antes do próximo
            // poll devolver registros delas (evita exceder um lote logo após o rebalance).
            Consumer<byte[], byte[]> c = consumer;
            if (c != null) {
                applyBackpressure(c);
            }
        }
    }
}
