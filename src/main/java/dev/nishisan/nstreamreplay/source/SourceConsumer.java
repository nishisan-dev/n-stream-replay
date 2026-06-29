package dev.nishisan.nstreamreplay.source;

import dev.nishisan.nstreamreplay.config.SourceProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Consumidor de uma origem: roda na própria thread, dono exclusivo do {@code KafkaConsumer} (não
 * thread-safe). Espelha o {@code NgrrdKafkaConsumerService}: {@code enable.auto.commit=false},
 * poll loop, e <b>commit só depois</b> de enfileirar o lote em todos os destinos — at-least-once.
 *
 * <p><b>Contrato de durabilidade (sync-on-commit):</b> antes do {@code commitSync()}, força
 * {@code sync()} (fsync) nas filas dos destinos que exigem (group-commit). Invariante: nada é
 * commitado antes de durável em todas as filas-destino. Crash antes do commit ⇒ reprocesso
 * (possíveis duplicatas — sem dedup no MVP).
 */
public final class SourceConsumer implements Runnable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(SourceConsumer.class);

    private final SourceProperties props;
    private final List<SinkTarget> targets;
    private final List<SinkTarget> syncTargets;
    private final ReplayMetrics metrics;
    private final Supplier<Consumer<byte[], byte[]>> consumerFactory;
    private final Duration pollTimeout;

    private volatile boolean running = true;
    private volatile Consumer<byte[], byte[]> consumer;

    public SourceConsumer(SourceProperties props, List<SinkTarget> targets, ReplayMetrics metrics) {
        this(props, targets, metrics, () -> new KafkaConsumer<>(consumerProperties(props)));
    }

    SourceConsumer(SourceProperties props, List<SinkTarget> targets, ReplayMetrics metrics,
                   Supplier<Consumer<byte[], byte[]>> consumerFactory) {
        this.props = props;
        this.targets = List.copyOf(targets);
        this.syncTargets = this.targets.stream().filter(SinkTarget::requiresSyncBeforeCommit).toList();
        this.metrics = metrics;
        this.consumerFactory = consumerFactory;
        this.pollTimeout = Duration.ofMillis(props.pollTimeoutMs());
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
        try (Consumer<byte[], byte[]> c = consumerFactory.get()) {
            consumer = c;
            c.subscribe(props.topics(), new LoggingRebalanceListener());
            LOG.info("[{}] origem inscrita nos tópicos {} (groupId={}, {} destino(s))",
                    props.id(), props.topics(), props.groupId(), targets.size());
            while (running) {
                ConsumerRecords<byte[], byte[]> records = c.poll(pollTimeout);
                if (records.isEmpty()) {
                    continue;
                }
                enqueueBatch(records);
                commit(c);
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        } catch (IOException e) {
            // HALT (onWriteError=halt) ou falha de sync: para a origem sem commitar.
            LOG.error("[{}] origem interrompida por falha de escrita local (HALT) — sem commit", props.id(), e);
        } finally {
            consumer = null;
            LOG.info("[{}] origem encerrada", props.id());
        }
    }

    /**
     * Enfileira o lote em todos os destinos (fan-out 1->N) e faz o group-commit (sync) das filas
     * que exigem. Visível ao pacote para teste sem subir a thread.
     */
    void enqueueBatch(ConsumerRecords<byte[], byte[]> records) throws IOException {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            ReplayRecord rr = toReplayRecord(record);
            metrics.onConsumed(props.id());
            for (SinkTarget target : targets) {
                target.offer(rr);
            }
        }
        // Group-commit: durabiliza as filas antes do commit do offset.
        for (SinkTarget target : syncTargets) {
            target.sync();
        }
    }

    private void commit(Consumer<byte[], byte[]> c) {
        try {
            c.commitSync();
        } catch (KafkaException e) {
            metrics.onCommitError(props.id());
            LOG.warn("[{}] commitSync falhou — lote será reprocessado", props.id(), e);
        }
    }

    private static ReplayRecord toReplayRecord(ConsumerRecord<byte[], byte[]> r) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        for (Header h : r.headers()) {
            headers.put(h.key(), h.value());
        }
        return new ReplayRecord(r.topic(), r.partition(), r.offset(), r.timestamp(), r.key(), r.value(), headers);
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

    private final class LoggingRebalanceListener implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.info("[{}] partições revogadas: {}", props.id(), partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            LOG.info("[{}] partições atribuídas: {}", props.id(), partitions);
        }
    }
}
