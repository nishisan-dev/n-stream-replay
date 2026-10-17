package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.config.SinkProperties;
import dev.nishisan.nstreamreplay.model.ReplayRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Sink de produção: publica um lote de {@link ReplayRecord} no tópico Kafka do destino,
 * preservando key/value da fila (chave original ou extraída), headers e timestamp. Key como
 * {@code byte[]} mantém o particionamento por chave no destino. Envia o lote de forma
 * assíncrona, faz {@code flush} e devolve o {@link PublishOutcome} (prefixo de cabeça a
 * confirmar no FIFO).
 *
 * <p>Timeouts são curtos (ordem de segundos) para o forwarder detectar broker-down rápido e
 * cair no backoff/retry da fila durável, em vez de bloquear pelo default de 2 min.
 *
 * <p><b>Proteção contra registro venenoso:</b> um payload acima de {@code max.request.size}
 * jamais será aceito; sem tratamento, o producer o rejeita a cada tentativa e trava o FIFO
 * (head-of-line block). Por isso o sink (1) faz uma <b>guarda pré-send</b> que pula payloads
 * acima do limite e (2) classifica erros <b>não-retryable</b> ({@code ApiException} que não é
 * {@link RetriableException}) como descarte — contabilizados como {@code poisoned} e jamais
 * retentados. Erros transitórios param o prefixo e mantêm o restante na fila.
 */
public final class KafkaSink implements Sink {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

    /**
     * Folga subtraída de {@code max.request.size} para a guarda pré-send: o limite do Kafka
     * incide sobre o record inteiro (key + value + headers + overhead), não só o value.
     */
    static final int RECORD_OVERHEAD_BYTES = 1024;

    private final Producer<byte[], byte[]> producer;
    private final int maxPayloadBytes;

    public KafkaSink(SinkProperties settings) {
        this(buildProducer(settings), settings.maxRequestSize() - RECORD_OVERHEAD_BYTES);
    }

    /** Visível para teste (injeção de producer simulado + teto da guarda). */
    KafkaSink(Producer<byte[], byte[]> producer, int maxPayloadBytes) {
        this.producer = producer;
        this.maxPayloadBytes = maxPayloadBytes;
    }

    private static Producer<byte[], byte[]> buildProducer(SinkProperties s) {
        return new KafkaProducer<>(producerProperties(s));
    }

    /**
     * Propriedades do producer (visível para teste). Garante a <b>ordem por partição</b> mesmo com
     * retry: com idempotência (acks=all) o broker deduplica e reordena por sequência, e
     * {@code max.in.flight <= 5} é seguro; sem idempotência, mais de um request em voo por conexão
     * permite que um batch retentado ultrapasse o seguinte, então fixa-se
     * {@code max.in.flight.requests.per.connection=1} (a menos que o operador o defina em
     * {@code extraProps}). O custo é pequeno: o forwarder já envia o lote, faz {@code flush} e
     * espera a confirmação antes de seguir.
     */
    static Properties producerProperties(SinkProperties s) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s.bootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, s.acks());
        p.put(ProducerConfig.LINGER_MS_CONFIG, s.lingerMs());
        p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, s.compressionType());
        p.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, s.maxRequestSize());
        // Timeouts curtos: o store-and-forward cobre a retentativa; não bloquear o forwarder.
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);
        // Idempotência remove duplicatas de retry intra-sessão do producer (exige acks=all).
        boolean acksAll = "all".equalsIgnoreCase(s.acks()) || "-1".equals(s.acks());
        p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, acksAll);
        // Passthrough cru: o usuário pode sobrescrever qualquer chave acima.
        s.extraProps().forEach(p::put);
        boolean idempotent = Boolean.parseBoolean(String.valueOf(p.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)));
        if (!idempotent) {
            p.putIfAbsent(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        }
        return p;
    }

    @Override
    public PublishOutcome publish(List<ReplayRecord> batch) {
        int n = batch.size();
        List<Future<RecordMetadata>> futures = new ArrayList<>(n);
        // Fase de envio: a guarda pré-send marca os oversize (future null = não enviado).
        for (ReplayRecord rec : batch) {
            if (isOversize(rec)) {
                LOG.warn("Registro oversize ({} B > limite {} B) descartado para não travar o stream "
                                + "(tópico={}, origem={}:{}@{})",
                        rec.valueSize(), maxPayloadBytes, rec.destinationTopic(),
                        rec.sourceTopic(), rec.partition(), rec.offset());
                futures.add(null);
            } else {
                Future<RecordMetadata> sent = producer.send(toProducerRecord(rec));
                futures.add(sent);
                if (failedTransientlyAlready(sent)) {
                    // O envio já falhou de forma transitória (ex.: partição/tópico ausente após
                    // max.block.ms). Enviar o resto do lote só gastaria max.block.ms por item e
                    // entregaria registros além do ponto de falha, que serão reenviados em ordem.
                    break;
                }
            }
        }
        producer.flush();
        int published = 0;
        int poisoned = 0;
        // Avaliação em ordem: prefixo contíguo de entregues + venenos; o primeiro erro
        // transitório encerra o prefixo (restante fica na fila). futures.size() < n quando o
        // envio foi interrompido por uma falha transitória imediata.
        for (int i = 0; i < futures.size(); i++) {
            Future<RecordMetadata> f = futures.get(i);
            if (f == null) {
                poisoned++;
                continue;
            }
            try {
                f.get();
                published++;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (isNonRetryable(cause)) {
                    LOG.warn("Kafka rejeitou registro em definitivo ({}) — descartado (tópico={}, origem={}:{}@{})",
                            cause.getClass().getSimpleName(), batch.get(i).destinationTopic(),
                            batch.get(i).sourceTopic(), batch.get(i).partition(), batch.get(i).offset());
                    poisoned++;
                    continue;
                }
                LOG.warn("Falha entregando ao Kafka — {} item(ns) ficam na fila", n - published - poisoned, cause);
                break;
            }
        }
        return new PublishOutcome(published, poisoned);
    }

    /** O future já terminou com erro que não é permanente (portanto o registro deve ser retentado)? */
    private static boolean failedTransientlyAlready(Future<RecordMetadata> future) {
        if (!future.isDone()) {
            return false;
        }
        try {
            future.get();
            return false;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException ee) {
            return !isNonRetryable(ee.getCause());
        }
    }

    private ProducerRecord<byte[], byte[]> toProducerRecord(ReplayRecord rec) {
        Long ts = rec.timestamp() >= 0 ? rec.timestamp() : null;
        List<Header> headers = new ArrayList<>(rec.headers().size());
        for (Map.Entry<String, byte[]> h : rec.headers().entrySet()) {
            headers.add(new RecordHeader(h.getKey(), h.getValue()));
        }
        // destinationPartition=null: o producer particiona por key (preserva ordem por chave no destino).
        // Preenchida (partitioning: preserve): a partição é fixa e a key segue intacta, em bytes crus.
        return new ProducerRecord<>(rec.destinationTopic(), rec.destinationPartition(), ts,
                rec.key(), rec.value(), headers);
    }

    private boolean isOversize(ReplayRecord rec) {
        return maxPayloadBytes > 0 && rec.valueSize() > maxPayloadBytes;
    }

    /**
     * Erro permanente: retentar não muda nada. {@link ApiException} que NÃO é
     * {@link RetriableException} (ex.: {@code RecordTooLargeException}) jamais será aceito —
     * descarta-se o item para o FIFO seguir. Causas desconhecidas são tratadas como transitórias.
     */
    private static boolean isNonRetryable(Throwable cause) {
        return cause instanceof ApiException && !(cause instanceof RetriableException);
    }

    @Override
    public void close() {
        try {
            producer.flush();
        } finally {
            producer.close(Duration.ofSeconds(5));
        }
    }
}
