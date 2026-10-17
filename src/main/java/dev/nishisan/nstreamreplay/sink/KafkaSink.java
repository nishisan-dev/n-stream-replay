package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.config.OnPoison;
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
 *
 * <p>Esse descarte é perda definitiva (o offset da origem já foi commitado). Com
 * {@code onPoison=halt} o sink <b>não descarta</b>: devolve o prefixo entregue com um
 * {@link PublishOutcome#haltReason() motivo de parada} e o registro rejeitado fica na fila; o
 * forwarder para de entregar aquele destino.
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
    private final OnPoison onPoison;

    public KafkaSink(SinkProperties settings) {
        this(buildProducer(settings), settings.maxRequestSize() - RECORD_OVERHEAD_BYTES,
                settings.queue().onPoison());
    }

    /** Visível para teste (injeção de producer simulado + teto da guarda); descarta venenos. */
    KafkaSink(Producer<byte[], byte[]> producer, int maxPayloadBytes) {
        this(producer, maxPayloadBytes, OnPoison.DROP);
    }

    /** Visível para teste (injeção de producer simulado + teto da guarda + política de veneno). */
    KafkaSink(Producer<byte[], byte[]> producer, int maxPayloadBytes, OnPoison onPoison) {
        this.producer = producer;
        this.maxPayloadBytes = maxPayloadBytes;
        this.onPoison = onPoison;
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
        boolean halt = onPoison == OnPoison.HALT;
        List<Future<RecordMetadata>> futures = new ArrayList<>(n);
        String oversizeHalt = null;   // motivo, se um oversize local parou o envio (onPoison=halt)
        // Fase de envio: a guarda pré-send marca os oversize (future null = não enviado).
        for (ReplayRecord rec : batch) {
            if (isOversize(rec)) {
                if (halt) {
                    oversizeHalt = haltReason(rec, "payload de " + rec.valueSize() + " B acima do limite de "
                            + maxPayloadBytes + " B (maxRequestSize)");
                    break;   // não envia nada além do registro inválido
                }
                LOG.warn("Registro oversize ({} B > limite {} B) descartado para não travar o stream "
                                + "(tópico={}, origem={}:{}@{})",
                        rec.valueSize(), maxPayloadBytes, rec.destinationTopic(),
                        rec.sourceTopic(), rec.partition(), rec.offset());
                futures.add(null);
            } else {
                Future<RecordMetadata> sent = producer.send(toProducerRecord(rec));
                futures.add(sent);
                Throwable immediate = immediateFailure(sent);
                if (immediate != null && (!isNonRetryable(immediate) || halt)) {
                    // O envio já falhou (transitório, ex.: partição/tópico ausente após max.block.ms;
                    // ou permanente sob halt). Enviar o resto do lote só gastaria max.block.ms por item
                    // e entregaria registros além do ponto de falha, que serão reenviados em ordem.
                    break;
                }
            }
        }
        producer.flush();
        int published = 0;
        int poisoned = 0;
        // Avaliação em ordem: prefixo contíguo de entregues + venenos; o primeiro erro
        // transitório encerra o prefixo (restante fica na fila). futures.size() < n quando o
        // envio foi interrompido por uma falha imediata ou por oversize sob halt.
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
                return new PublishOutcome(published, poisoned);
            } catch (StackOverflowError so) {
                // Bug conhecido do cliente: se o limite do tópico (max.message.bytes) for menor que o
                // batch.size do producer, o batch rejeitado é "dividido" em batches idênticos, em laço, e a
                // cadeia de futures estoura a pilha. Não deixa a thread do forwarder morrer: trata como
                // falha transitória (nada é confirmado nem descartado; o batch expira pelo delivery.timeout).
                LOG.error("Cadeia de futures do producer estourou a pilha (limite max.message.bytes do tópico de "
                        + "destino menor que o batch.size do producer?) — {} item(ns) ficam na fila",
                        n - published - poisoned);
                return new PublishOutcome(published, poisoned);
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (isNonRetryable(cause)) {
                    ReplayRecord rec = batch.get(i);
                    if (halt) {
                        String reason = haltReason(rec, cause.getClass().getSimpleName() + ": " + cause.getMessage());
                        LOG.error("Kafka rejeitou registro em definitivo — destino PARADO, registro mantido na fila: {}",
                                reason, cause);
                        return new PublishOutcome(published, poisoned, reason);
                    }
                    LOG.warn("Kafka rejeitou registro em definitivo ({}) — descartado (tópico={}, origem={}:{}@{})",
                            cause.getClass().getSimpleName(), rec.destinationTopic(),
                            rec.sourceTopic(), rec.partition(), rec.offset());
                    poisoned++;
                    continue;
                }
                LOG.warn("Falha entregando ao Kafka — {} item(ns) ficam na fila", n - published - poisoned, cause);
                return new PublishOutcome(published, poisoned);
            }
        }
        if (oversizeHalt != null && futures.size() == published + poisoned) {
            LOG.error("Registro oversize — destino PARADO, registro mantido na fila: {}", oversizeHalt);
            return new PublishOutcome(published, poisoned, oversizeHalt);
        }
        return new PublishOutcome(published, poisoned);
    }

    private static String haltReason(ReplayRecord rec, String detail) {
        return detail + " (destino=" + rec.destinationTopic()
                + (rec.destinationPartition() == null ? "" : "/" + rec.destinationPartition())
                + ", origem=" + rec.sourceTopic() + ":" + rec.partition() + "@" + rec.offset() + ")";
    }

    /** A causa, se o future já terminou com erro; {@code null} se pendente ou bem-sucedido. */
    private static Throwable immediateFailure(Future<RecordMetadata> future) {
        if (!future.isDone()) {
            return null;
        }
        try {
            future.get();
            return null;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException ee) {
            return ee.getCause();
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
