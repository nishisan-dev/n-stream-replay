package dev.nishisan.nstreamreplay.sink;

import dev.nishisan.nstreamreplay.model.ReplayRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSinkTest {

    /** Producer simulado com saídas roteirizadas (sucesso/erro) por chamada de send, em ordem. */
    static final class ScriptedProducer extends MockProducer<byte[], byte[]> {
        private final Deque<CompletableFuture<RecordMetadata>> outcomes = new ArrayDeque<>();
        private final List<ProducerRecord<byte[], byte[]>> sent = new ArrayList<>();

        void scriptSuccess() {
            outcomes.add(CompletableFuture.completedFuture(null));
        }

        void scriptError(RuntimeException e) {
            CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            outcomes.add(f);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
            sent.add(record);
            CompletableFuture<RecordMetadata> f = outcomes.poll();
            return f != null ? f : CompletableFuture.completedFuture(null);
        }

        @Override
        public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            return send(record);
        }

        @Override
        public void flush() {
            // no-op: as saídas já estão completas.
        }

        @Override
        public void close() {
        }

        @Override
        public void close(Duration timeout) {
        }
    }

    private static ReplayRecord rec(int valueBytes) {
        byte[] value = new byte[valueBytes];
        return new ReplayRecord("src", 0, valueBytes, 1L,
                "k".getBytes(StandardCharsets.UTF_8), value, Map.of(), "dest");
    }

    @Test
    void entregaTodosQuandoBrokerOk() {
        ScriptedProducer producer = new ScriptedProducer();
        try (KafkaSink sink = new KafkaSink(producer,0)) {
            PublishOutcome outcome = sink.publish(List.of(rec(1), rec(1), rec(1)));
            assertThat(outcome.published()).isEqualTo(3);
            assertThat(outcome.poisoned()).isZero();
            assertThat(outcome.acked()).isEqualTo(3);
            assertThat(producer.sent).hasSize(3);
            // publica no destino do record (destinationTopic), não num tópico fixo
            assertThat(producer.sent).extracting(ProducerRecord::topic).containsOnly("dest");
        }
    }

    @Test
    void descartaOversizeSemEnviar() {
        ScriptedProducer producer = new ScriptedProducer();
        try (KafkaSink sink = new KafkaSink(producer,10)) {  // teto de 10 bytes
            PublishOutcome outcome = sink.publish(List.of(rec(100), rec(1), rec(1)));
            assertThat(outcome.poisoned()).isEqualTo(1);     // o oversize
            assertThat(outcome.published()).isEqualTo(2);
            assertThat(outcome.acked()).isEqualTo(3);        // prefixo contíguo inteiro
            assertThat(producer.sent).hasSize(2);            // o oversize NÃO foi enviado
        }
    }

    @Test
    void classificaNaoRetryableComoPoisonEContinua() {
        ScriptedProducer producer = new ScriptedProducer();
        producer.scriptSuccess();
        producer.scriptError(new RecordTooLargeException("rejeitado em definitivo"));
        producer.scriptSuccess();
        try (KafkaSink sink = new KafkaSink(producer,0)) {
            PublishOutcome outcome = sink.publish(List.of(rec(1), rec(1), rec(1)));
            assertThat(outcome.published()).isEqualTo(2);
            assertThat(outcome.poisoned()).isEqualTo(1);
            assertThat(outcome.acked()).isEqualTo(3);        // poison não trava o FIFO
        }
    }

    @Test
    void paraNoPrimeiroErroTransitorioMantendoRestante() {
        ScriptedProducer producer = new ScriptedProducer();
        producer.scriptSuccess();
        producer.scriptError(new TimeoutException("broker fora"));   // retryable
        producer.scriptSuccess();
        try (KafkaSink sink = new KafkaSink(producer,0)) {
            PublishOutcome outcome = sink.publish(List.of(rec(1), rec(1), rec(1)));
            assertThat(outcome.published()).isEqualTo(1);    // só o primeiro
            assertThat(outcome.poisoned()).isZero();
            assertThat(outcome.acked()).isEqualTo(1);        // prefixo para no transitório
        }
    }

    @Test
    void publicaNaParticaoDeDestinoPreservandoKeyCruaEHeaders() {
        ScriptedProducer producer = new ScriptedProducer();
        byte[] rawKey = {0, (byte) 0xFF, 0x7F, (byte) 0x80};   // bytes não-UTF-8
        Map<String, byte[]> headers = new java.util.LinkedHashMap<>();
        headers.put("nishidb-reason", "reset".getBytes(StandardCharsets.UTF_8));
        headers.put("empty", null);
        ReplayRecord pinned = new ReplayRecord("cdc", 2, 10L, 5L, rawKey, new byte[]{9}, headers, "cdc.mirror", 2);
        ReplayRecord byKey = new ReplayRecord("cdc", 2, 11L, 5L, rawKey, new byte[]{9}, Map.of(), "cdc.mirror");
        try (KafkaSink sink = new KafkaSink(producer, 0)) {
            PublishOutcome outcome = sink.publish(List.of(pinned, byKey));
            assertThat(outcome.published()).isEqualTo(2);
        }
        assertThat(producer.sent.get(0).partition()).isEqualTo(2);
        assertThat(producer.sent.get(0).key()).isEqualTo(rawKey);
        assertThat(producer.sent.get(0).headers().lastHeader("nishidb-reason").value())
                .isEqualTo("reset".getBytes(StandardCharsets.UTF_8));
        assertThat(producer.sent.get(0).headers().lastHeader("empty").value()).isNull();
        assertThat(producer.sent.get(1).partition()).isNull();   // sem partição = pela key
        assertThat(producer.sent.get(1).key()).isEqualTo(rawKey);
    }

    @Test
    void naoEnviaORestoDoLoteAposFalhaTransitoriaImediata() {
        ScriptedProducer producer = new ScriptedProducer();
        producer.scriptSuccess();
        producer.scriptError(new TimeoutException("partição ausente no metadata"));
        producer.scriptSuccess();
        producer.scriptSuccess();
        try (KafkaSink sink = new KafkaSink(producer, 0)) {
            PublishOutcome outcome = sink.publish(List.of(rec(1), rec(1), rec(1), rec(1)));
            assertThat(outcome.published()).isEqualTo(1);
            assertThat(outcome.poisoned()).isZero();
            assertThat(outcome.acked()).isEqualTo(1);        // o registro que falhou NÃO é descartado
        }
        // Nada além do ponto de falha foi enviado: evita max.block.ms por item e reordenação.
        assertThat(producer.sent).hasSize(2);
    }

    @Test
    void falhaDeParticaoInvalidaMantemORegistroNaFila() {
        ScriptedProducer producer = new ScriptedProducer();
        producer.scriptError(new TimeoutException(
                "Partition 9 of topic dest with partition count 4 is not present in metadata after 5000 ms."));
        ReplayRecord pinned = new ReplayRecord("src", 9, 1L, 1L, null, new byte[]{1}, Map.of(), "dest", 9);
        try (KafkaSink sink = new KafkaSink(producer, 0)) {
            PublishOutcome outcome = sink.publish(List.of(pinned));
            assertThat(outcome.acked()).isZero();
            assertThat(outcome.poisoned()).isZero();
        }
    }

    private static dev.nishisan.nstreamreplay.config.SinkProperties sinkProps(String acks, Map<String, String> extra) {
        return new dev.nishisan.nstreamreplay.config.SinkProperties("d", "h:9092", acks, 0, "lz4", 1_048_576, extra,
                new dev.nishisan.nstreamreplay.config.QueueProperties("var/q", 10,
                        Duration.ZERO, dev.nishisan.nstreamreplay.config.Durability.OS_MANAGED,
                        dev.nishisan.nstreamreplay.config.OnWriteError.HALT, 10, 10L));
    }

    @Test
    void semIdempotenciaLimitaUmRequestEmVooParaPreservarOrdemNoRetry() {
        var p = KafkaSink.producerProperties(sinkProps("1", Map.of()));
        assertThat(p.get("enable.idempotence")).isEqualTo(false);
        assertThat(p.get("max.in.flight.requests.per.connection")).isEqualTo(1);
    }

    @Test
    void comAcksAllUsaIdempotenciaSemLimitarEmVoo() {
        var p = KafkaSink.producerProperties(sinkProps("all", Map.of()));
        assertThat(p.get("enable.idempotence")).isEqualTo(true);
        assertThat(p).doesNotContainKey("max.in.flight.requests.per.connection");
    }

    @Test
    void extraPropsDoOperadorTemPrecedenciaSobreOEmVoo() {
        var p = KafkaSink.producerProperties(sinkProps("1", Map.of("max.in.flight.requests.per.connection", "3")));
        assertThat(p.get("max.in.flight.requests.per.connection")).isEqualTo("3");
        var q = KafkaSink.producerProperties(sinkProps("1", Map.of("enable.idempotence", "true", "acks", "all")));
        assertThat(q).doesNotContainKey("max.in.flight.requests.per.connection");
    }
}
