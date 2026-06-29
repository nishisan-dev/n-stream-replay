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
                "k".getBytes(StandardCharsets.UTF_8), value, Map.of());
    }

    @Test
    void entregaTodosQuandoBrokerOk() {
        ScriptedProducer producer = new ScriptedProducer();
        try (KafkaSink sink = new KafkaSink(producer, "dest", 0)) {
            PublishOutcome outcome = sink.publish(List.of(rec(1), rec(1), rec(1)));
            assertThat(outcome.published()).isEqualTo(3);
            assertThat(outcome.poisoned()).isZero();
            assertThat(outcome.acked()).isEqualTo(3);
            assertThat(producer.sent).hasSize(3);
        }
    }

    @Test
    void descartaOversizeSemEnviar() {
        ScriptedProducer producer = new ScriptedProducer();
        try (KafkaSink sink = new KafkaSink(producer, "dest", 10)) {  // teto de 10 bytes
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
        try (KafkaSink sink = new KafkaSink(producer, "dest", 0)) {
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
        try (KafkaSink sink = new KafkaSink(producer, "dest", 0)) {
            PublishOutcome outcome = sink.publish(List.of(rec(1), rec(1), rec(1)));
            assertThat(outcome.published()).isEqualTo(1);    // só o primeiro
            assertThat(outcome.poisoned()).isZero();
            assertThat(outcome.acked()).isEqualTo(1);        // prefixo para no transitório
        }
    }
}
