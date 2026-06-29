package dev.nishisan.nstreamreplay.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.Map;

/**
 * Origem (consumer Kafka). Os tópicos consumidos são derivados das rotas que referenciam esta
 * origem (união dos {@code fromTopic}); os registros são enfileirados, sem transformação, nos
 * destinos resolvidos pela rota.
 *
 * @param id              identificador único; referência em rotas e nome de métrica
 * @param bootstrapServers servidores Kafka da origem
 * @param groupId         consumer group ({@code enable.auto.commit=false}, commit manual)
 * @param clientId        client id (opcional; default = {@link #id})
 * @param autoOffsetReset {@code earliest | latest | none}
 * @param maxPollRecords  {@code max.poll.records}
 * @param pollTimeoutMs   timeout do {@code consumer.poll(...)} em ms
 * @param extraProps      passthrough cru para o {@code KafkaConsumer} (SASL/TLS, etc.)
 */
public record SourceProperties(
        @NotBlank String id,
        @NotBlank String bootstrapServers,
        @NotBlank String groupId,
        String clientId,
        @Pattern(regexp = "earliest|latest|none") @DefaultValue("latest") String autoOffsetReset,
        @Positive @DefaultValue("500") int maxPollRecords,
        @Positive @DefaultValue("1000") long pollTimeoutMs,
        @DefaultValue Map<String, String> extraProps) {

    /** Client id efetivo: o configurado ou, na ausência, o próprio {@link #id}. */
    public String resolvedClientId() {
        return (clientId == null || clientId.isBlank()) ? id : clientId;
    }
}
