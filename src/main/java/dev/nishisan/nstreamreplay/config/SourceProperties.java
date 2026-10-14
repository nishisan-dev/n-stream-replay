package dev.nishisan.nstreamreplay.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
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
 * @param maxConsumeRatePerSec teto de consumo em registros/s ({@code 0} = ilimitado)
 * @param decoder         text (default), json ou yaml; custom reservado
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
        @PositiveOrZero @DefaultValue("0") long maxConsumeRatePerSec,
        @DefaultValue Map<String, String> extraProps,
        @DefaultValue("text") String decoder) {

    @ConstructorBinding
    public SourceProperties {
    }

    /** Compatibility for callers using the original text/pass-through configuration. */
    public SourceProperties(String id, String bootstrapServers, String groupId, String clientId,
                            String autoOffsetReset, int maxPollRecords, long pollTimeoutMs,
                            long maxConsumeRatePerSec, Map<String, String> extraProps) {
        this(id, bootstrapServers, groupId, clientId, autoOffsetReset, maxPollRecords,
                pollTimeoutMs, maxConsumeRatePerSec, extraProps, "text");
    }

    /** Client id efetivo: o configurado ou, na ausência, o próprio {@link #id}. */
    public String resolvedClientId() {
        return (clientId == null || clientId.isBlank()) ? id : clientId;
    }
}
