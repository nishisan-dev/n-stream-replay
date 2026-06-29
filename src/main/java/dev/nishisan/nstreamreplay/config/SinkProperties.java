package dev.nishisan.nstreamreplay.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.Map;

/**
 * Destino (producer Kafka + fila durável dedicada). Cada destino é uma unidade de
 * isolamento: sua fila, seu producer e seu forwarder são independentes; um destino offline
 * só faz crescer/dropar a sua própria fila, sem afetar a origem nem os demais destinos.
 *
 * @param id              identificador único; referência em pipelines e nome de métrica
 * @param bootstrapServers servidores Kafka do destino
 * @param acks            {@code 0 | 1 | all}
 * @param lingerMs        {@code linger.ms}
 * @param compressionType {@code none | gzip | snappy | lz4 | zstd}
 * @param maxRequestSize  {@code max.request.size} em bytes
 * @param extraProps      passthrough cru para o {@code KafkaProducer}
 * @param queue           configuração da fila durável deste destino
 */
public record SinkProperties(
        @NotBlank String id,
        @NotBlank String bootstrapServers,
        @NotBlank @DefaultValue("1") String acks,
        @PositiveOrZero @DefaultValue("50") int lingerMs,
        @NotBlank @DefaultValue("lz4") String compressionType,
        @Positive @DefaultValue("1048576") int maxRequestSize,
        @DefaultValue Map<String, String> extraProps,
        @Valid @DefaultValue QueueProperties queue) {
}
