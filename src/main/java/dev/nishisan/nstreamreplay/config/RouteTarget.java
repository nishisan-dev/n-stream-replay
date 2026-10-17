package dev.nishisan.nstreamreplay.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Alvo de uma rota: o destino ({@code sink}) e, opcionalmente, o tópico de destino
 * ({@code toTopic}). Se {@code toTopic} for omitido, a rota <b>preserva</b> o nome do tópico de
 * origem (modo espelho).
 *
 * @param sink    id do destino ({@link SinkProperties#id})
 * @param partitionKey caminho opcional no payload; exige decoder json/yaml na origem
 * @param toTopic tópico de destino; {@code null}/vazio = preserva o tópico de origem
 * @param partitioning modo de escolha da partição de destino ({@link Partitioning}); default
 *                     {@link Partitioning#KEY}. {@link Partitioning#PRESERVE} é incompatível com
 *                     {@code partitionKey}
 */
public record RouteTarget(
        @NotBlank String sink,
        String toTopic,
        String partitionKey,
        @DefaultValue("key") Partitioning partitioning) {

    @ConstructorBinding
    public RouteTarget {
        partitioning = partitioning == null ? Partitioning.KEY : partitioning;
    }

    public RouteTarget(String sink, String toTopic, String partitionKey) {
        this(sink, toTopic, partitionKey, Partitioning.KEY);
    }

    public RouteTarget(String sink, String toTopic) {
        this(sink, toTopic, null, Partitioning.KEY);
    }

    /** O registro da partição P da origem deve ir para a partição P do destino? */
    public boolean preservesPartition() {
        return partitioning == Partitioning.PRESERVE;
    }
}
