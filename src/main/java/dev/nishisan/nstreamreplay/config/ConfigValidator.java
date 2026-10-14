package dev.nishisan.nstreamreplay.config;

import dev.nishisan.nstreamreplay.decoder.Decoders;
import dev.nishisan.nstreamreplay.route.PartitionKey;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Validação cruzada da configuração, executada no boot (fail-fast): garante ids únicos
 * por categoria e que toda rota referencie uma origem e destinos existentes. Falhas
 * abortam a inicialização da aplicação — coerente com o {@code ignoreUnknownFields=false}
 * de {@link NStreamReplayProperties}.
 *
 * <p>Estas regras não são expressáveis por anotações JSR-380 de campo (são relacionais),
 * por isso ficam aqui. A lógica é estática ({@link #validate}) para teste sem contexto.
 */
@Component
public class ConfigValidator {

    public ConfigValidator(NStreamReplayProperties properties) {
        validate(properties);
    }

    /**
     * Valida a configuração. Lança {@link IllegalStateException} na primeira violação.
     *
     * @throws IllegalStateException se houver id duplicado ou referência de rota inexistente
     */
    public static void validate(NStreamReplayProperties props) {
        Set<String> sourceIds = uniqueOrThrow(props.sources(), SourceProperties::id, "source");
        Set<String> sinkIds = uniqueOrThrow(props.sinks(), SinkProperties::id, "sink");

        Map<String, SourceProperties> sources = new HashMap<>();
        for (SourceProperties source : props.sources()) {
            try {
                Decoders.validate(source.decoder());
            } catch (IllegalStateException e) {
                throw new IllegalStateException("source '" + source.id() + "': " + e.getMessage(), e);
            }
            sources.put(source.id(), source);
        }
        Map<Destination, Policy> policies = new HashMap<>();
        List<RouteProperties> routes = props.routes() == null ? List.of() : props.routes();
        uniqueOrThrow(routes, RouteProperties::id, "route");
        for (RouteProperties route : routes) {
            if (!sourceIds.contains(route.source())) {
                throw new IllegalStateException(
                        "rota '" + route.id() + "' referencia source inexistente: " + route.source());
            }
            String decoder = route.effectiveDecoder(sources.get(route.source()));
            try {
                Decoders.validate(decoder);
            } catch (IllegalStateException e) {
                throw new IllegalStateException("rota '" + route.id() + "': " + e.getMessage(), e);
            }
            for (RouteTarget target : route.to()) {
                String context = "rota '" + route.id() + "', sink '" + target.sink() + "': ";
                if (target.partitionKey() != null) {
                    try {
                        new PartitionKey(target.partitionKey());
                    } catch (IllegalStateException e) {
                        throw new IllegalStateException(context + e.getMessage(), e);
                    }
                    if ("text".equals(decoder)) {
                        throw new IllegalStateException(context + "partitionKey exige decoder json ou yaml, não text");
                    }
                }
                String topic = target.toTopic() == null || target.toTopic().isBlank()
                        ? route.fromTopic() : target.toTopic();
                Destination destination = new Destination(route.source(), route.fromTopic(), target.sink(), topic);
                Policy policy = new Policy(target.partitionKey(), target.partitionKey() == null ? null : decoder);
                if (policies.containsKey(destination)
                        && !Objects.equals(policies.get(destination), policy)) {
                    throw new IllegalStateException(context + "partitionKey conflitante para " + destination);
                }
                policies.put(destination, policy);
                if (!sinkIds.contains(target.sink())) {
                    throw new IllegalStateException(
                            "rota '" + route.id() + "' referencia sink inexistente: " + target.sink());
                }
            }
        }
    }

    private record Policy(String partitionKey, String decoder) { }

    private record Destination(String source, String fromTopic, String sink, String toTopic) { }

    private static <T> Set<String> uniqueOrThrow(java.util.List<T> items, Function<T, String> idOf, String kind) {
        Set<String> ids = new LinkedHashSet<>();
        for (T item : items) {
            String id = idOf.apply(item);
            if (!ids.add(id)) {
                throw new IllegalStateException("id de " + kind + " duplicado: " + id);
            }
        }
        return ids;
    }
}
