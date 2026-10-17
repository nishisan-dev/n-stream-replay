package dev.nishisan.nstreamreplay.config;

import dev.nishisan.nstreamreplay.decoder.Decoders;
import dev.nishisan.nstreamreplay.route.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger LOG = LoggerFactory.getLogger(ConfigValidator.class);

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

        for (SinkProperties sink : props.sinks()) {
            validateQueue(sink);
        }

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
                if (target.preservesPartition() && target.partitionKey() != null) {
                    throw new IllegalStateException(context
                            + "partitioning=preserve é incompatível com partitionKey (preserve mantém a partição "
                            + "da origem; partitionKey escolhe a partição pela key). Use um dos dois");
                }
                String topic = target.toTopic() == null || target.toTopic().isBlank()
                        ? route.fromTopic() : target.toTopic();
                Destination destination = new Destination(route.source(), route.fromTopic(), target.sink(), topic);
                Policy policy = new Policy(target.partitionKey(), target.partitionKey() == null ? null : decoder,
                        target.partitioning());
                if (policies.containsKey(destination)
                        && !Objects.equals(policies.get(destination), policy)) {
                    throw new IllegalStateException(context
                            + "partitionKey/partitioning conflitante para " + destination);
                }
                policies.put(destination, policy);
                if (!sinkIds.contains(target.sink())) {
                    throw new IllegalStateException(
                            "rota '" + route.id() + "' referencia sink inexistente: " + target.sink());
                }
            }
        }
    }

    /**
     * Regras da fila durável de um destino. {@code overflowPolicy=block} promete não descartar
     * nada; por isso não admite {@code retentionTime} (expiração por tempo descartaria backlog) e
     * deve ser usado com {@code onWriteError=halt} (com {@code drop} uma falha de escrita local
     * ainda descartaria o registro — apenas avisado, pois o operador pode aceitar esse risco).
     */
    private static void validateQueue(SinkProperties sink) {
        QueueProperties queue = sink.queue();
        if (queue == null || !queue.blocksOnOverflow()) {
            return;
        }
        if (queue.hasRetentionTime()) {
            throw new IllegalStateException("sink '" + sink.id() + "': overflowPolicy=block é incompatível com "
                    + "retentionTime > 0 (a expiração por tempo descartaria registros). Use retentionTime: 0s");
        }
        if (queue.onWriteError() != OnWriteError.HALT) {
            LOG.warn("sink '{}': overflowPolicy=block com onWriteError={} — uma falha de escrita local ainda "
                    + "descarta o registro; use onWriteError=halt para não perder nada", sink.id(),
                    queue.onWriteError());
        }
    }

    private record Policy(String partitionKey, String decoder, Partitioning partitioning) { }

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
