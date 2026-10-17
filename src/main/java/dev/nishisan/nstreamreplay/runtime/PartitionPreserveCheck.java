package dev.nishisan.nstreamreplay.runtime;

import dev.nishisan.nstreamreplay.config.NStreamReplayProperties;
import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.config.SinkProperties;
import dev.nishisan.nstreamreplay.config.SourceProperties;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Checagem de subida do {@code partitioning: preserve}: para cada alvo {@code preserve}, o tópico
 * de destino precisa <b>existir</b> e ter <b>pelo menos tantas partições</b> quanto o de origem —
 * senão o registro da partição P não teria onde cair no destino. O tópico de destino é criado pelo
 * operador; a aplicação nunca o cria.
 *
 * <p>Falha fechada: se algum cluster não puder ser consultado, a aplicação não sobe (não há como
 * provar a topologia). Rotas sem {@code preserve} não geram nenhuma conexão administrativa.
 * Todas as violações são reportadas de uma só vez numa única {@link IllegalStateException}.
 */
public final class PartitionPreserveCheck {

    private PartitionPreserveCheck() {
    }

    /** Um par origem/destino a verificar, vindo de um alvo {@code preserve}. */
    private record Pair(String routeId, SourceProperties source, String fromTopic,
                        SinkProperties sink, String toTopic) { }

    /**
     * Verifica todos os alvos {@code preserve} da configuração.
     *
     * @throws IllegalStateException se algum destino não existir, tiver menos partições que a
     *                               origem, ou se um cluster não puder ser consultado
     */
    public static void verify(NStreamReplayProperties props, TopicPartitionCounter counter) {
        List<Pair> pairs = collectPairs(props);
        if (pairs.isEmpty()) {
            return;
        }
        Map<String, Set<String>> sourceTopics = new LinkedHashMap<>();
        Map<String, Set<String>> sinkTopics = new LinkedHashMap<>();
        for (Pair p : pairs) {
            sourceTopics.computeIfAbsent(p.source().id(), k -> new LinkedHashSet<>()).add(p.fromTopic());
            sinkTopics.computeIfAbsent(p.sink().id(), k -> new LinkedHashSet<>()).add(p.toTopic());
        }
        List<String> errors = new ArrayList<>();
        Map<String, Map<String, Object>> sourceConfigs = new HashMap<>();
        props.sources().forEach(src -> sourceConfigs.put(src.id(),
                adminConfig(src.bootstrapServers(), src.extraProps())));
        Map<String, Map<String, Object>> sinkConfigs = new HashMap<>();
        props.sinks().forEach(snk -> sinkConfigs.put(snk.id(),
                adminConfig(snk.bootstrapServers(), snk.extraProps())));
        Map<String, Map<String, Integer>> sourceCounts = describe("origem", sourceTopics, sourceConfigs, counter, errors);
        Map<String, Map<String, Integer>> sinkCounts = describe("sink", sinkTopics, sinkConfigs, counter, errors);

        for (Pair p : pairs) {
            Integer from = sourceCounts.getOrDefault(p.source().id(), Map.of()).get(p.fromTopic());
            Integer to = sinkCounts.getOrDefault(p.sink().id(), Map.of()).get(p.toTopic());
            String where = "rota '" + p.routeId() + "' (partitioning=preserve): ";
            if (sourceCounts.containsKey(p.source().id()) && from == null) {
                errors.add(where + "o tópico de origem '" + p.fromTopic() + "' não existe na origem '"
                        + p.source().id() + "'");
            }
            if (sinkCounts.containsKey(p.sink().id()) && to == null) {
                errors.add(where + "o tópico de destino '" + p.toTopic() + "' não existe no sink '"
                        + p.sink().id() + "'; crie-o com pelo menos "
                        + (from == null ? "tantas partições quanto a origem" : from + " partição(ões)")
                        + " (a aplicação não cria tópicos)");
            }
            if (from != null && to != null && to < from) {
                errors.add(where + "o tópico de destino '" + p.toTopic() + "' (sink '" + p.sink().id() + "') tem "
                        + to + " partição(ões), menos que o de origem '" + p.fromTopic() + "' (origem '"
                        + p.source().id() + "', " + from + " partição(ões)); aumente as partições do destino");
            }
        }
        if (!errors.isEmpty()) {
            throw new IllegalStateException("verificação de partições falhou:\n  - " + String.join("\n  - ", errors));
        }
    }

    private static List<Pair> collectPairs(NStreamReplayProperties props) {
        List<Pair> pairs = new ArrayList<>();
        if (props.routes() == null) {
            return pairs;
        }
        Map<String, SourceProperties> sources = new HashMap<>();
        props.sources().forEach(s -> sources.put(s.id(), s));
        Map<String, SinkProperties> sinks = new HashMap<>();
        props.sinks().forEach(s -> sinks.put(s.id(), s));
        for (RouteProperties route : props.routes()) {
            for (RouteTarget target : route.to()) {
                if (!target.preservesPartition()) {
                    continue;
                }
                String toTopic = target.toTopic() == null || target.toTopic().isBlank()
                        ? route.fromTopic() : target.toTopic();
                pairs.add(new Pair(route.id(), sources.get(route.source()), route.fromTopic(),
                        sinks.get(target.sink()), toTopic));
            }
        }
        return pairs;
    }

    /** Consulta cada cluster uma vez; cluster inacessível gera erro e fica fora do resultado. */
    private static Map<String, Map<String, Integer>> describe(
            String kind, Map<String, Set<String>> topicsById,
            Map<String, Map<String, Object>> configs,
            TopicPartitionCounter counter, List<String> errors) {
        Map<String, Map<String, Integer>> result = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : topicsById.entrySet()) {
            try {
                result.put(entry.getKey(), counter.partitionCounts(configs.get(entry.getKey()), entry.getValue()));
            } catch (Exception e) {
                errors.add("não foi possível consultar as partições de " + entry.getValue() + " no " + kind + " '"
                        + entry.getKey() + "': " + e);
            }
        }
        return result;
    }

    private static Map<String, Object> adminConfig(String bootstrapServers, Map<String, String> extraProps) {
        Map<String, Object> config = new HashMap<>();
        if (extraProps != null) {
            config.putAll(extraProps);
        }
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return config;
    }
}
