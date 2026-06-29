package dev.nishisan.nstreamreplay.route;

import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.sink.SinkTarget;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tabela de roteamento resolvida no boot: <b>origem -&gt; (tópico de origem -&gt; alvos)</b>. Cada
 * alvo aponta para um {@link SinkTarget} já resolvido e o tópico de destino (nulo = preserva o
 * tópico de origem). Substitui o {@code PipelineRegistry} no modelo v2. Assume config já validada
 * pelo {@code ConfigValidator}.
 */
public final class RouteTable {

    /** Alvo resolvido: destino + tópico de destino (nulo/vazio = preserva o tópico de origem). */
    public record Target(SinkTarget sink, String toTopic) {
        public String resolveTopic(String sourceTopic) {
            return (toTopic == null || toTopic.isBlank()) ? sourceTopic : toTopic;
        }
    }

    private final Map<String, Map<String, List<Target>>> bySourceThenTopic;
    private final Set<String> referencedSinkIds;

    private RouteTable(Map<String, Map<String, List<Target>>> bySourceThenTopic,
                       Set<String> referencedSinkIds) {
        this.bySourceThenTopic = bySourceThenTopic;
        this.referencedSinkIds = referencedSinkIds;
    }

    public static RouteTable build(List<RouteProperties> routes,
                                   Map<String, ? extends SinkTarget> sinksById) {
        Map<String, Map<String, List<Target>>> map = new LinkedHashMap<>();
        Set<String> referenced = new LinkedHashSet<>();
        List<RouteProperties> safe = routes == null ? List.of() : routes;
        for (RouteProperties route : safe) {
            Map<String, List<Target>> byTopic =
                    map.computeIfAbsent(route.source(), k -> new LinkedHashMap<>());
            List<Target> targets = byTopic.computeIfAbsent(route.fromTopic(), k -> new ArrayList<>());
            for (RouteTarget rt : route.to()) {
                SinkTarget sink = sinksById.get(rt.sink());
                if (sink == null) {
                    throw new IllegalStateException(
                            "rota '" + route.id() + "' referencia sink inexistente: " + rt.sink());
                }
                targets.add(new Target(sink, rt.toTopic()));
                referenced.add(rt.sink());
            }
        }
        return new RouteTable(map, referenced);
    }

    /** Tópicos de origem (união dos {@code fromTopic}) que uma origem deve consumir. */
    public List<String> topicsFor(String sourceId) {
        Map<String, List<Target>> byTopic = bySourceThenTopic.get(sourceId);
        return byTopic == null ? List.of() : List.copyOf(byTopic.keySet());
    }

    /** Alvos para um {@code (origem, tópico)}; lista vazia se não houver rota. */
    public List<Target> targetsFor(String sourceId, String topic) {
        Map<String, List<Target>> byTopic = bySourceThenTopic.get(sourceId);
        if (byTopic == null) {
            return List.of();
        }
        return byTopic.getOrDefault(topic, List.of());
    }

    /** Origens que participam de ao menos uma rota. */
    public Set<String> activeSourceIds() {
        return bySourceThenTopic.keySet();
    }

    /** Ids dos destinos referenciados por ao menos uma rota. */
    public Set<String> referencedSinkIds() {
        return referencedSinkIds;
    }
}
