package dev.nishisan.nstreamreplay.pipeline;

import dev.nishisan.nstreamreplay.config.PipelineProperties;
import dev.nishisan.nstreamreplay.sink.SinkTarget;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolve, no boot, o roteamento <b>origem -&gt; destinos</b> a partir dos pipelines: para cada
 * origem, a <b>união deduplicada</b> (por id) dos destinos de todos os pipelines que a
 * referenciam. As referências já apontam para os {@link SinkTarget} resolvidos, evitando lookups
 * no hot-path. Assume config já validada pelo {@code ConfigValidator}.
 */
public final class PipelineRegistry {

    private final Map<String, List<SinkTarget>> targetsBySource;

    private PipelineRegistry(Map<String, List<SinkTarget>> targetsBySource) {
        this.targetsBySource = targetsBySource;
    }

    public static PipelineRegistry build(List<PipelineProperties> pipelines,
                                         Map<String, ? extends SinkTarget> sinksById) {
        Map<String, Set<String>> idsBySource = new LinkedHashMap<>();
        for (PipelineProperties p : pipelines) {
            Set<String> ids = idsBySource.computeIfAbsent(p.source(), k -> new LinkedHashSet<>());
            ids.addAll(p.sinks());
        }
        Map<String, List<SinkTarget>> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, Set<String>> e : idsBySource.entrySet()) {
            List<SinkTarget> targets = new ArrayList<>(e.getValue().size());
            for (String sinkId : e.getValue()) {
                SinkTarget t = sinksById.get(sinkId);
                if (t == null) {
                    throw new IllegalStateException(
                            "destino '" + sinkId + "' do pipeline da origem '" + e.getKey() + "' não resolvido");
                }
                targets.add(t);
            }
            resolved.put(e.getKey(), List.copyOf(targets));
        }
        return new PipelineRegistry(resolved);
    }

    /** Destinos ligados a uma origem (lista vazia se a origem não participa de nenhum pipeline). */
    public List<SinkTarget> targetsFor(String sourceId) {
        return targetsBySource.getOrDefault(sourceId, List.of());
    }

    /** Ids das origens que participam de ao menos um pipeline. */
    public Set<String> activeSourceIds() {
        return targetsBySource.keySet();
    }
}
