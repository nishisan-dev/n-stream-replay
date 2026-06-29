package dev.nishisan.nstreamreplay.config;

import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Validação cruzada da configuração, executada no boot (fail-fast): garante ids únicos
 * por categoria e que todo pipeline referencie uma origem e destinos existentes. Falhas
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
     * @throws IllegalStateException se houver id duplicado ou referência de pipeline inexistente
     */
    public static void validate(NStreamReplayProperties props) {
        Set<String> sourceIds = uniqueOrThrow(props.sources(), SourceProperties::id, "source");
        Set<String> sinkIds = uniqueOrThrow(props.sinks(), SinkProperties::id, "sink");
        uniqueOrThrow(props.pipelines(), PipelineProperties::id, "pipeline");

        for (PipelineProperties pipeline : props.pipelines()) {
            if (!sourceIds.contains(pipeline.source())) {
                throw new IllegalStateException(
                        "pipeline '" + pipeline.id() + "' referencia source inexistente: " + pipeline.source());
            }
            for (String sinkRef : pipeline.sinks()) {
                if (!sinkIds.contains(sinkRef)) {
                    throw new IllegalStateException(
                            "pipeline '" + pipeline.id() + "' referencia sink inexistente: " + sinkRef);
                }
            }
        }
    }

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
