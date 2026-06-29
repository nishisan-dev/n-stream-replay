package dev.nishisan.nstreamreplay.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * Pipeline: amarra uma origem a N destinos por referência de id (fan-out 1->N). Sem
 * transformação nem rate-limit (MVP). As referências são resolvidas e validadas no boot
 * pelo {@link ConfigValidator}.
 *
 * @param id     identificador único do pipeline
 * @param source id da origem ({@link SourceProperties#id})
 * @param sinks  ids dos destinos ({@link SinkProperties#id}) — 1..N
 */
public record PipelineProperties(
        @NotBlank String id,
        @NotBlank String source,
        @NotEmpty List<String> sinks) {
}
