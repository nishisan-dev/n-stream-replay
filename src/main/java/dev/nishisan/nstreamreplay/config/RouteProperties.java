package dev.nishisan.nstreamreplay.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * Rota: amarra um tópico de origem (nome exato) a N alvos ({@code sink} + {@code toTopic?}).
 * Substitui o {@code PipelineProperties} no modelo v2 (de-para por tópico, fan-out, merge,
 * espelho). As referências são resolvidas e validadas no boot pelo {@link ConfigValidator}.
 *
 * @param id        identificador único da rota
 * @param source    id da origem ({@link SourceProperties#id})
 * @param fromTopic tópico de origem (nome exato) que esta rota encaminha
 * @param to        alvos da rota (1..N) — fan-out
 */
public record RouteProperties(
        @NotBlank String id,
        @NotBlank String source,
        @NotBlank String fromTopic,
        @NotEmpty @Valid List<RouteTarget> to) {
}
