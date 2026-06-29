package dev.nishisan.nstreamreplay.config;

import jakarta.validation.constraints.NotBlank;

/**
 * Alvo de uma rota: o destino ({@code sink}) e, opcionalmente, o tópico de destino
 * ({@code toTopic}). Se {@code toTopic} for omitido, a rota <b>preserva</b> o nome do tópico de
 * origem (modo espelho).
 *
 * @param sink    id do destino ({@link SinkProperties#id})
 * @param toTopic tópico de destino; {@code null}/vazio = preserva o tópico de origem
 */
public record RouteTarget(
        @NotBlank String sink,
        String toTopic) {
}
