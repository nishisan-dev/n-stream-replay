package dev.nishisan.nstreamreplay.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

/**
 * Raiz da configuração de app, ligada ao prefixo {@code nstreamreplay} do YAML externo.
 *
 * <p>{@code ignoreUnknownFields=false} torna o boot <b>fail-loud</b>: uma chave desconhecida
 * (typo) aborta a inicialização em vez de ser silenciosamente ignorada. A validação JSR-380
 * roda na criação do bean ({@link Validated}); a validação cruzada de referências
 * (rota -> source/sink, ids únicos) é feita pelo {@link ConfigValidator}.
 *
 * @param sources origens declaradas (não vazio)
 * @param sinks   destinos declarados (não vazio)
 * @param routes  rotas declaradas (não vazio) — modelo de roteamento por tópico
 */
@ConfigurationProperties(prefix = "nstreamreplay", ignoreUnknownFields = false)
@Validated
public record NStreamReplayProperties(
        @NotEmpty @Valid List<SourceProperties> sources,
        @NotEmpty @Valid List<SinkProperties> sinks,
        @NotEmpty @Valid List<RouteProperties> routes) {
}
