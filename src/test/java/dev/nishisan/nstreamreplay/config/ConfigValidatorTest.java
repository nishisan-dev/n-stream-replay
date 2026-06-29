package dev.nishisan.nstreamreplay.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigValidatorTest {

    private static SourceProperties src(String id) {
        return new SourceProperties(id, "b:9092", List.of("t"), "g", null, "latest", 500, 1000L, Map.of());
    }

    private static QueueProperties queue() {
        return new QueueProperties("var/q", 100, Duration.ZERO,
                Durability.SYNC_ON_COMMIT, OnWriteError.DROP, 256, 1000L);
    }

    private static SinkProperties sink(String id) {
        return new SinkProperties(id, "b:9092", "t", "1", 50, "lz4", 1_048_576, Map.of(), queue());
    }

    private static PipelineProperties pipe(String id, String source, String... sinks) {
        return new PipelineProperties(id, source, List.of(sinks));
    }

    private static NStreamReplayProperties cfg(List<SourceProperties> sources,
                                               List<SinkProperties> sinks,
                                               List<PipelineProperties> pipelines) {
        return new NStreamReplayProperties(sources, sinks, pipelines, null);
    }

    @Test
    void aceitaConfiguracaoValidaComFanout() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")),
                List.of(sink("d1"), sink("d2")),
                List.of(pipe("p1", "s1", "d1", "d2")));
        assertThatCode(() -> ConfigValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void rejeitaSourceIdDuplicado() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1"), src("s1")),
                List.of(sink("d1")),
                List.of(pipe("p1", "s1", "d1")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("source duplicado")
                .hasMessageContaining("s1");
    }

    @Test
    void rejeitaSinkIdDuplicado() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")),
                List.of(sink("d1"), sink("d1")),
                List.of(pipe("p1", "s1", "d1")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sink duplicado");
    }

    @Test
    void rejeitaPipelineIdDuplicado() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")),
                List.of(sink("d1")),
                List.of(pipe("p1", "s1", "d1"), pipe("p1", "s1", "d1")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("pipeline duplicado");
    }

    @Test
    void rejeitaPipelineComSourceInexistente() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")),
                List.of(sink("d1")),
                List.of(pipe("p1", "ausente", "d1")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("source inexistente")
                .hasMessageContaining("ausente");
    }

    @Test
    void rejeitaPipelineComSinkInexistente() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")),
                List.of(sink("d1")),
                List.of(pipe("p1", "s1", "d1", "fantasma")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sink inexistente")
                .hasMessageContaining("fantasma");
    }

    @Test
    void resolvedClientIdUsaIdQuandoAusente() {
        assertThat(src("s1").resolvedClientId()).isEqualTo("s1");
        SourceProperties comClient = new SourceProperties(
                "s1", "b:9092", List.of("t"), "g", "cli-x", "latest", 500, 1000L, Map.of());
        assertThat(comClient.resolvedClientId()).isEqualTo("cli-x");
    }

    // ── rotas (v2) ──────────────────────────────────────────────────────────

    private static RouteProperties route(String id, String source, String fromTopic, String... sinkIds) {
        List<RouteTarget> to = new ArrayList<>();
        for (String s : sinkIds) {
            to.add(new RouteTarget(s, null));
        }
        return new RouteProperties(id, source, fromTopic, to);
    }

    private static NStreamReplayProperties withRoutes(List<RouteProperties> routes) {
        return new NStreamReplayProperties(List.of(src("s1")), List.of(sink("d1"), sink("d2")), null, routes);
    }

    @Test
    void aceitaRotasValidas() {
        NStreamReplayProperties props = withRoutes(List.of(route("r1", "s1", "orders.in", "d1", "d2")));
        assertThatCode(() -> ConfigValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void rejeitaRotaIdDuplicado() {
        NStreamReplayProperties props = withRoutes(List.of(
                route("r1", "s1", "a", "d1"), route("r1", "s1", "b", "d2")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("route duplicado");
    }

    @Test
    void rejeitaRotaComSourceInexistente() {
        NStreamReplayProperties props = withRoutes(List.of(route("r1", "ausente", "t", "d1")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("source inexistente").hasMessageContaining("ausente");
    }

    @Test
    void rejeitaRotaComSinkInexistente() {
        NStreamReplayProperties props = withRoutes(List.of(route("r1", "s1", "t", "fantasma")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sink inexistente").hasMessageContaining("fantasma");
    }
}
