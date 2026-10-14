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
        return new SourceProperties(id, "b:9092", "g", null, "latest", 500, 1000L, 0L, Map.of());
    }

    private static QueueProperties queue() {
        return new QueueProperties("var/q", 100, Duration.ZERO,
                Durability.SYNC_ON_COMMIT, OnWriteError.DROP, 256, 1000L);
    }

    private static SinkProperties sink(String id) {
        return new SinkProperties(id, "b:9092", "1", 50, "lz4", 1_048_576, Map.of(), queue());
    }

    private static RouteProperties route(String id, String source, String fromTopic, String... sinkIds) {
        List<RouteTarget> to = new ArrayList<>();
        for (String s : sinkIds) {
            to.add(new RouteTarget(s, null));
        }
        return new RouteProperties(id, source, fromTopic, to);
    }

    private static NStreamReplayProperties cfg(List<SourceProperties> sources,
                                               List<SinkProperties> sinks,
                                               List<RouteProperties> routes) {
        return new NStreamReplayProperties(sources, sinks, routes);
    }

    @Test
    void aceitaConfiguracaoValidaComFanout() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")),
                List.of(sink("d1"), sink("d2")),
                List.of(route("r1", "s1", "orders.in", "d1", "d2")));
        assertThatCode(() -> ConfigValidator.validate(props)).doesNotThrowAnyException();
    }

    @Test
    void rejeitaSourceIdDuplicado() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1"), src("s1")),
                List.of(sink("d1")),
                List.of(route("r1", "s1", "t", "d1")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("source duplicado").hasMessageContaining("s1");
    }

    @Test
    void rejeitaSinkIdDuplicado() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")),
                List.of(sink("d1"), sink("d1")),
                List.of(route("r1", "s1", "t", "d1")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sink duplicado");
    }

    @Test
    void rejeitaRotaIdDuplicado() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")),
                List.of(sink("d1"), sink("d2")),
                List.of(route("r1", "s1", "a", "d1"), route("r1", "s1", "b", "d2")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("route duplicado");
    }

    @Test
    void rejeitaRotaComSourceInexistente() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")), List.of(sink("d1")),
                List.of(route("r1", "ausente", "t", "d1")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("source inexistente").hasMessageContaining("ausente");
    }

    @Test
    void rejeitaRotaComSinkInexistente() {
        NStreamReplayProperties props = cfg(
                List.of(src("s1")), List.of(sink("d1")),
                List.of(route("r1", "s1", "t", "fantasma")));
        assertThatThrownBy(() -> ConfigValidator.validate(props))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sink inexistente").hasMessageContaining("fantasma");
    }

    @Test
    void resolvedClientIdUsaIdQuandoAusente() {
        assertThat(src("s1").resolvedClientId()).isEqualTo("s1");
        SourceProperties comClient = new SourceProperties(
                "s1", "b:9092", "g", "cli-x", "latest", 500, 1000L, 0L, Map.of());
        assertThat(comClient.resolvedClientId()).isEqualTo("cli-x");
    }

    private static NStreamReplayProperties keyedConfig(String decoder, String... keys) {
        SourceProperties source = new SourceProperties("s", "b:9092", "g", null, "latest", 500, 1000L, 0L, Map.of(), decoder);
        List<RouteProperties> routes = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            routes.add(new RouteProperties("r" + i, "s", "t",
                    List.of(new RouteTarget("d", i == 0 ? null : "t", keys[i]))));
        }
        return cfg(List.of(source), List.of(sink("d")), routes);
    }

    @Test
    void rejectsInvalidDecodersAndTextKeys() {
        for (String decoder : List.of("custom", "unknown", "", "JSON")) {
            assertThatThrownBy(() -> ConfigValidator.validate(keyedConfig(decoder, "id")))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("source 's'");
        }
        assertThatThrownBy(() -> ConfigValidator.validate(keyedConfig("text", "id")))
                .hasMessageContaining("partitionKey exige decoder");
    }

    @Test
    void rejectsInvalidPathsAndConflictingPoliciesAcrossRoutes() {
        for (String path : List.of("", " ", "a..b", "a[0]", "a.*")) {
            assertThatThrownBy(() -> ConfigValidator.validate(keyedConfig("json", path)))
                    .hasMessageContaining("partitionKey inválido");
        }
        for (String other : new String[]{"other", null}) {
            assertThatThrownBy(() -> ConfigValidator.validate(keyedConfig("json", "id", other)))
                    .hasMessageContaining("conflitante");
            assertThatThrownBy(() -> ConfigValidator.validate(keyedConfig("json", other, "id")))
                    .hasMessageContaining("conflitante");
        }
    }

    @Test
    void acceptsIdenticalPoliciesAndStructuredDecoders() {
        for (String decoder : List.of("json", "yaml")) {
            assertThatCode(() -> ConfigValidator.validate(keyedConfig(decoder, "payload.Id", "payload.Id")))
                    .doesNotThrowAnyException();
        }
        assertThatCode(() -> ConfigValidator.validate(keyedConfig("text", null, null))).doesNotThrowAnyException();
    }
}
