package dev.nishisan.nstreamreplay.pipeline;

import dev.nishisan.nstreamreplay.config.PipelineProperties;
import dev.nishisan.nstreamreplay.sink.SinkTarget;
import dev.nishisan.nstreamreplay.testutil.FakeSinkTarget;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelineRegistryTest {

    private static Map<String, SinkTarget> sinks(String... ids) {
        Map<String, SinkTarget> m = new LinkedHashMap<>();
        for (String id : ids) {
            m.put(id, new FakeSinkTarget(id, false));
        }
        return m;
    }

    @Test
    void unionDeduplicadaDeDestinosPorOrigem() {
        // Duas pipelines para a mesma origem, com sobreposição de destinos.
        List<PipelineProperties> pipelines = List.of(
                new PipelineProperties("p1", "s1", List.of("d1", "d2")),
                new PipelineProperties("p2", "s1", List.of("d2", "d3")));

        PipelineRegistry reg = PipelineRegistry.build(pipelines, sinks("d1", "d2", "d3"));

        assertThat(reg.targetsFor("s1")).extracting(SinkTarget::id)
                .containsExactly("d1", "d2", "d3");   // d2 não duplica
        assertThat(reg.activeSourceIds()).containsExactly("s1");
    }

    @Test
    void origemSemPipelineRetornaListaVazia() {
        PipelineRegistry reg = PipelineRegistry.build(
                List.of(new PipelineProperties("p1", "s1", List.of("d1"))), sinks("d1"));
        assertThat(reg.targetsFor("desconhecida")).isEmpty();
    }

    @Test
    void destinoNaoResolvidoLancaErro() {
        assertThatThrownBy(() -> PipelineRegistry.build(
                List.of(new PipelineProperties("p1", "s1", List.of("ausente"))), sinks("d1")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ausente");
    }
}
