package dev.nishisan.nstreamreplay.route;

import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.sink.SinkTarget;
import dev.nishisan.nstreamreplay.testutil.FakeSinkTarget;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouteTableTest {

    private static Map<String, SinkTarget> sinks(String... ids) {
        Map<String, SinkTarget> m = new LinkedHashMap<>();
        for (String id : ids) {
            m.put(id, new FakeSinkTarget(id, false));
        }
        return m;
    }

    @Test
    void uniaoDeTopicosPorOrigemEAlvosPorTopico() {
        List<RouteProperties> routes = List.of(
                new RouteProperties("r1", "s1", "orders.in",
                        List.of(new RouteTarget("dr", null), new RouteTarget("an", "orders.raw"))),
                new RouteProperties("r2", "s1", "returns.in",
                        List.of(new RouteTarget("an", "returns.raw"))));

        RouteTable t = RouteTable.build(routes, sinks("dr", "an"));

        assertThat(t.topicsFor("s1")).containsExactlyInAnyOrder("orders.in", "returns.in");
        assertThat(t.targetsFor("s1", "orders.in"))
                .extracting(x -> x.sink().id(), RouteTable.Target::toTopic)
                .containsExactly(Tuple.tuple("dr", null), Tuple.tuple("an", "orders.raw"));
        assertThat(t.targetsFor("s1", "returns.in"))
                .extracting(x -> x.sink().id()).containsExactly("an");
        assertThat(t.targetsFor("s1", "desconhecido")).isEmpty();
        assertThat(t.activeSourceIds()).containsExactly("s1");
        assertThat(t.referencedSinkIds()).containsExactlyInAnyOrder("dr", "an");
    }

    @Test
    void resolveTopicPreservaQuandoToTopicAusente() {
        RouteTable.Target preserva = new RouteTable.Target(new FakeSinkTarget("x", false), null);
        RouteTable.Target renomeia = new RouteTable.Target(new FakeSinkTarget("y", false), "dest");
        assertThat(preserva.resolveTopic("orig")).isEqualTo("orig");
        assertThat(renomeia.resolveTopic("orig")).isEqualTo("dest");
    }

    @Test
    void sinkInexistenteLancaErro() {
        List<RouteProperties> routes = List.of(
                new RouteProperties("r1", "s1", "t", List.of(new RouteTarget("fantasma", null))));
        assertThatThrownBy(() -> RouteTable.build(routes, sinks("dr")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fantasma");
    }
}
