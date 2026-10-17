package dev.nishisan.nstreamreplay.runtime;

import dev.nishisan.nstreamreplay.config.Durability;
import dev.nishisan.nstreamreplay.config.NStreamReplayProperties;
import dev.nishisan.nstreamreplay.config.OnWriteError;
import dev.nishisan.nstreamreplay.config.Partitioning;
import dev.nishisan.nstreamreplay.config.QueueProperties;
import dev.nishisan.nstreamreplay.config.RouteProperties;
import dev.nishisan.nstreamreplay.config.RouteTarget;
import dev.nishisan.nstreamreplay.config.SinkProperties;
import dev.nishisan.nstreamreplay.config.SourceProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionPreserveCheckTest {

    /** Contador simulado: partições por bootstrap; registra as consultas. */
    private static final class FakeCounter implements TopicPartitionCounter {
        final Map<String, Map<String, Integer>> byBootstrap = new HashMap<>();
        final List<String> queried = new ArrayList<>();
        Exception failure;

        FakeCounter cluster(String bootstrap, Map<String, Integer> topics) {
            byBootstrap.put(bootstrap, topics);
            return this;
        }

        @Override
        public Map<String, Integer> partitionCounts(Map<String, Object> adminConfig, Set<String> topics)
                throws Exception {
            String bootstrap = (String) adminConfig.get("bootstrap.servers");
            queried.add(bootstrap);
            if (failure != null) {
                throw failure;
            }
            Map<String, Integer> all = byBootstrap.getOrDefault(bootstrap, Map.of());
            Map<String, Integer> out = new HashMap<>();
            topics.stream().filter(all::containsKey).forEach(t -> out.put(t, all.get(t)));
            return out;
        }
    }

    private static NStreamReplayProperties config(RouteTarget... targets) {
        SourceProperties source = new SourceProperties("src", "origem:9092", "g", null, "earliest", 500, 1000L, 0L,
                Map.of("security.protocol", "SASL_SSL"));
        QueueProperties queue = new QueueProperties("var/q", 100, Duration.ZERO,
                Durability.SYNC_ON_COMMIT, OnWriteError.HALT, 256, 1000L);
        SinkProperties sink = new SinkProperties("dst", "destino:9092", "all", 0, "lz4", 1_048_576, Map.of(), queue);
        return new NStreamReplayProperties(List.of(source), List.of(sink),
                List.of(new RouteProperties("cdc-route", "src", "fault.alarms.cdc", List.of(targets))));
    }

    private static RouteTarget preserve(String toTopic) {
        return new RouteTarget("dst", toTopic, null, Partitioning.PRESERVE);
    }

    @Test
    void aceitaDestinoComPeloMenosAsParticoesDaOrigem() {
        FakeCounter counter = new FakeCounter()
                .cluster("origem:9092", Map.of("fault.alarms.cdc", 4))
                .cluster("destino:9092", Map.of("fault.alarms.cdc", 4, "cdc.big", 8));

        assertThatCode(() -> PartitionPreserveCheck.verify(config(preserve(null)), counter)).doesNotThrowAnyException();
        assertThatCode(() -> PartitionPreserveCheck.verify(config(preserve("cdc.big")), counter))
                .doesNotThrowAnyException();
    }

    @Test
    void rejeitaDestinoComMenosParticoes() {
        FakeCounter counter = new FakeCounter()
                .cluster("origem:9092", Map.of("fault.alarms.cdc", 4))
                .cluster("destino:9092", Map.of("cdc.small", 2));

        assertThatThrownBy(() -> PartitionPreserveCheck.verify(config(preserve("cdc.small")), counter))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cdc-route")
                .hasMessageContaining("fault.alarms.cdc").hasMessageContaining("cdc.small")
                .hasMessageContaining("2 partição").hasMessageContaining("4 partição");
    }

    @Test
    void rejeitaDestinoInexistenteSemCriarTopico() {
        FakeCounter counter = new FakeCounter()
                .cluster("origem:9092", Map.of("fault.alarms.cdc", 4))
                .cluster("destino:9092", Map.of());

        assertThatThrownBy(() -> PartitionPreserveCheck.verify(config(preserve("cdc.missing")), counter))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cdc-route").hasMessageContaining("cdc.missing")
                .hasMessageContaining("não existe").hasMessageContaining("4 partição");
    }

    @Test
    void rejeitaOrigemInexistente() {
        FakeCounter counter = new FakeCounter()
                .cluster("origem:9092", Map.of())
                .cluster("destino:9092", Map.of("fault.alarms.cdc", 4));

        assertThatThrownBy(() -> PartitionPreserveCheck.verify(config(preserve(null)), counter))
                .hasMessageContaining("tópico de origem").hasMessageContaining("não existe");
    }

    @Test
    void falhaFechadoQuandoClusterInacessivel() {
        FakeCounter counter = new FakeCounter();
        counter.failure = new java.util.concurrent.TimeoutException("sem resposta");

        assertThatThrownBy(() -> PartitionPreserveCheck.verify(config(preserve(null)), counter))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("não foi possível consultar").hasMessageContaining("sem resposta");
    }

    @Test
    void semAlvoPreserveNaoConsultaNenhumCluster() {
        FakeCounter counter = new FakeCounter();

        PartitionPreserveCheck.verify(config(new RouteTarget("dst", "qualquer")), counter);

        assertThat(counter.queried).isEmpty();
    }

    @Test
    void repassaBootstrapEPassthroughDeSegurancaAoClienteAdministrativo() {
        List<Map<String, Object>> configs = new ArrayList<>();
        TopicPartitionCounter counter = (adminConfig, topics) -> {
            configs.add(adminConfig);
            return Map.of("fault.alarms.cdc", 4);
        };

        PartitionPreserveCheck.verify(config(preserve(null)), counter);

        assertThat(configs).anySatisfy(c -> {
            assertThat(c).containsEntry("bootstrap.servers", "origem:9092");
            assertThat(c).containsEntry("security.protocol", "SASL_SSL");
        });
        assertThat(configs).anySatisfy(c -> assertThat(c).containsEntry("bootstrap.servers", "destino:9092"));
    }
}
