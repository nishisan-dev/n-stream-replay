package dev.nishisan.nstreamreplay.stats;

import dev.nishisan.utils.stats.StatsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class StatsConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
            .withUserConfiguration(StatsConfig.class);

    @Test
    void criaStatsUtilsEPonteQuandoHabilitadoEHaMeterRegistry() {
        runner.withPropertyValues("nishi.utils.stats.enabled=true").run(ctx -> {
            assertThat(ctx).hasSingleBean(StatsUtils.class);
            StatsUtils stats = ctx.getBean(StatsUtils.class);
            MeterRegistry registry = ctx.getBean(MeterRegistry.class);

            // A ponte registra um Gauge na criação do current-value, lido ao vivo do DTO.
            stats.notifyCurrentValue("nstreamreplay.test.depth", 7L);
            assertThat(registry.find("nstreamreplay.test.depth").gauge()).isNotNull();
            assertThat(registry.find("nstreamreplay.test.depth").gauge().value()).isEqualTo(7.0);
        });
    }

    @Test
    void naoCriaStatsUtilsQuandoPropriedadeDesabilitada() {
        // Sem nishi.utils.stats.enabled=true o bean não é criado (degrada para no-op no ReplayMetrics).
        runner.run(ctx -> assertThat(ctx).doesNotHaveBean(StatsUtils.class));
    }
}
