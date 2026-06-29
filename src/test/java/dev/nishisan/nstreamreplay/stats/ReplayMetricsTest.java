package dev.nishisan.nstreamreplay.stats;

import dev.nishisan.utils.spring.stats.StatsUtilsMetricBind;
import dev.nishisan.utils.stats.StatsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReplayMetricsTest {

    @Test
    void onConsumedPublicaGaugeCumulativoNoRegistry() {
        MeterRegistry registry = new SimpleMeterRegistry();
        StatsUtils stats = new StatsUtils();
        stats.registerListener(new StatsUtilsMetricBind(registry));
        ReplayMetrics metrics = new ReplayMetrics(stats);

        metrics.onConsumed("orders-src");
        metrics.onConsumed("orders-src");

        // id sanitizado (dash -> underscore) e total cumulativo.
        assertThat(registry.find("nstreamreplay.source.orders_src.consumed_total").gauge()).isNotNull();
        assertThat(registry.find("nstreamreplay.source.orders_src.consumed_total").gauge().value())
                .isEqualTo(2.0);
    }

    @Test
    void semStatsUtilsViraNoOp() {
        ReplayMetrics metrics = new ReplayMetrics((StatsUtils) null);
        // Não deve lançar mesmo sem backend de stats.
        metrics.onConsumed("s");
        metrics.onCommitError("s");
    }

    @Test
    void sanitizeMantemApenasCaracteresValidos() {
        assertThat(ReplayMetrics.sanitize("dc2-mirror.east")).isEqualTo("dc2_mirror_east");
    }
}
