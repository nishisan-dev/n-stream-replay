package dev.nishisan.nstreamreplay.stats;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StatsReporterTest {

    @Test
    void formatIncluiTaxasLagEEstadoDosSinks() {
        String out = StatsReporter.format(
                List.of(new StatsReporter.SourceLine("orders-sp", 1000L, 33.3, 42L, 1500L)),
                List.of(
                        new StatsReporter.SinkLine("dr-rio", 5L, 950L, 31.7, 2L, 0L, 1L, true, 3L, 0L),
                        new StatsReporter.SinkLine("analytics", 0L, 1000L, 33.3, 0L, 7L, 0L, false, 0L, 0L)));

        assertThat(out).contains("orders-sp").contains("33.3").contains("42").contains("1500");
        assertThat(out).contains("dr-rio").contains("up");
        assertThat(out).contains("analytics").contains("DOWN").contains("7");   // expired
    }

    @Test
    void lagNegativoApareceComoNa() {
        String out = StatsReporter.format(
                List.of(new StatsReporter.SourceLine("s", 0L, 0.0, -1L, -1L)),
                List.of());
        assertThat(out).contains("n/a");
    }
}
