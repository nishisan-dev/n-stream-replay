package dev.nishisan.nstreamreplay.source;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumeRateLimiterTest {

    @Test
    void desabilitadoNaoBloqueia() {
        ConsumeRateLimiter limiter = new ConsumeRateLimiter(0);
        assertThat(limiter.enabled()).isFalse();

        long t0 = System.nanoTime();
        for (int i = 0; i < 200_000; i++) {
            limiter.acquire();
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
        assertThat(elapsedMs).isLessThan(200L);   // ilimitado => instantâneo
    }

    @Test
    void pacingRespeitaOTeto() {
        long rate = 200;                          // 200 permits/s => intervalo de 5 ms
        ConsumeRateLimiter limiter = new ConsumeRateLimiter(rate);
        assertThat(limiter.enabled()).isTrue();
        assertThat(limiter.permitsPerSec()).isEqualTo(200L);

        int n = 100;                              // ~ (100-1) * 5 ms ≈ 495 ms
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            limiter.acquire();
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        // Não excede o teto (com folga p/ jitter) e não trava demais.
        assertThat(elapsedMs).isGreaterThanOrEqualTo(350L);
        assertThat(elapsedMs).isLessThan(3_000L);
    }
}
