package dev.nishisan.nstreamreplay.source;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumeRateLimiterTest {

    @Test
    void limita2000PorSegundoComRelogioDeterministico() {
        AtomicLong now = new AtomicLong();
        ConsumeRateLimiter limiter = new ConsumeRateLimiter(2_000L, now::get, now::addAndGet);

        int permits = 10_000;
        for (int i = 0; i < permits; i++) {
            limiter.acquire();
        }

        // O threshold permite microbursts de até 2 ms, mas a média fica no teto configurado.
        assertThat(now.get()).isBetween(4_990_000_000L, 5_000_000_000L);
        double measured = permits * 1_000_000_000.0 / now.get();
        assertThat(measured).isBetween(1_995.0, 2_005.0);
    }

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

    @Test
    void precisoEmTaxaAlta() {
        long rate = 5000;                         // 5000/s => intervalo de 200 µs (sub-ms)
        ConsumeRateLimiter limiter = new ConsumeRateLimiter(rate);

        int n = 5000;                             // ~ 1 s no total
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            limiter.acquire();
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        // Média precisa mesmo em sub-ms: ~1000 ms (sem o under/over-shoot do sleep por registro).
        assertThat(elapsedMs).isGreaterThanOrEqualTo(850L);
        assertThat(elapsedMs).isLessThan(1_600L);
    }

    @Test
    void recuperaAposStallSemPerderAMedia() {
        long rate = 1000;                          // 1000/s => intervalo de 1 ms
        ConsumeRateLimiter limiter = new ConsumeRateLimiter(rate);

        int n = 100;                               // sem stall, ~100 ms no total
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            limiter.acquire();
            if (i == 49) {
                sleepMs(50);                       // stall de 50 ms no meio (simula oversleep/jitter)
            }
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;

        // Com catch-up, o stall é absorvido pelo burst seguinte (dívida negativa) -> total ~100 ms.
        // Sem catch-up (clamp a now), seriam ~150 ms (50 + 50 stall + 50 repacing).
        assertThat(elapsedMs).isGreaterThanOrEqualTo(90L);
        assertThat(elapsedMs).isLessThan(135L);
    }

    private static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
