package dev.nishisan.nstreamreplay.source;

import java.util.concurrent.locks.LockSupport;

/**
 * Limitador de taxa de consumo de uma origem, em <b>registros por segundo</b>. Faz pacing por
 * espaçamento: cada {@link #acquire()} bloqueia o necessário para manter a cadência ≤ teto (sem
 * burst). {@code permitsPerSec <= 0} desabilita o limitador ({@link #acquire()} vira no-op).
 *
 * <p>Não é thread-safe: cada {@code SourceConsumer} tem o seu, usado apenas pela sua thread de poll.
 * Espelha o {@code ConsumeRateLimiter} do {@code ngrrd-consumer}.
 */
public final class ConsumeRateLimiter {

    private final long permitsPerSec;
    private final long intervalNanos;
    private long nextFreeNanos;

    public ConsumeRateLimiter(long permitsPerSec) {
        this.permitsPerSec = Math.max(0L, permitsPerSec);
        this.intervalNanos = this.permitsPerSec > 0 ? 1_000_000_000L / this.permitsPerSec : 0L;
        this.nextFreeNanos = System.nanoTime();
    }

    public boolean enabled() {
        return permitsPerSec > 0;
    }

    public long permitsPerSec() {
        return permitsPerSec;
    }

    /**
     * Bloqueia (pacing) até liberar um permit; no-op quando desabilitado. Reentra no park em
     * wakeups espúrios para garantir o intervalo mínimo entre permits.
     */
    public void acquire() {
        if (intervalNanos == 0L) {
            return;
        }
        long now = System.nanoTime();
        long scheduled = Math.max(now, nextFreeNanos);
        nextFreeNanos = scheduled + intervalNanos;
        long remaining = scheduled - now;
        while (remaining > 0L) {
            LockSupport.parkNanos(remaining);
            remaining = scheduled - System.nanoTime();
        }
    }
}
