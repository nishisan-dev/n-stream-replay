package dev.nishisan.nstreamreplay.source;

import java.util.concurrent.locks.LockSupport;

/**
 * Limitador de taxa de consumo de uma origem, em <b>registros por segundo</b>, via reserva de
 * tempo ({@code nextFreeNanos} avança {@code intervalNanos} por permit). Para ser <b>preciso em
 * taxas altas</b> (intervalo &lt; 1 ms), não dorme por registro — acumula a "dívida" e só dorme
 * quando ela passa de {@link #SLEEP_THRESHOLD_NANOS} (escala de ms, onde o {@code parkNanos} é
 * confiável). Isso permite um <b>burst pequeno</b> (≈ threshold/intervalo registros) entre sleeps,
 * mantendo a <b>média</b> no teto. {@code permitsPerSec <= 0} desabilita ({@link #acquire()} no-op).
 *
 * <p>Não é thread-safe: cada {@code SourceConsumer} tem o seu, usado só pela sua thread de poll.
 */
public final class ConsumeRateLimiter {

    /** Só dorme quando a dívida acumulada passa disto (sleeps sub-ms são imprecisos). */
    private static final long SLEEP_THRESHOLD_NANOS = 2_000_000L;   // 2 ms

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
     * Reserva um permit e bloqueia só quando a dívida acumulada ≥ {@link #SLEEP_THRESHOLD_NANOS};
     * no-op quando desabilitado. Reentra no park em wakeups espúrios.
     */
    public void acquire() {
        if (intervalNanos == 0L) {
            return;
        }
        long now = System.nanoTime();
        long scheduled = Math.max(now, nextFreeNanos);
        nextFreeNanos = scheduled + intervalNanos;       // reserva este permit
        long debt = scheduled - now;
        if (debt < SLEEP_THRESHOLD_NANOS) {
            return;                                       // burst: dívida pequena, segue sem dormir
        }
        long remaining = debt;                            // dorme a dívida inteira (≥ 2 ms, preciso)
        while (remaining > 0L) {
            LockSupport.parkNanos(remaining);
            remaining = scheduled - System.nanoTime();
        }
    }
}
