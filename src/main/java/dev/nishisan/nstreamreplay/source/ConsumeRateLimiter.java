package dev.nishisan.nstreamreplay.source;

import java.util.concurrent.locks.LockSupport;

/**
 * Limitador de taxa de consumo de uma origem, em <b>registros por segundo</b> — token-bucket
 * baseado em reserva de tempo ({@code nextFreeNanos} avança {@code intervalNanos} por permit).
 *
 * <p>Para ser <b>preciso em taxas altas</b> (intervalo &lt; 1 ms), não dorme por registro: acumula a
 * "dívida" e só dorme quando ela passa de {@link #SLEEP_THRESHOLD_NANOS} (escala de ms, onde o
 * {@code parkNanos} é confiável). E para ser <b>robusto a oversleep</b> (em VM sob carga o
 * {@code parkNanos} dorme MAIS que o pedido), {@code nextFreeNanos} <b>pode ficar atrás de
 * {@code now}</b>: a dívida fica negativa e os próximos permits passam sem dormir (<b>catch-up</b>),
 * recuperando a média. O atraso máximo recuperável é {@link #MAX_BURST_NANOS} (teto de burst após
 * ociosidade). {@code permitsPerSec <= 0} desabilita ({@link #acquire()} no-op).
 *
 * <p>Não é thread-safe: cada {@code SourceConsumer} tem o seu, usado só pela sua thread de poll.
 */
public final class ConsumeRateLimiter {

    /** Só dorme quando a dívida acumulada passa disto (sleeps sub-ms são imprecisos). */
    private static final long SLEEP_THRESHOLD_NANOS = 2_000_000L;   // 2 ms
    /** Crédito máximo de catch-up: quanto {@code nextFreeNanos} pode ficar atrás de {@code now}. */
    private static final long MAX_BURST_NANOS = 100_000_000L;       // 100 ms de permits

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
        // Limita o crédito de catch-up: nextFreeNanos não fica mais que MAX_BURST_NANOS no passado.
        long floor = now - MAX_BURST_NANOS;
        if (nextFreeNanos < floor) {
            nextFreeNanos = floor;
        }
        long scheduled = nextFreeNanos;                   // SEM clamp a now: pode estar no passado
        nextFreeNanos += intervalNanos;                   // reserva este permit
        long debt = scheduled - now;                      // < 0 => atrasado => catch-up sem dormir
        if (debt < SLEEP_THRESHOLD_NANOS) {
            return;                                       // dívida pequena/negativa: segue sem dormir
        }
        long remaining = debt;                            // dorme até o deadline (≥ 2 ms, preciso)
        while (remaining > 0L) {
            LockSupport.parkNanos(remaining);
            remaining = scheduled - System.nanoTime();
        }
    }
}
