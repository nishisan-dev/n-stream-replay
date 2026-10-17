package dev.nishisan.nstreamreplay.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Configuração da fila durável (store-and-forward) de um destino. A fila física fica em
 * {@code basePath/<sinkId>/} e é limitada por <b>contagem</b> ({@link #maxDepth}) e/ou
 * <b>tempo</b> ({@link #retentionTime}). Com {@link OverflowPolicy#DROP_OLDEST} (default), ao
 * estourar qualquer um dos limites os registros mais antigos ainda não entregues são descartados
 * (perda intencional). Com {@link OverflowPolicy#BLOCK} nada é descartado: ao atingir
 * {@link #maxDepth} a origem é freada (backpressure) e {@link #retentionTime} não é admitido.
 *
 * @param basePath                diretório base das filas (a do destino vai em {@code basePath/<sinkId>/})
 * @param maxDepth                teto por contagem de registros pendentes (drop-oldest ao atingir, ou
 *                                marca d'água de backpressure sob {@code overflowPolicy=block})
 * @param retentionTime           teto por idade dos registros não entregues ({@code 0s} = desligado)
 * @param durability              política de fsync/sync ({@link Durability})
 * @param onWriteError            comportamento em falha de escrita local ({@link OnWriteError})
 * @param batchSize               quantidade de registros drenada por iteração do forwarder
 * @param forwarderRetryBackoffMs backoff (ms) quando o broker do destino está fora
 * @param overflowPolicy          o que fazer ao atingir {@code maxDepth} ({@link OverflowPolicy});
 *                                default {@code drop-oldest}
 */
public record QueueProperties(
        @NotBlank @DefaultValue("var/n-stream-replay/queues") String basePath,
        @Positive @DefaultValue("100000") int maxDepth,
        @NotNull @DefaultValue("0s") Duration retentionTime,
        @NotNull @DefaultValue("sync-on-commit") Durability durability,
        @NotNull @DefaultValue("drop") OnWriteError onWriteError,
        @Positive @DefaultValue("256") int batchSize,
        @PositiveOrZero @DefaultValue("1000") long forwarderRetryBackoffMs,
        @NotNull @DefaultValue("drop-oldest") OverflowPolicy overflowPolicy) {

    @ConstructorBinding
    public QueueProperties {
        overflowPolicy = overflowPolicy == null ? OverflowPolicy.DROP_OLDEST : overflowPolicy;
    }

    /** Construtor sem {@code overflowPolicy}: mantém o comportamento histórico ({@code drop-oldest}). */
    public QueueProperties(String basePath, int maxDepth, Duration retentionTime, Durability durability,
                           OnWriteError onWriteError, int batchSize, long forwarderRetryBackoffMs) {
        this(basePath, maxDepth, retentionTime, durability, onWriteError, batchSize,
                forwarderRetryBackoffMs, OverflowPolicy.DROP_OLDEST);
    }

    /** A fila nunca descarta por contagem e freia a origem ao atingir {@link #maxDepth}? */
    public boolean blocksOnOverflow() {
        return overflowPolicy == OverflowPolicy.BLOCK;
    }

    /** Há limite por tempo configurado? */
    public boolean hasRetentionTime() {
        return retentionTime != null && !retentionTime.isZero() && !retentionTime.isNegative();
    }
}
