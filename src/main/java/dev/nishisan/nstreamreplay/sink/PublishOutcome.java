package dev.nishisan.nstreamreplay.sink;

/**
 * Resultado de uma publicação do {@link Sink}. Os itens são processados <b>em ordem</b>, a
 * partir da cabeça do lote; o resultado descreve o <b>prefixo contíguo</b> que o forwarder
 * deve <b>confirmar</b> ({@link #acked()}), parando no primeiro erro <em>transitório</em>
 * (broker indisponível/timeout — o restante fica na fila para retry).
 *
 * <ul>
 *   <li>{@code published}: itens efetivamente entregues ao broker;</li>
 *   <li>{@code poisoned}: itens <b>descartados por serem não-publicáveis em definitivo</b>
 *       (oversize ou {@code ApiException} não-retryable) — pulados, jamais retentados, para
 *       que um único item venenoso não trave o FIFO (head-of-line block).</li>
 * </ul>
 *
 * <p>{@code haltReason}: preenchido apenas sob {@code onPoison=halt} quando o próximo registro (logo
 * após o prefixo confirmável) foi rejeitado em definitivo. Esse registro <b>não</b> entra em
 * {@code poisoned} nem é confirmado; o forwarder confirma só o prefixo entregue e para de entregar.
 *
 * <p>Invariante: {@code acked() == published + poisoned} (ambos cobrem o prefixo contíguo).
 */
public record PublishOutcome(int published, int poisoned, String haltReason) {

    public PublishOutcome {
        if (published < 0 || poisoned < 0) {
            throw new IllegalArgumentException("published/poisoned devem ser >= 0");
        }
    }

    public PublishOutcome(int published, int poisoned) {
        this(published, poisoned, null);
    }

    /** O sink pede ao forwarder que pare (onPoison=halt)? */
    public boolean halted() {
        return haltReason != null;
    }

    /** Nada entregue nem descartado (ex.: broker indisponível no primeiro item). */
    public static PublishOutcome none() {
        return new PublishOutcome(0, 0);
    }

    /** Quantidade de itens de cabeça contíguos a confirmar (remover) na fila. */
    public int acked() {
        return published + poisoned;
    }
}
