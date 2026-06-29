package dev.nishisan.nstreamreplay.config;

/**
 * Política de durabilidade da fila durável de um destino.
 *
 * <ul>
 *   <li>{@link #SYNC_ON_COMMIT} (default): abre a {@code NQueue} com {@code fsync=false}
 *       (rápido por registro) e força {@code sync()} de todas as filas tocadas no boundary
 *       do batch, antes do {@code commitSync()} da origem — group-commit. Garante "sem perda
 *       + at-least-once" amortizando um fsync por destino por poll-batch.</li>
 *   <li>{@link #PER_RECORD_FSYNC}: abre a {@code NQueue} com {@code fsync=true} — o mais
 *       seguro e o mais lento (um fsync por registro).</li>
 *   <li>{@link #OS_MANAGED}: sem sync explícito; o flush fica a cargo do page cache do SO.
 *       O mais rápido; aceita janela de perda na cauda em crash de máquina.</li>
 * </ul>
 */
public enum Durability {
    SYNC_ON_COMMIT,
    PER_RECORD_FSYNC,
    OS_MANAGED
}
