package dev.nishisan.nstreamreplay.config;

/**
 * Comportamento quando a gravação local na fila durável de um destino falha
 * (ex.: disco cheio — {@code offer} lança {@code IOException}).
 *
 * <ul>
 *   <li>{@link #DROP} (default): isolamento vence — loga, contabiliza {@code offerErrors}
 *       e segue; a origem continua e commita (aceita perda apenas naquele destino).</li>
 *   <li>{@link #HALT}: propaga o erro e interrompe a origem (não commita), preservando
 *       at-least-once às custas de parar o pipeline daquela origem.</li>
 * </ul>
 */
public enum OnWriteError {
    DROP,
    HALT
}
