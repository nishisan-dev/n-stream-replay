package dev.nishisan.nstreamreplay.config;

/**
 * O que a fila durável de um destino faz quando a profundidade atinge {@code maxDepth}.
 *
 * <ul>
 *   <li>{@link #DROP_OLDEST} (default): descarta o registro mais antigo ainda não entregue para
 *       admitir o novo — a origem nunca é freada, ao custo de perda intencional e contabilizada
 *       ({@code dropped}).</li>
 *   <li>{@link #BLOCK}: <b>nunca descarta</b>. Ao atingir {@code maxDepth} a fila sinaliza
 *       saturação e a origem passa a ser freada com {@code pause()} nas partições que alimentam
 *       esse destino (o loop de {@code poll} continua, então o consumer group não sofre
 *       rebalance); retoma quando a fila drena até metade de {@code maxDepth}. Por isso
 *       {@code maxDepth} vira uma marca d'água: pode ser excedida em até um lote por origem. Exige
 *       {@code retentionTime: 0s} e deve ser usado com {@code onWriteError: halt}.</li>
 * </ul>
 */
public enum OverflowPolicy {
    DROP_OLDEST,
    BLOCK
}
