package dev.nishisan.nstreamreplay.config;

/**
 * Como a partição de destino de um registro é escolhida por um alvo de rota.
 *
 * <ul>
 *   <li>{@link #KEY} (default): o producer do destino particiona pela key do registro (a original
 *       ou a extraída por {@code partitionKey}) — comportamento histórico.</li>
 *   <li>{@link #PRESERVE}: o registro da partição P da origem vai para a partição P do destino.
 *       Mantém a ordem e o agrupamento por partição, o que é necessário quando o consumidor do
 *       destino reconstrói estado <b>por partição</b> (ex.: marcadores de controle de CDC
 *       publicados uma vez em cada partição, todos com a mesma key). Exige que o tópico de destino
 *       tenha pelo menos tantas partições quanto o de origem e é incompatível com
 *       {@code partitionKey}.</li>
 * </ul>
 */
public enum Partitioning {
    KEY,
    PRESERVE
}
