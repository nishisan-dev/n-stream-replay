package dev.nishisan.nstreamreplay.runtime;

import java.util.Map;
import java.util.Set;

/**
 * Consulta a quantidade de partições de tópicos de um cluster Kafka. Abstrai o {@code Admin}
 * para que a checagem de subida do modo {@code preserve} seja testável sem broker.
 */
@FunctionalInterface
public interface TopicPartitionCounter {

    /**
     * @param adminConfig configuração do cliente administrativo (bootstrap + passthrough de
     *                    SASL/TLS do source ou sink)
     * @param topics      tópicos a consultar
     * @return partições por tópico; tópicos inexistentes ficam <b>ausentes</b> do mapa
     * @throws Exception se o cluster não puder ser consultado (inacessível, sem permissão, timeout)
     */
    Map<String, Integer> partitionCounts(Map<String, Object> adminConfig, Set<String> topics) throws Exception;
}
