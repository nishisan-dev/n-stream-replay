package dev.nishisan.nstreamreplay.config;

/**
 * O que o forwarder de um destino faz com um registro que o Kafka rejeita em definitivo
 * (erro não-retryable, como {@code RecordTooLargeException}, {@code TopicAuthorizationException} ou
 * {@code OutOfOrderSequenceException}) ou que excede {@code maxRequestSize}.
 *
 * <ul>
 *   <li>{@link #DROP} (default): o registro é descartado e contabilizado em {@code poisoned}, para
 *       um único registro inválido não travar o FIFO. Como o offset da origem já foi commitado,
 *       o registro é perdido.</li>
 *   <li>{@link #HALT}: o forwarder <b>daquele destino</b> para de entregar e <b>não confirma</b> o
 *       registro, que permanece na cabeça da fila. Loga ERROR com tópico/partição/offset de origem
 *       e o motivo, e expõe o estado ({@code halted}). A fila segue crescendo até saturar (com
 *       {@code overflowPolicy=block} a origem pausa); os demais destinos e rotas seguem normais.
 *       Retomar exige corrigir o destino e reiniciar o processo, que reenvia o registro (se ainda
 *       for inválido, para de novo).</li>
 * </ul>
 */
public enum OnPoison {
    DROP,
    HALT
}
